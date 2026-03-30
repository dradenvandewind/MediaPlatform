#!/usr/bin/env python3
"""
GStreamer DASH streaming pipeline — async Python version.

Configuration via environment variables:
  VIDEO_HOST            TCP source host for video  (default: receiver)
  VIDEO_PORT            TCP source port for video  (default: 5565)
  AUDIO_HOST            TCP source host for audio  (default: receiver)
  AUDIO_PORT            TCP source port for audio  (default: 5566)
  MPD_FILENAME          Output MPD filename        (default: ./stream.mpd)
  MPD_ROOT_PATH         Root path for segments     (default: /opt/sandbox)
  TARGET_DURATION       Segment target duration(s) (default: 2)
  MIN_BUFFER_TIME       Min buffer time (ms)       (default: 1000)
  FRAGMENT_DURATION     Fragment duration (ms)     (default: 500)
  SEGMENT_LIST          Use segment list (0/1)     (default: 1)
"""

import asyncio
import logging
import os
import signal
import sys

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dash_pipeline")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def cfg(key: str, default: str) -> str:
    return os.environ.get(key, default)


VIDEO_HOST        = cfg("VIDEO_HOST",        "receiver")
VIDEO_PORT        = int(cfg("VIDEO_PORT",    "5565"))
AUDIO_HOST        = cfg("AUDIO_HOST",        "receiver")
AUDIO_PORT        = int(cfg("AUDIO_PORT",    "5566"))
MPD_FILENAME      = cfg("MPD_FILENAME",      "./stream.mpd")
MPD_ROOT_PATH     = cfg("MPD_ROOT_PATH",     "/opt/sandbox")
TARGET_DURATION   = int(cfg("TARGET_DURATION",   "2"))
MIN_BUFFER_TIME   = int(cfg("MIN_BUFFER_TIME",   "1000"))
FRAGMENT_DURATION = int(cfg("FRAGMENT_DURATION", "500"))
SEGMENT_LIST      = cfg("SEGMENT_LIST", "1") not in ("0", "false", "False", "no")

# dashsink muxer=2 → mp4/isobmff
DASH_MUXER = 2


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline() -> Gst.Pipeline:
    """
    Correct topology for dashsink:

      tcpclientsrc (video) → queue → qtdemux → queue → h264parse ─┐
                                                                    ├→ dashsink
      tcpclientsrc (audio) → queue → qtdemux → queue → aacparse  ─┘

    dashsink handles splitmuxsink internally (muxer property).
    Do NOT insert an external mp4mux before dashsink — that is what
    triggers the 'muxer != NULL' assertion failure.
    """
    Gst.init([])

    pipeline = Gst.Pipeline.new("dash-pipeline")

    def make(factory: str, name: str) -> Gst.Element:
        el = Gst.ElementFactory.make(factory, name)
        if el is None:
            raise RuntimeError(f"Could not create GStreamer element '{factory}' ({name}). "
                               "Is the plugin installed?")
        return el

    # ── Video branch ──────────────────────────────────────────────────────
    vid_src   = make("tcpclientsrc", "vid_src")
    vid_queue = make("queue",        "vid_queue")
    vid_demux = make("qtdemux",      "vid_demux")
    vid_q2    = make("queue",        "vid_q2")
    h264parse = make("h264parse",    "h264parse")

    vid_src.set_property("host", VIDEO_HOST)
    vid_src.set_property("port", VIDEO_PORT)

    # ── Audio branch ──────────────────────────────────────────────────────
    aud_src   = make("tcpclientsrc", "aud_src")
    aud_queue = make("queue",        "aud_queue")
    aud_demux = make("qtdemux",      "aud_demux")
    aud_q2    = make("queue",        "aud_q2")
    aacparse  = make("aacparse",     "aacparse")

    aud_src.set_property("host", AUDIO_HOST)
    aud_src.set_property("port", AUDIO_PORT)

    # ── DASH sink ─────────────────────────────────────────────────────────
    dash = make("dashsink", "dash")
    dash.set_property("min-buffer-time",    MIN_BUFFER_TIME)
    dash.set_property("target-duration",    TARGET_DURATION)
    dash.set_property("dynamic",            True)
    dash.set_property("muxer",              DASH_MUXER)
    dash.set_property("use-segment-list",   SEGMENT_LIST)
    dash.set_property("mpd-filename",       MPD_FILENAME)
    dash.set_property("mpd-root-path",      MPD_ROOT_PATH)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in (vid_src, vid_queue, vid_demux, vid_q2, h264parse,
               aud_src, aud_queue, aud_demux, aud_q2, aacparse, dash):
        pipeline.add(el)

    # ── Static links (before demux pads) ─────────────────────────────────
    ret = vid_src.link(vid_queue)
    if not ret:
        raise RuntimeError("Failed to link vid_src → vid_queue")
    ret = vid_queue.link(vid_demux)
    if not ret:
        raise RuntimeError("Failed to link vid_queue → vid_demux")
    # vid_demux → vid_q2 is dynamic (pad-added)

    ret = aud_src.link(aud_queue)
    if not ret:
        raise RuntimeError("Failed to link aud_src → aud_queue")
    ret = aud_queue.link(aud_demux)
    if not ret:
        raise RuntimeError("Failed to link aud_queue → aud_demux")
    # aud_demux → aud_q2 is dynamic (pad-added)

    ret = vid_q2.link(h264parse)
    if not ret:
        raise RuntimeError("Failed to link vid_q2 → h264parse")
    # h264parse → dashsink: linked dynamically once dashsink requests a pad

    ret = aud_q2.link(aacparse)
    if not ret:
        raise RuntimeError("Failed to link aud_q2 → aacparse")  
    # aacparse → dashsink: linked dynamically once dashsink requests a pad

    # ── Dynamic pad handlers ──────────────────────────────────────────────
    def on_demux_pad_added(demux, src_pad, sink_queue):
        caps = src_pad.get_current_caps() or src_pad.query_caps(None)
        structure = caps.get_structure(0)
        name = structure.get_name()
        log.info("New pad '%s' on %s (caps: %s)", src_pad.get_name(), demux.get_name(), name)

        sink_pad = sink_queue.get_static_pad("sink")
        if sink_pad.is_linked():
            log.debug("Sink pad already linked, ignoring.")
            return

        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            log.error("Failed to link %s → %s: %s",
                      src_pad.get_name(), sink_pad.get_name(), ret)
 
    vid_demux.connect("pad-added", on_demux_pad_added, vid_q2)
    aud_demux.connect("pad-added", on_demux_pad_added, aud_q2)

    # dashsink exposes request pads; link parsers once pads are available
    def link_to_dashsink(parser, pad_template_name: str):
        """Request a pad from dashsink and link the parser to it."""
        request_pad = dash.request_pad_simple(pad_template_name)
        if request_pad is None:
            log.error("dashsink did not provide a pad for template '%s'", pad_template_name)
            return
        src_pad = parser.get_static_pad("src")
        if src_pad is None:
            log.error("Parser %s has no src pad to link", parser.get_name())
            return
        ret = src_pad.link(request_pad)
        if ret != Gst.PadLinkReturn.OK:
            log.error("Failed to link %s to dashsink pad '%s': %s",
                      parser.get_name(), pad_template_name, ret)
        else:
            log.info("Linked %s → dashsink:%s", parser.get_name(), pad_template_name)

    link_to_dashsink(h264parse, "video_%u")
    link_to_dashsink(aacparse,  "audio_%u")

    return pipeline


# ---------------------------------------------------------------------------
# Async runner
# ---------------------------------------------------------------------------

class DashStreamer:
    def __init__(self):
        self.pipeline: Gst.Pipeline | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()

    async def start(self):
        self._loop = asyncio.get_running_loop()
        log.info("Building pipeline…")
        log.info("  Video: tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio: tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  MPD:   %s  (root: %s)", MPD_FILENAME, MPD_ROOT_PATH)

        self.pipeline = build_pipeline()

        # GLib main loop in a thread pool so it doesn't block asyncio
        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)
        
        Gst.debug_bin_to_dot_file(self.pipeline, Gst.DebugGraphDetails.ALL, "before_pipeline_dash")
         
        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Failed to set pipeline to PLAYING")
        
        Gst.debug_bin_to_dot_file(self.pipeline, Gst.DebugGraphDetails.ALL, "after_pipeline_dash")


        log.info("Pipeline PLAYING — waiting for EOS or error…")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, glib_loop.run)

    def _on_bus_message(self, bus, message, glib_loop: GLib.MainLoop):
        t = message.type
        if t == Gst.MessageType.EOS:
            log.info("End of stream received.")
            glib_loop.quit()
            self._loop.call_soon_threadsafe(self._stop_event.set)
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            log.error("GStreamer error: %s", err.message)
            if debug:
                log.debug("Debug info: %s", debug)
            glib_loop.quit()
            self._loop.call_soon_threadsafe(self._stop_event.set)
        elif t == Gst.MessageType.WARNING:
            warn, debug = message.parse_warning()
            log.warning("GStreamer warning: %s", warn.message)
            if debug:
                log.debug("Debug info: %s", debug)
        elif t == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old, new, _ = message.parse_state_changed()
                log.debug("Pipeline state: %s → %s",
                          Gst.Element.state_get_name(old),
                          Gst.Element.state_get_name(new))

    async def stop(self):
        if self.pipeline:
            log.info("Sending EOS…")
            self.pipeline.send_event(Gst.Event.new_eos())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                log.warning("EOS timeout — forcing NULL state.")
            self.pipeline.set_state(Gst.State.NULL)
            log.info("Pipeline stopped.")


async def main():
    streamer = DashStreamer()

    loop = asyncio.get_running_loop()

    def _shutdown():
        log.info("Shutdown signal received.")
        asyncio.ensure_future(streamer.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    try:
        await streamer.start()
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())