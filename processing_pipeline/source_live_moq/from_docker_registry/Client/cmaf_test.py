#!/usr/bin/env python3
"""
GStreamer CMAF streaming pipeline — async Python version.

Configuration via environment variables:
  VIDEO_HOST            TCP source host for video  (default: receiver)
  VIDEO_PORT            TCP source port for video  (default: 5565)
  AUDIO_HOST            TCP source host for audio  (default: receiver)
  AUDIO_PORT            TCP source port for audio  (default: 5566)
  SEGMENT_DURATION      Segment duration (ms)      (default: 2000)
  FRAGMENT_DURATION     Fragment duration (ms)      (default: 500)
  OUTPUT_DIR            Output directory            (default: /opt/sandbox)
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
log = logging.getLogger("cmaf_pipeline")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def cfg(key: str, default: str) -> str:
    return os.environ.get(key, default)


VIDEO_HOST        = cfg("VIDEO_HOST",        "receiver")
VIDEO_PORT        = int(cfg("VIDEO_PORT",    "5565"))
AUDIO_HOST        = cfg("AUDIO_HOST",        "receiver")
AUDIO_PORT        = int(cfg("AUDIO_PORT",    "5566"))
SEGMENT_DURATION  = int(cfg("SEGMENT_DURATION",  "2000"))   # ms
FRAGMENT_DURATION = int(cfg("FRAGMENT_DURATION", "500"))    # ms
OUTPUT_DIR        = cfg("OUTPUT_DIR", "/opt/sandbox")


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline() -> Gst.Pipeline:
    """
    Topology CMAF avec cmafmux + splitmuxsink :

      tcpclientsrc (video) → queue → qtdemux → queue → h264parse ─┐
                                                                    ├→ cmafmux → splitmuxsink (video segments)
      tcpclientsrc (audio) → queue → qtdemux → queue → aacparse  ─┘
                                                                    └→ cmafmux → splitmuxsink (audio segments)

    On crée deux branches séparées :
      - une branche vidéo : h264parse → cmafmux_video → splitmuxsink_video
      - une branche audio : aacparse  → cmafmux_audio → splitmuxsink_audio
    """
    Gst.init([])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "video"), exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_DIR, "audio"), exist_ok=True)

    pipeline = Gst.Pipeline.new("cmaf-pipeline")

    def make(factory: str, name: str) -> Gst.Element:
        el = Gst.ElementFactory.make(factory, name)
        if el is None:
            raise RuntimeError(
                f"Could not create GStreamer element '{factory}' ({name}). "
                "Is the plugin installed?"
            )
        return el

    # ── Video branch ──────────────────────────────────────────────────────
    vid_src   = make("tcpclientsrc", "vid_src")
    vid_queue = make("queue",        "vid_queue")
    vid_demux = make("qtdemux",      "vid_demux")
    vid_q2    = make("queue",        "vid_q2")
    h264parse = make("h264parse",    "h264parse")

    vid_src.set_property("host", VIDEO_HOST)
    vid_src.set_property("port", VIDEO_PORT)

    # cmafmux video
    cmafmux_video = make("cmafmux", "cmafmux_video")
    cmafmux_video.set_property("chunk-duration", FRAGMENT_DURATION)   # ms → fragment intra-segment
    cmafmux_video.set_property("fragment-duration", SEGMENT_DURATION) # ms → segment duration

    # splitmuxsink video
    splitmux_video = make("splitmuxsink", "splitmux_video")
    splitmux_video.set_property("location",       os.path.join(OUTPUT_DIR, "video", "seg%05d.m4s"))
    splitmux_video.set_property("max-size-time",  SEGMENT_DURATION * Gst.MSECOND)
    splitmux_video.set_property("muxer",          cmafmux_video)
    splitmux_video.set_property("send-keyframe-requests", True)

    # ── Audio branch ──────────────────────────────────────────────────────
    aud_src   = make("tcpclientsrc", "aud_src")
    aud_queue = make("queue",        "aud_queue")
    aud_demux = make("qtdemux",      "aud_demux")
    aud_q2    = make("queue",        "aud_q2")
    aacparse  = make("aacparse",     "aacparse")

    aud_src.set_property("host", AUDIO_HOST)
    aud_src.set_property("port", AUDIO_PORT)

    # cmafmux audio
    cmafmux_audio = make("cmafmux", "cmafmux_audio")
    cmafmux_audio.set_property("chunk-duration",    FRAGMENT_DURATION)
    cmafmux_audio.set_property("fragment-duration", SEGMENT_DURATION)

    # splitmuxsink audio
    splitmux_audio = make("splitmuxsink", "splitmux_audio")
    splitmux_audio.set_property("location",       os.path.join(OUTPUT_DIR, "audio", "seg%05d.m4s"))
    splitmux_audio.set_property("max-size-time",  SEGMENT_DURATION * Gst.MSECOND)
    splitmux_audio.set_property("muxer",          cmafmux_audio)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in (
        vid_src, vid_queue, vid_demux, vid_q2, h264parse, splitmux_video,
        aud_src, aud_queue, aud_demux, aud_q2, aacparse,  splitmux_audio,
    ):
        pipeline.add(el)

    # ── Static links ──────────────────────────────────────────────────────
    for src, dst, label in [
        (vid_src,   vid_queue, "vid_src → vid_queue"),
        (vid_queue, vid_demux, "vid_queue → vid_demux"),
        (aud_src,   aud_queue, "aud_src → aud_queue"),
        (aud_queue, aud_demux, "aud_queue → aud_demux"),
        (vid_q2,    h264parse, "vid_q2 → h264parse"),
        (aud_q2,    aacparse,  "aud_q2 → aacparse"),
    ]:
        if not src.link(dst):
            raise RuntimeError(f"Failed to link {label}")

    # h264parse → splitmuxsink_video (via request pad "video_%u")
    h264_src = h264parse.get_static_pad("src")
    vid_sink = splitmux_video.request_pad_simple("video")
    if vid_sink is None:
        raise RuntimeError("splitmux_video did not provide a 'video' pad")
    if h264_src.link(vid_sink) != Gst.PadLinkReturn.OK:
        raise RuntimeError("Failed to link h264parse → splitmux_video")
    log.info("Linked h264parse → splitmux_video:video")

    # aacparse → splitmuxsink_audio (via request pad "audio_%u")
    aac_src  = aacparse.get_static_pad("src")
    aud_sink = splitmux_audio.request_pad_simple("audio_%u")
    if aud_sink is None:
        raise RuntimeError("splitmux_audio did not provide an 'audio_%u' pad")
    if aac_src.link(aud_sink) != Gst.PadLinkReturn.OK:
        raise RuntimeError("Failed to link aacparse → splitmux_audio")
    log.info("Linked aacparse → splitmux_audio:audio_%u")

    # ── Dynamic pad handlers (qtdemux) ───────────────────────────────────
    def on_demux_pad_added(demux, src_pad, sink_queue):
        caps = src_pad.get_current_caps() or src_pad.query_caps(None)
        name = caps.get_structure(0).get_name()
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

    return pipeline


# ---------------------------------------------------------------------------
# Async runner
# ---------------------------------------------------------------------------

class CmafStreamer:
    def __init__(self):
        self.pipeline: Gst.Pipeline | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()

    async def start(self):
        self._loop = asyncio.get_running_loop()
        log.info("Building CMAF pipeline…")
        log.info("  Video: tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio: tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output: %s  (seg duration: %dms, fragment: %dms)",
                 OUTPUT_DIR, SEGMENT_DURATION, FRAGMENT_DURATION)

        self.pipeline = build_pipeline()

        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)

        Gst.debug_bin_to_dot_file(self.pipeline, Gst.DebugGraphDetails.ALL, "before_pipeline_cmaf")

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Failed to set pipeline to PLAYING")

        Gst.debug_bin_to_dot_file(self.pipeline, Gst.DebugGraphDetails.ALL, "after_pipeline_cmaf")

        log.info("Pipeline PLAYING — segments in %s/{video,audio}/seg*.m4s", OUTPUT_DIR)

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
    streamer = CmafStreamer()

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