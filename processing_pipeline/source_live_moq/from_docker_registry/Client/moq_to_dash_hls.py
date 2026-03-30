#!/usr/bin/env python3
"""
GStreamer CMAF pipeline → DASH + HLS (shared .m4s segments).

Topology :
  tcpclientsrc (video) → qtdemux → h264parse → tee ─┬→ cmafmux → splitmuxsink  (dash/video/)
                                                      └→ cmafmux → splitmuxsink  (hls/video/)

  tcpclientsrc (audio) → qtdemux → aacparse  → tee ─┬→ cmafmux → splitmuxsink  (dash/audio/)
                                                      └→ cmafmux → splitmuxsink  (hls/audio/)

  ManifestWriter (thread)  →  stream.mpd  +  master.m3u8

Env vars :
  VIDEO_HOST / VIDEO_PORT       (default: receiver / 5565)
  AUDIO_HOST / AUDIO_PORT       (default: receiver / 5566)
  OUTPUT_DIR                    (default: /opt/sandbox)
  SEGMENT_DURATION_MS           (default: 2000)
  FRAGMENT_DURATION_MS          (default: 500)
  MANIFEST_REFRESH_S            (default: 2)
  WINDOW_SEGMENTS               (default: 5  — live playlist window)
"""

import asyncio
import glob
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cmaf_multi")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def cfg(key: str, default: str) -> str:
    return os.environ.get(key, default)


VIDEO_HOST           = cfg("VIDEO_HOST",           "receiver")
VIDEO_PORT           = int(cfg("VIDEO_PORT",       "5565"))
AUDIO_HOST           = cfg("AUDIO_HOST",           "receiver")
AUDIO_PORT           = int(cfg("AUDIO_PORT",       "5566"))
OUTPUT_DIR           = Path(cfg("OUTPUT_DIR",      "/opt/sandbox"))
SEG_DURATION_MS      = int(cfg("SEGMENT_DURATION_MS",  "2000"))
FRAG_DURATION_MS     = int(cfg("FRAGMENT_DURATION_MS", "500"))
MANIFEST_REFRESH_S   = float(cfg("MANIFEST_REFRESH_S", "2"))
WINDOW_SEGMENTS      = int(cfg("WINDOW_SEGMENTS",  "5"))

# Derived paths
DASH_VIDEO_DIR = OUTPUT_DIR / "dash" / "video"
DASH_AUDIO_DIR = OUTPUT_DIR / "dash" / "audio"
HLS_VIDEO_DIR  = OUTPUT_DIR / "hls"  / "video"
HLS_AUDIO_DIR  = OUTPUT_DIR / "hls"  / "audio"
MPD_FILE       = OUTPUT_DIR / "stream.mpd"
M3U8_MASTER    = OUTPUT_DIR / "master.m3u8"
HLS_VIDEO_M3U8 = OUTPUT_DIR / "hls" / "video.m3u8"
HLS_AUDIO_M3U8 = OUTPUT_DIR / "hls" / "audio.m3u8"

SEG_DURATION_S = SEG_DURATION_MS / 1000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def makedirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def make_element(factory: str, name: str) -> Gst.Element:
    el = Gst.ElementFactory.make(factory, name)
    if el is None:
        raise RuntimeError(
            f"Cannot create GStreamer element '{factory}' (name='{name}'). "
            "Is the plugin installed?"
        )
    return el


def configure_cmafmux(el: Gst.Element) -> None:
    """Apply common cmafmux settings."""
    el.set_property("chunk-duration",    FRAG_DURATION_MS)
    el.set_property("fragment-duration", SEG_DURATION_MS)


def configure_splitmuxsink(el: Gst.Element, location: str, muxer: Gst.Element) -> None:
    """Apply common splitmuxsink settings."""
    el.set_property("location",       location)
    el.set_property("max-size-time",  SEG_DURATION_MS * Gst.MSECOND)
    el.set_property("muxer",          muxer)
    el.set_property("send-keyframe-requests", True)


# ---------------------------------------------------------------------------
# Manifest writer (runs in a background thread)
# ---------------------------------------------------------------------------

class ManifestWriter:
    """
    Watches the output directories and rewrites MPD + M3U8 playlists
    every MANIFEST_REFRESH_S seconds.

    Both DASH and HLS reference the same .m4s segment files; only
    the relative paths in the manifests differ.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()

    def start(self) -> threading.Thread:
        t = threading.Thread(target=self._loop, name="manifest-writer", daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        log.info("ManifestWriter started (refresh every %.1fs)", MANIFEST_REFRESH_S)
        while not self._stop.wait(timeout=MANIFEST_REFRESH_S):
            try:
                self._write_mpd()
                self._write_hls()
            except Exception as exc:
                log.warning("ManifestWriter error: %s", exc)
        log.info("ManifestWriter stopped.")

    # ── DASH MPD ──────────────────────────────────────────────────────────

    def _write_mpd(self) -> None:
        video_segs = self._latest_segments(DASH_VIDEO_DIR, "seg*.m4s")
        audio_segs = self._latest_segments(DASH_AUDIO_DIR, "seg*.m4s")

        if not video_segs and not audio_segs:
            return  # nothing yet

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        seg_dur_iso = f"PT{SEG_DURATION_S:.3f}S"
        window_dur  = f"PT{SEG_DURATION_S * WINDOW_SEGMENTS:.0f}S"

        video_tmpl, video_entries = self._segment_template(
            video_segs, DASH_VIDEO_DIR, OUTPUT_DIR, "video"
        )
        audio_tmpl, audio_entries = self._segment_template(
            audio_segs, DASH_AUDIO_DIR, OUTPUT_DIR, "audio"
        )

        mpd = f"""<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="urn:mpeg:dash:schema:mpd:2011 DASH-MPD.xsd"
     profiles="urn:mpeg:dash:profile:isoff-live:2011"
     type="dynamic"
     availabilityStartTime="{now}"
     minimumUpdatePeriod="{seg_dur_iso}"
     minBufferTime="PT{FRAG_DURATION_MS/1000:.3f}S"
     timeShiftBufferDepth="{window_dur}"
     suggestedPresentationDelay="{seg_dur_iso}">
  <Period id="1" start="PT0S">
    <AdaptationSet id="1" mimeType="video/mp4" codecs="avc1.42c01f"
                   segmentAlignment="true" startWithSAP="1">
      <Representation id="video" bandwidth="2000000" width="1280" height="720" frameRate="30">
        <SegmentList timescale="1000" duration="{SEG_DURATION_MS}">
          {video_tmpl}
          {video_entries}
        </SegmentList>
      </Representation>
    </AdaptationSet>
    <AdaptationSet id="2" mimeType="audio/mp4" codecs="mp4a.40.2" lang="und"
                   segmentAlignment="true">
      <Representation id="audio" bandwidth="128000">
        <SegmentList timescale="1000" duration="{SEG_DURATION_MS}">
          {audio_tmpl}
          {audio_entries}
        </SegmentList>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>
"""
        MPD_FILE.write_text(mpd)
        log.debug("MPD written (%d video, %d audio segs)", len(video_segs), len(audio_segs))

    def _segment_template(
        self,
        segs: list[Path],
        seg_dir: Path,
        base_dir: Path,
        kind: str,
    ) -> tuple[str, str]:
        """Return (initialization XML, segmentURL XML list)."""
        if not segs:
            return "", ""

        # init segment is the first seg (cmafmux writes init data at the top)
        init_rel  = os.path.relpath(segs[0], base_dir)
        init_xml  = f'<Initialization sourceURL="{init_rel}"/>'

        entries = "\n          ".join(
            f'<SegmentURL media="{os.path.relpath(s, base_dir)}"/>'
            for s in segs
        )
        return init_xml, entries

    # ── HLS M3U8 ─────────────────────────────────────────────────────────

    def _write_hls(self) -> None:
        video_segs = self._latest_segments(HLS_VIDEO_DIR, "seg*.m4s")
        audio_segs = self._latest_segments(HLS_AUDIO_DIR, "seg*.m4s")

        if not video_segs and not audio_segs:
            return

        self._write_media_playlist(
            HLS_VIDEO_M3U8, video_segs, HLS_VIDEO_M3U8.parent
        )
        self._write_media_playlist(
            HLS_AUDIO_M3U8, audio_segs, HLS_AUDIO_M3U8.parent
        )
        self._write_master_playlist()

    def _write_media_playlist(
        self, out: Path, segs: list[Path], base: Path
    ) -> None:
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(SEG_DURATION_S) + 1}",
            "#EXT-X-MEDIA-SEQUENCE:0",
            "#EXT-X-MAP:URI=\"" + (os.path.relpath(segs[0], base) if segs else "") + "\"",
        ]
        for seg in segs:
            lines.append(f"#EXTINF:{SEG_DURATION_S:.3f},")
            lines.append(os.path.relpath(seg, base))
        out.write_text("\n".join(lines) + "\n")
        log.debug("HLS playlist written: %s (%d segs)", out.name, len(segs))

    def _write_master_playlist(self) -> None:
        video_rel = os.path.relpath(HLS_VIDEO_M3U8, OUTPUT_DIR)
        audio_rel = os.path.relpath(HLS_AUDIO_M3U8, OUTPUT_DIR)
        master = f"""#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Default",DEFAULT=YES,URI="{audio_rel}"
#EXT-X-STREAM-INF:BANDWIDTH=2128000,CODECS="avc1.42c01f,mp4a.40.2",AUDIO="audio"
{video_rel}
"""
        M3U8_MASTER.write_text(master)

    # ── Utility ──────────────────────────────────────────────────────────

    def _latest_segments(self, directory: Path, pattern: str) -> list[Path]:
        """Return up to WINDOW_SEGMENTS most-recent matching files."""
        files = sorted(directory.glob(pattern))
        return files[-WINDOW_SEGMENTS:]


# ---------------------------------------------------------------------------
# GStreamer pipeline
# ---------------------------------------------------------------------------

def build_pipeline() -> Gst.Pipeline:
    """
    Two tees (video + audio) each feeding two cmafmux+splitmuxsink pairs
    (one for DASH output, one for HLS output).
    """
    Gst.init([])
    makedirs(DASH_VIDEO_DIR, DASH_AUDIO_DIR, HLS_VIDEO_DIR, HLS_AUDIO_DIR)

    pipeline = Gst.Pipeline.new("cmaf-multi-pipeline")

    # ── Video source branch ───────────────────────────────────────────────
    vid_src   = make_element("tcpclientsrc", "vid_src")
    vid_q1    = make_element("queue",        "vid_q1")
    vid_demux = make_element("qtdemux",      "vid_demux")
    vid_q2    = make_element("queue",        "vid_q2")
    h264parse = make_element("h264parse",    "h264parse")
    vid_tee   = make_element("tee",          "vid_tee")

    vid_src.set_property("host", VIDEO_HOST)
    vid_src.set_property("port", VIDEO_PORT)

    # DASH video sink
    cmaf_dash_vid  = make_element("cmafmux",      "cmaf_dash_vid")
    split_dash_vid = make_element("splitmuxsink", "split_dash_vid")
    configure_cmafmux(cmaf_dash_vid)
    configure_splitmuxsink(
        split_dash_vid,
        str(DASH_VIDEO_DIR / "seg%05d.m4s"),
        cmaf_dash_vid,
    )

    # HLS video sink
    cmaf_hls_vid  = make_element("cmafmux",      "cmaf_hls_vid")
    split_hls_vid = make_element("splitmuxsink", "split_hls_vid")
    configure_cmafmux(cmaf_hls_vid)
    configure_splitmuxsink(
        split_hls_vid,
        str(HLS_VIDEO_DIR / "seg%05d.m4s"),
        cmaf_hls_vid,
    )

    # ── Audio source branch ───────────────────────────────────────────────
    aud_src   = make_element("tcpclientsrc", "aud_src")
    aud_q1    = make_element("queue",        "aud_q1")
    aud_demux = make_element("qtdemux",      "aud_demux")
    aud_q2    = make_element("queue",        "aud_q2")
    aacparse  = make_element("aacparse",     "aacparse")
    aud_tee   = make_element("tee",          "aud_tee")

    aud_src.set_property("host", AUDIO_HOST)
    aud_src.set_property("port", AUDIO_PORT)

    # DASH audio sink
    cmaf_dash_aud  = make_element("cmafmux",      "cmaf_dash_aud")
    split_dash_aud = make_element("splitmuxsink", "split_dash_aud")
    configure_cmafmux(cmaf_dash_aud)
    configure_splitmuxsink(
        split_dash_aud,
        str(DASH_AUDIO_DIR / "seg%05d.m4s"),
        cmaf_dash_aud,
    )

    # HLS audio sink
    cmaf_hls_aud  = make_element("cmafmux",      "cmaf_hls_aud")
    split_hls_aud = make_element("splitmuxsink", "split_hls_aud")
    configure_cmafmux(cmaf_hls_aud)
    configure_splitmuxsink(
        split_hls_aud,
        str(HLS_AUDIO_DIR / "seg%05d.m4s"),
        cmaf_hls_aud,
    )

    # ── Add everything to pipeline ────────────────────────────────────────
    all_elements = [
        vid_src, vid_q1, vid_demux, vid_q2, h264parse, vid_tee,
        split_dash_vid, split_hls_vid,
        aud_src, aud_q1, aud_demux, aud_q2, aacparse, aud_tee,
        split_dash_aud, split_hls_aud,
    ]
    for el in all_elements:
        pipeline.add(el)

    # ── Static links ──────────────────────────────────────────────────────
    static_links = [
        (vid_src,   vid_q1,    "vid_src → vid_q1"),
        (vid_q1,    vid_demux, "vid_q1 → vid_demux"),
        (vid_q2,    h264parse, "vid_q2 → h264parse"),
        (h264parse, vid_tee,   "h264parse → vid_tee"),
        (aud_src,   aud_q1,    "aud_src → aud_q1"),
        (aud_q1,    aud_demux, "aud_q1 → aud_demux"),
        (aud_q2,    aacparse,  "aud_q2 → aacparse"),
        (aacparse,  aud_tee,   "aacparse → aud_tee"),
    ]
    for src, dst, label in static_links:
        if not src.link(dst):
            raise RuntimeError(f"Failed to link {label}")

    # ── Dynamic demux → queue (pad-added) ────────────────────────────────
    def on_demux_pad_added(demux, src_pad, sink_queue):
        caps = src_pad.get_current_caps() or src_pad.query_caps(None)
        log.info(
            "New pad '%s' on %s (caps: %s)",
            src_pad.get_name(), demux.get_name(),
            caps.get_structure(0).get_name(),
        )
        sink_pad = sink_queue.get_static_pad("sink")
        if sink_pad.is_linked():
            return
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            log.error("Pad link failed: %s → %s (%s)",
                      src_pad.get_name(), sink_pad.get_name(), ret)

    vid_demux.connect("pad-added", on_demux_pad_added, vid_q2)
    aud_demux.connect("pad-added", on_demux_pad_added, aud_q2)

    # ── Tee → splitmuxsink (request pads) ────────────────────────────────
    def link_tee_to_splitmux(tee, splitmux, pad_name, label):
        """
        Request a src pad from tee and the appropriate sink pad from
        splitmuxsink, then link them.
        """
        tee_src  = tee.request_pad_simple("src_%u")
        sink_pad = splitmux.request_pad_simple(pad_name)

        if tee_src is None:
            raise RuntimeError(f"tee did not provide a src pad for {label}")
        if sink_pad is None:
            raise RuntimeError(
                f"splitmuxsink did not provide pad '{pad_name}' for {label}"
            )

        ret = tee_src.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            raise RuntimeError(f"Failed to link {label}: {ret}")
        log.info("Linked %s", label)

    link_tee_to_splitmux(vid_tee, split_dash_vid, "video",    "vid_tee → split_dash_vid")
    link_tee_to_splitmux(vid_tee, split_hls_vid,  "video",    "vid_tee → split_hls_vid")
    link_tee_to_splitmux(aud_tee, split_dash_aud, "audio_%u", "aud_tee → split_dash_aud")
    link_tee_to_splitmux(aud_tee, split_hls_aud,  "audio_%u", "aud_tee → split_hls_aud")

    return pipeline


# ---------------------------------------------------------------------------
# Async streamer
# ---------------------------------------------------------------------------

class MultiFormatStreamer:
    def __init__(self):
        self.pipeline: Gst.Pipeline | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = asyncio.Event()
        self._manifest_writer = ManifestWriter()

    async def start(self):
        self._loop = asyncio.get_running_loop()

        log.info("Building CMAF multi-format pipeline…")
        log.info("  Video : tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio : tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output: %s", OUTPUT_DIR)
        log.info("  Segment duration : %d ms  |  Fragment : %d ms",
                 SEG_DURATION_MS, FRAG_DURATION_MS)

        self.pipeline = build_pipeline()

        # Start manifest writer thread
        self._manifest_writer.start()

        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)

        Gst.debug_bin_to_dot_file(
            self.pipeline, Gst.DebugGraphDetails.ALL, "before_cmaf_multi"
        )

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Failed to set pipeline to PLAYING")

        Gst.debug_bin_to_dot_file(
            self.pipeline, Gst.DebugGraphDetails.ALL, "after_cmaf_multi"
        )

        log.info(
            "Pipeline PLAYING\n"
            "  DASH  → %s\n"
            "  HLS   → %s\n"
            "  MPD   → %s\n"
            "  M3U8  → %s",
            OUTPUT_DIR / "dash",
            OUTPUT_DIR / "hls",
            MPD_FILE,
            M3U8_MASTER,
        )

        await asyncio.get_running_loop().run_in_executor(None, glib_loop.run)

    def _on_bus_message(self, bus, message, glib_loop: GLib.MainLoop):
        t = message.type
        if t == Gst.MessageType.EOS:
            log.info("EOS received.")
            glib_loop.quit()
            self._loop.call_soon_threadsafe(self._stop_event.set)
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            log.error("GStreamer error: %s", err.message)
            if debug:
                log.debug("Debug: %s", debug)
            glib_loop.quit()
            self._loop.call_soon_threadsafe(self._stop_event.set)
        elif t == Gst.MessageType.WARNING:
            warn, debug = message.parse_warning()
            log.warning("GStreamer warning: %s", warn.message)
        elif t == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old, new, _ = message.parse_state_changed()
                log.debug(
                    "Pipeline: %s → %s",
                    Gst.Element.state_get_name(old),
                    Gst.Element.state_get_name(new),
                )

    async def stop(self):
        self._manifest_writer.stop()
        if self.pipeline:
            log.info("Sending EOS…")
            self.pipeline.send_event(Gst.Event.new_eos())
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                log.warning("EOS timeout — forcing NULL.")
            self.pipeline.set_state(Gst.State.NULL)
            log.info("Pipeline stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    streamer = MultiFormatStreamer()
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