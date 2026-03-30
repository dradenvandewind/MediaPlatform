#!/usr/bin/env python3
"""
Low-latency CMAF pipeline : fMP4/TCP → DASH + HLS

Architecture :
  tcpclientsrc(video) → qtdemux → h264parse ─┐
                                               ├→ qtmux(frag) → appsink → SegmentWriter
  tcpclientsrc(audio) → qtdemux → aacparse  ─┘

  SegmentWriter  : parse BMFF en streaming
                   ftyp+moov → init.mp4  (dès le premier buffer)
                   moof+mdat × N         → seg%05d.m4s

  ManifestWriter : stream.mpd (SegmentTemplate) + master.m3u8 + hls/stream.m3u8

Env vars :
  VIDEO_HOST / VIDEO_PORT       (default: receiver / 5565)
  AUDIO_HOST / AUDIO_PORT       (default: receiver / 5566)
  OUTPUT_DIR                    (default: /var/cache/nginx)
  SEGMENT_DURATION_MS           (default: 2000)
  FRAGMENT_DURATION_MS          (default: 500)
  MANIFEST_REFRESH_S            (default: 1)
  WINDOW_SEGMENTS               (default: 8)
"""

import asyncio
import logging
import os
import signal
import sys
import threading
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
log = logging.getLogger("cmaf")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def cfg(key: str, default: str) -> str:
    return os.environ.get(key, default)


VIDEO_HOST         = cfg("VIDEO_HOST",               "receiver")
VIDEO_PORT         = int(cfg("VIDEO_PORT",           "5565"))
AUDIO_HOST         = cfg("AUDIO_HOST",               "receiver")
AUDIO_PORT         = int(cfg("AUDIO_PORT",           "5566"))
OUTPUT_DIR         = Path(cfg("OUTPUT_DIR",          "/var/cache/nginx/"))
SEG_DURATION_MS    = int(cfg("SEGMENT_DURATION_MS",  "2000"))
FRAG_DURATION_MS   = int(cfg("FRAGMENT_DURATION_MS", "500"))
MANIFEST_REFRESH_S = float(cfg("MANIFEST_REFRESH_S", "1"))
WINDOW_SEGMENTS    = int(cfg("WINDOW_SEGMENTS",      "8"))

SEG_DURATION_S = SEG_DURATION_MS / 1000
FRAGS_PER_SEG  = max(1, SEG_DURATION_MS // FRAG_DURATION_MS)

SEG_DIR     = OUTPUT_DIR / "segments"
INIT_FILE   = SEG_DIR / "init.mp4"
HLS_M3U8    = OUTPUT_DIR / "hls" / "stream.m3u8"
MPD_FILE    = OUTPUT_DIR / "stream.mpd"
M3U8_MASTER = OUTPUT_DIR / "master.m3u8"


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


# ---------------------------------------------------------------------------
# BMFF streaming parser
# ---------------------------------------------------------------------------

class BMFFParser:
    """
    Parse un flux d'octets fMP4 en streaming.
    Accumule les données jusqu'à avoir une box complète.
    """

    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[tuple[bytes, bytes]]:
        """
        Ingère des données brutes.
        Retourne la liste des (box_type, box_bytes) complètes disponibles.
        """
        self._buf += data
        boxes: list[tuple[bytes, bytes]] = []
        while len(self._buf) >= 8:
            size = int.from_bytes(self._buf[:4], 'big')
            if size == 0:
                # Box jusqu'à EOF : consommer tout
                box_type = bytes(self._buf[4:8])
                boxes.append((box_type, bytes(self._buf)))
                self._buf.clear()
                break
            if size < 8:
                log.warning("BMFF: invalid box size %d — resetting", size)
                self._buf.clear()
                break
            if len(self._buf) < size:
                break   # box incomplète
            box_type = bytes(self._buf[4:8])
            box_data = bytes(self._buf[:size])
            boxes.append((box_type, box_data))
            del self._buf[:size]
        return boxes


# ---------------------------------------------------------------------------
# Segment writer
# ---------------------------------------------------------------------------

class SegmentWriter:
    """
    Consomme les boxes fMP4 émises par qtmux via appsink.

    Séquence attendue (qtmux avec fragment-duration > 0) :
      ftyp → moov → [moof → mdat] × N → mfra?

    - ftyp + moov       → init.mp4   (écrit immédiatement au démarrage)
    - moof + mdat × N   → seg%05d.m4s (coupé tous les FRAGS_PER_SEG mdat)
    """

    _INIT_BOXES  = {b'ftyp', b'moov'}
    _SKIP_BOXES  = {b'mfra', b'mfro', b'free', b'skip'}

    def __init__(self):
        self._parser     = BMFFParser()
        self._init_buf   = bytearray()
        self._init_done  = False
        self._seg_buf    = bytearray()
        self._frag_count = 0
        self._seg_idx    = 0

    def feed(self, data: bytes) -> None:
        for box_type, box_data in self._parser.feed(data):
            self._dispatch(box_type, box_data)

    def _dispatch(self, box_type: bytes, box_data: bytes) -> None:

        # ── Phase init ────────────────────────────────────────────────────
        if not self._init_done:
            if box_type in self._INIT_BOXES:
                self._init_buf += box_data
                if box_type == b'moov':
                    self._flush_init()
                return
            # Pas de ftyp/moov → qtmux sans fragmentation
            log.error(
                "First box '%s' != ftyp/moov. "
                "qtmux n'est PAS en mode fMP4. "
                "Vérifier fragment-duration property.",
                box_type.decode(errors='replace')
            )
            return

        # ── Phase media ───────────────────────────────────────────────────
        if box_type in self._SKIP_BOXES:
            return

        if box_type == b'moof':
            self._seg_buf += box_data

        elif box_type in (b'mdat', b'sidx'):
            self._seg_buf += box_data
            if box_type == b'mdat':
                self._frag_count += 1
                if self._frag_count >= FRAGS_PER_SEG:
                    self._flush_segment()
        else:
            log.debug("BMFF: box inconnu '%s' (%d bytes)",
                      box_type.decode(errors='replace'), len(box_data))

    def _flush_init(self) -> None:
        INIT_FILE.write_bytes(bytes(self._init_buf))
        log.info("Init → %s (%d bytes)", INIT_FILE.name, len(self._init_buf))
        self._init_buf.clear()
        self._init_done = True

    def _flush_segment(self) -> None:
        if not self._seg_buf:
            return
        path = SEG_DIR / f"seg{self._seg_idx:05d}.m4s"
        tmp  = path.with_suffix(".tmp")
        tmp.write_bytes(bytes(self._seg_buf))
        tmp.replace(path)
        log.info("Segment → %s  (%d bytes  %d frags)",
                 path.name, len(self._seg_buf), self._frag_count)
        self._seg_buf.clear()
        self._frag_count = 0
        self._seg_idx   += 1


# ---------------------------------------------------------------------------
# Manifest writer
# ---------------------------------------------------------------------------

class ManifestWriter:
    def __init__(self) -> None:
        self._stop               = threading.Event()
        self._availability_start = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    def start(self) -> threading.Thread:
        t = threading.Thread(target=self._loop, name="manifest-writer", daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        log.info("ManifestWriter started (refresh %.1fs)", MANIFEST_REFRESH_S)
        while not self._stop.wait(timeout=MANIFEST_REFRESH_S):
            try:
                self._write_mpd()
                self._write_hls()
            except Exception as exc:
                log.warning("ManifestWriter error: %s", exc)
        log.info("ManifestWriter stopped.")

    def _seg_number(self, p: Path) -> int:
        return int(p.stem.replace("seg", ""))

    def _latest_segments(self) -> list[Path]:
        all_files = sorted(SEG_DIR.glob("seg*.m4s"))
        all_files = [f for f in all_files if f.stat().st_size > 0]
        if len(all_files) > 1:
            all_files = all_files[:-1]   # dernier potentiellement en cours
        window = all_files[-WINDOW_SEGMENTS:]
        for old in all_files[:-WINDOW_SEGMENTS]:
            try:
                old.unlink()
            except OSError:
                pass
        return window

    # ── DASH MPD ─────────────────────────────────────────────────────────

    def _write_mpd(self) -> None:
        segs = self._latest_segments()
        if not segs or not INIT_FILE.exists():
            log.debug("MPD: en attente (segs=%d init=%s)",
                      len(segs), INIT_FILE.exists())
            return

        init_rel  = os.path.relpath(INIT_FILE, OUTPUT_DIR)
        media_rel = os.path.relpath(SEG_DIR / "seg$Number%05d$.m4s", OUTPUT_DIR)
        start_num = self._seg_number(segs[0])

        mpd = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"\n'
            '     profiles="urn:mpeg:dash:profile:isoff-live:2011"\n'
            '     type="dynamic"\n'
            f'     availabilityStartTime="{self._availability_start}"\n'
            f'     minimumUpdatePeriod="PT{SEG_DURATION_S:.1f}S"\n'
            f'     minBufferTime="PT{SEG_DURATION_S * 2:.1f}S"\n'
            f'     timeShiftBufferDepth="PT{SEG_DURATION_S * WINDOW_SEGMENTS:.0f}S"\n'
            f'     suggestedPresentationDelay="PT{SEG_DURATION_S * 3:.0f}S">\n'
            '  <Period id="1" start="PT0S">\n'
            '    <AdaptationSet mimeType="video/mp4"'
            ' codecs="avc1.42c01f,mp4a.40.2"\n'
            '                   segmentAlignment="true" startWithSAP="1">\n'
            '      <Representation id="av" bandwidth="2128000"'
            ' width="1280" height="720" frameRate="30">\n'
            '        <SegmentTemplate timescale="1000"\n'
            f'                         duration="{SEG_DURATION_MS}"\n'
            f'                         startNumber="{start_num}"\n'
            f'                         initialization="{init_rel}"\n'
            f'                         media="{media_rel}"/>\n'
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '  </Period>\n'
            '</MPD>\n'
        )
        tmp = MPD_FILE.with_suffix(".tmp")
        tmp.write_text(mpd)
        tmp.replace(MPD_FILE)
        log.info("MPD  start=%d  %d segs", start_num, len(segs))

    # ── HLS ──────────────────────────────────────────────────────────────

    def _write_hls(self) -> None:
        segs = self._latest_segments()
        if not segs or not INIT_FILE.exists():
            return

        seq      = self._seg_number(segs[0])
        base     = HLS_M3U8.parent
        init_rel = os.path.relpath(INIT_FILE, base)

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(SEG_DURATION_S) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq}",
            f'#EXT-X-MAP:URI="{init_rel}"',
        ]
        for seg in segs:
            lines.append(f"#EXTINF:{SEG_DURATION_S:.3f},")
            lines.append(os.path.relpath(seg, base))

        tmp = HLS_M3U8.with_suffix(".tmp")
        tmp.write_text("\n".join(lines) + "\n")
        tmp.replace(HLS_M3U8)

        hls_rel = os.path.relpath(HLS_M3U8, OUTPUT_DIR)
        master  = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=2128000,'
            'CODECS="avc1.42c01f,mp4a.40.2"\n'
            f"{hls_rel}\n"
        )
        tmp = M3U8_MASTER.with_suffix(".tmp")
        tmp.write_text(master)
        tmp.replace(M3U8_MASTER)
        log.debug("HLS  seq=%d  %d segs", seq, len(segs))


# ---------------------------------------------------------------------------
# GStreamer pipeline
# ---------------------------------------------------------------------------

def build_pipeline(on_segment_written) -> Gst.Pipeline:
    Gst.init([])
    makedirs(SEG_DIR, OUTPUT_DIR / "hls")

    pipeline = Gst.Pipeline.new("cmaf-pipeline")

    # ── Video branch ──────────────────────────────────────────────────────
    vid_src   = make_element("tcpclientsrc", "vid_src")
    vid_q1    = make_element("queue",        "vid_q1")
    vid_demux = make_element("qtdemux",      "vid_demux")
    vid_q2    = make_element("queue",        "vid_q2")
    h264parse = make_element("h264parse",    "h264parse")
    vid_caps  = make_element("capsfilter",   "vid_caps")

    vid_src.set_property("host", VIDEO_HOST)
    vid_src.set_property("port", VIDEO_PORT)
    vid_q1.set_property("max-size-time", 4 * Gst.SECOND)
    vid_q2.set_property("max-size-time", 4 * Gst.SECOND)
    h264parse.set_property("config-interval", -1)
    vid_caps.set_property("caps", Gst.Caps.from_string(
        "video/x-h264,stream-format=avc,alignment=au"
    ))

    # ── Audio branch ──────────────────────────────────────────────────────
    aud_src   = make_element("tcpclientsrc", "aud_src")
    aud_q1    = make_element("queue",        "aud_q1")
    aud_demux = make_element("qtdemux",      "aud_demux")
    aud_q2    = make_element("queue",        "aud_q2")
    aacparse  = make_element("aacparse",     "aacparse")
    aud_caps  = make_element("capsfilter",   "aud_caps")

    aud_src.set_property("host", AUDIO_HOST)
    aud_src.set_property("port", AUDIO_PORT)
    aud_q1.set_property("max-size-time", 4 * Gst.SECOND)
    aud_q2.set_property("max-size-time", 4 * Gst.SECOND)
    aud_caps.set_property("caps", Gst.Caps.from_string(
        "audio/mpeg,mpegversion=4,stream-format=raw"
    ))

    # ── cmafmux + splitmuxsink ────────────────────────────────────────────
    cmafmux  = make_element("cmafmux",      "cmafmux")
    splitmux = make_element("splitmuxsink", "splitmux")

    cmafmux.set_property("chunk-duration",    FRAG_DURATION_MS * Gst.MSECOND)
    cmafmux.set_property("fragment-duration", SEG_DURATION_MS  * Gst.MSECOND)

    splitmux.set_property("muxer",                 cmafmux)
    splitmux.set_property("max-size-time",         SEG_DURATION_MS * Gst.MSECOND)
    splitmux.set_property("send-keyframe-requests", True)
    splitmux.set_property("reset-muxer",           False)  # cmafmux gère ses propres resets
    splitmux.set_property("async-finalize",        False)

    # format-location-full : retourner le chemin du fichier à écrire
    # fragment_id démarre à 0 et s'incrémente à chaque segment
    def on_format_location_full(splitmux, fragment_id, first_sample):
        path = str(SEG_DIR / f"seg{fragment_id:05d}.m4s")
        log.info("Nouveau segment → %s", Path(path).name)
        on_segment_written(fragment_id, Path(path))
        return path

    splitmux.connect("format-location-full", on_format_location_full)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in [
        vid_src, vid_q1, vid_demux, vid_q2, h264parse, vid_caps,
        aud_src, aud_q1, aud_demux, aud_q2, aacparse, aud_caps,
        splitmux,
    ]:
        pipeline.add(el)

    # ── Liens statiques ───────────────────────────────────────────────────
    for src, dst, label in [
        (vid_src,   vid_q1,    "vid_src → vid_q1"),
        (vid_q1,    vid_demux, "vid_q1 → vid_demux"),
        (vid_q2,    h264parse, "vid_q2 → h264parse"),
        (h264parse, vid_caps,  "h264parse → vid_caps"),
        (aud_src,   aud_q1,   "aud_src → aud_q1"),
        (aud_q1,    aud_demux,"aud_q1 → aud_demux"),
        (aud_q2,    aacparse, "aud_q2 → aacparse"),
        (aacparse,  aud_caps, "aacparse → aud_caps"),
    ]:
        if not src.link(dst):
            raise RuntimeError(f"Failed to link {label}")

    # ── qtdemux pad-added ─────────────────────────────────────────────────
    def on_demux_pad_added(demux, src_pad, sink_queue):
        caps = src_pad.get_current_caps() or src_pad.query_caps(None)
        name = caps.get_structure(0).get_name() if caps else "unknown"
        log.info("Pad '%s' on %s (%s)", src_pad.get_name(), demux.get_name(), name)
        sink_pad = sink_queue.get_static_pad("sink")
        if sink_pad.is_linked():
            return
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            log.error("Pad link failed: %s → %s (%s)",
                      src_pad.get_name(), sink_pad.get_name(), ret)

    vid_demux.connect("pad-added", on_demux_pad_added, vid_q2)
    aud_demux.connect("pad-added", on_demux_pad_added, aud_q2)

    # ── READY avant request pads ──────────────────────────────────────────
    ret = pipeline.set_state(Gst.State.READY)
    if ret == Gst.StateChangeReturn.FAILURE:
        raise RuntimeError("Pipeline failed to go to READY")
    pipeline.get_state(timeout=Gst.SECOND * 5)

    def link_to_splitmux(src_el, pad_name, label):
        src_pad  = src_el.get_static_pad("src")
        sink_pad = splitmux.request_pad_simple(pad_name)
        if sink_pad is None:
            raise RuntimeError(
                f"splitmuxsink pad '{pad_name}' indisponible pour {label}"
            )
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            raise RuntimeError(f"Failed to link {label}: {ret}")
        log.info("Linked %s", label)

    link_to_splitmux(vid_caps, "video",    "vid_caps → splitmux (video)")
    link_to_splitmux(aud_caps, "audio_%u", "aud_caps → splitmux (audio)")

    return pipeline

# ---------------------------------------------------------------------------
# Async streamer
# ---------------------------------------------------------------------------

class MultiFormatStreamer:
    def __init__(self):
        self.pipeline         : Gst.Pipeline | None = None
        self._loop            : asyncio.AbstractEventLoop | None = None
        self._stop_event      = asyncio.Event()
        self._manifest_writer = ManifestWriter()
        self._init_extracted  = False

    def _on_segment_written(self, fragment_id: int, path: Path) -> None:
        """Appelé depuis le thread GLib au moment où splitmuxsink ouvre
        le nouveau fichier — l'ancien est donc fermé et complet."""
        if not self._init_extracted and fragment_id > 0:
            # Le segment 0 vient d'être finalisé (fragment_id==1 = on ouvre le 2e)
            self._extract_init(SEG_DIR / "seg00000.m4s")

    def _extract_init(self, seg0: Path) -> None:
        if not seg0.exists():
            return
        data = seg0.read_bytes()
        pos, init_buf = 0, bytearray()
        while pos + 8 <= len(data):
            size = int.from_bytes(data[pos:pos+4], 'big')
            btype = data[pos+4:pos+8]
            if size < 8:
                break
            if btype in (b'ftyp', b'moov'):
                init_buf += data[pos:pos+size]
            pos += size
        if init_buf:
            INIT_FILE.write_bytes(bytes(init_buf))
            self._init_extracted = True
            log.info("Init extrait → %s (%d bytes)", INIT_FILE.name, len(init_buf))

    async def start(self):
        self._loop = asyncio.get_running_loop()
        log.info("Building CMAF pipeline (splitmuxsink)…")
        log.info("  Video  : tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio  : tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output : %s", OUTPUT_DIR)
        log.info("  Seg %d ms  Frag %d ms", SEG_DURATION_MS, FRAG_DURATION_MS)

        self.pipeline = build_pipeline(self._on_segment_written)
        self._manifest_writer.start()

        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Pipeline failed to go to PLAYING")

        log.info("Pipeline PLAYING  MPD=%s  M3U8=%s", MPD_FILE, M3U8_MASTER)
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
            warn, _ = message.parse_warning()
            log.warning("GStreamer warning: %s", warn.message)
        elif t == Gst.MessageType.STATE_CHANGED:
            if message.src == self.pipeline:
                old, new, _ = message.parse_state_changed()
                log.debug("Pipeline: %s → %s",
                          Gst.Element.state_get_name(old),
                          Gst.Element.state_get_name(new))

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
    loop     = asyncio.get_running_loop()

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