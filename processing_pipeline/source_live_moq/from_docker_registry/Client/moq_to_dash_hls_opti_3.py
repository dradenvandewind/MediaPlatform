#!/usr/bin/env python3
"""
GStreamer CMAF pipeline → DASH + HLS (shared .m4s segments).

Topology :
  tcpclientsrc (video) → qtdemux → h264parse → splitmuxsink  (segments/video/)
  tcpclientsrc (audio) → qtdemux → aacparse  → splitmuxsink  (segments/audio/)
  ManifestWriter (thread)  →  stream.mpd  +  master.m3u8

Env vars :
  VIDEO_HOST / VIDEO_PORT       (default: receiver / 5565)
  AUDIO_HOST / AUDIO_PORT       (default: receiver / 5566)
  OUTPUT_DIR                    (default: /var/cache/nginx)
  SEGMENT_DURATION_MS           (default: 2000)
  FRAGMENT_DURATION_MS          (default: 500)
  MANIFEST_REFRESH_S            (default: 2)
  WINDOW_SEGMENTS               (default: 5)
"""

import asyncio
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


VIDEO_HOST         = cfg("VIDEO_HOST",          "receiver")
VIDEO_PORT         = int(cfg("VIDEO_PORT",      "5565"))
AUDIO_HOST         = cfg("AUDIO_HOST",          "receiver")
AUDIO_PORT         = int(cfg("AUDIO_PORT",      "5566"))
OUTPUT_DIR         = Path(cfg("OUTPUT_DIR",     "/var/cache/nginx/"))
SEG_DURATION_MS    = int(cfg("SEGMENT_DURATION_MS",  "2000"))
FRAG_DURATION_MS   = int(cfg("FRAGMENT_DURATION_MS", "500"))
MANIFEST_REFRESH_S = float(cfg("MANIFEST_REFRESH_S", "2"))
WINDOW_SEGMENTS    = int(cfg("WINDOW_SEGMENTS", "5"))

SEG_VIDEO_DIR  = OUTPUT_DIR / "segments" / "video"
SEG_AUDIO_DIR  = OUTPUT_DIR / "segments" / "audio"
HLS_VIDEO_M3U8 = OUTPUT_DIR / "hls" / "video.m3u8"
HLS_AUDIO_M3U8 = OUTPUT_DIR / "hls" / "audio.m3u8"
MPD_FILE       = OUTPUT_DIR / "stream.mpd"
M3U8_MASTER    = OUTPUT_DIR / "master.m3u8"
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


# ---------------------------------------------------------------------------
# ISO BMFF helpers
# ---------------------------------------------------------------------------

def extract_init_from_segment(seg_path: Path, init_path: Path) -> bool:
    """
    Extrait ftyp+moov depuis le premier segment → init.mp4.
    S'arrête AVANT styp ou moof — n'inclut PAS styp dans l'init.
    """
    if init_path.exists():
        return True
    try:
        data = seg_path.read_bytes()
        if len(data) < 8:
            return False
        offset = 0
        while offset < len(data) - 8:
            box_size = int.from_bytes(data[offset:offset + 4], 'big')
            box_type = data[offset + 4:offset + 8]
            if box_type in (b'styp', b'moof'):
                if offset == 0:
                    log.warning("No init data in %s (starts with %s)", seg_path.name, box_type)
                    return False
                init_path.write_bytes(data[:offset])
                log.info("Init extracted → %s (%d bytes)", init_path.name, offset)
                return True
            if box_size < 8:
                break
            offset += box_size
    except Exception as e:
        log.warning("extract_init error %s: %s", seg_path.name, e)
    return False


def strip_init_boxes(seg_path: Path, init_path: Path) -> bool:
    """
    Réécrit seg_path en supprimant ftyp+moov du début (atomique via .tmp).
    Extrait init.mp4 au passage si absent.
    Retourne True si le fichier a été modifié.
    """
    try:
        data = seg_path.read_bytes()
        if len(data) < 8:
            return False
        offset = 0
        while offset < len(data) - 8:
            box_size = int.from_bytes(data[offset:offset + 4], 'big')
            box_type = data[offset + 4:offset + 8]
            if box_type in (b'styp', b'moof'):
                if offset == 0:
                    return False  # déjà strippé
                # Sauvegarder init si absent
                if not init_path.exists():
                    init_path.write_bytes(data[:offset])
                    log.info("Init → %s (%d bytes)", init_path.name, offset)
                # Réécriture atomique
                tmp = seg_path.with_suffix(".tmp")
                tmp.write_bytes(data[offset:])
                tmp.replace(seg_path)
                log.debug("Stripped %s (%d→%d bytes)",
                          seg_path.name, len(data), len(data) - offset)
                return True
            if box_size < 8:
                break
            offset += box_size
    except Exception as e:
        log.warning("strip_init_boxes error %s: %s", seg_path.name, e)
    return False


# ---------------------------------------------------------------------------
# Manifest writer
# ---------------------------------------------------------------------------

class ManifestWriter:
    def __init__(self) -> None:
        self._stop     = threading.Event()
        self._stripped : set[Path] = set()   # segments déjà strippés
        #self._availability_start : str | None = None
        self._availability_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        self._stream_start_time  : float | None = None   # timestamp Unix du seg00000

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

    # ── Utility ──────────────────────────────────────────────────────────

    def _seg_number(self, seg_path: Path) -> int:
        """seg00042.m4s → 42"""
        return int(seg_path.stem.replace("seg", ""))

    def _latest_segments(self, directory: Path) -> list[Path]:
        all_files = sorted(directory.glob("seg*.m4s"))
        all_files = [f for f in all_files if f.stat().st_size > 0]
        if len(all_files) > 1:
            all_files = all_files[:-1]
        window = all_files[-WINDOW_SEGMENTS:]
        # Supprimer les segments hors fenêtre
        for old in all_files[:-WINDOW_SEGMENTS]:
            try:
                old.unlink()
            except OSError:
                pass
        return window

    # ── DASH MPD ─────────────────────────────────────────────────────────
    """
    def _write_mpd(self) -> None:
        video_segs = self._latest_segments(SEG_VIDEO_DIR)
        audio_segs = self._latest_segments(SEG_AUDIO_DIR)

        if not video_segs or not audio_segs:
            log.debug("MPD: waiting for segments (vid=%d aud=%d)",
                      len(video_segs), len(audio_segs))
            return

        video_init = SEG_VIDEO_DIR / "init.mp4"
        audio_init = SEG_AUDIO_DIR / "init.mp4"

        if not video_init.exists() or not audio_init.exists():
            log.debug("MPD: waiting for init segments")
            return
        
        if self._availability_start is None:
            all_vid = sorted(SEG_VIDEO_DIR.glob("seg*.m4s"))
            if all_vid:
                # mtime du premier segment = vrai début du stream
                first_seg_time = all_vid[0].stat().st_mtime
                self._availability_start = datetime.fromtimestamp(
                    first_seg_time, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                log.info("AST fixed to first segment mtime: %s",
                        self._availability_start)
        if self._availability_start is None:
            return
                
        now = self._availability_start

        #now       = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        window_s  = SEG_DURATION_S * WINDOW_SEGMENTS

        vid_start     = self._seg_number(video_segs[0])
        aud_start     = self._seg_number(audio_segs[0])
        vid_init_rel  = os.path.relpath(video_init, OUTPUT_DIR)
        aud_init_rel  = os.path.relpath(audio_init, OUTPUT_DIR)

        # $Number%05d$ : template DASH avec padding 5 chiffres
        vid_media_rel = os.path.relpath(
            SEG_VIDEO_DIR / "seg$Number%05d$.m4s", OUTPUT_DIR
        )
        aud_media_rel = os.path.relpath(
            SEG_AUDIO_DIR / "seg$Number%05d$.m4s", OUTPUT_DIR
        )

        mpd = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"\n'
            '     profiles="urn:mpeg:dash:profile:isoff-live:2011"\n'
            '     type="dynamic"\n'
            f'     availabilityStartTime="{now}"\n'
            f'     minimumUpdatePeriod="PT{SEG_DURATION_S:.1f}S"\n'
            f'     minBufferTime="PT{SEG_DURATION_S:.1f}S"\n'
            f'     timeShiftBufferDepth="PT{window_s:.0f}S"\n'
            f'     suggestedPresentationDelay="PT{SEG_DURATION_S * 2:.0f}S">\n'
            '  <Period id="1" start="PT0S">\n'
            '\n'
            '    <AdaptationSet id="1" mimeType="video/mp4" codecs="avc1.42c01f"\n'
            '                   segmentAlignment="true" startWithSAP="1">\n'
            '      <Representation id="video" bandwidth="2000000"'
            ' width="1280" height="720" frameRate="30">\n'
            '        <SegmentTemplate timescale="1000"\n'
            f'                         duration="{SEG_DURATION_MS}"\n'
            f'                         startNumber="{vid_start}"\n'
            f'                         initialization="{vid_init_rel}"\n'
            f'                         media="{vid_media_rel}"/>\n'
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '\n'
            '    <AdaptationSet id="2" mimeType="audio/mp4" codecs="mp4a.40.2"'
            ' lang="und" segmentAlignment="true">\n'
            '      <Representation id="audio" bandwidth="128000">\n'
            '        <SegmentTemplate timescale="1000"\n'
            f'                         duration="{SEG_DURATION_MS}"\n'
            f'                         startNumber="{aud_start}"\n'
            f'                         initialization="{aud_init_rel}"\n'
            f'                         media="{aud_media_rel}"/>\n'
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '\n'
            '  </Period>\n'
            '</MPD>\n'
        )

        # Écriture atomique
        tmp = MPD_FILE.with_suffix(".tmp")
        tmp.write_text(mpd)
        tmp.replace(MPD_FILE)
        log.info("MPD written (vid_start=%d aud_start=%d)", vid_start, aud_start)
    """
    def _write_mpd(self) -> None:
        video_segs = self._latest_segments(SEG_VIDEO_DIR)
        audio_segs = self._latest_segments(SEG_AUDIO_DIR)
        if not video_segs or not audio_segs:
            return

        video_init = SEG_VIDEO_DIR / "init.mp4"
        audio_init = SEG_AUDIO_DIR / "init.mp4"
        if not video_init.exists() or not audio_init.exists():
            return

        vid_init_rel = os.path.relpath(video_init, OUTPUT_DIR)
        aud_init_rel = os.path.relpath(audio_init, OUTPUT_DIR)

        def seg_entries(segs):
            return "\n          ".join(
                f'<SegmentURL media="{os.path.relpath(s, OUTPUT_DIR)}"/>'
                for s in segs
            )

        mpd = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"\n'
            '     profiles="urn:mpeg:dash:profile:isoff-live:2011"\n'
            '     type="dynamic"\n'
            f'     availabilityStartTime="{datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}"\n'
            f'     minimumUpdatePeriod="PT{SEG_DURATION_S:.1f}S"\n'
            f'     minBufferTime="PT{SEG_DURATION_S:.1f}S"\n'
            f'     timeShiftBufferDepth="PT{SEG_DURATION_S * WINDOW_SEGMENTS:.0f}S"\n'
            f'     suggestedPresentationDelay="PT{SEG_DURATION_S * 2:.0f}S">\n'
            '  <Period id="1" start="PT0S">\n'
            '\n'
            '    <AdaptationSet id="1" mimeType="video/mp4" codecs="avc1.42c01f"\n'
            '                   segmentAlignment="true" startWithSAP="1">\n'
            '      <Representation id="video" bandwidth="2000000"'
            ' width="1280" height="720" frameRate="30">\n'
            f'        <SegmentList timescale="1000" duration="{SEG_DURATION_MS}">\n'
            f'          <Initialization sourceURL="{vid_init_rel}"/>\n'
            f'          {seg_entries(video_segs)}\n'
            '        </SegmentList>\n'
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '\n'
            '    <AdaptationSet id="2" mimeType="audio/mp4" codecs="mp4a.40.2"'
            ' lang="und" segmentAlignment="true">\n'
            '      <Representation id="audio" bandwidth="128000">\n'
            f'        <SegmentList timescale="1000" duration="{SEG_DURATION_MS}">\n'
            f'          <Initialization sourceURL="{aud_init_rel}"/>\n'
            f'          {seg_entries(audio_segs)}\n'
            '        </SegmentList>\n'
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '\n'
            '  </Period>\n'
            '</MPD>\n'
        )

        tmp = MPD_FILE.with_suffix(".tmp")
        tmp.write_text(mpd)
        tmp.replace(MPD_FILE)
        log.info("MPD written (vid=%d segs, aud=%d segs)",
                len(video_segs), len(audio_segs))
    
    # ── HLS M3U8 ─────────────────────────────────────────────────────────

    def _write_hls(self) -> None:
        video_segs = self._latest_segments(SEG_VIDEO_DIR)
        audio_segs = self._latest_segments(SEG_AUDIO_DIR)

        if not video_segs or not audio_segs:
            return

        self._write_media_playlist(HLS_VIDEO_M3U8, video_segs,
                                   SEG_VIDEO_DIR, HLS_VIDEO_M3U8.parent)
        self._write_media_playlist(HLS_AUDIO_M3U8, audio_segs,
                                   SEG_AUDIO_DIR, HLS_AUDIO_M3U8.parent)
        self._write_master_playlist()

    def _write_media_playlist(self, out: Path, segs: list[Path],
                               seg_dir: Path, base: Path) -> None:
        init_path = seg_dir / "init.mp4"
        if not init_path.exists():
            log.warning("HLS: init missing for %s", out.name)
            return

        # MEDIA-SEQUENCE = numéro du premier segment de la fenêtre
        seq = self._seg_number(segs[0])

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(SEG_DURATION_S) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq}",
            f'#EXT-X-MAP:URI="{os.path.relpath(init_path, base)}"',
        ]
        for seg in segs:
            lines.append(f"#EXTINF:{SEG_DURATION_S:.3f},")
            lines.append(os.path.relpath(seg, base))

        tmp = out.with_suffix(".tmp")
        tmp.write_text("\n".join(lines) + "\n")
        tmp.replace(out)
        log.debug("HLS playlist written: %s (%d segs)", out.name, len(segs))

    def _write_master_playlist(self) -> None:
        video_rel = os.path.relpath(HLS_VIDEO_M3U8, OUTPUT_DIR)
        audio_rel = os.path.relpath(HLS_AUDIO_M3U8, OUTPUT_DIR)
        master = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Default",'
            f'DEFAULT=YES,URI="{audio_rel}"\n'
            f'#EXT-X-STREAM-INF:BANDWIDTH=2128000,'
            f'CODECS="avc1.42c01f,mp4a.40.2",AUDIO="audio"\n'
            f"{video_rel}\n"
        )
        tmp = M3U8_MASTER.with_suffix(".tmp")
        tmp.write_text(master)
        tmp.replace(M3U8_MASTER)


# ---------------------------------------------------------------------------
# GStreamer pipeline
# ---------------------------------------------------------------------------

def build_pipeline() -> Gst.Pipeline:
    Gst.init([])
    makedirs(SEG_VIDEO_DIR, SEG_AUDIO_DIR, OUTPUT_DIR / "hls")

    pipeline = Gst.Pipeline.new("cmaf-pipeline")

    # ── Video branch ──────────────────────────────────────────────────────
    vid_src   = make_element("tcpclientsrc", "vid_src")
    vid_q1    = make_element("queue",        "vid_q1")
    vid_demux = make_element("qtdemux",      "vid_demux")
    vid_q2    = make_element("queue",        "vid_q2")
    h264parse = make_element("h264parse",    "h264parse")
    cmaf_vid  = make_element("cmafmux",      "cmaf_vid")
    split_vid = make_element("splitmuxsink", "split_vid")

    vid_src.set_property("host", VIDEO_HOST)
    vid_src.set_property("port", VIDEO_PORT)

    cmaf_vid.set_property("chunk-duration",    FRAG_DURATION_MS)
    cmaf_vid.set_property("fragment-duration", SEG_DURATION_MS)

    split_vid.set_property("location",               str(SEG_VIDEO_DIR / "seg%05d.m4s"))
    split_vid.set_property("max-size-time",          SEG_DURATION_MS * Gst.MSECOND)
    split_vid.set_property("muxer",                  cmaf_vid)
    split_vid.set_property("send-keyframe-requests", True)
    # reset-muxer=True (défaut) : chaque segment reçoit ftyp+moov
    # → strippé par ManifestWriter._latest_segments()

    # ── Audio branch ──────────────────────────────────────────────────────
    aud_src   = make_element("tcpclientsrc", "aud_src")
    aud_q1    = make_element("queue",        "aud_q1")
    aud_demux = make_element("qtdemux",      "aud_demux")
    aud_q2    = make_element("queue",        "aud_q2")
    aacparse  = make_element("aacparse",     "aacparse")
    cmaf_aud  = make_element("cmafmux",      "cmaf_aud")
    split_aud = make_element("splitmuxsink", "split_aud")

    aud_src.set_property("host", AUDIO_HOST)
    aud_src.set_property("port", AUDIO_PORT)

    cmaf_aud.set_property("chunk-duration",    FRAG_DURATION_MS)
    cmaf_aud.set_property("fragment-duration", SEG_DURATION_MS)

    split_aud.set_property("location",               str(SEG_AUDIO_DIR / "seg%05d.m4s"))
    split_aud.set_property("max-size-time",          SEG_DURATION_MS * Gst.MSECOND)
    split_aud.set_property("muxer",                  cmaf_aud)
    split_aud.set_property("send-keyframe-requests", True)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in [
        vid_src, vid_q1, vid_demux, vid_q2, h264parse, split_vid,
        aud_src, aud_q1, aud_demux, aud_q2, aacparse, split_aud,
    ]:
        pipeline.add(el)

    # ── Static links ──────────────────────────────────────────────────────
    for src, dst, label in [
        (vid_src,  vid_q1,   "vid_src → vid_q1"),
        (vid_q1,   vid_demux,"vid_q1 → vid_demux"),
        (vid_q2,   h264parse,"vid_q2 → h264parse"),
        (aud_src,  aud_q1,   "aud_src → aud_q1"),
        (aud_q1,   aud_demux,"aud_q1 → aud_demux"),
        (aud_q2,   aacparse, "aud_q2 → aacparse"),
    ]:
        if not src.link(dst):
            raise RuntimeError(f"Failed to link {label}")

    # ── qtdemux pad-added (dynamic) ───────────────────────────────────────
    def on_demux_pad_added(demux, src_pad, sink_queue):
        caps = src_pad.get_current_caps() or src_pad.query_caps(None)
        struct_name = caps.get_structure(0).get_name() if caps else "unknown"
        log.info("New pad '%s' on %s (%s)",
                 src_pad.get_name(), demux.get_name(), struct_name)
        sink_pad = sink_queue.get_static_pad("sink")
        if sink_pad.is_linked():
            return
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            log.error("Pad link failed: %s → %s (%s)",
                      src_pad.get_name(), sink_pad.get_name(), ret)

    vid_demux.connect("pad-added", on_demux_pad_added, vid_q2)
    aud_demux.connect("pad-added", on_demux_pad_added, aud_q2)

    # ── parse → splitmuxsink ──────────────────────────────────────────────
    def link_to_splitmux(src_el: Gst.Element, splitmux: Gst.Element,
                         pad_name: str, label: str) -> None:
        src_pad  = src_el.get_static_pad("src")
        sink_pad = splitmux.request_pad_simple(pad_name)
        if sink_pad is None:
            tmpl = splitmux.get_pad_template(pad_name)
            if tmpl is None:
                templates = splitmux.get_pad_template_list()
                log.warning("Pad '%s' not found. Available: %s",
                            pad_name, [t.name_template for t in templates])
                raise RuntimeError(
                    f"splitmuxsink did not provide pad '{pad_name}' for {label}"
                )
            sink_pad = splitmux.request_pad(tmpl, None, None)
        if sink_pad is None:
            raise RuntimeError(
                f"splitmuxsink did not provide pad '{pad_name}' for {label}"
            )
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            raise RuntimeError(f"Failed to link {label}: {ret}")
        log.info("Linked %s", label)

    # Passer en READY pour initialiser les pads request de splitmuxsink
    ret = pipeline.set_state(Gst.State.READY)
    if ret == Gst.StateChangeReturn.FAILURE:
        raise RuntimeError("Failed to set pipeline to READY")
    pipeline.get_state(timeout=Gst.SECOND * 5)

    link_to_splitmux(h264parse, split_vid, "video",    "h264parse → split_vid")
    link_to_splitmux(aacparse,  split_aud, "audio_%u", "aacparse  → split_aud")
    
    def on_format_location(splitmux, fragment_id, seg_dir: Path):
        """
        Appelé par splitmuxsink AVANT d'ouvrir le nouveau fichier.
        On en profite pour stripper le segment PRÉCÉDENT qui vient d'être fermé.
        """
        prev_id = fragment_id - 1
        if prev_id >= 0:
            prev_path = seg_dir / f"seg{prev_id:05d}.m4s"
            init_path = seg_dir / "init.mp4"
            if prev_path.exists() and prev_path.stat().st_size > 0:
                strip_init_boxes(prev_path, init_path)
        # Retourner None = utiliser le location par défaut
        return None
    def make_format_location_cb(seg_dir: Path):
    init_path = seg_dir / "init.mp4"

    def on_format_location(splitmux, fragment_id):
        prev_id = fragment_id - 1
        if prev_id >= 0:
            prev_path = seg_dir / f"seg{prev_id:05d}.m4s"
            if prev_path.exists() and prev_path.stat().st_size > 0:
                stripped = strip_init_boxes(prev_path, init_path)
                if stripped:
                    log.info("[%s] Stripped seg%05d → init OK",
                             seg_dir.parent.name, prev_id)
                else:
                    log.warning("[%s] seg%05d NOT stripped (already clean or error)",
                                seg_dir.parent.name, prev_id)
        return str(seg_dir / f"seg{fragment_id:05d}.m4s")

    return on_format_location
    
    #split_vid.connect("format-location", on_format_location, SEG_VIDEO_DIR)
    #split_aud.connect("format-location", on_format_location, SEG_AUDIO_DIR)
    
    split_vid.connect("format-location", make_format_location_cb(SEG_VIDEO_DIR))
    split_aud.connect("format-location", make_format_location_cb(SEG_AUDIO_DIR))

    return pipeline


# ---------------------------------------------------------------------------
# Async streamer
# ---------------------------------------------------------------------------

class MultiFormatStreamer:
    def __init__(self):
        self.pipeline : Gst.Pipeline | None = None
        self._loop    : asyncio.AbstractEventLoop | None = None
        self._stop_event      = asyncio.Event()
        self._manifest_writer = ManifestWriter()

    async def start(self):
        self._loop = asyncio.get_running_loop()

        log.info("Building CMAF multi-format pipeline…")
        log.info("  Video : tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio : tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output: %s", OUTPUT_DIR)
        log.info("  Segment: %d ms  |  Fragment: %d ms",
                 SEG_DURATION_MS, FRAG_DURATION_MS)

        self.pipeline = build_pipeline()
        self._manifest_writer.start()

        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Failed to set pipeline to PLAYING")

        log.info("Pipeline PLAYING — MPD: %s  M3U8: %s", MPD_FILE, M3U8_MASTER)
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