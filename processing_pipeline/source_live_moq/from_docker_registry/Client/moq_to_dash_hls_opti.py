#!/usr/bin/env python3
"""
Low-latency CMAF pipeline : fMP4/TCP → DASH + HLS

Merge de :
  - opti_6 : pipeline splitmuxsink+cmafmux (anti-freeze, vrai CMAF)
  - opti_7 : ManifestWriter avec ready-sets + MPD dynamic + ffprobe HLS

Architecture :
  tcpclientsrc(video) → qtdemux → h264parse → capsfilter ─┐
                                                            ├→ splitmuxsink(cmafmux) → seg*.m4s
  tcpclientsrc(audio) → qtdemux → aacparse  → capsfilter ─┘

  ManifestWriter : ne publie que les segments confirmés via ready-sets
                   stream.mpd (dynamic SegmentList)
                   hls/video.m3u8 + hls/audio.m3u8 + master.m3u8

Env vars :
  VIDEO_HOST / VIDEO_PORT       (default: receiver / 5565)
  AUDIO_HOST / AUDIO_PORT       (default: receiver / 5566)
  OUTPUT_DIR                    (default: /var/cache/nginx)
  SEGMENT_DURATION_MS           (default: 2000)
  FRAGMENT_DURATION_MS          (default: 500)
  MANIFEST_REFRESH_S            (default: 0.5)
  WINDOW_SEGMENTS               (default: 6)
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
import time 
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
#MANIFEST_REFRESH_S = float(cfg("MANIFEST_REFRESH_S", "0.5"))
#MANIFEST_REFRESH_S = float(cfg("MANIFEST_REFRESH_S", str(SEG_DURATION_MS / 1000)))
MANIFEST_REFRESH_S=1.0
WINDOW_SEGMENTS    = int(cfg("WINDOW_SEGMENTS",      "15"))

SEG_DURATION_S = SEG_DURATION_MS / 1000
FRAGS_PER_SEG  = max(1, SEG_DURATION_MS // FRAG_DURATION_MS)

SEG_DIR_VIDEO   = OUTPUT_DIR / "segments" / "video"
SEG_DIR_AUDIO   = OUTPUT_DIR / "segments" / "audio"
INIT_FILE_VIDEO = SEG_DIR_VIDEO / "init.mp4"
INIT_FILE_AUDIO = SEG_DIR_AUDIO / "init.mp4"
HLS_VIDEO_M3U8  = OUTPUT_DIR / "hls" / "video.m3u8"
HLS_AUDIO_M3U8  = OUTPUT_DIR / "hls" / "audio.m3u8"
MPD_FILE        = OUTPUT_DIR / "stream.mpd"
M3U8_MASTER     = OUTPUT_DIR / "master.m3u8"

AST_FILE = OUTPUT_DIR / ".ast_locked"

# Video encoding parameters
VIDEO_WIDTH      = int(cfg("VIDEO_WIDTH",      "1280"))
VIDEO_HEIGHT     = int(cfg("VIDEO_HEIGHT",     "720"))
VIDEO_FRAMERATE  = int(cfg("VIDEO_FRAMERATE",  "30"))   # fps entier
AUDIO_RATE       = int(cfg("AUDIO_RATE",       "48000"))
AUDIO_CHANNELS   = int(cfg("AUDIO_CHANNELS",   "2"))

# Timescales dérivées (ne pas toucher)
VIDEO_TIMESCALE  = 90000                                 # standard RTP/MPEG, fixe
AUDIO_TIMESCALE  = AUDIO_RATE                            # 1 unité = 1 sample


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
# Manifest writer (v7 : ready-sets + dynamic MPD + ffprobe HLS)
# ---------------------------------------------------------------------------

class ManifestWriter:
    def __init__(self,
                 ready_video: set[int],
                 ready_audio: set[int],
                 ready_lock:  threading.Lock,
                 stream_start_epoch_ref) -> None:
        self._ready_video             = ready_video
        self._ready_audio             = ready_audio
        self._ready_lock              = ready_lock
        self._stop                    = threading.Event()
        self._availability_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._stream_start_epoch   = stream_start_epoch_ref

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
                # Snapshot atomique : un seul appel par piste par cycle
                # pour éviter les effets de bord (suppression + ready-set)
                # entre _write_mpd et _write_hls.
                segs_v, segs_a = self._get_windows()
                if segs_v and segs_a:
                    self._write_mpd(segs_v, segs_a)
                    self._write_hls(segs_v, segs_a)
            except Exception as exc:
                log.warning("ManifestWriter error: %s", exc)
        log.info("ManifestWriter stopped.")

    def _get_windows(self) -> tuple[list[Path], list[Path]]:
        with self._ready_lock:
            snap_v = set(self._ready_video)
            snap_a = set(self._ready_audio)

        segs_v = self._latest_segments_for(SEG_DIR_VIDEO, snap_v, self._ready_video)
        segs_a = self._latest_segments_for(SEG_DIR_AUDIO, snap_a, self._ready_audio)

        # Les deux splitmuxsink ont des compteurs indépendants —
        # on aligne uniquement sur le nombre de segments disponibles
        n = min(len(segs_v), len(segs_a), WINDOW_SEGMENTS)
        if n == 0:
            return [], []

        return segs_v[-n:], segs_a[-n:]

    def _seg_number(self, p: Path) -> int:
        return int(p.stem.replace("seg", ""))

    def _latest_segments_for(self,
                              seg_dir:    Path,
                              snap:       set[int],
                              live_ready: set[int]) -> list[Path]:
        """
        Retourne la fenêtre glissante des segments confirmés.

        - snap       : copie (thread-safe) du ready-set au moment du snapshot
        - live_ready : set partagé avec le streamer, utilisé UNIQUEMENT pour
                       discard() lors du nettoyage
        """
        if not (seg_dir / "init.mp4").exists():
            return []

        all_files = sorted(
            f for f in seg_dir.glob("seg*.m4s")
            if self._seg_number(f) in snap
        )

        window = all_files[-WINDOW_SEGMENTS:]

        # Nettoyage hors fenêtre uniquement quand elle est pleine
        KEEP_ON_DISK = WINDOW_SEGMENTS + 6   # 6 segments de garde = 12s de marge
        window = all_files[-WINDOW_SEGMENTS:]
        if len(all_files) > KEEP_ON_DISK:
            for old in all_files[:-KEEP_ON_DISK]:
                try:
                    old.unlink()
                except OSError:
                    pass
                with self._ready_lock:
                    live_ready.discard(self._seg_number(old))

        log.debug("%s: snap=%d window=%d [%s..%s]",
                  seg_dir.name, len(snap), len(window),
                  window[0].name if window else "-",
                  window[-1].name if window else "-")

        return window

    # ── BMFF helpers ──────────────────────────────────────────────────────

    def _read_timescale(self, init_file: Path) -> int:
        """Lit la timescale depuis init.mp4 (moov → trak → mdia → mdhd)."""
        try:
            data = init_file.read_bytes()
            pos = 0
            while pos + 8 <= len(data):
                size  = int.from_bytes(data[pos:pos+4], 'big')
                btype = data[pos+4:pos+8]
                if size < 8:
                    break
                if btype == b'moov':
                    inner    = pos + 8
                    moov_end = pos + size
                    while inner + 8 <= moov_end:
                        isize = int.from_bytes(data[inner:inner+4], 'big')
                        itype = data[inner+4:inner+8]
                        if itype == b'trak':
                            trak_inner = inner + 8
                            trak_end   = inner + isize
                            while trak_inner + 8 <= trak_end:
                                tsize = int.from_bytes(data[trak_inner:trak_inner+4], 'big')
                                ttype = data[trak_inner+4:trak_inner+8]
                                if ttype == b'mdia':
                                    mdia_inner = trak_inner + 8
                                    mdia_end   = trak_inner + tsize
                                    while mdia_inner + 8 <= mdia_end:
                                        msize = int.from_bytes(data[mdia_inner:mdia_inner+4], 'big')
                                        mtype = data[mdia_inner+4:mdia_inner+8]
                                        if mtype == b'mdhd':
                                            version = data[mdia_inner+8]
                                            if version == 1:
                                                return int.from_bytes(
                                                    data[mdia_inner+28:mdia_inner+32], 'big')
                                            else:
                                                return int.from_bytes(
                                                    data[mdia_inner+20:mdia_inner+24], 'big')
                                        mdia_inner += msize
                                trak_inner += tsize
                        inner += isize
                pos += size
        except Exception as e:
            log.warning("_read_timescale(%s): %s", init_file.name, e)
        return 1000

    def _read_segment_pts(self, seg_path: Path) -> int | None:
        """
        Lit le baseMediaDecodeTime du premier tfdt dans le segment.
        Parcourt récursivement toutes les boxes conteneurs (moof, traf, ...)
        pour être robuste aux variantes cmafmux / qtmux.
        """
        def scan(data: bytes, start: int, end: int) -> int | None:
            pos = start
            while pos + 8 <= end:
                size  = int.from_bytes(data[pos:pos+4], 'big')
                btype = data[pos+4:pos+8]
                if size < 8:
                    break
                box_end = pos + size
                if btype == b'tfdt':
                    version = data[pos+8]
                    if version == 1:
                        return int.from_bytes(data[pos+12:pos+20], 'big')
                    else:
                        return int.from_bytes(data[pos+12:pos+16], 'big')
                # Boxes conteneurs connues : descendre dedans
                if btype in (b'moof', b'traf', b'mfhd', b'tfhd', b'trun'):
                    result = scan(data, pos + 8, box_end)
                    if result is not None:
                        return result
                pos += size
            return None

        try:
            data   = seg_path.read_bytes()
            result = scan(data, 0, len(data))
            if result is None:
                # Debug : lister les box types de premier niveau pour aider au diagnostic
                boxes, pos = [], 0
                while pos + 8 <= min(len(data), 512):
                    s = int.from_bytes(data[pos:pos+4], 'big')
                    t = data[pos+4:pos+8].decode(errors='replace')
                    boxes.append(f'{t}({s})')
                    if s < 8: break
                    pos += s
                log.debug("_read_segment_pts(%s): tfdt non trouvé — boxes: %s",
                          seg_path.name, ' '.join(boxes))
            return result
        except Exception as e:
            log.warning("_read_segment_pts(%s): %s", seg_path.name, e)
        return None

    def _read_segment_duration_s(self, seg_path: Path) -> float:
        """
        Calcule la durée réelle du segment depuis les boxes trun (somme des
        sample_duration). Fallback sur SEG_DURATION_S si introuvable.
        """
        try:
            data = seg_path.read_bytes()

            # Cherche la timescale dans le moof/traf/tfhd ou depuis init
            # On utilise la timescale déjà connue via _read_timescale
            # Détermine la piste depuis le chemin
            if "video" in str(seg_path):
                timescale = self._read_timescale(INIT_FILE_VIDEO)
            else:
                timescale = self._read_timescale(INIT_FILE_AUDIO)

            total_duration = 0

            def scan(data: bytes, start: int, end: int) -> None:
                nonlocal total_duration
                pos = start
                while pos + 8 <= end:
                    size  = int.from_bytes(data[pos:pos+4], 'big')
                    btype = data[pos+4:pos+8]
                    if size < 8:
                        break
                    box_end = pos + size
                    if btype == b'trun':
                        version    = data[pos+8]
                        flags      = int.from_bytes(data[pos+9:pos+12], 'big')
                        has_dur    = bool(flags & 0x100)
                        sample_count = int.from_bytes(data[pos+12:pos+16], 'big')
                        offset     = pos + 16
                        # sauter data_offset si présent (flag 0x001)
                        if flags & 0x001:
                            offset += 4
                        # sauter first_sample_flags si présent (flag 0x004)
                        if flags & 0x004:
                            offset += 4
                        if has_dur:
                            for _ in range(sample_count):
                                if offset + 4 > len(data):
                                    break
                                total_duration += int.from_bytes(
                                    data[offset:offset+4], 'big')
                                # avancer selon les flags présents par sample
                                sample_size = 0
                                if flags & 0x200: sample_size += 4  # size
                                if flags & 0x400: sample_size += 4  # flags
                                if flags & 0x800: sample_size += 8  # cts
                                offset += 4 + sample_size
                    elif btype in (b'moof', b'traf'):
                        scan(data, pos + 8, box_end)
                    pos += size

            scan(data, 0, len(data))

            if total_duration > 0 and timescale > 0:
                return total_duration / timescale

        except Exception as e:
            log.warning("_read_segment_duration_s(%s): %s", seg_path.name, e)

        return SEG_DURATION_S

    # ── DASH MPD (dynamic) ────────────────────────────────────────────────

    def _write_mpd(self, segs_v: list[Path], segs_a: list[Path]) -> None:
        
        

        timescale_v   = self._read_timescale(INIT_FILE_VIDEO)
        timescale_a   = self._read_timescale(INIT_FILE_AUDIO)
        first_bmdt_v  = self._read_segment_pts(segs_v[0])
        first_bmdt_a  = self._read_segment_pts(segs_a[0])
        


        if first_bmdt_v is None:
            log.warning("MPD: tfdt introuvable dans %s — PTO forcé à 0", segs_v[0].name)
            first_bmdt_v = 0
        if first_bmdt_a is None:
            log.warning("MPD: tfdt introuvable dans %s — PTO forcé à 0", segs_a[0].name)
            first_bmdt_a = 0

        pto_s             = first_bmdt_v / timescale_v
        seg_duration_ts_v = int(SEG_DURATION_S * timescale_v)
        seg_duration_ts_a = int(SEG_DURATION_S * timescale_a)
        window_s          = len(segs_v) * SEG_DURATION_S
        pto_a_aligned     = int(pto_s * timescale_a)
        
        timescale_v = 90000
        seg_duration_ts_v = 180000
        timescale_a = 48000
        seg_duration_ts_a = 96000

        # AST figé une seule fois au premier appel
        # if not hasattr(self, '_ast_locked'):
        #     first_seg_num = self._seg_number(segs_v[0])
        #     ast_epoch     = datetime.now(timezone.utc).timestamp() \
        #                     - (first_seg_num * SEG_DURATION_S) \
        #                     - SEG_DURATION_S
        #     self._ast_locked   = ast_epoch
        #     self._ast_str      = datetime.fromtimestamp(
        #         ast_epoch, tz=timezone.utc
        #     ).strftime("%Y-%m-%dT%H:%M:%SZ")
        #     log.info("AST figé à %s (seg0=%d)", self._ast_str, first_seg_num)

        # startNumber = numéro réel du premier segment de la fenêtre
        # AST étant fixe, dash.js calcule correctement :
        #   N = floor((now - AST - suggestedPresentationDelay) / SEG_DURATION_S)
        # qui doit tomber dans [startNumber_v .. startNumber_v + WINDOW_SEGMENTS]
        start_v = self._seg_number(segs_v[0])
        start_a = self._seg_number(segs_a[0])
        
        last_v    = self._seg_number(segs_v[-1])
        now_s     = datetime.now(timezone.utc).timestamp()
        spd_s     = SEG_DURATION_S * 2
        ast_epoch = now_s - spd_s - (last_v - 1) * SEG_DURATION_S

        
        #ast_epoch = now_s - (last_v * SEG_DURATION_S) - SEG_DURATION_S
        self._ast_str   = datetime.fromtimestamp(ast_epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        def make_seg_list(segs, init_file, timescale, duration_ts, pto, start_number):
            init_rel    = os.path.relpath(init_file, OUTPUT_DIR)
            seg_rel_dir = os.path.relpath(segs[0].parent, OUTPUT_DIR)
            return (
                f'        <SegmentTemplate timescale="{timescale}"\n'
                f'                         duration="{duration_ts}"\n'
                f'                         startNumber="{start_number}"\n'
                f'                         initialization="{init_rel}"\n'
                f'                         media="{seg_rel_dir}/seg$Number%05d$.m4s"/>\n'
            )
            # f'                         presentationTimeOffset="{pto}"\n'


        mpd = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"\n'
            '     profiles="urn:mpeg:dash:profile:isoff-live:2011"\n'
            '     type="dynamic"\n'
            f'     availabilityStartTime="{self._ast_str}"\n'
            f'     minimumUpdatePeriod="PT{SEG_DURATION_S:.1f}S"\n'
            f'     timeShiftBufferDepth="PT{window_s:.1f}S"\n'
            f'     suggestedPresentationDelay="PT{SEG_DURATION_S * 3:.1f}S"\n'
            f'     minBufferTime="PT{SEG_DURATION_S:.1f}S">\n'
            '  <UTCTiming schemeIdUri="urn:mpeg:dash:utc:http-xsdate:2014"\n'
            '             value="http://time.akamai.com/?iso"/>\n'
            '  <Period id="1" start="PT0S">\n'
            '    <AdaptationSet id="1" mimeType="video/mp4" codecs="avc1.42c01f"\n'
            f'             frameRate="{VIDEO_FRAMERATE}" segmentAlignment="true" startWithSAP="1">\n'
            f'      <Representation id="video" bandwidth="2000000" width="{VIDEO_WIDTH}" height="{VIDEO_HEIGHT}">\n'
            + make_seg_list(segs_v, INIT_FILE_VIDEO, timescale_v,
                            seg_duration_ts_v, first_bmdt_v, start_v) +
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '    <AdaptationSet id="2" mimeType="audio/mp4" codecs="mp4a.40.2"\n'
            '    lang="fr" segmentAlignment="true">\n'
            f'      <Representation id="audio" bandwidth="128000" audioSamplingRate="{AUDIO_RATE}">\n'
            '        <AudioChannelConfiguration\n'
            '            schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011"\n'
            '            value="2"/>\n'
            + make_seg_list(segs_a, INIT_FILE_AUDIO, timescale_a,
                            seg_duration_ts_a, pto_a_aligned, start_a) +
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '  </Period>\n'
            '</MPD>\n'
        )
        tmp = MPD_FILE.with_suffix(".tmp")
        tmp.write_text(mpd)
        tmp.replace(MPD_FILE)
        log.info("MPD  ast=%s  pto=%.1fs  start_v=%d  start_a=%d  v=%d segs  a=%d segs",
                self._ast_str, pto_s, start_v, start_a, len(segs_v), len(segs_a))
    
    # ── HLS (ffprobe duration) ────────────────────────────────────────────

    def _write_hls(self, segs_v: list[Path], segs_a: list[Path]) -> None:

        seq_v  = self._seg_number(segs_v[0])
        seq_a  = self._seg_number(segs_a[0])
        base   = HLS_VIDEO_M3U8.parent
        init_v = os.path.relpath(INIT_FILE_VIDEO, base)
        init_a = os.path.relpath(INIT_FILE_AUDIO, base)

        max_dur_v = max(
            (self._read_segment_duration_s(s) for s in segs_v),
            default=SEG_DURATION_S,
        )
        max_dur_a = max(
            (self._read_segment_duration_s(s) for s in segs_a),
            default=SEG_DURATION_S,
        )

        # ── Video playlist ────────────────────────────────────────────────
        lines_v = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(max_dur_v) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq_v}",
            f'#EXT-X-MAP:URI="{init_v}"',
        ]
        for seg in segs_v:
            dur = self._read_segment_duration_s(seg)
            lines_v.append(f"#EXTINF:{dur:.6f},")
            lines_v.append(os.path.relpath(seg, base))

        tmp = HLS_VIDEO_M3U8.with_suffix(".tmp")
        tmp.write_text("\n".join(lines_v) + "\n")
        tmp.replace(HLS_VIDEO_M3U8)

        # ── Audio playlist ────────────────────────────────────────────────
        lines_a = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(max_dur_a) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq_a}",
            f'#EXT-X-MAP:URI="{init_a}"',
        ]
        for seg in segs_a:
            dur = self._read_segment_duration_s(seg)
            lines_a.append(f"#EXTINF:{dur:.6f},")
            lines_a.append(os.path.relpath(seg, base))

        tmp = HLS_AUDIO_M3U8.with_suffix(".tmp")
        tmp.write_text("\n".join(lines_a) + "\n")
        tmp.replace(HLS_AUDIO_M3U8)

        # ── Master playlist ───────────────────────────────────────────────
        vid_rel = os.path.relpath(HLS_VIDEO_M3U8, OUTPUT_DIR)
        aud_rel = os.path.relpath(HLS_AUDIO_M3U8, OUTPUT_DIR)

        master = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            "#EXT-X-MEDIA:TYPE=AUDIO,"
            'GROUP-ID="audio",'
            'NAME="fr",'
            "DEFAULT=YES,"
            "AUTOSELECT=YES,"
            'LANGUAGE="fr",'
            f'URI="{aud_rel}"\n'
            "\n"
            "#EXT-X-STREAM-INF:"
            "BANDWIDTH=2128000,"
            'CODECS="avc1.42c01f,mp4a.40.2",'
            'AUDIO="audio"\n'
            f"{vid_rel}\n"
        )
        tmp = M3U8_MASTER.with_suffix(".tmp")
        tmp.write_text(master)
        tmp.replace(M3U8_MASTER)

        log.info("HLS  seq_v=%d seq_a=%d  v=%d segs  a=%d segs",
                 seq_v, seq_a, len(segs_v), len(segs_a))


# ---------------------------------------------------------------------------
# GStreamer pipeline (v6 : splitmuxsink + cmafmux, anti-freeze)
# ---------------------------------------------------------------------------

def build_pipeline(on_segment_written) -> Gst.Pipeline:
    Gst.init([])
    makedirs(SEG_DIR_VIDEO, SEG_DIR_AUDIO, OUTPUT_DIR / "hls")

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
    # vid_caps.set_property("caps", Gst.Caps.from_string(
    #     "video/x-h264,stream-format=avc,alignment=au"
    # ))
    
    vid_caps.set_property("caps", Gst.Caps.from_string(
    f"video/x-h264,stream-format=avc,alignment=au,"
    f"width={VIDEO_WIDTH},height={VIDEO_HEIGHT},"
    f"framerate={VIDEO_FRAMERATE}/1"
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
    # aud_caps.set_property("caps", Gst.Caps.from_string(
    #     "audio/mpeg,mpegversion=4,stream-format=raw"
    # ))
    aud_caps.set_property("caps", Gst.Caps.from_string(
    f"audio/mpeg,mpegversion=4,stream-format=raw,"
    f"rate={AUDIO_RATE},channels={AUDIO_CHANNELS}"
    ))
    

    # ── splitmuxsink vidéo (cmafmux vidéo seule) ──────────────────────────
    cmafmux_v  = make_element("cmafmux",      "cmafmux_v")
    splitmux_v = make_element("splitmuxsink", "splitmux_v")

    cmafmux_v.set_property("chunk-duration",    FRAG_DURATION_MS * Gst.MSECOND)
    #cmafmux_v.set_property("fragment-duration", SEG_DURATION_MS  * Gst.MSECOND)
    cmafmux_v.set_property("fragment-duration", 0)  # Laisser splitmux gérer la durée des fragments pour éviter les segments courts en fin de segment

    splitmux_v.set_property("muxer",                  cmafmux_v)
    splitmux_v.set_property("max-size-time",          SEG_DURATION_MS * Gst.MSECOND)
    splitmux_v.set_property("send-keyframe-requests", True)
    splitmux_v.set_property("reset-muxer",            True)
    splitmux_v.set_property("async-finalize",         False)

    def on_format_location_video(splitmux, fragment_id, first_sample):
        path = str(SEG_DIR_VIDEO / f"seg{fragment_id:05d}.m4s")
        log.info("Video segment → seg%05d.m4s (nouveau)", fragment_id)
        on_segment_written("video", fragment_id, Path(path))
        return path

    splitmux_v.connect("format-location-full", on_format_location_video)

    # ── splitmuxsink audio (cmafmux audio seul) ───────────────────────────
    cmafmux_a  = make_element("cmafmux",      "cmafmux_a")
    splitmux_a = make_element("splitmuxsink", "splitmux_a")

    cmafmux_a.set_property("chunk-duration",    FRAG_DURATION_MS * Gst.MSECOND)
    #cmafmux_a.set_property("fragment-duration", SEG_DURATION_MS  * Gst.MSECOND)
    cmafmux_a.set_property("fragment-duration", 0)
    
    splitmux_a.set_property("muxer",                  cmafmux_a)
    splitmux_a.set_property("max-size-time",          SEG_DURATION_MS * Gst.MSECOND)
    splitmux_a.set_property("send-keyframe-requests", False)
    splitmux_a.set_property("reset-muxer",            True)
    splitmux_a.set_property("async-finalize",         False)

    def on_format_location_audio(splitmux, fragment_id, first_sample):
        path = str(SEG_DIR_AUDIO / f"seg{fragment_id:05d}.m4s")
        log.info("Audio segment → seg%05d.m4s (nouveau)", fragment_id)
        on_segment_written("audio", fragment_id, Path(path))
        return path

    splitmux_a.connect("format-location-full", on_format_location_audio)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in [
        vid_src, vid_q1, vid_demux, vid_q2, h264parse, vid_caps,
        aud_src, aud_q1, aud_demux, aud_q2, aacparse, aud_caps,
        splitmux_v, splitmux_a,
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
    
    def set_timescales(pipeline):
        
            #test  fix
        cmafmux_v.set_property("movie-timescale", 90000)
        cmafmux_v.set_property("fragment-duration", 2000000) 
        cmafmux_a.set_property("movie-timescale", 48000)
        cmafmux_a.set_property("fragment-duration", 2000000)
    
        # Accès au pad sink de cmafmux_v
        pad_v = cmafmux_v.get_static_pad("sink")
        if pad_v:
            pad_v.set_property("trak-timescale", 90000)
        else:
            # splitmuxsink crée un pad "video" ou "sink"
            pad_v = cmafmux_v.get_static_pad("video")
            if pad_v:
                pad_v.set_property("trak-timescale", 90000)

        pad_a = cmafmux_a.get_static_pad("sink")
        if pad_a:
            pad_a.set_property("trak-timescale", 48000)

    # ── READY → request pads ─────────────────────────────────────────────
    ret = pipeline.set_state(Gst.State.READY)
    if ret == Gst.StateChangeReturn.FAILURE:
        raise RuntimeError("Pipeline failed to go to READY")
    
    set_timescales(pipeline)


    pipeline.get_state(timeout=Gst.SECOND * 5)

    def link_to_splitmux(src_el, splitmux, pad_name, label):
        src_pad  = src_el.get_static_pad("src")
        sink_pad = splitmux.request_pad_simple(pad_name)
        if sink_pad is None:
            raise RuntimeError(f"pad '{pad_name}' indisponible pour {label}")
        ret = src_pad.link(sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            raise RuntimeError(f"Failed to link {label}: {ret}")
        log.info("Linked %s", label)

    link_to_splitmux(vid_caps, splitmux_v, "video",    "vid_caps → splitmux_v")
    link_to_splitmux(aud_caps, splitmux_a, "audio_%u", "aud_caps → splitmux_a")

    return pipeline


# ---------------------------------------------------------------------------
# Async streamer
# ---------------------------------------------------------------------------

class MultiFormatStreamer:
    """
    Gère le cycle de vie du pipeline et synchronise les ready-sets.

    Sémantique de format-location-full :
      Le signal est émis AVANT d'écrire le segment fragment_id,
      donc le segment (fragment_id - 1) vient d'être FINALISÉ.

    On ajoute fragment_id-1 au ready-set uniquement quand fragment_id ≥ 1,
    ce qui garantit que seuls les segments totalement écrits sont publiés.
    """

    def __init__(self):
        self.pipeline              : Gst.Pipeline | None = None
        self._loop                 : asyncio.AbstractEventLoop | None = None
        self._stop_event           = asyncio.Event()
        self._ready_video          : set[int] = set()
        self._ready_audio          : set[int] = set()
        self._ready_lock           = threading.Lock()   # protège les deux sets
        self._init_video_extracted = False
        self._init_audio_extracted = False
        self._stream_start_epoch : float | None = None
        self._manifest_writer      = ManifestWriter(
            self._ready_video,
            self._ready_audio,
            self._ready_lock,
            stream_start_epoch_ref=lambda: self._stream_start_epoch
        )

    async def start(self):
        self._loop = asyncio.get_running_loop()

        log.info("Building low-latency CMAF pipeline…")
        log.info("  Video  : tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio  : tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output : %s", OUTPUT_DIR)
        log.info("  Seg %d ms  Frag %d ms  (%d frags/seg)",
                 SEG_DURATION_MS, FRAG_DURATION_MS, FRAGS_PER_SEG)
        log.info("  Video  : %dx%d @ %d fps", VIDEO_WIDTH, VIDEO_HEIGHT, VIDEO_FRAMERATE)
        log.info("  Audio  : %d Hz  %d ch", AUDIO_RATE, AUDIO_CHANNELS)

        self.pipeline = build_pipeline(self._on_segment_written)
        self._manifest_writer.start()

        glib_loop = GLib.MainLoop()
        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message, glib_loop)

        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            raise RuntimeError("Pipeline failed to go to PLAYING")
        Gst.debug_bin_to_dot_file(
            self.pipeline, Gst.DebugGraphDetails.ALL, "after_cmaf_multi"
        )

        log.info("Pipeline PLAYING  MPD=%s  M3U8=%s", MPD_FILE, M3U8_MASTER)
        await asyncio.get_running_loop().run_in_executor(None, glib_loop.run)

    def _on_bus_message(self, bus, message, glib_loop: GLib.MainLoop):
        t = message.type
        if t == Gst.MessageType.EOS:
            log.info("EOS received.")
            glib_loop.quit()
            if self._loop:
                self._loop.call_soon_threadsafe(self._stop_event.set)
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            log.error("GStreamer error: %s", err.message)
            if debug:
                log.debug("Debug: %s", debug)
            glib_loop.quit()
            if self._loop:
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

    def _on_segment_written(self, track: str, fragment_id: int, path: Path) -> None:
        if fragment_id == 0:
            return

        prev_id   = fragment_id - 1
        prev_path = (SEG_DIR_VIDEO if track == "video" else SEG_DIR_AUDIO) / f"seg{prev_id:05d}.m4s"

        # Extraction init au passage fragment_id=1 (seg0 vient d'être finalisé)
        if prev_id == 0:
            if track == "video" and not self._init_video_extracted:
                self._extract_init(prev_path, INIT_FILE_VIDEO)
                self._init_video_extracted = True
            elif track == "audio" and not self._init_audio_extracted:
                self._extract_init(prev_path, INIT_FILE_AUDIO)
                self._init_audio_extracted = True

        # Confirmation différée : on attend que le fichier soit stable sur disque
        threading.Thread(
            target=self._confirm_and_add_ready,
            args=(track, prev_id, prev_path),
            daemon=True,
            name=f"confirm-{track}-{prev_id}",
        ).start()
        
    def _confirm_and_add_ready(self, track: str, seg_id: int, path: Path,
                            retries: int = 10) -> None:
        """
        Attend que le segment soit entièrement écrit sur disque
        (taille stable sur deux mesures consécutives) avant de l'ajouter
        au ready-set. Évite d'exposer un .m4s tronqué au ManifestWriter.
        """
        for attempt in range(retries):
            time.sleep(0.08)
            if not path.exists():
                continue
            try:
                size1 = path.stat().st_size
            except OSError:
                continue
            time.sleep(0.05)
            try:
                size2 = path.stat().st_size
            except OSError:
                continue
            if size1 == size2 and size1 > 0:
                break
            log.debug("confirm %s seg%05d attempt %d (size %d→%d)",
                    track, seg_id, attempt, size1, size2)
        else:
            log.warning("confirm %s seg%05d: timeout après %d tentatives — ignoré",
                        track, seg_id, retries)
            return

        with self._ready_lock:
            if track == "video":
                self._ready_video.add(seg_id)
                log.debug("Ready video +%d  total=%d", seg_id, len(self._ready_video))
                if seg_id == 0 and self._stream_start_epoch is None:
                    self._stream_start_epoch = time.time()
                    log.info("stream_start_epoch=%.3f (seg0 confirmé)", self._stream_start_epoch)
            elif track == "audio":
                self._ready_audio.add(seg_id)
                log.debug("Ready audio +%d  total=%d", seg_id, len(self._ready_audio))

    def _extract_init(self, seg0: Path, dest: Path) -> None:
        """Extrait ftyp+moov de seg00000.m4s vers init.mp4."""
        if not seg0.exists():
            log.warning("_extract_init: %s not found", seg0)
            return
        data = seg0.read_bytes()
        pos, init_buf = 0, bytearray()
        while pos + 8 <= len(data):
            size  = int.from_bytes(data[pos:pos+4], 'big')
            btype = data[pos+4:pos+8]
            if size < 8:
                break
            if btype in (b'ftyp', b'moov'):
                init_buf += data[pos:pos+size]
            pos += size
        if init_buf:
            dest.write_bytes(bytes(init_buf))
            log.info("Init extrait → %s (%d bytes)", dest.name, len(init_buf))
        else:
            log.warning("_extract_init: no ftyp/moov found in %s", seg0.name)


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