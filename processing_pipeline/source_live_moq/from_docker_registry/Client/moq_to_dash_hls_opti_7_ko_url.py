#!/usr/bin/env python3
"""
Low-latency CMAF pipeline : fMP4/TCP → DASH + HLS
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

import gi
import time
gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import GLib, Gst

logging.basicConfig(
    level=logging.DEBUG,
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
MANIFEST_REFRESH_S = float(cfg("MANIFEST_REFRESH_S", "0.5"))
WINDOW_SEGMENTS    = int(cfg("WINDOW_SEGMENTS",      "6"))

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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def makedirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)

def make_element(factory: str, name: str) -> Gst.Element:
    el = Gst.ElementFactory.make(factory, name)
    if el is None:
        raise RuntimeError(f"Cannot create GStreamer element '{factory}'")
    return el

# ---------------------------------------------------------------------------
# Manifest writer
# ---------------------------------------------------------------------------

class ManifestWriter:
    def __init__(self,
                 ready_video: set[int],
                 ready_audio: set[int]) -> None:
        self._ready_video = ready_video
        self._ready_audio = ready_audio
        self._stop        = threading.Event()

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

    def _latest_segments_for(self,
                              seg_dir: Path,
                              ready: set[int]) -> list[Path]:
        if not (seg_dir / "init.mp4").exists():
            return []

        all_files = sorted(
            f for f in seg_dir.glob("seg*.m4s")
            if self._seg_number(f) in ready
        )

        # Exclure le dernier segment (peut encore être en cours de flush)
        if len(all_files) > 1:
            all_files = all_files[:-1]

        window = all_files[-WINDOW_SEGMENTS:]

        # Supprimer hors fenêtre seulement si fenêtre pleine
        if len(window) >= WINDOW_SEGMENTS:
            for old in all_files[:-WINDOW_SEGMENTS]:
                try:
                    old.unlink()
                    ready.discard(self._seg_number(old))
                except OSError:
                    pass

        log.debug("%s: ready=%d window=%d [%s..%s]",
                  seg_dir.name, len(ready), len(window),
                  window[0].name if window else "-",
                  window[-1].name if window else "-")

        return window

    def _read_timescale(self, init_file: Path) -> int:
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

    def _read_segment_duration_s(self, seg_path: Path, timescale: int) -> float:
        try:
            result = subprocess.run(
                ["ffprobe", "-v", "quiet",
                 "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1",
                 str(seg_path)],
                capture_output=True, text=True, timeout=2,
            )
            val = result.stdout.strip()
            if val and val != "N/A":
                return float(val)
        except Exception as e:
            log.warning("ffprobe duration(%s): %s", seg_path.name, e)
        return SEG_DURATION_S

    def _read_segment_pts_ms(self, seg_path: Path) -> int | None:
        try:
            data = seg_path.read_bytes()
            pos  = 0
            while pos + 8 <= len(data):
                size  = int.from_bytes(data[pos:pos+4], 'big')
                btype = data[pos+4:pos+8]
                if size < 8:
                    break
                if btype == b'moof':
                    moof_end = pos + size
                    inner    = pos + 8
                    while inner + 8 <= moof_end:
                        isize = int.from_bytes(data[inner:inner+4], 'big')
                        itype = data[inner+4:inner+8]
                        if itype == b'traf':
                            traf_end   = inner + isize
                            traf_inner = inner + 8
                            while traf_inner + 8 <= traf_end:
                                tsize = int.from_bytes(data[traf_inner:traf_inner+4], 'big')
                                ttype = data[traf_inner+4:traf_inner+8]
                                if ttype == b'tfdt':
                                    version = data[traf_inner+8]
                                    if version == 1:
                                        return int.from_bytes(
                                            data[traf_inner+12:traf_inner+20], 'big')
                                    else:
                                        return int.from_bytes(
                                            data[traf_inner+12:traf_inner+16], 'big')
                                traf_inner += tsize
                        inner += isize
                pos += size
        except Exception as e:
            log.warning("_read_segment_pts_ms(%s): %s", seg_path.name, e)
        return None

    # ── DASH MPD ──────────────────────────────────────────────────────────

    def _write_mpd(self) -> None:
        segs_v = self._latest_segments_for(SEG_DIR_VIDEO, self._ready_video)
        segs_a = self._latest_segments_for(SEG_DIR_AUDIO, self._ready_audio)
        if not segs_v or not segs_a:
            return

        timescale_v = self._read_timescale(INIT_FILE_VIDEO)
        timescale_a = self._read_timescale(INIT_FILE_AUDIO)

        first_bmdt_v = self._read_segment_pts_ms(segs_v[0])
        first_bmdt_a = self._read_segment_pts_ms(segs_a[0])
        if first_bmdt_v is None or first_bmdt_a is None:
            return

        pto_s             = first_bmdt_v / timescale_v
        seg_duration_ts_v = int(SEG_DURATION_S * timescale_v)
        seg_duration_ts_a = int(SEG_DURATION_S * timescale_a)
        window_s          = len(segs_v) * SEG_DURATION_S

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        def make_seg_list(segs, init_file, timescale, duration_ts):
            init_rel = os.path.relpath(init_file, OUTPUT_DIR)
            entries  = "".join(
                f'          <SegmentURL media="{os.path.relpath(s, OUTPUT_DIR)}"/>\n'
                for s in segs
            )
            return (
                f'        <SegmentList timescale="{timescale}"\n'
                f'                     duration="{duration_ts}"\n'
                f'                     presentationTimeOffset="{first_bmdt_v}">\n'
                f'          <Initialization sourceURL="{init_rel}"/>\n'
                f'{entries}'
                f'        </SegmentList>\n'
            )

        mpd = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"\n'
            '     profiles="urn:mpeg:dash:profile:isoff-live:2011"\n'
            '     type="dynamic"\n'
            f'     availabilityStartTime="{now}"\n'
            f'     timeShiftBufferDepth="PT{window_s:.1f}S"\n'
            f'     suggestedPresentationDelay="PT{SEG_DURATION_S * 2:.1f}S"\n'
            f'     minBufferTime="PT{SEG_DURATION_S:.1f}S">\n'
            '  <Period id="1" start="PT0S">\n'
            '    <AdaptationSet id="1" mimeType="video/mp4" codecs="avc1.42c01f"\n'
            '                   frameRate="30" segmentAlignment="true" startWithSAP="1">\n'
            '      <Representation id="video" bandwidth="2000000" width="1280" height="720">\n'
            + make_seg_list(segs_v, INIT_FILE_VIDEO, timescale_v, seg_duration_ts_v) +
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '    <AdaptationSet id="2" mimeType="audio/mp4" codecs="mp4a.40.2"\n'
            '                   lang="fr" segmentAlignment="true">\n'
            '      <Representation id="audio" bandwidth="128000" audioSamplingRate="48000">\n'
            '        <AudioChannelConfiguration\n'
            '            schemeIdUri="urn:mpeg:dash:23003:3:audio_channel_configuration:2011"\n'
            '            value="2"/>\n'
            + make_seg_list(segs_a, INIT_FILE_AUDIO, timescale_a, seg_duration_ts_a) +
            '      </Representation>\n'
            '    </AdaptationSet>\n'
            '  </Period>\n'
            '</MPD>\n'
        )
        tmp = MPD_FILE.with_suffix(".tmp")
        tmp.write_text(mpd)
        tmp.replace(MPD_FILE)
        log.info("MPD  pto=%.1fs  v=%d segs  a=%d segs", pto_s, len(segs_v), len(segs_a))

    # ── HLS ───────────────────────────────────────────────────────────────

    def _write_hls(self) -> None:
        segs_v = self._latest_segments_for(SEG_DIR_VIDEO, self._ready_video)
        segs_a = self._latest_segments_for(SEG_DIR_AUDIO, self._ready_audio)
        if not segs_v or not segs_a:
            return

        ts_v  = self._read_timescale(INIT_FILE_VIDEO)
        ts_a  = self._read_timescale(INIT_FILE_AUDIO)
        seq_v = self._seg_number(segs_v[0])
        seq_a = self._seg_number(segs_a[0])
        base  = HLS_VIDEO_M3U8.parent
        init_v = os.path.relpath(INIT_FILE_VIDEO, base)
        init_a = os.path.relpath(INIT_FILE_AUDIO, base)

        max_dur_v = max(
            (self._read_segment_duration_s(s, ts_v) for s in segs_v),
            default=SEG_DURATION_S,
        )
        max_dur_a = max(
            (self._read_segment_duration_s(s, ts_a) for s in segs_a),
            default=SEG_DURATION_S,
        )

        lines_v = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(max_dur_v) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq_v}",
            f'#EXT-X-MAP:URI="{init_v}"',
        ]
        for seg in segs_v:
            dur = self._read_segment_duration_s(seg, ts_v)
            lines_v.append(f"#EXTINF:{dur:.6f},")
            lines_v.append(os.path.relpath(seg, base))

        tmp = HLS_VIDEO_M3U8.with_suffix(".tmp")
        tmp.write_text("\n".join(lines_v) + "\n")
        tmp.replace(HLS_VIDEO_M3U8)

        lines_a = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            f"#EXT-X-TARGETDURATION:{int(max_dur_a) + 1}",
            f"#EXT-X-MEDIA-SEQUENCE:{seq_a}",
            f'#EXT-X-MAP:URI="{init_a}"',
        ]
        for seg in segs_a:
            dur = self._read_segment_duration_s(seg, ts_a)
            lines_a.append(f"#EXTINF:{dur:.6f},")
            lines_a.append(os.path.relpath(seg, base))

        tmp = HLS_AUDIO_M3U8.with_suffix(".tmp")
        tmp.write_text("\n".join(lines_a) + "\n")
        tmp.replace(HLS_AUDIO_M3U8)

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
# GStreamer pipeline
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
    # ── Video branch : qtmux → appsink → fichiers manuels ─────────────────
    qtmux_v   = make_element("qtmux",      "qtmux_v")
    appsink_v = make_element("appsink",    "appsink_v")

    qtmux_v.set_property("fragment-duration", FRAG_DURATION_MS)
    qtmux_v.set_property("streamable",        True)
    qtmux_v.set_property("fragment-mode",     "first-moov-then-finalise")

    appsink_v.set_property("emit-signals", True)
    appsink_v.set_property("sync",         False)
    appsink_v.set_property("max-buffers",  0)

    # ── Audio branch : qtmux → appsink ────────────────────────────────────
    qtmux_a   = make_element("qtmux",      "qtmux_a")
    appsink_a = make_element("appsink",    "appsink_a")

    qtmux_a.set_property("fragment-duration", FRAG_DURATION_MS)
    qtmux_a.set_property("streamable",        True)
    qtmux_a.set_property("fragment-mode",     "first-moov-then-finalise")

    appsink_a.set_property("emit-signals", True)
    appsink_a.set_property("sync",         False)
    appsink_a.set_property("max-buffers",  0)

    # Writers
    from gi.repository import Gst as _Gst

    class SegmentFileWriter:
        def __init__(self, seg_dir: Path, track: str, on_done):
            self._dir        = seg_dir
            self._track      = track
            self._on_done    = on_done
            self._idx        = 0
            self._media_idx  = 0          # compteur séparé pour les vrais segments
            self._buf        = bytearray()
            self._raw        = bytearray()
            self._init_done  = False      # init (ftyp+moov) déjà flushé ?

        def _handle(self, data: bytes):
            self._raw += data
            self._drain()

        def _drain(self):
            while len(self._raw) >= 8:
                size  = int.from_bytes(self._raw[0:4], 'big')
                btype = bytes(self._raw[4:8])

                if size == 0:
                    if btype in (b'ftyp', b'moov', b'moof', b'mdat', b'sidx'):
                        box = bytes(self._raw)
                        self._raw = bytearray()
                        self._buf += box
                        if btype == b'moov':
                            self._flush_init()
                        elif btype == b'mdat' and self._init_done:  # ← guard
                            self._flush_media()
                        # si mdat avant moov : on accumule sans flusher
                    else:
                        self._raw = bytearray()
                    break

                if size < 8:
                    del self._raw[:1]
                    continue

                if len(self._raw) < size:
                    break

                box = bytes(self._raw[:size])
                del self._raw[:size]

                if btype in (b'ftyp', b'moov', b'moof', b'mdat', b'sidx'):
                    self._buf += box
                    if btype == b'moov':
                        self._flush_init()
                    elif btype == b'mdat' and self._init_done:  # ← guard
                        self._flush_media()

        def on_new_sample(self, appsink):
            sample = appsink.emit("pull-sample")
            if sample is None:
                print(f"[DIAG][{self._track}] pull-sample returned None", flush=True)
                return _Gst.FlowReturn.OK
            buf = sample.get_buffer()
            ok, info = buf.map(_Gst.MapFlags.READ)
            if ok:
                data = bytes(info.data)
                buf.unmap(info)
                print(f"[DIAG][{self._track}] sample={len(data)} "
                    f"hdr={data[:8].hex()} init_done={self._init_done}", flush=True)
                self._handle(data)
            else:
                print(f"[DIAG][{self._track}] buf.map() FAILED", flush=True)
            return _Gst.FlowReturn.OK

        def _flush_init(self):
            print(f"[DIAG][{self._track}] _flush_init buf={len(self._buf)}", flush=True)
            if not self._buf:
                return
            path = self._dir / "seg_init_tmp.mp4"
            path.write_bytes(bytes(self._buf))
            log.info("[%s] init flushed → %s (%d bytes)", self._track, path.name, len(self._buf))
            self._on_done(self._track, -1, path)
            self._buf.clear()
            self._init_done = True

        def _flush_media(self):
            print(f"[DIAG][{self._track}] _flush_media buf={len(self._buf)} idx={self._media_idx}", flush=True)
            if not self._buf:
                return
            path = self._dir / f"seg{self._media_idx:05d}.m4s"
            path.write_bytes(bytes(self._buf))
            log.info("[%s] segment → seg%05d.m4s (%d bytes)",
                    self._track, self._media_idx, len(self._buf))
            self._on_done(self._track, self._media_idx, path)
            self._buf.clear()
            self._media_idx += 1
            
    writer_v = SegmentFileWriter(SEG_DIR_VIDEO, "video", on_segment_written)
    writer_a = SegmentFileWriter(SEG_DIR_AUDIO, "audio", on_segment_written)

    appsink_v.connect("new-sample", writer_v.on_new_sample)
    appsink_a.connect("new-sample", writer_a.on_new_sample)

    # ── Add all elements ──────────────────────────────────────────────────
    for el in [
        vid_src, vid_q1, vid_demux, vid_q2, h264parse, vid_caps,
        aud_src, aud_q1, aud_demux, aud_q2, aacparse, aud_caps,
        qtmux_v, appsink_v,
        qtmux_a, appsink_a,
    ]:
        pipeline.add(el)

    # ── Liens statiques ───────────────────────────────────────────────────
    for src, dst, label in [
        (vid_src,   vid_q1,    "vid_src → vid_q1"),
        (vid_q1,    vid_demux, "vid_q1 → vid_demux"),
        (vid_q2,    h264parse, "vid_q2 → h264parse"),
        (h264parse, vid_caps,  "h264parse → vid_caps"),
        (vid_caps,  qtmux_v,   "vid_caps → qtmux_v"),
        (qtmux_v,   appsink_v, "qtmux_v → appsink_v"),
        (aud_src,   aud_q1,   "aud_src → aud_q1"),
        (aud_q1,    aud_demux,"aud_q1 → aud_demux"),
        (aud_q2,    aacparse, "aud_q2 → aacparse"),
        (aacparse,  aud_caps, "aacparse → aud_caps"),
        (aud_caps,  qtmux_a,  "aud_caps → qtmux_a"),
        (qtmux_a,   appsink_a,"qtmux_a → appsink_a"),
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

    # ── READY → request pads ─────────────────────────────────────────────
    ret = pipeline.set_state(Gst.State.READY)
    if ret == Gst.StateChangeReturn.FAILURE:
        raise RuntimeError("Pipeline failed to go to READY")
    pipeline.get_state(timeout=Gst.SECOND * 5)

    return pipeline
# ---------------------------------------------------------------------------
# Async streamer
# ---------------------------------------------------------------------------

class MultiFormatStreamer:
    def __init__(self):
        self.pipeline              : Gst.Pipeline | None = None
        self._loop                 : asyncio.AbstractEventLoop | None = None
        self._stop_event           = asyncio.Event()
        self._ready_video          : set[int] = set()
        self._ready_audio          : set[int] = set()
        self._init_video_extracted = False
        self._init_audio_extracted = False
        self._manifest_writer      = ManifestWriter(
            self._ready_video,
            self._ready_audio,
        )

    async def start(self):
        self._loop = asyncio.get_running_loop()

        log.info("Building low-latency CMAF pipeline…")
        log.info("  Video  : tcp://%s:%d", VIDEO_HOST, VIDEO_PORT)
        log.info("  Audio  : tcp://%s:%d", AUDIO_HOST, AUDIO_PORT)
        log.info("  Output : %s", OUTPUT_DIR)
        log.info("  Seg %d ms  Frag %d ms  (%d frags/seg)",
                 SEG_DURATION_MS, FRAG_DURATION_MS, FRAGS_PER_SEG)

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

    def _on_segment_written(self, track: str, fragment_id: int, path: Path) -> None:

        if fragment_id == -1:
            # ── Init segment ─────────────────────────────────────────────────
            if track == "video" and not self._init_video_extracted:
                self._extract_init(path, INIT_FILE_VIDEO)
                self._init_video_extracted = True
            elif track == "audio" and not self._init_audio_extracted:
                self._extract_init(path, INIT_FILE_AUDIO)
                self._init_audio_extracted = True
            return                        # ← ne pas ajouter à _ready_*

        # ── Fragment média ────────────────────────────────────────────────────
        if track == "video":
            self._ready_video.add(fragment_id)
            log.debug("ready_video: %d segments", len(self._ready_video))
        elif track == "audio":
            self._ready_audio.add(fragment_id)
            log.debug("ready_audio: %d segments", len(self._ready_audio))

    def _extract_init(self, seg0: Path, dest: Path) -> None:
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