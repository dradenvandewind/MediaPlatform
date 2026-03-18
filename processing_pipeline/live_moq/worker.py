"""
LiveIngestWorker – ingère un flux live via MoQ (Media over QUIC Transport).

Architecture :
  1. Lance moq-relay comme sous-processus (serveur QUIC local sur :4443)
  2. Souscrit aux tracks du publisher externe via moq-sub
  3. Accumule les segments fMP4 dans un fichier temporaire
  4. Upload l'init segment (init.mp4 = ftyp+moov) puis les media segments (NNNNNN.m4s = moof+mdat)
  5. Regénère et upload le manifest DASH dynamique (MPD type="dynamic") après chaque media segment
  6. Publie les événements de progression vers l'orchestrateur

Flux de données :
  Publisher (OBS/ffmpeg) ──MOQT/QUIC──► moq-relay :4443
                                              │
                                        moq-sub (subprocess)
                                              │
                                        /tmp/segments/
                                              │
                                           S3Manager
                                          ┌──┴──┐
                                       segment  manifest.mpd (mis à jour)
                                              │
                                        orchestrateur /next
"""

import asyncio
import logging
import math
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker
from processing_pipeline.live_moq.live_manifest import (
    LiveManifestWriter, SegmentEntry, TrackInfo,
)

log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

MOQ_RELAY_BIN   = os.getenv("MOQ_RELAY_BIN",   "/usr/local/bin/moq-relay")
MOQ_SUB_BIN     = os.getenv("MOQ_SUB_BIN",     "/usr/local/bin/moq-cli")
MOQ_RELAY_ADDR  = os.getenv("MOQ_RELAY_ADDR",  "0.0.0.0:4443")
MOQ_RELAY_URL   = os.getenv("MOQ_RELAY_URL",   "https://localhost:4443")
MOQ_CERT_PATH   = os.getenv("MOQ_CERT_PATH",   "/certs/cert.pem")
MOQ_KEY_PATH    = os.getenv("MOQ_KEY_PATH",    "/certs/key.pem")

SEGMENT_DURATION  = int(os.getenv("SEGMENT_DURATION",   "4"))   # secondes
MAX_STREAM_HOURS  = int(os.getenv("MAX_STREAM_HOURS",   "12"))  # sécurité
SEGMENT_BUFFER_MB = int(os.getenv("SEGMENT_BUFFER_MB",  "32"))  # flush threshold
DVR_WINDOW_S      = float(os.getenv("DVR_WINDOW_S",     "60"))  # fenêtre DVR
MPD_UPDATE_S      = float(os.getenv("MPD_UPDATE_S",      "2"))  # minimumUpdatePeriod
PLAYER_DELAY_S    = float(os.getenv("PLAYER_DELAY_S",   "10"))  # suggestedPresentationDelay


# ── State interne d'un stream actif ──────────────────────────────────────────

@dataclass
class LiveStreamState:
    stream_id:   str
    job_id:      str
    namespace:   str
    tracks:      list[str]
    s3_prefix:   str
    started_at:    float = field(default_factory=time.time)
    bytes_total:   int   = 0
    segments:      list[str] = field(default_factory=list)

    # fMP4 : compteur de flush indépendant par track
    # flush 0 → init.mp4 (ftyp+moov),  flush 1..N → NNNNNN.m4s (moof+mdat)
    track_idx:     dict[str, int] = field(default_factory=dict)
    init_uploaded: set[str]       = field(default_factory=set)

    # sous-processus gérés
    relay_proc: Optional[asyncio.subprocess.Process] = None
    sub_procs:  list[asyncio.subprocess.Process]     = field(default_factory=list)

    # signaux de contrôle
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    error:      Optional[str] = None

    # manifest DASH dynamique
    manifest_writer: Optional[LiveManifestWriter] = None


# ── Worker ────────────────────────────────────────────────────────────────────

class LiveIngestWorker(BaseWorker):
    """
    Worker d'ingestion live via MoQ-rs.

    job_data attendu (POST /process) :
    {
        "stream_id":  "stream-obs-001",
        "namespace":  "live/my-channel",
        "tracks":     ["video", "audio"],
        "duration":   3600
    }
    """

    def __init__(self, **kwargs):
        super().__init__(node_type="live_ingest", **kwargs)
        self._active: dict[str, LiveStreamState] = {}

    # ── Interface BaseWorker ──────────────────────────────────────────────────

    async def run(self, job_id: str, job_data: dict) -> dict:
        stream_id = job_data.get("stream_id", job_id)
        namespace = job_data["namespace"]
        tracks    = job_data.get("tracks", ["video", "audio"])
        duration  = int(job_data.get("duration", MAX_STREAM_HOURS * 3600))

        log.info("[%s] live ingest start – ns=%s tracks=%s", job_id, namespace, tracks)

        state = LiveStreamState(
            stream_id = stream_id,
            job_id    = job_id,
            namespace = namespace,
            tracks    = tracks,
            s3_prefix = f"live/{stream_id}",
        )
        self._active[stream_id] = state

        try:
            await self._start_relay(state)
            await asyncio.sleep(1.0)

            async with asyncio.timeout(duration):
                await self._ingest_loop(state)

        except asyncio.TimeoutError:
            log.info("[%s] max duration reached (%.0fs)", job_id, duration)
        except asyncio.CancelledError:
            log.info("[%s] ingest cancelled", job_id)
            raise
        except Exception as exc:
            state.error = str(exc)
            log.error("[%s] ingest error: %s", job_id, exc, exc_info=True)
            raise
        finally:
            await self._cleanup(state)
            self._active.pop(stream_id, None)

        return {
            "stream_id":      stream_id,
            "namespace":      namespace,
            "s3_prefix":      state.s3_prefix,
            "manifest_key":   state.manifest_writer.get_manifest_s3_key()
                              if state.manifest_writer else None,
            "segments":       state.segments,
            "segments_count": len(state.segments),
            "bytes_total":    state.bytes_total,
            "duration_s":     round(time.time() - state.started_at, 1),
        }

    # ── Relay MoQ ────────────────────────────────────────────────────────────

    async def _start_relay(self, state: LiveStreamState) -> None:
        """Démarre moq-relay et initialise le manifest writer."""
        cmd = [
            MOQ_RELAY_BIN,
            "--listen",   MOQ_RELAY_ADDR,
            "--tls-cert", MOQ_CERT_PATH,
            "--tls-key",  MOQ_KEY_PATH,
        ]
        log.info("[%s] starting moq-relay: %s", state.job_id, " ".join(cmd))

        state.relay_proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        asyncio.create_task(
            self._drain_stderr(state.relay_proc, f"relay/{state.stream_id}")
        )
        log.info("[%s] moq-relay PID=%d", state.job_id, state.relay_proc.pid)

        # URL publique S3 : utilisée comme BaseURL dans le MPD
        endpoint = os.getenv("AWS_ENDPOINT_URL", "https://s3.amazonaws.com")
        s3_public_base = f"{endpoint}/{self.s3_bucket}/live/{state.stream_id}"

        state.manifest_writer = LiveManifestWriter(
            stream_id         = state.stream_id,
            s3_base_url       = s3_public_base,
            segment_duration  = SEGMENT_DURATION,
            time_shift_buffer = DVR_WINDOW_S,
            min_update_period = MPD_UPDATE_S,
            suggested_delay   = PLAYER_DELAY_S,
        )
        log.info("[%s] manifest writer ready – base=%s", state.job_id, s3_public_base)

    # ── Boucle d'ingestion ────────────────────────────────────────────────────

    async def _ingest_loop(self, state: LiveStreamState) -> None:
        tasks = [
            asyncio.create_task(
                self._subscribe_track(state, track),
                name=f"sub-{state.stream_id}-{track}",
            )
            for track in state.tracks
        ]

        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for t in done:
                if t.exception():
                    raise t.exception()
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Souscription track ────────────────────────────────────────────────────

    async def _subscribe_track(self, state: LiveStreamState, track: str) -> None:
        cmd = [
            MOQ_SUB_BIN,
            "--url", f"{MOQ_RELAY_URL}/{state.namespace}",
            "--track", track,
            "--dev",
        ]
        log.info("[%s/%s] subscribing: %s", state.job_id, track, " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        state.sub_procs.append(proc)
        asyncio.create_task(
            self._drain_stderr(proc, f"sub/{state.stream_id}/{track}")
        )

        seg_dir = Path(tempfile.mkdtemp(prefix=f"moq_{state.stream_id}_{track}_"))
        try:
            await self._read_and_segment(state, proc, track, seg_dir)
        finally:
            for f in seg_dir.glob("*.m4s"):
                f.unlink(missing_ok=True)
            for f in seg_dir.glob("*.mp4"):
                f.unlink(missing_ok=True)
            seg_dir.rmdir()
            if proc.returncode is None:
                proc.terminate()

    async def _read_and_segment(
        self,
        state:   LiveStreamState,
        proc:    asyncio.subprocess.Process,
        track:   str,
        seg_dir: Path,
    ) -> None:
        buf           = bytearray()
        segment_start = time.monotonic()
        flush_bytes   = SEGMENT_BUFFER_MB * 1024 * 1024

        assert proc.stdout is not None
        while True:
            if state.stop_event.is_set():
                log.info("[%s/%s] stop_event – flushing last segment", state.job_id, track)
                break

            try:
                chunk = await asyncio.wait_for(proc.stdout.read(65536), timeout=2.0)
            except asyncio.TimeoutError:
                if buf and (time.monotonic() - segment_start) >= SEGMENT_DURATION:
                    await self._flush_segment(state, buf, track, seg_dir)
                    buf           = bytearray()
                    segment_start = time.monotonic()
                continue

            if not chunk:
                log.info("[%s/%s] moq-sub EOF", state.job_id, track)
                break

            buf += chunk

            if len(buf) >= flush_bytes or (time.monotonic() - segment_start) >= SEGMENT_DURATION:
                await self._flush_segment(state, buf, track, seg_dir)
                buf           = bytearray()
                segment_start = time.monotonic()

        if buf:
            await self._flush_segment(state, buf, track, seg_dir)

    # ── Flush segment → S3 + mise à jour manifest ─────────────────────────────

    async def _flush_segment(
        self,
        state:   LiveStreamState,
        data:    bytearray,
        track:   str,
        seg_dir: Path,
    ) -> None:
        """
        Gère les deux types de segments fMP4 :

          flush_idx == 0  →  init segment  →  init.mp4   (ftyp + moov)
          flush_idx >= 1  →  media segment →  NNNNNN.m4s (moof + mdat)

        Le numéro de media segment commence à 1 (startNumber DASH).
        Le manifest n'est uploadé qu'à partir du premier media segment de
        chaque track (l'init seul ne suffit pas à construire un MPD valide).
        """
        flush_idx  = state.track_idx.get(track, 0)
        byte_size  = len(data)
        is_init    = flush_idx == 0

        if is_init:
            # ── Init segment (ftyp + moov) ────────────────────────────────────
            filename = "init.mp4"
            s3_key   = f"{state.s3_prefix}/{track}/init.mp4"
        else:
            # ── Media segment (moof + mdat) ───────────────────────────────────
            # Le numéro DASH commence à 1, cohérent avec startNumber=1 dans le MPD
            media_num = flush_idx          # flush 1 → seg 1, flush 2 → seg 2…
            filename  = f"{media_num:06d}.m4s"
            s3_key    = f"{state.s3_prefix}/{track}/{filename}"

        local = seg_dir / filename
        local.write_bytes(data)

        # ── Upload S3 ─────────────────────────────────────────────────────────
        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            await s3.upload(str(local), s3_key)

        local.unlink(missing_ok=True)
        state.track_idx[track] = flush_idx + 1
        state.bytes_total += byte_size

        if is_init:
            state.init_uploaded.add(track)
            log.info("[%s/%s] init.mp4 → s3 (%.1f KB)", state.job_id, track, byte_size / 1024)
            return   # pas de mise à jour manifest sur l'init

        # ── Media segment uploadé ─────────────────────────────────────────────
        state.segments.append(s3_key)
        log.info(
            "[%s/%s] seg %06d.m4s → s3 (%.1f KB, total %d media segs)",
            state.job_id, track, media_num, byte_size / 1024, len(state.segments),
        )

        # ── Mise à jour manifest ──────────────────────────────────────────────
        if state.manifest_writer is None:
            return

        state.manifest_writer.add_segment(SegmentEntry(
            track_id   = track,
            number     = media_num,
            s3_key     = s3_key,
            duration_s = float(SEGMENT_DURATION),
            byte_size  = byte_size,
            wall_time  = time.time(),
        ))

        # Attendre que :
        #   1. toutes les tracks aient uploadé leur init.mp4
        #   2. chaque track ait au moins un media segment
        missing_init  = [t for t in state.tracks if t not in state.init_uploaded]
        missing_media = [t for t in state.tracks if state.manifest_writer.segment_count(t) < 1]

        if missing_init or missing_media:
            log.debug(
                "[%s] manifest upload deferred – init missing: %s  media missing: %s",
                state.job_id, missing_init, missing_media,
            )
            return

        await self._upload_manifest(state)

    # ── Upload manifest.mpd ───────────────────────────────────────────────────

    async def _upload_manifest(self, state: LiveStreamState) -> None:
        """Regénère le MPD et l'écrase sur S3."""
        assert state.manifest_writer is not None

        mpd_content = state.manifest_writer.render()
        mpd_s3_key  = state.manifest_writer.get_manifest_s3_key()

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".mpd", delete=False, encoding="utf-8"
        ) as tmp:
            tmp.write(mpd_content)
            tmp_path = tmp.name

        try:
            async with S3Manager(self.s3_bucket, self.s3_region) as s3:
                await s3.upload(tmp_path, mpd_s3_key)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        log.debug(
            "[%s] manifest.mpd → s3://%s/%s (%d B)",
            state.job_id, self.s3_bucket, mpd_s3_key, len(mpd_content),
        )

    # ── Arrêt propre ─────────────────────────────────────────────────────────

    async def _cleanup(self, state: LiveStreamState) -> None:
        log.info("[%s] cleanup – stopping %d sub(s) + relay", state.job_id, len(state.sub_procs))

        for proc in state.sub_procs:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    proc.kill()

        if state.relay_proc and state.relay_proc.returncode is None:
            state.relay_proc.terminate()
            try:
                await asyncio.wait_for(state.relay_proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                state.relay_proc.kill()

        log.info("[%s] cleanup done", state.job_id)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _drain_stderr(proc: asyncio.subprocess.Process, label: str) -> None:
        assert proc.stderr is not None
        while True:
            line = await proc.stderr.readline()
            if not line:
                break
            log.debug("[%s] %s", label, line.decode().rstrip())

    # ── API de contrôle (routes FastAPI) ─────────────────────────────────────

    def get_stream_state(self, stream_id: str) -> Optional[LiveStreamState]:
        return self._active.get(stream_id)

    async def stop_stream(self, stream_id: str) -> bool:
        state = self._active.get(stream_id)
        if state is None:
            return False
        state.stop_event.set()
        log.info("stop requested for stream %s", stream_id)
        return True

    def list_streams(self) -> list[dict]:
        now = time.time()
        return [
            {
                "stream_id":   s.stream_id,
                "job_id":      s.job_id,
                "namespace":   s.namespace,
                "tracks":      s.tracks,
                "segments":    len(s.segments),
                "bytes_total": s.bytes_total,
                "uptime_s":    round(now - s.started_at, 1),
                "manifest":    s.manifest_writer.get_manifest_s3_key()
                               if s.manifest_writer else None,
                "error":       s.error,
            }
            for s in self._active.values()
        ]