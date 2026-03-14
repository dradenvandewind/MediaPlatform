"""
IngestWorker – télécharge la source, la valide via ffprobe, l'upload sur S3
et publie un événement vers l'orchestrateur.
"""
import json
import logging
import tempfile
from pathlib import Path

import aiohttp

from processing_pipeline.shared.config import CHUNK_SIZE
from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker
from processing_pipeline.ingest.schemas import IngestRequest

log = logging.getLogger(__name__)


class IngestWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="ingest", **kwargs)

    # ------------------------------------------------------------------ #
    #  BaseWorker interface                                                #
    # ------------------------------------------------------------------ #

    async def run(self, job_id: str, job_data: dict) -> dict:
        req = IngestRequest.model_validate(job_data)
        log.info("[%s] start – %s", job_id, req.title)

        with tempfile.TemporaryDirectory(prefix=f"ingest_{job_id}_") as tmpdir:
            # 1. Récupérer la source
            if req.source_url:
                local_path = str(Path(tmpdir) / "source.mp4")
                await self._download_url(str(req.source_url), local_path)
            else:
                local_path = req.source_path

            # 2. Valider le média
            probe = await self._probe(local_path)
            duration = float(probe.get("format", {}).get("duration", 0))
            if duration < 1:
                raise ValueError("Not a valid video file (duration < 1s)")
            log.info("[%s] probe ok – duration=%.1fs", job_id, duration)

            # 3. Upload vers S3
            s3_key = f"raw/{job_id}/source.mp4"
            meta   = {
                "job_id":   job_id,
                "title":    req.title,
                "duration": str(round(duration, 2)),
                **req.metadata,
            }
            async with S3Manager(self.s3_bucket, self.s3_region) as s3:
                s3_uri = await s3.upload(local_path, s3_key, extra_meta=meta)

        log.info("[%s] done → %s", job_id, s3_uri)
        return {
            "s3_key":   s3_key,
            "s3_uri":   s3_uri,
            "title":    req.title,
            "duration": duration,
            "profiles": req.profiles,
            "metadata": meta,
        }

    # ------------------------------------------------------------------ #
    #  Internals                                                           #
    # ------------------------------------------------------------------ #

    async def _download_url(self, url: str, dest: str) -> None:
        downloaded = 0
        async with aiohttp.ClientSession() as http:
            async with http.get(url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                with open(dest, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded * 100 // total
                            log.info("downloading... %d%% (%d MB)", pct, downloaded // 1024 // 1024)
        log.info("downloaded %s → %s", url, dest)

    async def _probe(self, path: str) -> dict:
        import asyncio
        cmd = (
            f"ffprobe -v quiet -print_format json "
            f"-show_streams -show_format {path}"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe: {stderr.decode()}")
        return json.loads(stdout)
