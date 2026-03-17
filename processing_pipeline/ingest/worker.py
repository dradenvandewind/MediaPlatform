"""
IngestWorker – télécharge la source, la valide via ffprobe, l'upload sur S3.
"""
import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path

import aiofiles
import aiohttp

from processing_pipeline.shared.config import CHUNK_SIZE
from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker
from processing_pipeline.ingest.schemas import IngestRequest

log = logging.getLogger(__name__)

DOWNLOAD_WORKERS = 8


class IngestWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="ingest", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        #req = IngestRequest.model_validate(job_data)
        log.info("[%s] validating input...", job_data)
        req = IngestRequest.model_validate(job_data["input"] or job_data)
        log.info("job[%s] start – %s", job_id, req.title)

        with tempfile.TemporaryDirectory(prefix=f"ingest_{job_id}_") as tmpdir:
            if req.source_url:
                local_path = str(Path(tmpdir) / "source.mp4")
                await self._download_url(str(req.source_url), local_path)
            else:
                local_path = req.source_path

            probe    = await self._probe(local_path)
            duration = float(probe.get("format", {}).get("duration", 0))
            if duration < 1:
                raise ValueError("Not a valid video file (duration < 1s)")
            log.info("[%s] probe ok – duration=%.1fs", job_id, duration)

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
    """
    #todo need to fix
    async def _download_url(self, url: str, dest: str) -> None:
        
        timeout = aiohttp.ClientTimeout(connect=30, sock_read=300)

        # HEAD pour taille + support Range
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.head(url) as resp:
                total        = int(resp.headers.get("Content-Length", 0))
                accept_range = resp.headers.get("Accept-Ranges", "none")

        if not total or accept_range == "none":
            log.warning("No Range support, fallback to sequential")
            await self._download_sequential(url, dest)
            return

        log.info("download %s — %.1f MB — %d workers",
                 url, total / 1024 / 1024, DOWNLOAD_WORKERS)

        # Découper en chunks
        chunk_size = max(CHUNK_SIZE, total // DOWNLOAD_WORKERS)
        ranges: list[tuple[int, int]] = []
        start = 0
        while start < total:
            end = min(start + chunk_size - 1, total - 1)
            ranges.append((start, end))
            start = end + 1

        log.info("%d chunks of ~%.1f MB", len(ranges), chunk_size / 1024 / 1024)

        # Pré-allouer le fichier
        async with aiofiles.open(dest, "wb") as f:
            await f.seek(total - 1)
            await f.write(b"\x00")

        downloaded_bytes = [0] * len(ranges)
        semaphore = asyncio.Semaphore(DOWNLOAD_WORKERS)

        async def fetch_chunk(idx: int, byte_start: int, byte_end: int) -> None:
            async with semaphore:
                tmp_path = f"{dest}.part{idx}"
                headers  = {"Range": f"bytes={byte_start}-{byte_end}"}
                timeout_chunk = aiohttp.ClientTimeout(total=600, connect=30, sock_read=120)

                async with aiohttp.ClientSession(timeout=timeout_chunk) as http:
                    async with http.get(url, headers=headers) as resp:
                        resp.raise_for_status()
                        async with aiofiles.open(tmp_path, "wb") as f:
                            async for data in resp.content.iter_chunked(CHUNK_SIZE):
                                await f.write(data)

                downloaded_bytes[idx] = byte_end - byte_start + 1
                total_done = sum(downloaded_bytes)
                log.info("chunk %02d/%02d — %.1f%% (%.1f / %.1f MB)",
                        idx + 1, len(ranges),
                        total_done / total * 100,
                        total_done / 1024 / 1024,
                        total / 1024 / 1024)

        tasks = [
            asyncio.create_task(fetch_chunk(i, s, e))
            for i, (s, e) in enumerate(ranges)
        ]
        await asyncio.gather(*tasks)

        # Assembler les parts dans l'ordre
        log.info("assembling %d parts...", len(ranges))
        async with aiofiles.open(dest, "wb") as out:
            for i in range(len(ranges)):
                tmp_path = f"{dest}.part{i}"
                async with aiofiles.open(tmp_path, "rb") as part:
                    while True:
                        data = await part.read(CHUNK_SIZE)
                        if not data:
                            break
                        await out.write(data)
                os.remove(tmp_path)

        log.info("✅ download complete → %s (%.1f MB)", dest, total / 1024 / 1024)
    """
    async def _download_url(self, url: str, dest: str) -> None:
        """Téléchargement séquentiel avec timeout généreux."""
        timeout = aiohttp.ClientTimeout(
            total=7200,    # 2 max
            connect=30,
            sock_read=300, # 5 min sans données
        )
        downloaded = 0
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.get(url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                with open(dest, "wb") as fh:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        fh.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            log.info("downloading %.1f%% (%.1f / %.1f MB)",
                                    downloaded / total * 100,
                                    downloaded / 1024 / 1024,
                                    total / 1024 / 1024)
        log.info("✅ downloaded %s → %s (%.1f MB)", url, dest, downloaded / 1024 / 1024)

    async def _download_sequential(self, url: str, dest: str) -> None:
        """Fallback séquentiel."""
        timeout    = aiohttp.ClientTimeout(total=3600, connect=30, sock_read=300)
        downloaded = 0
        async with aiohttp.ClientSession(timeout=timeout) as http:
            async with http.get(url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", 0))
                async with aiofiles.open(dest, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            log.info("downloading %.1f%% (%.1f / %.1f MB)",
                                     downloaded / total * 100,
                                     downloaded / 1024 / 1024,
                                     total / 1024 / 1024)
        log.info("✅ downloaded %s → %s", url, dest)

    async def _probe(self, path: str) -> dict:
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