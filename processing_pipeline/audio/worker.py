"""AudioWorker – extrait et encode la piste audio depuis la source originale."""
import asyncio
import logging
import os
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)


class AudioWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="audio", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        # ── Télécharger la source originale (pas les transcodés) ──────────
        s3_key      = job_data.get("s3_key")          # clé de la source raw
        video_keys  = job_data.get("transcoded_videos", [])
        encoded: list[str] = []

        local_source = f"/tmp/{job_id}_original.mp4"

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            # Télécharger la source originale
            await s3.download(s3_key, local_source)
            log.info("[audio] downloaded source %s → %s", s3_key, local_source)

            # Encoder une seule piste audio depuis la source
            audio_path = f"/tmp/{job_id}_audio.aac"
            await self._encode(local_source, audio_path)

            # Uploader une piste audio par profil vidéo
            for key in video_keys:
                audio_key = key.replace(".mp4", "_audio.aac")
                await s3.upload(audio_path, audio_key)

                try:
                    meta = await s3.head(audio_key)
                    log.info("✅ S3 confirmed: %s (%.1f MB)",
                             audio_key, meta["size"] / 1024 / 1024)
                except Exception as exc:
                    raise RuntimeError(f"Upload verification failed for {audio_key}: {exc}")

                encoded.append(audio_key)
                log.info("uploaded → s3://%s/%s", self.s3_bucket, audio_key)

        # Nettoyage
        for f in [local_source, audio_path]:
            try:
                os.remove(f)
                log.info("deleted %s", f)
            except FileNotFoundError:
                pass

        return {"encoded_audios": encoded}

    async def _encode(self, input_path: str, output_path: str) -> None:
        cmd = (
            f"ffmpeg -i {input_path} "
            f"-vn -c:a aac -b:a 192k -ar 48000 -bsf:a aac_adtstoasc "
            f"{output_path}"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg audio: {stderr.decode()}")
        log.info("encoded audio → %s", output_path)