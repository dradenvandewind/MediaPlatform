"""AudioWorker – extrait et encode la piste audio de chaque profil vidéo."""
import asyncio
import logging
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)


class AudioWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="audio-encoding", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        video_keys = job_data["transcoded_videos"]
        encoded: list[str] = []

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            for key in video_keys:
                local_video = f"/tmp/{Path(key).name}"
                await s3.download(key, local_video)

                audio_path = local_video.replace(".mp4", "_audio.aac")
                await self._encode(local_video, audio_path)

                audio_key = key.replace(".mp4", "_audio.aac")
                await s3.upload(audio_path, audio_key)
                encoded.append(audio_key)

        return {"encoded_audios": encoded}

    async def _encode(self, input_path: str, output_path: str) -> None:
        cmd = f"ffmpeg -i {input_path} -vn -c:a aac -b:a 192k -ar 48000 {output_path}"
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg audio: {stderr.decode()}")
        log.info("encoded audio → %s", output_path)
