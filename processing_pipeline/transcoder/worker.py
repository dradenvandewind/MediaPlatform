"""TranscoderWorker – transcode une vidéo source vers plusieurs profils."""
import asyncio
import logging

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)

FFMPEG_CONFIGS: dict[str, str] = {
    "1080p": "-vf scale=-2:1080 -c:v libx264 -preset medium -crf 23 -b:v 5000k",
    "720p":  "-vf scale=-2:720  -c:v libx264 -preset medium -crf 23 -b:v 2500k",
    "480p":  "-vf scale=-2:480  -c:v libx264 -preset medium -crf 23 -b:v 1000k",
}


class TranscoderWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="transcoding", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        s3_key   = job_data["s3_key"]
        profiles = job_data.get("profiles", list(FFMPEG_CONFIGS.keys()))

        local_input = f"/tmp/{job_id}_source.mp4"
        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            await s3.download(s3_key, local_input)

            outputs: list[str] = []
            for profile in profiles:
                out_path = f"/tmp/{job_id}_{profile}.mp4"
                await self._transcode(local_input, out_path, profile)
                out_key = f"transcoded/{job_id}/{profile}.mp4"
                await s3.upload(out_path, out_key)
                outputs.append(out_key)

        return {"transcoded_videos": outputs}

    async def _transcode(self, input_path: str, output_path: str, profile: str) -> None:
        if profile not in FFMPEG_CONFIGS:
            raise ValueError(f"Unknown profile: {profile}")
        cmd = f"ffmpeg -i {input_path} {FFMPEG_CONFIGS[profile]} {output_path}"
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg [{profile}]: {stderr.decode()}")
        log.info("[%s] transcoded %s", profile, output_path)
