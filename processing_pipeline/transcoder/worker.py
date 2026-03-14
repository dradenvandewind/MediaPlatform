"""TranscoderWorker – transcode une vidéo source vers plusieurs profils."""
import asyncio
import logging

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)
#todo  extract fps to inject it
FFMPEG_CONFIGS: dict[str, str] = {
    "1080p": "-vf scale=-2:1080 -c:v libx264 -preset medium -profile:v high -level 4.2 -b:v 5800k -maxrate 6200k -bufsize 8700k -g 48 -keyint_min 48 -sc_threshold 0",

    "720p":  "-vf scale=-2:720  -c:v libx264 -preset medium -profile:v high -level 4.0 -b:v 3000k -maxrate 3200k -bufsize 4500k -g 48 -keyint_min 48 -sc_threshold 0",

    "540p":  "-vf scale=-2:540  -c:v libx264 -preset medium -profile:v main -level 3.2 -b:v 1800k -maxrate 1900k -bufsize 2700k -g 48 -keyint_min 48 -sc_threshold 0",

    "480p":  "-vf scale=-2:480  -c:v libx264 -preset medium -profile:v main -level 3.1 -b:v 1200k -maxrate 1300k -bufsize 1800k -g 48 -keyint_min 48 -sc_threshold 0",

    "360p":  "-vf scale=-2:360  -c:v libx264 -preset medium -profile:v main -level 3.0 -b:v 800k  -maxrate 856k  -bufsize 1200k -g 48 -keyint_min 48 -sc_threshold 0",

    "240p":  "-vf scale=-2:240  -c:v libx264 -preset medium -profile:v baseline -level 3.0 -b:v 400k -maxrate 428k -bufsize 600k -g 48 -keyint_min 48 -sc_threshold 0",
    
    "hevc_2160p": "-vf scale=-2:2160 -c:v libx265 -preset medium -x265-params profile=main10:level=5.1 -b:v 16000k -maxrate 17000k -bufsize 24000k -g 48 -keyint_min 48",

    "hevc_1440p": "-vf scale=-2:1440 -c:v libx265 -preset medium -x265-params profile=main10 -b:v 10000k -maxrate 10500k -bufsize 15000k -g 48 -keyint_min 48",

    "hevc_1080p": "-vf scale=-2:1080 -c:v libx265 -preset medium -x265-params profile=main -b:v 6000k -maxrate 6300k -bufsize 9000k -g 48 -keyint_min 48",

    "hevc_720p": "-vf scale=-2:720 -c:v libx265 -preset medium -b:v 3500k -maxrate 3700k -bufsize 5250k -g 48 -keyint_min 48",

    "hevc_540p": "-vf scale=-2:540 -c:v libx265 -preset medium -b:v 2000k -maxrate 2100k -bufsize 3000k -g 48 -keyint_min 48",

    "hevc_480p": "-vf scale=-2:480 -c:v libx265 -preset medium -b:v 1300k -maxrate 1400k -bufsize 1950k -g 48 -keyint_min 48",

    "hevc_360p": "-vf scale=-2:360 -c:v libx265 -preset medium -b:v 850k -maxrate 900k -bufsize 1275k -g 48 -keyint_min 48",
    
    "h264_sport_1080p60": "-vf scale=-2:1080 -r 60 -c:v libx264 -preset medium -b:v 8000k -maxrate 8500k -bufsize 12000k -g 120",

    "h264_sport_720p60": "-vf scale=-2:720 -r 60 -c:v libx264 -b:v 4500k -maxrate 4800k -bufsize 7000k -g 120",

    "h264_sport_540p60": "-vf scale=-2:540 -r 60 -c:v libx264 -b:v 2800k -maxrate 3000k -bufsize 4200k -g 120",

    "h264_sport_480p60": "-vf scale=-2:480 -r 60 -c:v libx264 -b:v 1800k -maxrate 1900k -bufsize 2700k -g 120",

    "h264_sport_360p60": "-vf scale=-2:360 -r 60 -c:v libx264 -b:v 1100k -maxrate 1200k -bufsize 1650k -g 120",
    
    "svtav1_1080p": "-vf scale=-2:1080 -c:v libsvtav1 -preset 6 -rc 1 -b:v 4200k -maxrate 4500k -bufsize 6300k -g 48 -svtav1-params tune=0",

    "svtav1_720p": "-vf scale=-2:720 -c:v libsvtav1 -preset 6 -rc 1 -b:v 2300k -maxrate 2500k -bufsize 3500k -g 48 -svtav1-params tune=0",

    "svtav1_540p": "-vf scale=-2:540 -c:v libsvtav1 -preset 6 -rc 1 -b:v 1400k -maxrate 1500k -bufsize 2100k -g 48 -svtav1-params tune=0",

    "svtav1_480p": "-vf scale=-2:480 -c:v libsvtav1 -preset 6 -rc 1 -b:v 950k -maxrate 1000k -bufsize 1400k -g 48 -svtav1-params tune=0",

    "svtav1_360p": "-vf scale=-2:360 -c:v libsvtav1 -preset 6 -rc 1 -b:v 600k -maxrate 650k -bufsize 900k -g 48 -svtav1-params tune=0",

    "svtav1_240p": "-vf scale=-2:240 -c:v libsvtav1 -preset 6 -rc 1 -b:v 350k -maxrate 380k -bufsize 500k -g 48 -svtav1-params tune=0"

}


class TranscoderWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="transcoder", **kwargs)

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
