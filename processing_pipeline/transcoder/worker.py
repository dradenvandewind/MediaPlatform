"""TranscoderWorker – transcode une vidéo source vers plusieurs profils."""
import asyncio
import logging
import os 
from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker
import re 
log = logging.getLogger(__name__)
#todo  extract fps to inject it
FFMPEG_CONFIGS: dict[str, str] = {
    "1080p": "-vf scale=-2:1080 -c:v libx264 -preset medium -profile:v high -level 4.2 -b:v 5800k -maxrate 6200k -bufsize 8700k -g 48 -keyint_min 48 -sc_threshold 0 -an",

    "720p":  "-vf scale=-2:720  -c:v libx264 -preset medium -profile:v high -level 4.0 -b:v 3000k -maxrate 3200k -bufsize 4500k -g 48 -keyint_min 48 -sc_threshold 0 -an",

    "540p":  "-vf scale=-2:540  -c:v libx264 -preset medium -profile:v main -level 3.2 -b:v 1800k -maxrate 1900k -bufsize 2700k -g 48 -keyint_min 48 -sc_threshold 0 -an",

    "480p":  "-vf scale=-2:480  -c:v libx264 -preset medium -profile:v main -level 3.1 -b:v 1200k -maxrate 1300k -bufsize 1800k -g 48 -keyint_min 48 -sc_threshold 0 -an",

    "360p":  "-vf scale=-2:360  -c:v libx264 -preset medium -profile:v main -level 3.0 -b:v 800k  -maxrate 856k  -bufsize 1200k -g 48 -keyint_min 48 -sc_threshold 0 -an",

    "240p":  "-vf scale=-2:240  -c:v libx264 -preset medium -profile:v baseline -level 3.0 -b:v 400k -maxrate 428k -bufsize 600k -g 48 -keyint_min 48 -sc_threshold 0 -an",
    
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
            
            # ── ffprobe framerate ─────────
            framerate = await self._probe_framerate(local_input)
            log.info("[transcoder] job=%s framerate=%.3f fps", job_id, framerate)
            # ── configs with GOP 2xfps ────────────
            configs = self._build_configs(framerate)
        
            outputs: list[str] = []
            for profile in profiles:
                out_path = f"/tmp/{job_id}_{profile}.mp4"
                await self._transcode(local_input, out_path, profile)
                out_key = f"transcoded/{job_id}/{profile}.mp4"
                await s3.upload(out_path, out_key)

                # ── Vérification que le fichier est bien dans S3 ──
                try:
                    meta = await s3.head(out_key)
                    log.info("[%s] ✅ S3 confirmed: %s (%.1f MB)",
                            profile, out_key, meta["size"] / 1024 / 1024)
                except Exception as exc:
                    raise RuntimeError(f"Upload verification failed for {out_key}: {exc}")

                outputs.append(out_key)
                log.info("[%s] uploaded → s3://%s/%s", profile, self.s3_bucket, out_key)

        log.info("[transcoder] job=%s done — %d profiles: %s", job_id, len(outputs), outputs)
        try:
            os.remove(local_input)
            log.info(f"File '{local_input}' deleted successfully.")
        except FileNotFoundError:
            log.error(f"File '{local_input}' not found.")

        for out_path in [f"/tmp/{job_id}_{p}.mp4" for p in profiles]:
            try:
                os.remove(out_path)
                log.info(f"File '{out_path}' deleted successfully.")
            except FileNotFoundError:
                log.error(f"File '{out_path}' not found.")
                    
        return {
            "transcoded_videos": outputs,
            "framerate": framerate,
            "s3_key": s3_key,
        }


    async def _transcode(self, input_path: str, output_path: str, profile: str) -> None:
        if profile not in FFMPEG_CONFIGS:
            raise ValueError(f"Unknown profile: {profile}")
        cmd = f"ffmpeg -i {input_path} {FFMPEG_CONFIGS[profile]} {output_path}"
        log.info(" input [%s] transcoded by %s  in output %s",input_path, cmd, output_path)
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg [{profile}]: {stderr.decode()}")
        log.info("[%s] transcoded %s", profile, output_path)


    async def _probe_framerate(self, path: str) -> float:
        """Extract framerate in using ffprobe.  todo add ffropbe after ingest and backup database"""
        cmd = (
            "ffprobe -v quiet "
            "-select_streams v:0 "
            "-show_entries stream=r_frame_rate "
            "-of default=noprint_wrappers=1:nokey=1 "
            f"{path}"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe framerate: {stderr.decode()}")

        # ffprobe retourne une fraction ex: "24000/1001" ou "25/1"
        raw = stdout.decode().strip()
        try:
            num, den = raw.split("/")
            return round(float(num) / float(den), 3)
        except Exception:
            log.warning("Could not parse framerate '%s', defaulting to 25.0", raw)
            return 25.0
    
    def _build_configs(self, framerate: float) -> dict[str, str]:
        """Replace -g et -keyint_min by framerate*2 in all profils."""
        gop = int(round(framerate * 2))
        updated = {}
        for profile, params in FFMPEG_CONFIGS.items():
            # Replace -g <value> et -keyint_min <value>
            new_params = re.sub(r"-g \d+", f"-g {gop}", params)
            new_params = re.sub(r"-keyint_min \d+", f"-keyint_min {gop}", new_params)
            updated[profile] = new_params
            log.debug("profile=%s gop=%d params=%s", profile, gop, new_params)
        return updated