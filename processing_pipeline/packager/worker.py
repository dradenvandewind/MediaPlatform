"""PackagerWorker – produces HLS, DASH and CMAF manifests."""
import asyncio
import logging
import os
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

from processing_pipeline.shared.metrics import JobTracker, track_ffmpeg


log = logging.getLogger(__name__)


class PackagerWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="packager", **kwargs)
    """
    async def run(self, job_id: str, job_data: dict) -> dict:
        muxed_files = job_data.get("transcoded_videos") or job_data.get("muxed_files", [])

        if not muxed_files:
            log.error("No video files to package (transcoded_videos is empty)")
            raise ValueError("No video files to package (transcoded_videos is empty)")

        log.info("audio and video files available: %s", muxed_files)

        results: dict[str, list[str]] = {"hls": [], "dash": [], "cmaf": []}  # ← initialization

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            for key in muxed_files:
                local = f"/tmp/{Path(key).name}"
                await s3.download(key, local)
                log.info("s3 download %s → %s", key, local)

                results["hls"].append(await self._hls(local, s3))
                results["dash"].append(await self._dash(local, s3))
                results["cmaf"].append(await self._cmaf(local, s3))

                try:
                    os.remove(local)
                except FileNotFoundError:
                    pass

        log.info("packager done: %s", results)  # ← corrected
        return results
    """
    async def run(self, job_id: str, job_data: dict) -> dict:
        log.info("[packager] job_data keys: %s", list(job_data.keys()))

        transcoded_videos = job_data.get("transcoded_videos") or job_data.get("muxed_files", [])
        audio_files       = job_data.get("encoded_audios", [])

        if not transcoded_videos:
            raise ValueError("No video files to package")

        
        if len(transcoded_videos) != len(audio_files):
            log.warning("video/audio count mismatch: %d vs %d — using video only",
                        len(transcoded_videos), len(audio_files))
            muxed_files = [(v, None) for v in transcoded_videos]
        else:
            muxed_files = list(zip(transcoded_videos, audio_files))

        log.info("[packager] %d video+audio pairs: %s", len(muxed_files), muxed_files)

        results: dict[str, list[str]] = {"hls": [], "dash": [], "cmaf": []}
        async with JobTracker(node="packager"):
            
            async with S3Manager(self.s3_bucket, self.s3_region) as s3:
                for video_key, audio_key in muxed_files:
                    local_video = f"/tmp/{Path(video_key).name}"
                    local_audio = f"/tmp/{Path(audio_key).name}" if audio_key else None

                    await s3.download(video_key, local_video)
                    if audio_key and local_audio:
                        await s3.download(audio_key, local_audio)

                    log.info("downloaded video=%s audio=%s", local_video, local_audio)

                    results["hls"].append(await self._hls(local_video, local_audio, s3, job_id))
                    results["dash"].append(await self._dash(local_video, local_audio, s3, job_id))
                    results["cmaf"].append(await self._cmaf(local_video, local_audio, s3, job_id))

                    for local_path in filter(None, [local_video, local_audio]):
                        try:
                            os.remove(local_path)
                        except FileNotFoundError:
                            pass

        log.info("packager done: %s", results)
        return results


    async def _run_cmd(self, cmd: str) -> None:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(stderr.decode())

    async def _hls(self, video_path: str, audio_path: str | None, s3: S3Manager, job_id: str) -> str:
        stem = Path(video_path).stem
        out  = Path(f"/tmp/hls_{stem}")
        out.mkdir(exist_ok=True)

        inputs = f"-i {video_path} "
        maps   = "-map 0:v:0 "
        if audio_path:
            inputs += f"-i {audio_path} "
            maps   += "-map 1:a:0 "
            maps   += "-bsf:a aac_adtstoasc "
        else:
            maps += "-map 0:a:0 "
            maps += "-bsf:a aac_adtstoasc "

        await self._run_cmd(
            f"ffmpeg -y {inputs}"
            f"{maps}"
            f"-c copy -f hls -hls_time 6 "
            f"-hls_playlist_type vod "
            f"-hls_segment_type fmp4 "
            f"-hls_fmp4_init_filename init.mp4 "
            f"-hls_segment_filename {out}/segment_%03d.m4s "
            f"-hls_flags independent_segments "
            f"{out}/playlist.m3u8"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"{job_id}/hls/{stem}/{f.name}")
        log.info("HLS fMP4 packaged → %s/hls/%s/playlist.m3u8", job_id, stem)
        return f"{job_id}/hls/{stem}/playlist.m3u8"


    async def _dash(self, video_path: str, audio_path: str | None, s3: S3Manager, job_id: str) -> str:
        stem = Path(video_path).stem
        out  = Path(f"/tmp/dash_{stem}")
        out.mkdir(exist_ok=True)

        inputs = f"-i {video_path} "
        maps   = "-map 0:v:0 "
        if audio_path:
            inputs += f"-i {audio_path} "
            maps   += "-map 1:a:0 "
        else:
            maps += "-map 0:a:0 "

        await self._run_cmd(
            f"ffmpeg -y {inputs}"
            f"{maps}"
            f"-c copy -f dash "
            f"-seg_duration 6 -use_template 1 -use_timeline 1 "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"{out}/manifest.mpd"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"{job_id}/dash/{stem}/{f.name}")
        log.info("DASH packaged → %s/dash/%s/manifest.mpd", job_id, stem)
        return f"{job_id}/dash/{stem}/manifest.mpd"


    async def _cmaf(self, video_path: str, audio_path: str | None, s3: S3Manager, job_id: str) -> str:
        stem = Path(video_path).stem
        out  = Path(f"/tmp/cmaf_{stem}")
        out.mkdir(exist_ok=True)

        inputs = f"-i {video_path} "
        maps   = "-map 0:v:0 "
        if audio_path:
            inputs += f"-i {audio_path} "
            maps   += "-map 1:a:0 "
        else:
            maps += "-map 0:a:0 "

        await self._run_cmd(
            f"ffmpeg -y {inputs}"
            f"{maps}"
            f"-c copy -f dash "
            f"-seg_duration 6 -use_template 1 -use_timeline 1 "
            f"-movflags cmaf+frag_keyframe+empty_moov+default_base_moof "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"-hls_playlist 1 -hls_master_name master.m3u8 "
            f"{out}/manifest.mpd"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"{job_id}/cmaf/{stem}/{f.name}")
        log.info("CMAF packaged → %s/cmaf/%s/manifest.mpd", job_id, stem)
        return f"{job_id}/cmaf/{stem}/manifest.mpd"
        
    async def _package_all(self, video_path: str, audio_path: str, s3: S3Manager, job_id: str) -> dict:
        stem = Path(video_path).stem
        out  = Path(f"/tmp/package_{stem}")
        out.mkdir(exist_ok=True)

        hls_dir  = out / "hls"
        dash_dir = out / "dash"
        hls_dir.mkdir(exist_ok=True)
        dash_dir.mkdir(exist_ok=True)

        cmd = (
            f"ffmpeg -y "
            f"-i {video_path} "      # ← video
            f"-i {audio_path} "      # ← separate audio
            f"-map 0:v:0 -map 1:a:0 "  # ← mux video + audio

            # HLS fMP4
            f"-c copy "
            f"-f hls "
            f"-hls_time 6 "
            f"-hls_playlist_type vod "
            f"-hls_segment_type fmp4 "
            f"-hls_fmp4_init_filename init.mp4 "
            f"-hls_segment_filename {hls_dir}/segment_%03d.m4s "
            f"-hls_flags independent_segments "
            f"{hls_dir}/playlist.m3u8 "

            # DASH + CMAF
            f"-map 0:v:0 -map 1:a:0 "
            f"-c copy "
            f"-f dash "
            f"-seg_duration 6 "
            f"-use_template 1 -use_timeline 1 "
            f"-movflags cmaf+frag_keyframe+empty_moov+default_base_moof "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"-hls_playlist 1 "
            f"-hls_master_name master.m3u8 "
            f"{dash_dir}/manifest.mpd"
        )
        

        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg package: {stderr.decode()}")

        # Upload HLS
        for f in hls_dir.glob("*"):
            await s3.upload(str(f), f"hls/{stem}/{f.name}")

        # Upload DASH + CMAF
        for f in dash_dir.glob("*"):
            await s3.upload(str(f), f"dash/{stem}/{f.name}")

        log.info("✅ HLS fMP4 + DASH + CMAF packaged → %s", stem)

        return {
            "hls":  f"hls/{stem}/playlist.m3u8",
            "dash": f"dash/{stem}/manifest.mpd",
            "cmaf": f"dash/{stem}/master.m3u8",
        }