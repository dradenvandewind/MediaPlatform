"""PackagerWorker – produit les manifests HLS, DASH et CMAF."""
import asyncio
import logging
import os
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

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

        results: dict[str, list[str]] = {"hls": [], "dash": [], "cmaf": []}  # ← initialisation

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

        log.info("packager done: %s", results)  # ← corrigé
        return results
    """
    async def run(self, job_id: str, job_data: dict) -> dict:
        muxed_files = job_data.get("transcoded_videos") or job_data.get("muxed_files", [])

        if not muxed_files:
            raise ValueError("No video files to package")

        results: dict[str, list[str]] = {"hls": [], "dash": [], "cmaf": []}

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            for key in muxed_files:
                local = f"/tmp/{Path(key).name}"
                await s3.download(key, local)
                log.info("downloaded %s → %s", key, local)

                output = await self._package_all(local, s3, job_id)
                results["hls"].append(output["hls"])
                results["dash"].append(output["dash"])
                results["cmaf"].append(output["cmaf"])

                try:
                    os.remove(local)
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

    async def _hls(self, input_path: str, s3: S3Manager) -> str:
        stem = Path(input_path).stem
        out  = Path(f"/tmp/hls_{stem}")
        out.mkdir(exist_ok=True)

        # fMP4 segments pour HLS (requiert HLS v7+)
        await self._run_cmd(
            f"ffmpeg -i {input_path} "
            f"-c copy "
            f"-f hls "
            f"-hls_time 6 "
            f"-hls_playlist_type vod "
            f"-hls_segment_type fmp4 "                         
            f"-hls_fmp4_init_filename init.mp4 "               
            f"-hls_segment_filename {out}/segment_%03d.m4s "   
            f"-hls_flags independent_segments "                
            f"{out}/playlist.m3u8"
        )

        for f in out.glob("*"):
            await s3.upload(str(f), f"hls/{stem}/{f.name}")

        log.info("HLS fMP4 packaged → hls/%s/playlist.m3u8", stem)
        return f"hls/{stem}/playlist.m3u8"

    async def _dash(self, input_path: str, s3: S3Manager) -> str:
        stem = Path(input_path).stem
        out  = Path(f"/tmp/dash_{stem}")
        out.mkdir(exist_ok=True)
        await self._run_cmd(
            f"ffmpeg -i {input_path} -c copy -f dash "
            f"-seg_duration 4 -use_template 1 -use_timeline 1 "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"{out}/manifest.mpd"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"dash/{stem}/{f.name}")
        log.info("DASH packaged → dash/%s/manifest.mpd", stem)
        return f"dash/{stem}/manifest.mpd"

    async def _cmaf(self, input_path: str, s3: S3Manager) -> str:
        stem = Path(input_path).stem
        out  = Path(f"/tmp/cmaf_{stem}")
        out.mkdir(exist_ok=True)
        await self._run_cmd(
            f"ffmpeg -i {input_path} -c copy -f dash "
            f"-seg_duration 4 -use_template 1 -use_timeline 1 "
            f"-movflags cmaf+frag_keyframe "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"{out}/manifest.mpd"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"cmaf/{stem}/{f.name}")
        log.info("CMAF packaged → cmaf/%s/manifest.mpd", stem)
        return f"cmaf/{stem}/manifest.mpd"
    
    async def _package_all(self, input_path: str, s3: S3Manager, job_id: str) -> dict[str, str]:
        stem = Path(input_path).stem
        out  = Path(f"/tmp/package_{stem}")
        out.mkdir(exist_ok=True)

        hls_dir  = out / "hls"
        dash_dir = out / "dash"
        hls_dir.mkdir(exist_ok=True)
        dash_dir.mkdir(exist_ok=True)

        # Une seule commande ffmpeg → HLS fMP4 + DASH + CMAF en parallèle
        cmd = (
            f"ffmpeg -i {input_path} "

            # ── Tee : duplique le flux vers plusieurs sorties ──
            f"-filter_complex \"[0:v]split=1[v1];[0:a]asplit=1[a1]\" "

            # ── HLS fMP4 ──
            f"-map \"[v1]\" -map \"[a1]\" "
            f"-c copy "
            f"-f hls "
            f"-hls_time 6 "
            f"-hls_playlist_type vod "
            f"-hls_segment_type fmp4 "
            f"-hls_fmp4_init_filename init.mp4 "
            f"-hls_segment_filename {hls_dir}/segment_%03d.m4s "
            f"-hls_flags independent_segments "
            f"{hls_dir}/playlist.m3u8 "

            # ── DASH + CMAF (même segments fMP4 réutilisés) ──
            f"-map 0:v -map 0:a "
            f"-c copy "
            f"-f dash "
            f"-seg_duration 6 "
            f"-use_template 1 "
            f"-use_timeline 1 "
            f"-movflags cmaf+frag_keyframe+empty_moov+default_base_moof "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"-hls_playlist 1 "                          # ← génère aussi un .m3u8 CMAF
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