"""PackagerWorker – produit les manifests HLS, DASH et CMAF."""
import asyncio
import logging
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)


class PackagerWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="packager", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        muxed_files = job_data["muxed_files"]
        results: dict[str, list[str]] = {"hls": [], "dash": [], "cmaf": []}

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            for key in muxed_files:
                local = f"/tmp/{Path(key).name}"
                await s3.download(key, local)

                results["hls"].append(await self._hls(local, s3))
                results["dash"].append(await self._dash(local, s3))
                results["cmaf"].append(await self._cmaf(local, s3))

        return results

    # ------------------------------------------------------------------ #

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
        await self._run_cmd(
            f"ffmpeg -i {input_path} -c copy -f hls -hls_time 6 "
            f"-hls_playlist_type vod "
            f"-hls_segment_filename {out}/segment_%03d.ts {out}/playlist.m3u8"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"hls/{stem}/{f.name}")
        log.info("HLS packaged → hls/%s/playlist.m3u8", stem)
        return f"hls/{stem}/playlist.m3u8"

    async def _dash(self, input_path: str, s3: S3Manager) -> str:
        stem = Path(input_path).stem
        out  = Path(f"/tmp/dash_{stem}")
        out.mkdir(exist_ok=True)
        # ffmpeg produit un MPD + segments fmp4
        await self._run_cmd(
            f"ffmpeg -i {input_path} "
            f"-c copy "
            f"-f dash "
            f"-seg_duration 4 "
            f"-use_template 1 -use_timeline 1 "
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
        # CMAF = fMP4 fragmenté compatible HLS + DASH
        await self._run_cmd(
            f"ffmpeg -i {input_path} "
            f"-c copy "
            f"-f dash "
            f"-seg_duration 4 "
            f"-use_template 1 -use_timeline 1 "
            f"-movflags cmaf+frag_keyframe "
            f"-adaptation_sets \"id=0,streams=v id=1,streams=a\" "
            f"{out}/manifest.mpd"
        )
        for f in out.glob("*"):
            await s3.upload(str(f), f"cmaf/{stem}/{f.name}")
        log.info("CMAF packaged → cmaf/%s/manifest.mpd", stem)
        return f"cmaf/{stem}/manifest.mpd"
