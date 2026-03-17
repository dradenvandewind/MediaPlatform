"""VmafWorker – calcule les scores VMAF pour chaque profil transcodé."""
import asyncio
import json
import logging
import re
import tempfile
from pathlib import Path

from processing_pipeline.shared.s3 import S3Manager
from processing_pipeline.shared.worker import BaseWorker

log = logging.getLogger(__name__)

VMAF_BITRATES = [500, 800, 1200, 1800, 2500, 3500, 5000]


class VmafWorker(BaseWorker):
    def __init__(self, **kwargs):
        super().__init__(node_type="vmaf", **kwargs)

    async def run(self, job_id: str, job_data: dict) -> dict:
        s3_key   = job_data["s3_key"]          # source originale
        bitrates = job_data.get("vmaf_bitrates", VMAF_BITRATES)

        results: list[dict] = []

        async with S3Manager(self.s3_bucket, self.s3_region) as s3:
            with tempfile.TemporaryDirectory(prefix=f"vmaf_{job_id}_") as tmpdir:
                # 1. Télécharger la source originale
                original = f"{tmpdir}/original.mp4"
                await s3.download(s3_key, original)
                log.info("[%s] source downloaded → %s", job_id, original)

                for br in bitrates:
                    encoded  = f"{tmpdir}/encoded_{br}.mp4"
                    vmaf_log = f"{tmpdir}/vmaf_{br}.json"

                    # 2. Encoder à ce bitrate
                    await self._encode(original, encoded, br)

                    # 3. Calculer VMAF
                    score = await self._vmaf(encoded, original, vmaf_log)

                    # 4. Upload encoded + rapport VMAF sur S3
                    enc_key  = f"vmaf/{job_id}/encoded_{br}k.mp4"
                    log_key  = f"vmaf/{job_id}/vmaf_{br}k.json"
                    await s3.upload(encoded,  enc_key)
                    await s3.upload(vmaf_log, log_key)

                    results.append({
                        "bitrate_kbps": br,
                        "vmaf_score":   score,
                        "encoded_key":  enc_key,
                        "report_key":   log_key,
                    })
                    log.info("[vmaf] bitrate=%dk → score=%.2f", br, score)

        # Bitrate optimal = meilleur compromis score/bitrate
        optimal = self._pick_optimal(results)
        log.info("[%s] optimal bitrate=%dk (score=%.2f)", job_id,
                 optimal["bitrate_kbps"], optimal["vmaf_score"])

        return {
            "vmaf_results":      results,
            "optimal_bitrate":   optimal,
        }

    # ── Internals ─────────────────────────────────────────────────────────

    async def _encode(self, input_path: str, output_path: str, bitrate_kbps: int) -> None:
        cmd = [
            "ffmpeg", "-y", "-i", input_path,
            "-vf", "scale=-2:720",
            "-c:v", "libx264",
            "-b:v", f"{bitrate_kbps}k",
            "-preset", "slow",
            "-an",          # pas d'audio pour le test VMAF
            output_path,
        ]
        await self._run(cmd, f"encode@{bitrate_kbps}k")

    async def _vmaf(self, encoded: str, reference: str, log_path: str) -> float:
        cmd = [
            "ffmpeg", "-i", encoded, "-i", reference,
            "-lavfi", f"libvmaf=log_fmt=json:log_path={log_path}:n_threads=4",
            "-f", "null", "-",
        ]
        await self._run(cmd, "vmaf")

        # Parser le score depuis le JSON produit par libvmaf
        with open(log_path) as fh:
            data = json.load(fh)
        score = data["pooled_metrics"]["vmaf"]["mean"]
        return round(score, 3)

    async def _run(self, cmd: list[str], label: str) -> None:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"ffmpeg [{label}] failed: {stderr.decode()[-500:]}")

    @staticmethod
    def _pick_optimal(results: list[dict]) -> dict:
        """
        Sélectionne le bitrate le plus bas avec VMAF >= 93,
        sinon le meilleur score disponible.
        """
        candidates = [r for r in results if r["vmaf_score"] >= 93.0]
        if candidates:
            return min(candidates, key=lambda r: r["bitrate_kbps"])
        return max(results, key=lambda r: r["vmaf_score"])