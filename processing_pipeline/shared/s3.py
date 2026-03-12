"""
S3Manager – gestionnaire S3 réutilisable (pattern OOP, session partagée).

Usage :
    async with S3Manager() as s3:
        uri = await s3.upload("/tmp/video.mp4", "raw/job123/source.mp4")
        meta = await s3.head("raw/job123/source.mp4")
"""
import logging
from pathlib import Path

import aioboto3

from processing_pipeline.shared.config import (
    CHUNK_SIZE,
    MULTIPART_THRESHOLD,
    S3_BUCKET,
    S3_REGION,
)
from processing_pipeline.shared.retry import async_retry

log = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, bucket: str = S3_BUCKET, region: str = S3_REGION):
        self.bucket  = bucket
        self.region  = region
        self._client = None

    async def __aenter__(self) -> "S3Manager":
        self._session = aioboto3.Session()
        self._ctx     = self._session.client("s3", region_name=self.region)
        self._s3      = await self._ctx.__aenter__()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self._ctx.__aexit__(*exc_info)

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    @async_retry()
    async def upload(
        self,
        local_path: str,
        s3_key: str,
        extra_meta: dict | None = None,
    ) -> str:
        """Upload simple ou multipart selon la taille du fichier."""
        size     = Path(local_path).stat().st_size
        metadata = {k: str(v) for k, v in (extra_meta or {}).items()}

        if size >= MULTIPART_THRESHOLD:
            await self._multipart_upload(local_path, s3_key, metadata)
        else:
            await self._s3.upload_file(
                local_path, self.bucket, s3_key,
                ExtraArgs={"Metadata": metadata},
            )

        uri = f"s3://{self.bucket}/{s3_key}"
        log.info("uploaded %s  (%.1f MB)", uri, size / 1024 / 1024)
        return uri

    @async_retry()
    async def download(self, s3_key: str, local_path: str) -> None:
        await self._s3.download_file(self.bucket, s3_key, local_path)
        log.info("downloaded s3://%s/%s → %s", self.bucket, s3_key, local_path)

    @async_retry()
    async def head(self, s3_key: str) -> dict:
        resp = await self._s3.head_object(Bucket=self.bucket, Key=s3_key)
        return {
            "size":          resp["ContentLength"],
            "last_modified": resp["LastModified"].isoformat(),
            "etag":          resp["ETag"],
            "metadata":      resp.get("Metadata", {}),
        }

    # ------------------------------------------------------------------ #
    #  Internals                                                           #
    # ------------------------------------------------------------------ #

    async def _multipart_upload(
        self, local_path: str, s3_key: str, metadata: dict
    ) -> None:
        mpu       = await self._s3.create_multipart_upload(
            Bucket=self.bucket, Key=s3_key, Metadata=metadata
        )
        upload_id = mpu["UploadId"]
        parts: list[dict] = []

        try:
            with open(local_path, "rb") as fh:
                part_num = 1
                while chunk := fh.read(CHUNK_SIZE):
                    resp = await self._s3.upload_part(
                        Bucket=self.bucket,
                        Key=s3_key,
                        UploadId=upload_id,
                        PartNumber=part_num,
                        Body=chunk,
                    )
                    parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})
                    log.debug("  part %d – %.0f KB", part_num, len(chunk) / 1024)
                    part_num += 1

            await self._s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        except Exception:
            await self._s3.abort_multipart_upload(
                Bucket=self.bucket, Key=s3_key, UploadId=upload_id
            )
            raise
