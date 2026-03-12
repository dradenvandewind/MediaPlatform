"""Configuration centralisée via variables d'environnement."""
import os

S3_BUCKET           = os.getenv("S3_BUCKET", "my-ott-bucket")
S3_REGION           = os.getenv("AWS_REGION", "eu-west-1")
ORCHESTRATOR_URL    = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:6379")

MULTIPART_THRESHOLD = int(os.getenv("MULTIPART_THRESHOLD", str(100 * 1024 * 1024)))  # 100 MB
CHUNK_SIZE          = int(os.getenv("CHUNK_SIZE",          str(8   * 1024 * 1024)))  #   8 MB
MAX_RETRIES         = int(os.getenv("MAX_RETRIES",         "3"))
RETRY_DELAY         = float(os.getenv("RETRY_DELAY",       "1.0"))
