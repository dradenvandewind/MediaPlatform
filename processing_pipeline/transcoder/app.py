from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.transcoder.worker import TranscoderWorker


def create_transcoder_app(worker: TranscoderWorker | None = None):
    worker = worker or TranscoderWorker()
    return create_worker_app(
        worker,
        title   = "OTT – Transcoder Node",
        version = "1.0.0",
    )
