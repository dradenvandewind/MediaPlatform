from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.audio.worker import AudioWorker


def create_audio_app(worker: AudioWorker | None = None):
    worker = worker or AudioWorker()
    return create_worker_app(
        worker,
        title   = "OTT – Audio Encoder Node",
        version = "1.0.0",
    )
