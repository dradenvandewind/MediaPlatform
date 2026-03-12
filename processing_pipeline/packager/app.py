from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.packager.worker import PackagerWorker


def create_packager_app(worker: PackagerWorker | None = None):
    worker = worker or PackagerWorker()
    return create_worker_app(
        worker,
        title   = "OTT – Packager Node",
        version = "1.0.0",
    )
