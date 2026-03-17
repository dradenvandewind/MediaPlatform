from processing_pipeline.shared.app_factory import create_worker_app
from processing_pipeline.vmaf.worker import VmafWorker


def create_vmaf_app(worker: VmafWorker | None = None):
    worker = worker or VmafWorker()
    return create_worker_app(
        worker,
        title   = "OTT – Vmaf Node",
        version = "1.0.0",
    )
