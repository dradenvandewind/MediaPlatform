"""
Entrypoints CLI – un fichier, quatre nœuds.
    python main.py ingest
    python main.py transcoder
    python main.py audio
    python main.py packager
    python main.py vmaf
    python main.py live_moq
"""
import asyncio
import sys
import uvicorn


def main():
    nodes = {
        "ingest":     _ingest,
        "transcoder": _transcoder,
        "audio":      _audio,
        "packager":   _packager,
         "vmaf":       _vmaf,
    }

    node = sys.argv[1] if len(sys.argv) > 1 else "ingest"
    if node not in nodes:
        print(f"Unknown node '{node}'. Choose: {', '.join(nodes)}")
        sys.exit(1)

    app, worker = nodes[node]()

    async def start():
        #asyncio.create_task(worker.consume())  # ← active event loop here
        config = uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="debug")
        server = uvicorn.Server(config)
        await server.serve()

    asyncio.run(start())


def _ingest():
    from processing_pipeline.ingest.app import create_ingest_app
    from processing_pipeline.ingest.worker import IngestWorker
    worker = IngestWorker()
    return create_ingest_app(worker), worker  # ← tuple app + worker


def _transcoder():
    from processing_pipeline.transcoder.app import create_transcoder_app
    from processing_pipeline.transcoder.worker import TranscoderWorker
    worker = TranscoderWorker()
    return create_transcoder_app(worker), worker  # ← worker passé


def _audio():
    from processing_pipeline.audio.app import create_audio_app
    from processing_pipeline.audio.worker import AudioWorker
    worker = AudioWorker()
    return create_audio_app(worker), worker  # ← worker passé


def _packager():
    from processing_pipeline.packager.app import create_packager_app
    from processing_pipeline.packager.worker import PackagerWorker
    worker = PackagerWorker()
    return create_packager_app(worker), worker  # ← worker passé

""" def _vmaf():
    from processing_pipeline.vmaf.app import create_vmaf_app
    from processing_pipeline.vmaf.worker import VmafWorker
    worker = VmafWorker()
    return create_vmaf_app(worker), worker  # ← worker passé    
 """
 
def _live_ingest():
    from processing_pipeline.live_moq.app import create_live_ingest_app
    from processing_pipeline.live_moq.worker import LiveIngestWorker
    worker = LiveIngestWorker()
    return create_live_ingest_app(worker), worker

if __name__ == "__main__":
    main()