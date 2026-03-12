"""
Entrypoints CLI – un fichier, quatre nœuds.

    python main.py ingest
    python main.py transcoder
    python main.py audio
    python main.py packager
"""
import sys
import uvicorn


def main():
    nodes = {
        "ingest":     _ingest,
        "transcoder": _transcoder,
        "audio":      _audio,
        "packager":   _packager,
    }

    node = sys.argv[1] if len(sys.argv) > 1 else "ingest"
    if node not in nodes:
        print(f"Unknown node '{node}'. Choose: {', '.join(nodes)}")
        sys.exit(1)

    app = nodes[node]()
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")


def _ingest():
    from processing_pipeline.ingest.app import create_ingest_app
    return create_ingest_app()


def _transcoder():
    from processing_pipeline.transcoder.app import create_transcoder_app
    return create_transcoder_app()


def _audio():
    from processing_pipeline.audio.app import create_audio_app
    return create_audio_app()


def _packager():
    from processing_pipeline.packager.app import create_packager_app
    return create_packager_app()


if __name__ == "__main__":
    main()