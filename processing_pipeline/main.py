"""
Entrypoints CLI – un fichier, quatre nœuds.

    python main.py ingest
    python main.py transcoder
    python main.py audio
    python main.py packager
"""
import sys
import importlib
import pkgutil
import uvicorn


def _find_package():
    """
    Retourne le nom du package racine, ou None si shared.app_factory est un top-level.
    """
    # Essai en top-level d'abord
    try:
        importlib.import_module("shared.app_factory")
        return None  # Pas de package racine, les modules sont top-level
    except ModuleNotFoundError:
        pass

    # Recherche d'un package contenant shared.app_factory
    for pkg in pkgutil.iter_modules():
        if pkg.ispkg:
            try:
                importlib.import_module(f"{pkg.name}.shared.app_factory")
                return pkg.name
            except ModuleNotFoundError:
                continue
    raise RuntimeError(
        "Impossible de trouver le package pipeline. "
        "Vérifiez que PYTHONPATH pointe vers la racine du projet."
    )


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
    pkg = _find_package()
    if pkg is None:
        mod = importlib.import_module("ingest.app")
    else:
        mod = importlib.import_module(f"{pkg}.ingest.app")
    return mod.create_ingest_app()



def _transcoder():
    pkg = _find_package()
    if pkg is None:
        mod = importlib.import_module("transcoder.app")
    else:
        mod = importlib.import_module(f"{pkg}.transcoder.app")
    return mod.create_transcoder_app()


def _audio():
    pkg = _find_package()
    if pkg is None:
        mod = importlib.import_module("audio.app")
    else:
        mod = importlib.import_module(f"{pkg}.audio.app")
        
    return mod.create_audio_app()


def _packager():
    pkg = _find_package()
    if pkg is None:
        mod = importlib.import_module("packager.app")
    else:
        mod = importlib.import_module(f"{pkg}.packager.app")
    return mod.create_packager_app()


if __name__ == "__main__":
    main()
