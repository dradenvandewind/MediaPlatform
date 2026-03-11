import os
import sys
from pathlib import Path
from setuptools import setup
from Cython.Build import cythonize
from setuptools.extension import Extension

modules = []
for path in Path("app").rglob("*.py"):
    if path.name == "__init__.py":
        continue
    module = str(path.with_suffix("")).replace(os.sep, ".")
    modules.append(module)

if not modules:
    print("ERROR: no .py files found under app/", file=sys.stderr)
    sys.exit(1)

print("Modules to cythonize:")
for m in modules:
    print(" -", m)

extensions = []
for m in modules:
    parts = m.split(".")
    filepath = os.path.join(*parts) + ".py"
    extensions.append(Extension(m, [filepath]))

setup(
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
        },
        nthreads=4,
    )
)
