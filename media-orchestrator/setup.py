import os
from pathlib import Path

modules = []
for path in Path("app").rglob("*.py"):
    if path.name == "__init__.py":
        continue
    module = str(path.with_suffix("")).replace(os.sep, ".")
    modules.append(module)

lines = []
for m in modules:
    parts = m.split(".")
    filepath = os.path.join(*parts) + ".py"
    lines.append('    Extension("' + m + '", ["' + filepath + '"]),')

extensions_block = "\n".join(lines)

setup_content = (
    "from setuptools import setup\n"
    "from Cython.Build import cythonize\n"
    "from setuptools.extension import Extension\n\n"
    "extensions = [\n"
    + extensions_block + "\n"
    "]\n\n"
    "setup(\n"
    "    ext_modules=cythonize(\n"
    "        extensions,\n"
    "        compiler_directives={\n"
    '            "language_level": "3",\n'
    '            "boundscheck": False,\n'
    '            "wraparound": False,\n'
    "        },\n"
    "        nthreads=4,\n"
    "    )\n"
    ")\n"
)

with open("setup.py", "w") as f:
    f.write(setup_content)

print("setup.py generated with modules:")
for m in modules:
    print(" -", m)