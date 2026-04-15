import importlib
from pathlib import Path

# Auto-import all tool modules so their @register_tool decorators run.
for _f in Path(__file__).parent.glob("*.py"):
    if _f.name not in ("__init__.py", "base.py"):
        importlib.import_module(f".{_f.stem}", __package__)

# Auto-import sub-packages (e.g. historical/) so nested @register_tool decorators run.
for _d in Path(__file__).parent.iterdir():
    if _d.is_dir() and (_d / "__init__.py").exists() and _d.name != "__pycache__":
        importlib.import_module(f".{_d.name}", __package__)

from .base import TOOL_REGISTRY, execute_tool, get_ollama_tools  # noqa: E402, F401
