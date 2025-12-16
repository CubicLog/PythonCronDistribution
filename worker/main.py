from __future__ import annotations
import asyncio
import sys
from pathlib import Path
import importlib.util
from modules import get_registry
from core.redis import init_redis, close_redis

def import_modules_from_dir(root: Path, prefix: str = "modules") -> None:
    root = root.resolve()

    for py in root.rglob("*.py"):
        if py.name == "__init__.py":
            continue

        rel = py.relative_to(root).with_suffix("")  # e.g. nested/foo
        modname = prefix + "." + ".".join(rel.parts)  # modules.nested.foo

        spec = importlib.util.spec_from_file_location(modname, py)
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[modname] = module  # important so imports/reference work
        spec.loader.exec_module(module)

async def main():
    try:
        await init_redis()

        import_modules_from_dir(Path(__file__).parent / "modules")
        
        tasks_meta = get_registry()
        if not tasks_meta:
            raise RuntimeError("No scheduled tasks registered in modules/")

        print("Discovered:", [t.name for t in tasks_meta])
    finally:
        await close_redis()

if __name__ == "__main__":
    asyncio.run(main())