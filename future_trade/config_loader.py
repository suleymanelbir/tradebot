import json, asyncio
from pathlib import Path

async def load_config(path: Path):
    return await asyncio.to_thread(lambda: json.loads(Path(path).read_text(encoding="utf-8")))
