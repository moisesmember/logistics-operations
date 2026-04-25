from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DatasetAsset:
    relative_path: str
    local_path: Path
    content_type: str | None = None
