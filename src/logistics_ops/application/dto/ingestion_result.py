from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IngestionResult:
    bucket: str
    prefix: str
    total_files: int
    uploaded_files: int
    skipped_files: int
