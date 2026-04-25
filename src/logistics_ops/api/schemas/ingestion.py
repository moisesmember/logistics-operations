from pydantic import BaseModel


class IngestionResponse(BaseModel):
    bucket: str
    prefix: str
    total_files: int
    uploaded_files: int
    skipped_files: int
