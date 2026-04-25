from pydantic import BaseModel, Field


class IngestionResponse(BaseModel):
    bucket: str = Field(description="Bucket de destino no MinIO.")
    prefix: str = Field(description="Prefixo utilizado para organizar os objetos do dataset.")
    total_files: int = Field(description="Quantidade total de arquivos descobertos no dataset.")
    uploaded_files: int = Field(description="Quantidade de arquivos enviados para o MinIO.")
    skipped_files: int = Field(
        description="Quantidade de arquivos ignorados porque já existiam no MinIO."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "bucket": "logistics-lake",
                "prefix": "raw/kaggle/yogape/logistics-operations-database",
                "total_files": 15,
                "uploaded_files": 10,
                "skipped_files": 5,
            }
        }
    }
