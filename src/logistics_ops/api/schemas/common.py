from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(description="Estado atual da aplicação.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "ok",
            }
        }
    }


class ErrorResponse(BaseModel):
    detail: str = Field(description="Mensagem de erro amigável para o consumidor da API.")
    path: str = Field(description="Rota HTTP onde o erro ocorreu.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "detail": (
                    "Nao ha fonte de destino disponivel para a ingestao. "
                    "Suba o MinIO e tente novamente."
                ),
                "path": "/api/v1/ingestions/logistics-dataset/sync",
            }
        }
    }
