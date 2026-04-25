class LogisticsOperationsError(Exception):
    """Base exception for project-specific failures."""


class DestinationUnavailableError(LogisticsOperationsError):
    """Raised when the destination storage is unavailable."""

    def __init__(
        self,
        message: str = (
            "Nao ha fonte de destino disponivel para a ingestao. "
            "Suba o MinIO e tente novamente."
        ),
    ) -> None:
        super().__init__(message)


class DestinationAccessError(LogisticsOperationsError):
    """Raised when the destination storage responds but cannot be used."""
