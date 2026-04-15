# src/kraken_api/exceptions.py


class KrakenApiError(Exception):
    """Base exception for Kraken API errors."""

    def __init__(self, message: str, error_code: str | None = None):
        self.message = message
        self.error_code = error_code
        super().__init__(message)


class KrakenAuthError(KrakenApiError):
    """Raised when authentication fails (invalid API key/secret or permission denied)."""
    pass


class KrakenRateLimitError(KrakenApiError):
    """Raised when rate limit is exceeded."""
    pass


class KrakenInsufficientFundsError(KrakenApiError):
    """Raised when there are insufficient funds for an order."""
    pass


class KrakenOrderError(KrakenApiError):
    """Raised when order placement or validation fails."""
    pass


class KrakenInvalidPairError(KrakenApiError):
    """Raised when an invalid or unsupported trading pair is specified."""
    pass
