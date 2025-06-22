"""Base protocols and shared types for I/O layer."""

from typing import Protocol, runtime_checkable


class RangeNotSupportedError(RuntimeError):
    """Raised when server rejects Range and file size > RANGE_FALLBACK_MAX."""


RANGE_FALLBACK_MAX = 10 * 1024 * 1024  # 10 MB


@runtime_checkable
class ByteReader(Protocol):
    """Protocol for synchronous byte readers."""
    
    bytes_fetched: int  # running total

    def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`.
        If not enough data can be fetched → raise IOError.
        """
        ...


@runtime_checkable
class AsyncByteReader(Protocol):
    """Protocol for asynchronous byte readers."""
    
    bytes_fetched: int  # running total

    async def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`.
        If not enough data can be fetched → raise IOError.
        """
        ...
