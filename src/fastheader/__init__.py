"""FastHeader - A python library for extracting file headers quickly."""

from .core.model import Result, UnknownFormatError, ParseError        # re-export
from .core.registry import _REGISTRY                                  # singleton
from .io import open_reader, open_reader_async                        # Task 1

# Import parsers to trigger registration
from .parsers import mrc  # noqa: F401


async def read_header(source, *, bytes_peek: int | None = None) -> Result:
    """Read header asynchronously from a source (path, URL, or file-like object)."""
    # 1) open reader + sniff first 4 KB (or whatever is available)
    reader = await open_reader_async(source)
    try:
        first_kb = await reader.fetch(0, 4096)
    except (IOError, OSError):
        # File might be smaller than 4KB, try to get what we can
        try:
            first_kb = await reader.fetch(0, 1024)
        except (IOError, OSError):
            # Even smaller, get first 512 bytes
            first_kb = await reader.fetch(0, 512)
    parser_cls = _REGISTRY.choose(source, first_kb)
    # 2) delegate - pass the already-fetched data to avoid double-fetch
    return await parser_cls.read(reader, bytes_peek=bytes_peek, _prefetched_header=first_kb)


def read_header_sync(source, *, bytes_peek: int | None = None) -> Result:
    """Read header synchronously from a source (path, URL, or file-like object)."""
    reader = open_reader(source)
    try:
        first_kb = reader.fetch(0, 4096)
    except (IOError, OSError):
        # File might be smaller than 4KB, try to get what we can
        try:
            first_kb = reader.fetch(0, 1024)
        except (IOError, OSError):
            # Even smaller, get first 512 bytes
            first_kb = reader.fetch(0, 512)
    parser_cls = _REGISTRY.choose(source, first_kb)
    return parser_cls.read_sync(reader, bytes_peek=bytes_peek, _prefetched_header=first_kb)


__all__ = [
    "read_header", "read_header_sync",
    "Result", "UnknownFormatError", "ParseError",
]
