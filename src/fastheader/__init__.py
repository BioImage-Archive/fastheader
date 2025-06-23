"""FastHeader - A python library for extracting file headers quickly."""

from .core.model import Result, UnknownFormatError, ParseError        # re-export
from .core.registry import _REGISTRY                                  # singleton
from .io import open_reader, open_reader_async                        # Task 1


async def read_header(source, *, bytes_peek: int | None = None) -> Result:
    """Read header asynchronously from a source (path, URL, or file-like object)."""
    # 1) open reader + sniff first 4 KB
    reader = await open_reader_async(source)
    first_kb = await reader.fetch(0, 4096)
    parser_cls = _REGISTRY.choose(source, first_kb)
    # 2) delegate
    return await parser_cls.read(reader, bytes_peek=bytes_peek)


def read_header_sync(source, *, bytes_peek: int | None = None) -> Result:
    """Read header synchronously from a source (path, URL, or file-like object)."""
    reader = open_reader(source)
    first_kb = reader.fetch(0, 4096)
    parser_cls = _REGISTRY.choose(source, first_kb)
    return parser_cls.read_sync(reader, bytes_peek=bytes_peek)


__all__ = [
    "read_header", "read_header_sync",
    "Result", "UnknownFormatError", "ParseError",
]
