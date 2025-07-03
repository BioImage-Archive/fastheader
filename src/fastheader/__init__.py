"""FastHeader - A python library for extracting file headers quickly."""

from .core.model import Result, UnknownFormatError, ParseError        # re-export
from .core.registry import _REGISTRY                                  # singleton
from .io import open_reader, open_reader_async                        # Task 1

# Import parsers to trigger registration
from .parsers import mrc, jpeg, tiff, png  # noqa: F401


async def read_header(source, *, bytes_peek: int | None = None, **parser_options) -> Result:
    """Read header asynchronously from a source (path, URL, or file-like object)."""
    # 1) open reader + sniff first 4 KB (or whatever is available)
    reader = await open_reader_async(source)
    first_kb = b''
    sniff_sizes_to_try = [4096, 1024, 512, 256, 128, 64]

    for size_attempt in sniff_sizes_to_try:
        if size_attempt <= 0:
            continue
        try:
            first_kb = await reader.fetch(0, size_attempt)
            break  # Success
        except (IOError, OSError) as e:
            err_msg = str(e).lower()
            # If file is smaller or empty, try next smaller size. Otherwise, re-raise.
            if not ("not enough data" in err_msg or "requested" in err_msg or "empty file" in err_msg):
                raise
    # If all attempts failed (e.g. file is < 64 bytes or empty), first_kb remains b''.
    # Parsers must handle small/empty _prefetched_header.

    parser_cls = _REGISTRY.choose(source, first_kb)
    # 2) delegate - pass the already-fetched data to avoid double-fetch
    return await parser_cls.read(reader, bytes_peek=bytes_peek, _prefetched_header=first_kb, **parser_options)


def read_header_sync(source, *, bytes_peek: int | None = None, **parser_options) -> Result:
    """Read header synchronously from a source (path, URL, or file-like object)."""
    reader = open_reader(source)
    first_kb = b''
    sniff_sizes_to_try = [4096, 1024, 512, 256, 128, 64]

    for size_attempt in sniff_sizes_to_try:
        if size_attempt <= 0:
            continue
        try:
            first_kb = reader.fetch(0, size_attempt)
            break  # Success
        except (IOError, OSError) as e:
            err_msg = str(e).lower()
            # If file is smaller or empty, try next smaller size. Otherwise, re-raise.
            if not ("not enough data" in err_msg or "requested" in err_msg or "empty file" in err_msg):
                raise
    # If all attempts failed (e.g. file is < 64 bytes or empty), first_kb remains b''.
    # Parsers must handle small/empty _prefetched_header.

    parser_cls = _REGISTRY.choose(source, first_kb)
    return parser_cls.read_sync(reader, bytes_peek=bytes_peek, _prefetched_header=first_kb, **parser_options)


__all__ = [
    "read_header", "read_header_sync",
    "Result", "UnknownFormatError", "ParseError",
]
