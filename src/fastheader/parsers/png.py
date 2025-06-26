

from __future__ import annotations

import base64
from typing import ClassVar

from ..core.model import Result, ParseError
from ..core.parser_base import HeaderParser

# PNG signature
PNG_SIG = b'\x89PNG\r\n\x1a\n'
IHDR_CHUNK_TYPE = b'IHDR'


class PNGParser(HeaderParser):
    """PNG header reader (width/height only)."""

    formats: ClassVar = ("png",)
    signatures: ClassVar = ((0, PNG_SIG),)
    priority: ClassVar = 40

    # ------------------------------------------------------------------ #
    @classmethod
    def _find_ihdr_sync(cls, reader, _prefetched_header: bytes | None = None) -> tuple[int, int, int]:
        """Return (width, height, final_offset) - sync version."""
        buf = bytearray()
        if _prefetched_header:
            buf.extend(_prefetched_header)
        
        if len(buf) < 24:
            try:
                chunk = reader.fetch(len(buf), 24 - len(buf))
                buf.extend(chunk)
            except (IOError, OSError):
                raise ParseError("File too small to be a valid PNG")

        if buf[:8] != PNG_SIG:
            raise ParseError("Invalid PNG signature")

        if buf[12:16] != IHDR_CHUNK_TYPE:
            raise ParseError("IHDR chunk not found")

        width = int.from_bytes(buf[16:20], "big")
        height = int.from_bytes(buf[20:24], "big")
        return width, height, 24

    # ------------------------------------------------------------------ #
    @classmethod
    def _build_result(cls, width: int, height: int, reader, bytes_peek: int | None, header_end: int) -> Result:
        meta = {
            "format": "PNG",
            "width": width,
            "height": height,
            "dtype": "uint8",  # Assuming 8-bit depth for now
        }

        # optional peek
        if bytes_peek and bytes_peek > 0:
            peek_len = min(bytes_peek, reader.size)
            if peek_len <= header_end:
                peek = reader.fetch(0, peek_len)
            else:
                peek = reader.fetch(0, header_end) + reader.fetch(header_end, peek_len - header_end)
            meta["peek_bytes_b64"] = base64.b64encode(peek).decode()

        return Result(True, meta, None, reader.bytes_fetched)

    # --------------------------- sync ---------------------------------- #
    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        try:
            w, h, end_off = cls._find_ihdr_sync(reader, _prefetched_header)
            return cls._build_result(w, h, reader, bytes_peek, end_off)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))

    # -------------------------- async ---------------------------------- #
    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        try:
            w, h, end_off = await cls._find_ihdr_async(reader, _prefetched_header)
            return cls._build_result(w, h, reader, bytes_peek, end_off)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))

    @classmethod
    async def _find_ihdr_async(cls, reader, _prefetched_header: bytes | None = None) -> tuple[int, int, int]:
        """Async version of _find_ihdr."""
        buf = bytearray()
        if _prefetched_header:
            buf.extend(_prefetched_header)

        if len(buf) < 24:
            try:
                chunk = await reader.fetch(len(buf), 24 - len(buf))
                buf.extend(chunk)
            except (IOError, OSError):
                raise ParseError("File too small to be a valid PNG")

        if buf[:8] != PNG_SIG:
            raise ParseError("Invalid PNG signature")

        if buf[12:16] != IHDR_CHUNK_TYPE:
            raise ParseError("IHDR chunk not found")

        width = int.from_bytes(buf[16:20], "big")
        height = int.from_bytes(buf[20:24], "big")
        return width, height, 24
