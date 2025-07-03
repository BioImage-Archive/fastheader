from __future__ import annotations

import base64
from typing import ClassVar

from ..core.model import Result, ParseError
from ..core.parser_base import HeaderParser

# Marker constants
SOI = b"\xFF\xD8"
EOI = b"\xFF\xD9"
RST0 = 0xD0
RST7 = 0xD7
SOF_RANGE = set(range(0xC0, 0xCF + 1)) - {0xC4, 0xC8, 0xCC}   # valid SOF*

_CHUNK = 4096
_CAP = 64 * 1024


class JPEGParser(HeaderParser):
    """Baseline & progressive JPEG header reader (width/height only)."""

    formats: ClassVar = ("jpg", "jpeg")
    signatures: ClassVar = ((0, SOI),)
    priority: ClassVar = 50

    # ------------------------------------------------------------------ #
    @classmethod
    def _find_sof_sync(cls, reader, _prefetched_header: bytes | None = None) -> tuple[int, int, int]:
        """Return (width, height, final_offset) - sync version."""
        buf = bytearray()
        offset = 0

        def _extend_sync(n: int):
            nonlocal offset
            if len(buf) < offset + n:
                to_fetch = offset + n - len(buf)
                if len(buf) + to_fetch > _CAP:
                    raise ParseError("SOF not found within 64 KiB")
                try:
                    chunk = reader.fetch(len(buf), to_fetch)
                    buf.extend(chunk)
                except (IOError, OSError):
                    # File might be smaller than requested, get what we can
                    try:
                        # Try to get remaining bytes in file
                        remaining = reader.fetch(len(buf), min(to_fetch, 1024))
                        buf.extend(remaining)
                        if len(buf) < offset + n:
                            raise ParseError("Unexpected end of file")
                    except (IOError, OSError):
                        raise ParseError("Unexpected end of file")

        # Use prefetched header if available
        if _prefetched_header:
            buf.extend(_prefetched_header)
        else:
            # initial pull - get what we can, starting with _CHUNK
            try:
                chunk = reader.fetch(0, _CHUNK)
            except (IOError, OSError):
                try:
                    chunk = reader.fetch(0, reader.size)
                except (IOError, OSError):
                    raise ParseError("File too small to be a valid JPEG")
            buf.extend(chunk)

        if len(buf) < 2 or buf[:2] != SOI:
            raise ParseError("Missing SOI marker")

        offset = 2
        while True:
            _extend_sync(4)
            
            if buf[offset] != 0xFF:
                raise ParseError("Marker sync lost")
            marker = buf[offset + 1]
            offset += 2

            # standalone markers (RST*, EOI, SOI) – no length field
            if marker == 0xD8 or marker == 0xD9 or RST0 <= marker <= RST7:
                continue

            _extend_sync(2)
            
            seg_len = int.from_bytes(buf[offset:offset + 2], "big")
            if seg_len < 2:
                raise ParseError("Invalid segment length")
            offset += 2

            if marker in SOF_RANGE:
                # make sure SOF body present
                _extend_sync(seg_len - 2)
                
                precision = buf[offset]
                height = int.from_bytes(buf[offset + 1:offset + 3], "big")
                width = int.from_bytes(buf[offset + 3:offset + 5], "big")
                if precision != 8:
                    # still return; note in future we may map dtype
                    pass
                return width, height, offset + seg_len - 2

            # skip this segment
            offset += seg_len - 2

    # ------------------------------------------------------------------ #
    @classmethod
    def _build_result(cls, width: int, height: int, reader, bytes_peek: int | None, header_end: int) -> Result:
        meta = {
            "format": "JPEG",
            "width": width,
            "height": height,
            "dtype": "uint8",
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
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            w, h, end_off = cls._find_sof_sync(reader, _prefetched_header)
            return cls._build_result(w, h, reader, bytes_peek, end_off)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))

    # -------------------------- async ---------------------------------- #
    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            w, h, end_off = await cls._find_sof_async(reader, _prefetched_header)
            return cls._build_result(w, h, reader, bytes_peek, end_off)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))

    @classmethod
    async def _find_sof_async(cls, reader, _prefetched_header: bytes | None = None) -> tuple[int, int, int]:
        """Async version of _find_sof."""
        buf = bytearray()
        offset = 0

        # helper to extend buffer
        async def _extend(n: int):
            nonlocal offset
            if len(buf) < offset + n:
                to_fetch = offset + n - len(buf)
                if len(buf) + to_fetch > _CAP:
                    raise ParseError("SOF not found within 64 KiB")
                try:
                    chunk = await reader.fetch(len(buf), to_fetch)
                    buf.extend(chunk)
                except (IOError, OSError):
                    # File might be smaller than requested, get what we can
                    try:
                        # Try to get remaining bytes in file
                        remaining = await reader.fetch(len(buf), min(to_fetch, 1024))
                        buf.extend(remaining)
                        if len(buf) < offset + n:
                            raise ParseError("Unexpected end of file")
                    except (IOError, OSError):
                        raise ParseError("Unexpected end of file")

        # Use prefetched header if available
        if _prefetched_header:
            buf.extend(_prefetched_header)
        else:
            # initial pull - get what we can, starting with _CHUNK
            try:
                chunk = await reader.fetch(0, _CHUNK)
            except (IOError, OSError):
                try:
                    chunk = await reader.fetch(0, reader.size)
                except (IOError, OSError):
                    raise ParseError("File too small to be a valid JPEG")
            buf.extend(chunk)

        if len(buf) < 2 or buf[:2] != SOI:
            raise ParseError("Missing SOI marker")

        offset = 2
        while True:
            await _extend(4)
            if buf[offset] != 0xFF:
                raise ParseError("Marker sync lost")
            marker = buf[offset + 1]
            offset += 2

            # standalone markers (RST*, EOI, SOI) – no length field
            if marker == 0xD8 or marker == 0xD9 or RST0 <= marker <= RST7:
                continue

            await _extend(2)
            seg_len = int.from_bytes(buf[offset:offset + 2], "big")
            if seg_len < 2:
                raise ParseError("Invalid segment length")
            offset += 2

            if marker in SOF_RANGE:
                # make sure SOF body present
                await _extend(seg_len - 2)
                precision = buf[offset]
                height = int.from_bytes(buf[offset + 1:offset + 3], "big")
                width = int.from_bytes(buf[offset + 3:offset + 5], "big")
                if precision != 8:
                    # still return; note in future we may map dtype
                    pass
                return width, height, offset + seg_len - 2

            # skip this segment
            offset += seg_len - 2
