from __future__ import annotations
import base64
import struct
import warnings
from typing import ClassVar, Sequence

from ..core.parser_base import HeaderParser, Signature
from ..core.model import Result, ParseError
from ..core.util import dtype_from_code

_A2M = 1e-10  # Angstrom to meters


class MRCParser(HeaderParser):
    formats: ClassVar[tuple[str, ...]] = ("mrc", "map")
    signatures: ClassVar[Sequence[Signature]] = ((208, b"MAP "),)
    priority: ClassVar[int] = 10  # very early

    _HEADER_SIZE = 1024

    @classmethod
    def _parse_header(cls, header: bytes) -> dict:
        if len(header) < cls._HEADER_SIZE:
            raise ParseError("Header truncated (<1024 B)")
        
        # Check magic bytes
        magic = header[208:212]
        if magic != b"MAP ":
            warnings.warn(f"Invalid MRC magic bytes: {magic!r}")
        
        nx, ny, nz, mode = struct.unpack_from("<4i", header, 0)
        cella_x, cella_y, cella_z = struct.unpack_from("<3f", header, 40)

        # Get dtype string
        dtype = dtype_from_code(mode)
        if dtype is None:
            raise ParseError(f"Unsupported mode {mode}")

        # Calculate physical voxel sizes
        # physical spacing (Å → m) — axis-by-axis, ignore zeros
        sx = (cella_x * _A2M / nx) if (nx and cella_x) else None
        sy = (cella_y * _A2M / ny) if (ny and cella_y) else None
        sz = (cella_z * _A2M / nz) if (nz and cella_z) else None

        result = {
            "format": "MRC",
            "width": nx,
            "height": ny,
            "dtype": dtype,
        }
        
        # Only include depth if > 1
        if nz > 1:
            result["depth"] = nz
            
        # Only include physical sizes if they could be calculated
        if sx is not None:
            result["single_voxel_physical_size_x"] = sx
        if sy is not None:
            result["single_voxel_physical_size_y"] = sy
        if sz is not None:
            result["single_voxel_physical_size_z"] = sz

        return result

    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            if _prefetched_header and len(_prefetched_header) >= cls._HEADER_SIZE:
                hdr = _prefetched_header[:cls._HEADER_SIZE]
            else:
                hdr = reader.fetch(0, cls._HEADER_SIZE)
            meta = cls._parse_header(hdr)

            # Optional peek
            peek_b64 = None
            if bytes_peek and bytes_peek > 0:
                if bytes_peek <= cls._HEADER_SIZE:
                    peek = hdr[:bytes_peek]
                else:
                    extra = reader.fetch(cls._HEADER_SIZE, bytes_peek - cls._HEADER_SIZE)
                    peek = hdr + extra
                peek_b64 = base64.b64encode(peek).decode()

            if peek_b64:
                meta["peek_bytes_b64"] = peek_b64

            return Result(True, meta, None, reader.bytes_fetched)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))

    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            if _prefetched_header and len(_prefetched_header) >= cls._HEADER_SIZE:
                hdr = _prefetched_header[:cls._HEADER_SIZE]
            else:
                hdr = await reader.fetch(0, cls._HEADER_SIZE)
            meta = cls._parse_header(hdr)

            # Optional peek
            peek_b64 = None
            if bytes_peek and bytes_peek > 0:
                if bytes_peek <= cls._HEADER_SIZE:
                    peek = hdr[:bytes_peek]
                else:
                    extra = await reader.fetch(cls._HEADER_SIZE, bytes_peek - cls._HEADER_SIZE)
                    peek = hdr + extra
                peek_b64 = base64.b64encode(peek).decode()

            if peek_b64:
                meta["peek_bytes_b64"] = peek_b64

            return Result(True, meta, None, reader.bytes_fetched)
        except ParseError as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0))
