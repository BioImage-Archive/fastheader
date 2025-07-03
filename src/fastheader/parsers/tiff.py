from __future__ import annotations
import base64
import struct
import warnings
from typing import ClassVar, Sequence

from ..core.parser_base import HeaderParser, Signature
from ..core.model import Result, ParseError
from ..core.util import dtype_from_code

class TIFFParser(HeaderParser):
    formats: ClassVar[tuple[str, ...]] = ("tiff", "tif")
    signatures: ClassVar[Sequence[Signature]] = (
        (0, b"II*\x00"),  # Little-endian
        (0, b"MM\x00*"),  # Big-endian
    )
    priority: ClassVar[int] = 10

    _HEADER_SIZE = 8 # TIFF header is 8 bytes

    _FIELD_TYPE_SIZES = {
        1: 1,  # BYTE
        2: 1,  # ASCII
        3: 2,  # SHORT
        4: 4,  # LONG
        5: 8,  # RATIONAL
        6: 1,  # SBYTE
        7: 1,  # UNDEFINED
        8: 2,  # SSHORT
        9: 4,  # SLONG
        10: 8, # SRATIONAL
        11: 4, # FLOAT
        12: 8, # DOUBLE
    }

    @classmethod
    def _get_value_sync(cls, reader, endian: str, field_type: int, count: int, value_offset: bytes):
        data_size = cls._FIELD_TYPE_SIZES.get(field_type, 0) * count
        if data_size == 0:
            return None

        if data_size <= 4:
            value_bytes = value_offset
        else:
            offset = struct.unpack(endian + "I", value_offset)[0]
            value_bytes = reader.fetch(offset, data_size)
            if len(value_bytes) < data_size:
                warnings.warn(f"Fetched less data than expected for IFD value. Expected {data_size}, got {len(value_bytes)}")
                return None

        if field_type == 3: # SHORT
            return struct.unpack(endian + "H", value_bytes[:2])[0]
        elif field_type == 4: # LONG
            return struct.unpack(endian + "I", value_bytes[:4])[0]
        return None

    @classmethod
    async def _get_value_async(cls, reader, endian: str, field_type: int, count: int, value_offset: bytes):
        data_size = cls._FIELD_TYPE_SIZES.get(field_type, 0) * count
        if data_size == 0:
            return None

        if data_size <= 4:
            value_bytes = value_offset
        else:
            offset = struct.unpack(endian + "I", value_offset)[0]
            value_bytes = await reader.fetch(offset, data_size)
            if len(value_bytes) < data_size:
                warnings.warn(f"Fetched less data than expected for IFD value. Expected {data_size}, got {len(value_bytes)}")
                return None

        if field_type == 3: # SHORT
            return struct.unpack(endian + "H", value_bytes[:2])[0]
        elif field_type == 4: # LONG
            return struct.unpack(endian + "I", value_bytes[:4])[0]
        return None

    @classmethod
    def _parse_ifd_sync(cls, ifd_data: bytes, endian: str, reader) -> tuple[int | None, int | None, str | None, dict, int]:
        width, height, bits_per_sample = None, None, None
        tags = {}
        num_entries = struct.unpack(endian + "H", ifd_data[:2])[0]
        ifd_entries_start = 2

        for i in range(num_entries):
            entry_offset = ifd_entries_start + i * 12
            entry_slice = ifd_data[entry_offset:entry_offset+12]
            if len(entry_slice) < 12:
                warnings.warn(f"IFD entry slice too short. Expected 12 bytes, got {len(entry_slice)}")
                break
            tag, field_type, count, value_offset = struct.unpack(endian + "HHII", entry_slice)
            value = cls._get_value_sync(reader, endian, field_type, count, value_offset.to_bytes(4, byteorder=endian.replace('<', 'little').replace('>', 'big')))
            tags[tag] = value

            if tag == 256: width = value
            elif tag == 257: height = value
            elif tag == 258: bits_per_sample = value

        next_ifd_offset_offset = ifd_entries_start + num_entries * 12
        next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset:next_ifd_offset_offset+4]
        next_ifd_offset = struct.unpack(endian + "I", next_ifd_offset_bytes)[0]

        dtype = None
        if bits_per_sample in (8, 16, 32):
            dtype = f"uint{bits_per_sample}"
        return width, height, dtype, tags, next_ifd_offset

    @classmethod
    async def _parse_ifd_async(cls, ifd_data: bytes, endian: str, reader) -> tuple[int | None, int | None, str | None, dict, int]:
        width, height, bits_per_sample = None, None, None
        tags = {}
        num_entries = struct.unpack(endian + "H", ifd_data[:2])[0]
        ifd_entries_start = 2

        for i in range(num_entries):
            entry_offset = ifd_entries_start + i * 12
            entry_slice = ifd_data[entry_offset:entry_offset+12]
            if len(entry_slice) < 12:
                warnings.warn(f"IFD entry slice too short. Expected 12 bytes, got {len(entry_slice)}")
                break
            tag, field_type, count, value_offset = struct.unpack(endian + "HHII", entry_slice)
            value = await cls._get_value_async(reader, endian, field_type, count, value_offset.to_bytes(4, byteorder=endian.replace('<', 'little').replace('>', 'big')))
            tags[tag] = value

            if tag == 256: width = value
            elif tag == 257: height = value
            elif tag == 258: bits_per_sample = value

        next_ifd_offset_offset = ifd_entries_start + num_entries * 12
        next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset:next_ifd_offset_offset+4]
        next_ifd_offset = struct.unpack(endian + "I", next_ifd_offset_bytes)[0]

        dtype = None
        if bits_per_sample in (8, 16, 32):
            dtype = f"uint{bits_per_sample}"
        return width, height, dtype, tags, next_ifd_offset

    @classmethod
    def _count_ifds_sync(cls, reader, endian: str, first_ifd_offset: int) -> int:
        ifd_count = 0
        next_ifd_offset = first_ifd_offset
        while next_ifd_offset != 0:
            ifd_count += 1
            ifd_count_bytes = reader.fetch(next_ifd_offset, 2)
            if len(ifd_count_bytes) < 2:
                break
            num_entries = struct.unpack(endian + "H", ifd_count_bytes)[0]
            ifd_size = 2 + num_entries * 12 + 4
            ifd_data = reader.fetch(next_ifd_offset, ifd_size)
            if len(ifd_data) < ifd_size:
                break
            next_ifd_offset_offset = 2 + num_entries * 12
            next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset:next_ifd_offset_offset+4]
            next_ifd_offset = struct.unpack(endian + "I", next_ifd_offset_bytes)[0]
        return ifd_count

    @classmethod
    async def _count_ifds_async(cls, reader, endian: str, first_ifd_offset: int) -> int:
        ifd_count = 0
        next_ifd_offset = first_ifd_offset
        while next_ifd_offset != 0:
            ifd_count += 1
            ifd_count_bytes = await reader.fetch(next_ifd_offset, 2)
            if len(ifd_count_bytes) < 2:
                break
            num_entries = struct.unpack(endian + "H", ifd_count_bytes)[0]
            ifd_size = 2 + num_entries * 12 + 4
            ifd_data = await reader.fetch(next_ifd_offset, ifd_size)
            if len(ifd_data) < ifd_size:
                break
            next_ifd_offset_offset = 2 + num_entries * 12
            next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset:next_ifd_offset_offset+4]
            next_ifd_offset = struct.unpack(endian + "I", next_ifd_offset_bytes)[0]
        return ifd_count

    @classmethod
    def _parse_header_sync(cls, header: bytes, reader, count_ifds: bool) -> dict:
        if len(header) < cls._HEADER_SIZE:
            raise ParseError("Header truncated (<8 B)")

        byte_order_indicator = header[0:2]
        endian = {"II": "<", "MM": ">"}.get(byte_order_indicator.decode('ascii', 'ignore'))
        if not endian:
            raise ParseError(f"Invalid TIFF byte order indicator: {byte_order_indicator!r}")

        magic_number = struct.unpack(endian + "H", header[2:4])[0]
        if magic_number != 42:
            raise ParseError(f"Invalid TIFF magic number: {magic_number}")

        first_ifd_offset = struct.unpack(endian + "I", header[4:8])[0]
        ifd_count = cls._count_ifds_sync(reader, endian, first_ifd_offset) if count_ifds else None

        ifd_count_bytes = reader.fetch(first_ifd_offset, 2)
        if len(ifd_count_bytes) < 2:
            raise ParseError("Not enough data to read IFD entry count.")

        num_entries = struct.unpack(endian + "H", ifd_count_bytes)[0]
        ifd_size = 2 + num_entries * 12 + 4
        ifd_data = reader.fetch(first_ifd_offset, ifd_size)
        if len(ifd_data) < ifd_size:
            warnings.warn(f"Fetched less IFD data than expected. Expected {ifd_size}, got {len(ifd_data)}")

        width, height, dtype, tags, _ = cls._parse_ifd_sync(ifd_data, endian, reader)
        if not all((width, height, dtype)):
            warnings.warn("Could not parse all required TIFF IFD tags (Width, Height, BitsPerSample).")

        return {"format": "TIFF", "width": width, "height": height, "dtype": dtype, "tags": tags, "ifd_count": ifd_count}

    @classmethod
    async def _parse_header_async(cls, header: bytes, reader, count_ifds: bool) -> dict:
        if len(header) < cls._HEADER_SIZE:
            raise ParseError("Header truncated (<8 B)")

        byte_order_indicator = header[0:2]
        endian = {"II": "<", "MM": ">"}.get(byte_order_indicator.decode('ascii', 'ignore'))
        if not endian:
            raise ParseError(f"Invalid TIFF byte order indicator: {byte_order_indicator!r}")

        magic_number = struct.unpack(endian + "H", header[2:4])[0]
        if magic_number != 42:
            raise ParseError(f"Invalid TIFF magic number: {magic_number}")

        first_ifd_offset = struct.unpack(endian + "I", header[4:8])[0]
        ifd_count = await cls._count_ifds_async(reader, endian, first_ifd_offset) if count_ifds else None

        ifd_count_bytes = await reader.fetch(first_ifd_offset, 2)
        if len(ifd_count_bytes) < 2:
            raise ParseError("Not enough data to read IFD entry count.")

        num_entries = struct.unpack(endian + "H", ifd_count_bytes)[0]
        ifd_size = 2 + num_entries * 12 + 4
        ifd_data = await reader.fetch(first_ifd_offset, ifd_size)
        if len(ifd_data) < ifd_size:
            warnings.warn(f"Fetched less IFD data than expected. Expected {ifd_size}, got {len(ifd_data)}")

        width, height, dtype, tags, _ = await cls._parse_ifd_async(ifd_data, endian, reader)
        if not all((width, height, dtype)):
            warnings.warn("Could not parse all required TIFF IFD tags (Width, Height, BitsPerSample).")

        return {"format": "TIFF", "width": width, "height": height, "dtype": dtype, "tags": tags, "ifd_count": ifd_count}

    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            count_ifds = kwargs.get("count_ifds", False)
            hdr = _prefetched_header[:cls._HEADER_SIZE] if _prefetched_header and len(_prefetched_header) >= cls._HEADER_SIZE else reader.fetch(0, cls._HEADER_SIZE)
            meta = cls._parse_header_sync(hdr, reader, count_ifds)

            if bytes_peek and bytes_peek > 0:
                peek = hdr[:bytes_peek] if bytes_peek <= cls._HEADER_SIZE else hdr + reader.fetch(cls._HEADER_SIZE, bytes_peek - cls._HEADER_SIZE)
                meta["peek_bytes_b64"] = base64.b64encode(peek).decode()

            return Result(True, meta, None, reader.bytes_fetched, reader.requests_made)
        except (ParseError, struct.error) as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))

    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            count_ifds = kwargs.get("count_ifds", False)
            hdr = _prefetched_header[:cls._HEADER_SIZE] if _prefetched_header and len(_prefetched_header) >= cls._HEADER_SIZE else await reader.fetch(0, cls._HEADER_SIZE)
            meta = await cls._parse_header_async(hdr, reader, count_ifds)

            if bytes_peek and bytes_peek > 0:
                peek = hdr[:bytes_peek] if bytes_peek <= cls._HEADER_SIZE else hdr + await reader.fetch(cls._HEADER_SIZE, bytes_peek - cls._HEADER_SIZE)
                meta["peek_bytes_b64"] = base64.b64encode(peek).decode()

            return Result(True, meta, None, reader.bytes_fetched, reader.requests_made)
        except (ParseError, struct.error) as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))


