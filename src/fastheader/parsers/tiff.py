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
        (0, b"II+\x00"),  # BigTIFF Little-endian
        (0, b"MM\x00+"),  # BigTIFF Big-endian
    )
    priority: ClassVar[int] = 10

    _CLASSIC_HEADER_SIZE = 8
    _BIGTIFF_HEADER_SIZE = 16

    _FIELD_TYPE_SIZES = {
        1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1,
        8: 2, 9: 4, 10: 8, 11: 4, 12: 8, 16: 8, 17: 8, 18: 8
    }

    @classmethod
    def _get_value(cls, reader, endian: str, field_type: int, count: int, value_offset: bytes, is_bigtiff: bool):
        offset_size = 8 if is_bigtiff else 4
        data_size = cls._FIELD_TYPE_SIZES.get(field_type, 0) * count
        if data_size == 0:
            return None

        if data_size <= offset_size:
            value_bytes = value_offset[:data_size]
        else:
            offset_format = endian + ('Q' if is_bigtiff else 'I')
            offset = struct.unpack(offset_format, value_offset)[0]
            value_bytes = reader.fetch(offset, data_size)
            if len(value_bytes) < data_size:
                warnings.warn(f"Fetched less data than expected for IFD value. Expected {data_size}, got {len(value_bytes)}")
                return None

        if field_type == 3: # SHORT
            return struct.unpack(endian + "H", value_bytes[:2])[0]
        elif field_type == 4: # LONG
            return struct.unpack(endian + "I", value_bytes[:4])[0]
        elif field_type == 16: # LONG8
            return struct.unpack(endian + "Q", value_bytes[:8])[0]
        return None

    @classmethod
    async def _get_value_async(cls, reader, endian: str, field_type: int, count: int, value_offset: bytes, is_bigtiff: bool):
        offset_size = 8 if is_bigtiff else 4
        data_size = cls._FIELD_TYPE_SIZES.get(field_type, 0) * count
        if data_size == 0:
            return None

        if data_size <= offset_size:
            value_bytes = value_offset[:data_size]
        else:
            offset_format = endian + ('Q' if is_bigtiff else 'I')
            offset = struct.unpack(offset_format, value_offset)[0]
            value_bytes = await reader.fetch(offset, data_size)
            if len(value_bytes) < data_size:
                warnings.warn(f"Fetched less data than expected for IFD value. Expected {data_size}, got {len(value_bytes)}")
                return None

        if field_type == 3: # SHORT
            return struct.unpack(endian + "H", value_bytes[:2])[0]
        elif field_type == 4: # LONG
            return struct.unpack(endian + "I", value_bytes[:4])[0]
        elif field_type == 16: # LONG8
            return struct.unpack(endian + "Q", value_bytes[:8])[0]
        return None

    @classmethod
    def _parse_ifd(cls, ifd_data: bytes, endian: str, reader, is_bigtiff: bool):
        width, height, bits_per_sample = None, None, None
        tags = {}
        
        if is_bigtiff:
            num_entries = struct.unpack(endian + "Q", ifd_data[:8])[0]
            ifd_entries_start = 8
            entry_size = 20
            entry_format = endian + "HHQQ"
        else:
            num_entries = struct.unpack(endian + "H", ifd_data[:2])[0]
            ifd_entries_start = 2
            entry_size = 12
            entry_format = endian + "HHII"

        for i in range(num_entries):
            entry_offset = ifd_entries_start + i * entry_size
            entry_slice = ifd_data[entry_offset : entry_offset + entry_size]
            if len(entry_slice) < entry_size:
                warnings.warn(f"IFD entry slice too short. Expected {entry_size} bytes, got {len(entry_slice)}")
                break
            
            tag, field_type, count, value_or_offset = struct.unpack(entry_format, entry_slice)
            offset_size = 8 if is_bigtiff else 4
            value = cls._get_value(reader, endian, field_type, count, value_or_offset.to_bytes(offset_size, byteorder=endian.replace('<', 'little').replace('>', 'big')), is_bigtiff)
            tags[tag] = value

            if tag == 256: width = value
            elif tag == 257: height = value
            elif tag == 258: bits_per_sample = value

        next_ifd_offset_offset = ifd_entries_start + num_entries * entry_size
        offset_format = endian + ('Q' if is_bigtiff else 'I')
        offset_size = 8 if is_bigtiff else 4
        next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset : next_ifd_offset_offset + offset_size]
        next_ifd_offset = struct.unpack(offset_format, next_ifd_offset_bytes)[0]

        dtype = None
        if bits_per_sample in (8, 16, 32, 64):
            dtype = f"uint{bits_per_sample}"
        return width, height, dtype, tags, next_ifd_offset

    @classmethod
    async def _parse_ifd_async(cls, ifd_data: bytes, endian: str, reader, is_bigtiff: bool):
        width, height, bits_per_sample = None, None, None
        tags = {}

        if is_bigtiff:
            num_entries = struct.unpack(endian + "Q", ifd_data[:8])[0]
            ifd_entries_start = 8
            entry_size = 20
            entry_format = endian + "HHQQ"
        else:
            num_entries = struct.unpack(endian + "H", ifd_data[:2])[0]
            ifd_entries_start = 2
            entry_size = 12
            entry_format = endian + "HHII"

        for i in range(num_entries):
            entry_offset = ifd_entries_start + i * entry_size
            entry_slice = ifd_data[entry_offset : entry_offset + entry_size]
            if len(entry_slice) < entry_size:
                warnings.warn(f"IFD entry slice too short. Expected {entry_size} bytes, got {len(entry_slice)}")
                break
            
            tag, field_type, count, value_or_offset = struct.unpack(entry_format, entry_slice)
            offset_size = 8 if is_bigtiff else 4
            value = await cls._get_value_async(reader, endian, field_type, count, value_or_offset.to_bytes(offset_size, byteorder=endian.replace('<', 'little').replace('>', 'big')), is_bigtiff)
            tags[tag] = value

            if tag == 256: width = value
            elif tag == 257: height = value
            elif tag == 258: bits_per_sample = value

        next_ifd_offset_offset = ifd_entries_start + num_entries * entry_size
        offset_format = endian + ('Q' if is_bigtiff else 'I')
        offset_size = 8 if is_bigtiff else 4
        next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset : next_ifd_offset_offset + offset_size]
        next_ifd_offset = struct.unpack(offset_format, next_ifd_offset_bytes)[0]

        dtype = None
        if bits_per_sample in (8, 16, 32, 64):
            dtype = f"uint{bits_per_sample}"
        return width, height, dtype, tags, next_ifd_offset

    @classmethod
    def _count_ifds(cls, reader, endian: str, first_ifd_offset: int, is_bigtiff: bool) -> int:
        ifd_count = 0
        next_ifd_offset = first_ifd_offset
        while next_ifd_offset != 0:
            ifd_count += 1
            
            if is_bigtiff:
                count_size = 8
                count_format = endian + "Q"
                entry_size = 20
                offset_size = 8
                offset_format = endian + "Q"
            else:
                count_size = 2
                count_format = endian + "H"
                entry_size = 12
                offset_size = 4
                offset_format = endian + "I"

            ifd_count_bytes = reader.fetch(next_ifd_offset, count_size)
            if len(ifd_count_bytes) < count_size: break
            
            num_entries = struct.unpack(count_format, ifd_count_bytes)[0]
            ifd_size = count_size + num_entries * entry_size + offset_size
            ifd_data = reader.fetch(next_ifd_offset, ifd_size)
            if len(ifd_data) < ifd_size: break

            next_ifd_offset_offset = count_size + num_entries * entry_size
            next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset : next_ifd_offset_offset + offset_size]
            if len(next_ifd_offset_bytes) < offset_size: break
            next_ifd_offset = struct.unpack(offset_format, next_ifd_offset_bytes)[0]
        return ifd_count

    @classmethod
    async def _count_ifds_async(cls, reader, endian: str, first_ifd_offset: int, is_bigtiff: bool) -> int:
        ifd_count = 0
        next_ifd_offset = first_ifd_offset
        while next_ifd_offset != 0:
            ifd_count += 1

            if is_bigtiff:
                count_size = 8
                count_format = endian + "Q"
                entry_size = 20
                offset_size = 8
                offset_format = endian + "Q"
            else:
                count_size = 2
                count_format = endian + "H"
                entry_size = 12
                offset_size = 4
                offset_format = endian + "I"

            ifd_count_bytes = await reader.fetch(next_ifd_offset, count_size)
            if len(ifd_count_bytes) < count_size: break

            num_entries = struct.unpack(count_format, ifd_count_bytes)[0]
            ifd_size = count_size + num_entries * entry_size + offset_size
            ifd_data = await reader.fetch(next_ifd_offset, ifd_size)
            if len(ifd_data) < ifd_size: break

            next_ifd_offset_offset = count_size + num_entries * entry_size
            next_ifd_offset_bytes = ifd_data[next_ifd_offset_offset : next_ifd_offset_offset + offset_size]
            if len(next_ifd_offset_bytes) < offset_size: break
            next_ifd_offset = struct.unpack(offset_format, next_ifd_offset_bytes)[0]
        return ifd_count

    @classmethod
    def _parse_header(cls, header: bytes, reader, count_ifds: bool):
        byte_order_indicator = header[0:2]
        endian = {"II": "<", "MM": ">"}.get(byte_order_indicator.decode('ascii', 'ignore'))
        if not endian:
            raise ParseError(f"Invalid TIFF byte order indicator: {byte_order_indicator!r}")

        magic_number = struct.unpack(endian + "H", header[2:4])[0]
        is_bigtiff = magic_number == 43
        
        if magic_number not in (42, 43):
            raise ParseError(f"Invalid TIFF magic number: {magic_number}")

        if is_bigtiff:
            if len(header) < cls._BIGTIFF_HEADER_SIZE:
                header += reader.fetch(len(header), cls._BIGTIFF_HEADER_SIZE - len(header))
            
            offset_bytes = struct.unpack(endian + "H", header[4:6])[0]
            if offset_bytes != 8:
                raise ParseError(f"BigTIFF offset size must be 8, not {offset_bytes}")
            if struct.unpack(endian + "H", header[6:8])[0] != 0:
                raise ParseError("BigTIFF constant not zero")
            
            first_ifd_offset = struct.unpack(endian + "Q", header[8:16])[0]
            count_size, entry_size, offset_size = 8, 20, 8
            count_format, offset_format = endian + "Q", endian + "Q"
        else:
            if len(header) < cls._CLASSIC_HEADER_SIZE:
                 raise ParseError("Header truncated")
            first_ifd_offset = struct.unpack(endian + "I", header[4:8])[0]
            count_size, entry_size, offset_size = 2, 12, 4
            count_format = endian + "H"

        ifd_count = cls._count_ifds(reader, endian, first_ifd_offset, is_bigtiff) if count_ifds else None

        ifd_header_bytes = reader.fetch(first_ifd_offset, count_size)
        if len(ifd_header_bytes) < count_size:
            raise ParseError("Not enough data to read IFD entry count.")
        
        num_entries = struct.unpack(count_format, ifd_header_bytes)[0]
        ifd_size = count_size + num_entries * entry_size + offset_size
        ifd_data = ifd_header_bytes + reader.fetch(first_ifd_offset + count_size, ifd_size - count_size)
        
        if len(ifd_data) < ifd_size:
            warnings.warn(f"Fetched less IFD data than expected. Expected {ifd_size}, got {len(ifd_data)}")

        width, height, dtype, tags, _ = cls._parse_ifd(ifd_data, endian, reader, is_bigtiff)
        if not all((width, height, dtype)):
            warnings.warn("Could not parse all required TIFF IFD tags (Width, Height, BitsPerSample).")

        return {"format": "BigTIFF" if is_bigtiff else "TIFF", "width": width, "height": height, "dtype": dtype, "tags": tags, "ifd_count": ifd_count}

    @classmethod
    async def _parse_header_async(cls, header: bytes, reader, count_ifds: bool):
        byte_order_indicator = header[0:2]
        endian = {"II": "<", "MM": ">"}.get(byte_order_indicator.decode('ascii', 'ignore'))
        if not endian:
            raise ParseError(f"Invalid TIFF byte order indicator: {byte_order_indicator!r}")

        magic_number = struct.unpack(endian + "H", header[2:4])[0]
        is_bigtiff = magic_number == 43

        if magic_number not in (42, 43):
            raise ParseError(f"Invalid TIFF magic number: {magic_number}")

        if is_bigtiff:
            if len(header) < cls._BIGTIFF_HEADER_SIZE:
                header += await reader.fetch(len(header), cls._BIGTIFF_HEADER_SIZE - len(header))

            offset_bytes = struct.unpack(endian + "H", header[4:6])[0]
            if offset_bytes != 8:
                raise ParseError(f"BigTIFF offset size must be 8, not {offset_bytes}")
            if struct.unpack(endian + "H", header[6:8])[0] != 0:
                raise ParseError("BigTIFF constant not zero")

            first_ifd_offset = struct.unpack(endian + "Q", header[8:16])[0]
            count_size, entry_size, offset_size = 8, 20, 8
            count_format, offset_format = endian + "Q", endian + "Q"
        else:
            if len(header) < cls._CLASSIC_HEADER_SIZE:
                 raise ParseError("Header truncated")
            first_ifd_offset = struct.unpack(endian + "I", header[4:8])[0]
            count_size, entry_size, offset_size = 2, 12, 4
            count_format = endian + "H"

        ifd_count = await cls._count_ifds_async(reader, endian, first_ifd_offset, is_bigtiff) if count_ifds else None

        ifd_header_bytes = await reader.fetch(first_ifd_offset, count_size)
        if len(ifd_header_bytes) < count_size:
            raise ParseError("Not enough data to read IFD entry count.")

        num_entries = struct.unpack(count_format, ifd_header_bytes)[0]
        ifd_size = count_size + num_entries * entry_size + offset_size
        ifd_data = ifd_header_bytes + await reader.fetch(first_ifd_offset + count_size, ifd_size - count_size)

        if len(ifd_data) < ifd_size:
            warnings.warn(f"Fetched less IFD data than expected. Expected {ifd_size}, got {len(ifd_data)}")

        width, height, dtype, tags, _ = await cls._parse_ifd_async(ifd_data, endian, reader, is_bigtiff)
        if not all((width, height, dtype)):
            warnings.warn("Could not parse all required TIFF IFD tags (Width, Height, BitsPerSample).")

        return {"format": "BigTIFF" if is_bigtiff else "TIFF", "width": width, "height": height, "dtype": dtype, "tags": tags, "ifd_count": ifd_count}

    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        try:
            count_ifds = kwargs.get("count_ifds", False)
            hdr = _prefetched_header or reader.fetch(0, cls._BIGTIFF_HEADER_SIZE)
            meta = cls._parse_header(hdr, reader, count_ifds)

            if bytes_peek and bytes_peek > 0:
                peek = hdr[:bytes_peek] if bytes_peek <= len(hdr) else hdr + reader.fetch(len(hdr), bytes_peek - len(hdr))
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
            hdr = _prefetched_header or await reader.fetch(0, cls._BIGTIFF_HEADER_SIZE)
            meta = await cls._parse_header_async(hdr, reader, count_ifds)

            if bytes_peek and bytes_peek > 0:
                peek = hdr[:bytes_peek] if bytes_peek <= len(hdr) else hdr + await reader.fetch(len(hdr), bytes_peek - len(hdr))
                meta["peek_bytes_b64"] = base64.b64encode(peek).decode()

            return Result(True, meta, None, reader.bytes_fetched, reader.requests_made)
        except (ParseError, struct.error) as e:
            return Result(False, None, str(e), getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))
        except Exception as e:
            return Result(False, None, f"Unexpected error: {e}", getattr(reader, 'bytes_fetched', 0), getattr(reader, 'requests_made', 0))


