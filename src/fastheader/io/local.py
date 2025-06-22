"""Local file readers using mmap."""

import asyncio
import mmap
from pathlib import Path
from typing import BinaryIO, Union
import sys

from .base import ByteReader, AsyncByteReader


class LocalByteReader:
    """Synchronous local file reader using mmap."""
    
    def __init__(self, source: Union[Path, str, BinaryIO]):
        self.bytes_fetched = 0
        self._file = None
        self._mmap = None
        self._should_close_file = False
        
        if hasattr(source, 'read'):
            # BinaryIO object
            self._file = source
        else:
            # Path or str
            self._file = open(source, 'rb')
            self._should_close_file = True
    
    def _ensure_mmap(self):
        """Create mmap on first access."""
        if self._mmap is None:
            if self._file.seekable():
                self._file.seek(0, 2)  # Seek to end
                file_size = self._file.tell()
                if file_size == 0:
                    raise IOError("Cannot mmap empty file")
                self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            else:
                raise IOError("File is not seekable, cannot use mmap")
    
    def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`."""
        self._ensure_mmap()
        
        if start < 0:
            raise IOError("Start offset cannot be negative")
        
        if start + length > len(self._mmap):
            raise IOError(f"Not enough data: requested {length} bytes at offset {start}, "
                         f"but file only has {len(self._mmap)} bytes")
        
        data = self._mmap[start:start + length]
        self.bytes_fetched += len(data)
        return data
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self):
        """Close mmap and file if we opened it."""
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._should_close_file and self._file is not None:
            self._file.close()
            self._file = None


class LocalAsyncByteReader:
    """Asynchronous local file reader - thin wrapper around sync reader."""
    
    def __init__(self, source: Union[Path, str, BinaryIO]):
        self._sync_reader = LocalByteReader(source)
    
    @property
    def bytes_fetched(self) -> int:
        return self._sync_reader.bytes_fetched
    
    async def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`."""
        # Use asyncio.to_thread for Python 3.9+, fallback to run_in_executor
        if sys.version_info >= (3, 9):
            return await asyncio.to_thread(self._sync_reader.fetch, start, length)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self._sync_reader.fetch, start, length)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def close(self):
        """Close the underlying sync reader."""
        if sys.version_info >= (3, 9):
            await asyncio.to_thread(self._sync_reader.close)
        else:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_reader.close)


def open_local_reader(source: Union[Path, str, BinaryIO]) -> LocalByteReader:
    """Create a synchronous local byte reader."""
    return LocalByteReader(source)


async def open_local_reader_async(source: Union[Path, str, BinaryIO]) -> LocalAsyncByteReader:
    """Create an asynchronous local byte reader."""
    return LocalAsyncByteReader(source)
