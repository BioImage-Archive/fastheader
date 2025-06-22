"""Asynchronous HTTP byte reader using httpx."""

import httpx
from typing import Optional
from contextlib import asynccontextmanager

from .base import AsyncByteReader, RangeNotSupportedError, RANGE_FALLBACK_MAX


# Global async client
_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def _get_client():
    """Get or create the global httpx AsyncClient."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=60.0)
    
    try:
        yield _client
    finally:
        # Don't close the client here - it's shared
        pass


def _decide_full_get(content_length: Optional[int], accept_ranges: bool) -> bool:
    """Return True only when not accept_ranges and content_length and content_length < RANGE_FALLBACK_MAX."""
    return (not accept_ranges and 
            content_length is not None and 
            content_length < RANGE_FALLBACK_MAX)


class HTTPAsyncByteReader:
    """Asynchronous HTTP byte reader with Range support."""
    
    def __init__(self, url: str):
        self.url = url
        self.bytes_fetched = 0
        self.content_length: Optional[int] = None
        self._accept_ranges = False
        self._full_content: Optional[bytes] = None
        self._initialized = False
    
    async def _ensure_initialized(self):
        """Perform HEAD request to check capabilities if not already done."""
        if self._initialized:
            return
        
        async with _get_client() as client:
            try:
                response = await client.head(self.url)
                if response.status_code >= 400:
                    raise IOError(f"HEAD request failed with status {response.status_code}")
                
                # Check content length
                content_length_header = response.headers.get('content-length')
                if content_length_header:
                    self.content_length = int(content_length_header)
                
                # Check range support
                accept_ranges = response.headers.get('accept-ranges', '').lower()
                self._accept_ranges = accept_ranges == 'bytes'
                
                self._initialized = True
                
            except httpx.RequestError as e:
                raise IOError(f"HEAD request failed: {e}")
    
    async def _fetch_full_content(self):
        """Download entire file content for small files without range support."""
        if self._full_content is not None:
            return
        
        async with _get_client() as client:
            try:
                response = await client.get(self.url)
                if response.status_code >= 400:
                    raise IOError(f"GET request failed with status {response.status_code}")
                
                self._full_content = response.content
                self.bytes_fetched = len(self._full_content)
                
            except httpx.RequestError as e:
                raise IOError(f"GET request failed: {e}")
    
    async def _fetch_range(self, start: int, length: int, retry_count: int = 0) -> bytes:
        """Fetch a specific byte range."""
        end = start + length - 1
        headers = {'Range': f'bytes={start}-{end}'}
        
        async with _get_client() as client:
            try:
                response = await client.get(self.url, headers=headers)
                
                if response.status_code == 200:
                    # Server doesn't support ranges, got full content
                    if self.content_length and self.content_length >= RANGE_FALLBACK_MAX:
                        raise RangeNotSupportedError("Server doesn't support ranges and file is too large")
                    
                    # Treat as full GET for small files
                    self._full_content = response.content
                    self.bytes_fetched = len(self._full_content)
                    
                    if start + length > len(self._full_content):
                        raise IOError(f"Not enough data: requested {length} bytes at offset {start}")
                    
                    return self._full_content[start:start + length]
                
                elif response.status_code == 206:
                    # Partial content - what we want
                    data = response.content
                    
                    # Server might return less than requested - handle this
                    if len(data) < length and retry_count == 0:
                        # Try once more with adjusted range
                        remaining = length - len(data)
                        additional_data = await self._fetch_range(start + len(data), remaining, retry_count + 1)
                        data += additional_data
                    
                    self.bytes_fetched += len(data)
                    return data
                
                else:
                    raise IOError(f"Range request failed with status {response.status_code}")
                    
            except httpx.RequestError as e:
                if retry_count == 0:
                    # One automatic retry
                    return await self._fetch_range(start, length, retry_count + 1)
                raise IOError(f"Range request failed: {e}")
    
    async def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`."""
        await self._ensure_initialized()
        
        if start < 0:
            raise IOError("Start offset cannot be negative")
        
        if length <= 0:
            raise IOError("Length must be positive")
        
        # If we already have full content, serve from it
        if self._full_content is not None:
            if start + length > len(self._full_content):
                raise IOError(f"Not enough data: requested {length} bytes at offset {start}")
            return self._full_content[start:start + length]
        
        # Decide strategy based on server capabilities
        if _decide_full_get(self.content_length, self._accept_ranges):
            await self._fetch_full_content()
            return await self.fetch(start, length)  # Recursive call with full content
        
        # Use range requests
        if not self._accept_ranges:
            raise RangeNotSupportedError("Server doesn't support ranges and file is too large")
        
        return await self._fetch_range(start, length)
    
    async def __aenter__(self):
        await self._ensure_initialized()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Client is shared, don't close it here
        pass


async def open_http_reader_async(url: str) -> HTTPAsyncByteReader:
    """Create an asynchronous HTTP byte reader."""
    reader = HTTPAsyncByteReader(url)
    await reader._ensure_initialized()
    return reader


async def close_global_client():
    """Close the global httpx client. Call this at application shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
