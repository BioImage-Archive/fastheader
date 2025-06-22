"""Synchronous HTTP byte reader using requests."""

import requests
from typing import Optional
from urllib.parse import urlparse

from .base import ByteReader, RangeNotSupportedError, RANGE_FALLBACK_MAX


# Module-level session for connection pooling
_session = None


def _get_session():
    """Get or create the global requests session."""
    global _session
    if _session is None:
        _session = requests.Session()
    return _session


def _decide_full_get(content_length: Optional[int], accept_ranges: bool) -> bool:
    """Return True only when not accept_ranges and content_length and content_length < RANGE_FALLBACK_MAX."""
    return (not accept_ranges and 
            content_length is not None and 
            content_length < RANGE_FALLBACK_MAX)


class HTTPByteReader:
    """Synchronous HTTP byte reader with Range support."""
    
    def __init__(self, url: str):
        self.url = url
        self.bytes_fetched = 0
        self.content_length: Optional[int] = None
        self._accept_ranges = False
        self._full_content: Optional[bytes] = None
        self._session = _get_session()
        
        # Perform HEAD request immediately
        self._perform_head()
    
    def _perform_head(self):
        """Perform HEAD request to check capabilities."""
        try:
            response = self._session.head(self.url, timeout=30)
            if response.status_code >= 400:
                raise IOError(f"HEAD request failed with status {response.status_code}")
            
            # Check content length
            content_length_header = response.headers.get('content-length')
            if content_length_header:
                self.content_length = int(content_length_header)
            
            # Check range support
            accept_ranges = response.headers.get('accept-ranges', '').lower()
            self._accept_ranges = accept_ranges == 'bytes'
            
        except requests.RequestException as e:
            raise IOError(f"HEAD request failed: {e}")
    
    def _fetch_full_content(self):
        """Download entire file content for small files without range support."""
        if self._full_content is not None:
            return
        
        try:
            response = self._session.get(self.url, timeout=60)
            if response.status_code >= 400:
                raise IOError(f"GET request failed with status {response.status_code}")
            
            self._full_content = response.content
            self.bytes_fetched = len(self._full_content)
            
        except requests.RequestException as e:
            raise IOError(f"GET request failed: {e}")
    
    def _fetch_range(self, start: int, length: int, retry_count: int = 0) -> bytes:
        """Fetch a specific byte range."""
        end = start + length - 1
        headers = {'Range': f'bytes={start}-{end}'}
        
        try:
            response = self._session.get(self.url, headers=headers, timeout=30)
            
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
                    additional_data = self._fetch_range(start + len(data), remaining, retry_count + 1)
                    data += additional_data
                
                self.bytes_fetched += len(data)
                return data
            
            else:
                raise IOError(f"Range request failed with status {response.status_code}")
                
        except requests.RequestException as e:
            if retry_count == 0:
                # One automatic retry
                return self._fetch_range(start, length, retry_count + 1)
            raise IOError(f"Range request failed: {e}")
    
    def fetch(self, start: int, length: int) -> bytes:
        """Return exactly `length` bytes starting at absolute offset `start`."""
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
            self._fetch_full_content()
            return self.fetch(start, length)  # Recursive call with full content
        
        # Use range requests
        if not self._accept_ranges:
            raise RangeNotSupportedError("Server doesn't support ranges and file is too large")
        
        return self._fetch_range(start, length)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Session is shared, don't close it here
        pass


def open_http_reader(url: str) -> HTTPByteReader:
    """Create a synchronous HTTP byte reader."""
    return HTTPByteReader(url)
