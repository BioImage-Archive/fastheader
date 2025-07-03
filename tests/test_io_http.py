"""Tests for HTTP I/O."""

import pytest
from pytest_httpserver import HTTPServer
from werkzeug import Request, Response
import threading
import time

from fastheader.io.http_sync import HTTPByteReader, open_http_reader
from fastheader.io.http_async import HTTPAsyncByteReader, open_http_reader_async
from fastheader.io.base import RangeNotSupportedError, RANGE_FALLBACK_MAX


class TestHTTPByteReader:
    """Test synchronous HTTP byte reader."""
    
    def setup_method(self):
        """Set up test HTTP server."""
        self.test_data = b"0123456789" * 100  # 1000 bytes
        self.server = HTTPServer(host="127.0.0.1", port=0)
        self.server.expect_request("/test").respond_with_handler(self._handle_request)
        self.server.expect_request("/no-ranges").respond_with_handler(self._handle_no_ranges)
        self.server.expect_request("/big-no-ranges").respond_with_handler(self._handle_big_no_ranges)
        self.server.start()
        self.base_url = f"http://127.0.0.1:{self.server.port}"
    
    def teardown_method(self):
        """Clean up test HTTP server."""
        self.server.stop()
    
    def _handle_request(self, request: Request) -> Response:
        """Handle requests with range support."""
        if request.method == "HEAD":
            return Response(
                status=200,
                headers={
                    "Content-Length": str(len(self.test_data)),
                    "Accept-Ranges": "bytes"
                }
            )
        
        range_header = request.headers.get("Range")
        if range_header:
            # Parse range header: bytes=start-end
            range_spec = range_header.replace("bytes=", "")
            start, end = map(int, range_spec.split("-"))
            data = self.test_data[start:end+1]
            
            return Response(
                data,
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(self.test_data)}",
                    "Content-Length": str(len(data))
                }
            )
        
        return Response(self.test_data, status=200)
    
    def _handle_no_ranges(self, request: Request) -> Response:
        """Handle requests without range support (small file)."""
        small_data = b"0123456789"  # 10 bytes < RANGE_FALLBACK_MAX
        
        if request.method == "HEAD":
            return Response(
                status=200,
                headers={
                    "Content-Length": str(len(small_data)),
                    "Accept-Ranges": "none"
                }
            )
        
        # Always return full content regardless of Range header
        return Response(small_data, status=200)
    
    def _handle_big_no_ranges(self, request: Request) -> Response:
        """Handle requests without range support (big file)."""
        big_data = b"x" * (RANGE_FALLBACK_MAX + 1000)  # Bigger than fallback limit
        
        if request.method == "HEAD":
            return Response(
                status=200,
                headers={
                    "Content-Length": str(len(big_data)),
                    "Accept-Ranges": "none"
                }
            )
        
        return Response(big_data, status=200)
    
    def test_basic_range_requests(self):
        """Test basic range request functionality."""
        reader = HTTPByteReader(f"{self.base_url}/test")
        
        # Test various ranges
        assert reader.fetch(0, 10) == b"0123456789"
        assert reader.fetch(10, 10) == b"0123456789"
        assert reader.fetch(5, 5) == b"56789"
        
        # Check bytes_fetched accounting
        assert reader.bytes_fetched == 25  # 10 + 10 + 5
        assert reader.requests_made == 4  # 1 HEAD + 3 GET
    
    def test_overlapping_ranges(self):
        """Test overlapping range requests."""
        reader = HTTPByteReader(f"{self.base_url}/test")
        
        # Overlapping requests
        assert reader.fetch(0, 5) == b"01234"
        assert reader.fetch(3, 5) == b"34567"
        assert reader.fetch(1, 3) == b"123"
        
        assert reader.bytes_fetched == 13  # 5 + 5 + 3
        assert reader.requests_made == 4  # 1 HEAD + 3 GET
    
    def test_no_ranges_small_file(self):
        """Test fallback to full GET for small files without range support."""
        reader = HTTPByteReader(f"{self.base_url}/no-ranges")
        
        # Should work via full-GET fallback
        assert reader.fetch(0, 5) == b"01234"
        assert reader.fetch(5, 5) == b"56789"
        
        # bytes_fetched should be the full file size (downloaded once)
        assert reader.bytes_fetched == 10
        assert reader.requests_made == 2  # 1 HEAD + 1 GET
    
    def test_no_ranges_big_file(self):
        """Test error for big files without range support."""
        with pytest.raises(RangeNotSupportedError):
            reader = HTTPByteReader(f"{self.base_url}/big-no-ranges")
            reader.fetch(0, 10)
    
    def test_context_manager(self):
        """Test context manager usage."""
        with HTTPByteReader(f"{self.base_url}/test") as reader:
            assert reader.fetch(0, 10) == b"0123456789"
            assert reader.bytes_fetched == 10
            assert reader.requests_made == 2  # 1 HEAD + 1 GET
    
    def test_factory_function(self):
        """Test factory function."""
        reader = open_http_reader(f"{self.base_url}/test")
        assert isinstance(reader, HTTPByteReader)
        assert reader.fetch(0, 10) == b"0123456789"
        assert reader.requests_made == 2  # 1 HEAD + 1 GET


class TestHTTPAsyncByteReader:
    """Test asynchronous HTTP byte reader."""
    
    def setup_method(self):
        """Set up test HTTP server."""
        self.test_data = b"0123456789" * 100  # 1000 bytes
        self.server = HTTPServer(host="127.0.0.1", port=0)
        self.server.expect_request("/test").respond_with_handler(self._handle_request)
        self.server.expect_request("/no-ranges").respond_with_handler(self._handle_no_ranges)
        self.server.start()
        self.base_url = f"http://127.0.0.1:{self.server.port}"
    
    def teardown_method(self):
        """Clean up test HTTP server."""
        self.server.stop()
    
    def _handle_request(self, request: Request) -> Response:
        """Handle requests with range support."""
        if request.method == "HEAD":
            return Response(
                status=200,
                headers={
                    "Content-Length": str(len(self.test_data)),
                    "Accept-Ranges": "bytes"
                }
            )
        
        range_header = request.headers.get("Range")
        if range_header:
            # Parse range header: bytes=start-end
            range_spec = range_header.replace("bytes=", "")
            start, end = map(int, range_spec.split("-"))
            data = self.test_data[start:end+1]
            
            return Response(
                data,
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(self.test_data)}",
                    "Content-Length": str(len(data))
                }
            )
        
        return Response(self.test_data, status=200)
    
    def _handle_no_ranges(self, request: Request) -> Response:
        """Handle requests without range support (small file)."""
        small_data = b"0123456789"  # 10 bytes < RANGE_FALLBACK_MAX
        
        if request.method == "HEAD":
            return Response(
                status=200,
                headers={
                    "Content-Length": str(len(small_data)),
                    "Accept-Ranges": "none"
                }
            )
        
        return Response(small_data, status=200)
    
    @pytest.mark.asyncio
    async def test_basic_range_requests(self):
        """Test basic async range request functionality."""
        reader = await open_http_reader_async(f"{self.base_url}/test")
        
        # Test various ranges
        assert await reader.fetch(0, 10) == b"0123456789"
        assert await reader.fetch(10, 10) == b"0123456789"
        assert await reader.fetch(5, 5) == b"56789"
        
        # Check bytes_fetched accounting
        assert reader.bytes_fetched == 25  # 10 + 10 + 5
        assert reader.requests_made == 4  # 1 HEAD + 3 GET
    
    @pytest.mark.asyncio
    async def test_no_ranges_small_file(self):
        """Test async fallback to full GET for small files without range support."""
        reader = await open_http_reader_async(f"{self.base_url}/no-ranges")
        
        # Should work via full-GET fallback
        assert await reader.fetch(0, 5) == b"01234"
        assert await reader.fetch(5, 5) == b"56789"
        
        # bytes_fetched should be the full file size (downloaded once)
        assert reader.bytes_fetched == 10
        assert reader.requests_made == 2  # 1 HEAD + 1 GET
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager usage."""
        async with await open_http_reader_async(f"{self.base_url}/test") as reader:
            assert await reader.fetch(0, 10) == b"0123456789"
            assert reader.bytes_fetched == 10
            assert reader.requests_made == 2  # 1 HEAD + 1 GET
