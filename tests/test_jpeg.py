from __future__ import annotations

import base64
import io
from pathlib import Path

import pytest
from httpx import Request, Response

from fastheader import read_header, read_header_sync
from fastheader.core.model import ParseError
from fastheader.parsers.jpeg import JPEGParser


class TestJPEGParser:
    """Test JPEG parser functionality."""

    @pytest.fixture
    def tiny_jpeg_bytes(self) -> bytes:
        """Create a minimal valid JPEG for testing."""
        # Minimal JPEG: SOI + SOF0 + minimal data + EOI
        soi = b"\xFF\xD8"  # Start of Image
        
        # SOF0 marker (baseline JPEG)
        sof0_marker = b"\xFF\xC0"
        sof0_length = b"\x00\x11"  # 17 bytes
        sof0_precision = b"\x08"   # 8 bits
        sof0_height = b"\x00\x10"  # 16 pixels
        sof0_width = b"\x00\x20"   # 32 pixels
        sof0_components = b"\x03"  # 3 components (RGB)
        # Component data (3 components × 3 bytes each)
        sof0_comp_data = b"\x01\x11\x00\x02\x11\x01\x03\x11\x01"
        
        sof0_segment = sof0_marker + sof0_length + sof0_precision + sof0_height + sof0_width + sof0_components + sof0_comp_data
        
        # SOS (Start of Scan) - minimal
        sos_marker = b"\xFF\xDA"
        sos_length = b"\x00\x0C"  # 12 bytes
        sos_data = b"\x03\x01\x00\x02\x11\x03\x11\x00\x3F\x00"
        sos_segment = sos_marker + sos_length + sos_data
        
        # Minimal image data
        image_data = b"\xFF\x00" * 10  # Some stuffed bytes
        
        # EOI
        eoi = b"\xFF\xD9"  # End of Image
        
        return soi + sof0_segment + sos_segment + image_data + eoi

    @pytest.fixture
    def tiny_jpeg_file(self, tmp_path: Path, tiny_jpeg_bytes: bytes) -> Path:
        """Create a temporary JPEG file."""
        jpeg_file = tmp_path / "tiny.jpg"
        jpeg_file.write_bytes(tiny_jpeg_bytes)
        return jpeg_file

    @pytest.fixture
    def large_app1_jpeg_bytes(self) -> bytes:
        """Create a JPEG with large APP1 segment to test chunked reading."""
        soi = b"\xFF\xD8"
        
        # Large APP1 segment (EXIF-like)
        app1_marker = b"\xFF\xE1"
        app1_size = 8000  # 8KB APP1 segment
        app1_length = (app1_size + 2).to_bytes(2, 'big')
        app1_data = b"Exif\x00\x00" + b"\x00" * (app1_size - 6)
        app1_segment = app1_marker + app1_length + app1_data
        
        # SOF0 after the large APP1
        sof0_marker = b"\xFF\xC0"
        sof0_length = b"\x00\x11"
        sof0_precision = b"\x08"
        sof0_height = b"\x00\x64"  # 100 pixels
        sof0_width = b"\x00\xC8"   # 200 pixels
        sof0_components = b"\x03"
        sof0_comp_data = b"\x01\x11\x00\x02\x11\x01\x03\x11\x01"
        sof0_segment = sof0_marker + sof0_length + sof0_precision + sof0_height + sof0_width + sof0_components + sof0_comp_data
        
        # Minimal SOS and EOI
        sos_segment = b"\xFF\xDA\x00\x0C\x03\x01\x00\x02\x11\x03\x11\x00\x3F\x00"
        image_data = b"\xFF\x00" * 10
        eoi = b"\xFF\xD9"
        
        return soi + app1_segment + sof0_segment + sos_segment + image_data + eoi

    @pytest.fixture
    def large_app1_jpeg_file(self, tmp_path: Path, large_app1_jpeg_bytes: bytes) -> Path:
        """Create a temporary JPEG file with large APP1."""
        jpeg_file = tmp_path / "large_app1.jpg"
        jpeg_file.write_bytes(large_app1_jpeg_bytes)
        return jpeg_file

    def test_tiny_jpeg_sync(self, tiny_jpeg_file: Path):
        """Test parsing a tiny JPEG file synchronously."""
        result = read_header_sync(tiny_jpeg_file)
        
        assert result.success is True
        assert result.error is None
        assert result.data["format"] == "JPEG"
        assert result.data["width"] == 32
        assert result.data["height"] == 16
        assert result.data["dtype"] == "uint8"
        assert result.bytes_fetched <= 4096  # Should fit in first chunk

    @pytest.mark.asyncio
    async def test_tiny_jpeg_async(self, tiny_jpeg_file: Path):
        """Test parsing a tiny JPEG file asynchronously."""
        result = await read_header(tiny_jpeg_file)
        
        assert result.success is True
        assert result.error is None
        assert result.data["format"] == "JPEG"
        assert result.data["width"] == 32
        assert result.data["height"] == 16
        assert result.data["dtype"] == "uint8"
        assert result.bytes_fetched <= 4096

    def test_large_app1_jpeg_sync(self, large_app1_jpeg_file: Path):
        """Test parsing JPEG with large APP1 segment."""
        result = read_header_sync(large_app1_jpeg_file)
        
        assert result.success is True
        assert result.data["format"] == "JPEG"
        assert result.data["width"] == 200
        assert result.data["height"] == 100
        assert result.data["dtype"] == "uint8"
        # Should need more than one chunk but less than 64KB
        assert 4096 < result.bytes_fetched < 65536

    def test_bytes_peek_option(self, tiny_jpeg_file: Path):
        """Test the bytes_peek option."""
        result = read_header_sync(tiny_jpeg_file, bytes_peek=100)
        
        assert result.success is True
        assert "peek_bytes_b64" in result.data
        
        # Decode and verify we got 100 bytes
        peek_bytes = base64.b64decode(result.data["peek_bytes_b64"])
        assert len(peek_bytes) == 100
        assert peek_bytes.startswith(b"\xFF\xD8")  # Should start with SOI

    def test_invalid_jpeg_no_soi(self, tmp_path: Path):
        """Test handling of invalid JPEG (no SOI marker)."""
        bad_jpeg = tmp_path / "bad.jpg"
        bad_jpeg.write_bytes(b"\xFF\xFF" + b"\x00" * 100)  # Wrong start
        
        result = read_header_sync(bad_jpeg)
        assert result.success is False
        assert "Missing SOI marker" in str(result.error)

    def test_jpeg_no_sof_within_cap(self, tmp_path: Path):
        """Test JPEG that exceeds 64KB without SOF marker."""
        # Create a JPEG with SOI but no SOF within 64KB
        soi = b"\xFF\xD8"
        # Create a huge APP1 segment that exceeds our cap
        app1_marker = b"\xFF\xE1"
        app1_size = 65000  # Larger than our 64KB cap
        app1_length = (app1_size + 2).to_bytes(2, 'big')
        app1_data = b"Exif\x00\x00" + b"\x00" * (app1_size - 6)
        
        bad_jpeg_bytes = soi + app1_marker + app1_length + app1_data
        
        bad_jpeg = tmp_path / "no_sof.jpg"
        bad_jpeg.write_bytes(bad_jpeg_bytes)
        
        result = read_header_sync(bad_jpeg)
        assert result.success is False
        assert "SOF not found within 64 KiB" in str(result.error)

    def test_progressive_jpeg(self, tmp_path: Path):
        """Test progressive JPEG (SOF2) parsing."""
        soi = b"\xFF\xD8"
        
        # SOF2 marker (progressive JPEG)
        sof2_marker = b"\xFF\xC2"
        sof2_length = b"\x00\x11"
        sof2_precision = b"\x08"
        sof2_height = b"\x01\x00"  # 256 pixels
        sof2_width = b"\x01\x80"   # 384 pixels
        sof2_components = b"\x03"
        sof2_comp_data = b"\x01\x11\x00\x02\x11\x01\x03\x11\x01"
        
        sof2_segment = sof2_marker + sof2_length + sof2_precision + sof2_height + sof2_width + sof2_components + sof2_comp_data
        
        # Minimal ending
        sos_segment = b"\xFF\xDA\x00\x0C\x03\x01\x00\x02\x11\x03\x11\x00\x3F\x00"
        eoi = b"\xFF\xD9"
        
        progressive_jpeg_bytes = soi + sof2_segment + sos_segment + eoi
        
        progressive_jpeg = tmp_path / "progressive.jpg"
        progressive_jpeg.write_bytes(progressive_jpeg_bytes)
        
        result = read_header_sync(progressive_jpeg)
        assert result.success is True
        assert result.data["format"] == "JPEG"
        assert result.data["width"] == 384
        assert result.data["height"] == 256
        assert result.data["dtype"] == "uint8"

    def test_parser_registration(self):
        """Test that JPEG parser is properly registered."""
        from fastheader.core.registry import _REGISTRY
        
        # Test extension registration
        parsers = _REGISTRY._by_ext.get("jpg", [])
        assert any(parser_cls.__name__ == "JPEGParser" for _, _, parser_cls in parsers)
        
        parsers = _REGISTRY._by_ext.get("jpeg", [])
        assert any(parser_cls.__name__ == "JPEGParser" for _, _, parser_cls in parsers)

    def test_signature_detection(self):
        """Test signature-based detection."""
        from fastheader.core.registry import _REGISTRY
        
        # JPEG signature should be detected
        jpeg_start = b"\xFF\xD8" + b"\x00" * 100
        parser_cls = _REGISTRY._sniff(jpeg_start)
        assert parser_cls is not None
        assert parser_cls.__name__ == "JPEGParser"


class TestJPEGHTTP:
    """Test JPEG parser with HTTP sources."""
    
    def setup_method(self):
        """Set up mock HTTP server for each test."""
        import httpx
        self.transport = httpx.MockTransport(self._handle_request)
        
        # Create test JPEG data
        soi = b"\xFF\xD8"
        sof0_marker = b"\xFF\xC0"
        sof0_length = b"\x00\x11"
        sof0_precision = b"\x08"
        sof0_height = b"\x00\x10"  # 16 pixels
        sof0_width = b"\x00\x20"   # 32 pixels
        sof0_components = b"\x03"
        sof0_comp_data = b"\x01\x11\x00\x02\x11\x01\x03\x11\x01"
        sof0_segment = sof0_marker + sof0_length + sof0_precision + sof0_height + sof0_width + sof0_components + sof0_comp_data
        
        sos_segment = b"\xFF\xDA\x00\x0C\x03\x01\x00\x02\x11\x03\x11\x00\x3F\x00"
        image_data = b"\xFF\x00" * 50
        eoi = b"\xFF\xD9"
        
        self.test_jpeg_data = soi + sof0_segment + sos_segment + image_data + eoi

    def _handle_request(self, request: Request) -> Response:
        """Handle mock HTTP requests."""
        if request.method == "HEAD":
            return Response(
                200,
                headers={
                    "content-length": str(len(self.test_jpeg_data)),
                    "accept-ranges": "bytes",
                    "content-type": "image/jpeg"
                }
            )
        elif request.method == "GET":
            range_header = request.headers.get("range")
            if range_header:
                # Parse range header: "bytes=start-end"
                range_spec = range_header.replace("bytes=", "")
                start, end = range_spec.split("-")
                start = int(start)
                end = int(end) if end else len(self.test_jpeg_data) - 1
                
                data = self.test_jpeg_data[start:end + 1]
                return Response(
                    206,
                    content=data,
                    headers={
                        "content-range": f"bytes {start}-{end}/{len(self.test_jpeg_data)}",
                        "content-length": str(len(data))
                    }
                )
            else:
                return Response(200, content=self.test_jpeg_data)
        
        return Response(404)

    @pytest.mark.asyncio
    async def test_http_jpeg_parsing(self):
        """Test parsing JPEG from HTTP source."""
        # Patch the global HTTP client
        import fastheader.io.http_async
        original_client = fastheader.io.http_async._client
        
        try:
            import httpx
            fastheader.io.http_async._client = httpx.AsyncClient(transport=self.transport)
            
            result = await read_header("http://example.com/test.jpg")
            
            assert result.success is True
            assert result.data["format"] == "JPEG"
            assert result.data["width"] == 32
            assert result.data["height"] == 16
            assert result.data["dtype"] == "uint8"
            # Should use range requests efficiently
            assert result.bytes_fetched <= 4096
            
        finally:
            fastheader.io.http_async._client = original_client

    def test_http_jpeg_parsing_sync(self):
        """Test parsing JPEG from HTTP source synchronously."""
        # Mock the requests session
        import fastheader.io.http_sync
        from unittest.mock import Mock, patch
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-length": str(len(self.test_jpeg_data)),
            "accept-ranges": "bytes",
            "content-type": "image/jpeg"
        }
        
        def mock_request(method, url, **kwargs):
            if method == "HEAD":
                head_response = Mock()
                head_response.status_code = 200
                head_response.headers = mock_response.headers
                return head_response
            elif method == "GET":
                headers = kwargs.get("headers", {})
                range_header = headers.get("Range")
                if range_header:
                    # Parse range header
                    range_spec = range_header.replace("bytes=", "")
                    start, end = range_spec.split("-")
                    start = int(start)
                    end = int(end) if end else len(self.test_jpeg_data) - 1
                    
                    data = self.test_jpeg_data[start:end + 1]
                    response = Mock()
                    response.status_code = 206
                    response.content = data
                    response.headers = {
                        "content-range": f"bytes {start}-{end}/{len(self.test_jpeg_data)}",
                        "content-length": str(len(data))
                    }
                    return response
                else:
                    response = Mock()
                    response.status_code = 200
                    response.content = self.test_jpeg_data
                    return response
            
            return Mock(status_code=404)
        
        with patch.object(fastheader.io.http_sync, '_get_session') as mock_get_session:
            mock_session = Mock()
            mock_session.request = mock_request
            mock_session.head = lambda url, **kwargs: mock_request("HEAD", url, **kwargs)
            mock_get_session.return_value = mock_session
            
            result = read_header_sync("http://example.com/test.jpg")
            
            assert result.success is True
            assert result.data["format"] == "JPEG"
            assert result.data["width"] == 32
            assert result.data["height"] == 16
            assert result.data["dtype"] == "uint8"
