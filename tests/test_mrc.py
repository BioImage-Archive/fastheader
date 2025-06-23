import pytest
import base64
from pathlib import Path
from unittest.mock import Mock

from fastheader import read_header_sync, read_header
from fastheader.parsers.mrc import MRCParser
from fastheader.core.model import Result, ParseError


class TestMRCParser:
    """Test MRC parser functionality."""
    
    @pytest.fixture
    def tiny_mrc_path(self):
        """Path to the tiny MRC test fixture."""
        return Path(__file__).parent / "fixtures" / "tiny.mrc"
    
    @pytest.fixture
    def tiny_mrc_data(self, tiny_mrc_path):
        """Raw bytes of the tiny MRC file."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found - run tests/create_tiny_mrc.py first")
        return tiny_mrc_path.read_bytes()

    def test_parse_tiny_mrc_sync(self, tiny_mrc_path):
        """Test synchronous parsing of tiny MRC file."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = read_header_sync(tiny_mrc_path)
        
        assert result.success is True
        assert result.error is None
        assert result.bytes_fetched == 1024
        
        data = result.data
        assert data["format"] == "MRC"
        assert data["width"] == 10
        assert data["height"] == 20
        assert data.get("depth") is None  # depth=1 should be omitted
        assert data["dtype"] == "float32"
        
        # Check physical voxel sizes
        # cella=[100.0, 200.0, 10.0] Å, sampling=[10, 20, 1]
        # Expected: 100.0 * 1e-10 / 10 = 1e-9 m
        assert abs(data["single_voxel_physical_size_x"] - 1e-9) < 1e-15
        assert abs(data["single_voxel_physical_size_y"] - 1e-9) < 1e-15
        assert abs(data["single_voxel_physical_size_z"] - 1e-9) < 1e-15

    @pytest.mark.asyncio
    async def test_parse_tiny_mrc_async(self, tiny_mrc_path):
        """Test asynchronous parsing of tiny MRC file."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = await read_header(tiny_mrc_path)
        
        assert result.success is True
        assert result.error is None
        assert result.bytes_fetched == 1024
        
        data = result.data
        assert data["format"] == "MRC"
        assert data["width"] == 10
        assert data["height"] == 20
        assert data.get("depth") is None
        assert data["dtype"] == "float32"

    def test_bytes_peek_functionality(self, tiny_mrc_path):
        """Test bytes_peek parameter."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = read_header_sync(tiny_mrc_path, bytes_peek=100)
        
        assert result.success is True
        assert "peek_bytes_b64" in result.data
        
        # Decode and verify peek data
        peek_data = base64.b64decode(result.data["peek_bytes_b64"])
        assert len(peek_data) == 100

    def test_invalid_mode_error(self):
        """Test that invalid mode raises ParseError."""
        # Create header with invalid mode
        header = bytearray(1024)
        # Valid dimensions
        import struct
        struct.pack_into("<3i", header, 0, 10, 20, 1)
        # Invalid mode
        struct.pack_into("<i", header, 12, 99)  # unsupported mode
        # Valid magic
        header[208:212] = b"MAP "
        
        mock_reader = Mock()
        mock_reader.fetch.return_value = bytes(header)
        mock_reader.bytes_fetched = 1024
        
        result = MRCParser.read_sync(mock_reader, bytes_peek=None)
        
        assert result.success is False
        assert "Unsupported mode 99" in result.error

    def test_truncated_header_error(self):
        """Test that truncated header raises ParseError."""
        mock_reader = Mock()
        mock_reader.fetch.return_value = b"short"  # too short
        mock_reader.bytes_fetched = 5
        
        result = MRCParser.read_sync(mock_reader, bytes_peek=None)
        
        assert result.success is False
        assert "Header truncated" in result.error

    def test_invalid_magic_bytes(self):
        """Test that invalid magic bytes raise ParseError."""
        header = bytearray(1024)
        # Valid dimensions and mode
        import struct
        struct.pack_into("<3i", header, 0, 10, 20, 1)
        struct.pack_into("<i", header, 12, 2)
        # Invalid magic
        header[208:212] = b"XXXX"
        
        mock_reader = Mock()
        mock_reader.fetch.return_value = bytes(header)
        mock_reader.bytes_fetched = 1024
        
        result = MRCParser.read_sync(mock_reader, bytes_peek=None)
        
        assert result.success is False
        assert "Invalid MRC magic bytes" in result.error

    def test_depth_handling(self):
        """Test that depth is only included when nz > 1."""
        header = bytearray(1024)
        import struct
        
        # Test with nz = 5 (should include depth)
        struct.pack_into("<3i", header, 0, 10, 20, 5)
        struct.pack_into("<i", header, 12, 2)
        header[208:212] = b"MAP "
        struct.pack_into("<3i", header, 92, 10, 20, 5)
        struct.pack_into("<3f", header, 40, 100.0, 200.0, 50.0)
        
        mock_reader = Mock()
        mock_reader.fetch.return_value = bytes(header)
        mock_reader.bytes_fetched = 1024
        
        result = MRCParser.read_sync(mock_reader, bytes_peek=None)
        
        assert result.success is True
        assert result.data["depth"] == 5

    def test_parser_registration(self):
        """Test that MRCParser is properly registered."""
        from fastheader.core.registry import _REGISTRY
        
        # Test signature detection
        test_data = b"\x00" * 208 + b"MAP " + b"\x00" * 100
        parser = _REGISTRY._sniff(test_data)
        assert parser == MRCParser
        
        # Test extension detection
        parser = _REGISTRY.choose("test.mrc", b"\x00" * 1000)
        assert parser == MRCParser
        
        parser = _REGISTRY.choose("test.map", b"\x00" * 1000)
        assert parser == MRCParser
