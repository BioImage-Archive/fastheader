
import pytest
from pathlib import Path

from fastheader.io.local import open_local_reader, open_local_reader_async
from fastheader.parsers.tiff import TIFFParser

@pytest.fixture(scope="module")
def big_tiff_path():
    return Path(__file__).parent.parent / "test_data" / "2-Image-Export-02_c1-3.tif"

@pytest.mark.asyncio
async def test_tiff_parser_async(big_tiff_path):
    reader = await open_local_reader_async(big_tiff_path)
    result = await TIFFParser.read(reader, bytes_peek=None)
    assert result.success
    assert result.data["width"] == 1388
    assert result.data["height"] == 1040
    assert result.data["format"] == "TIFF"
    assert result.data["ifd_count"] is None
    assert result.requests_made > 0

@pytest.mark.asyncio
async def test_tiff_parser_async_with_ifd_count(big_tiff_path):
    reader = await open_local_reader_async(big_tiff_path)
    result = await TIFFParser.read(reader, bytes_peek=None, count_ifds=True)
    assert result.success
    assert result.data["width"] == 1388
    assert result.data["height"] == 1040
    assert result.data["format"] == "TIFF"
    assert result.data["ifd_count"] == 1
    assert result.requests_made > 0

def test_tiff_parser_sync(big_tiff_path):
    reader = open_local_reader(big_tiff_path)
    result = TIFFParser.read_sync(reader, bytes_peek=None)
    assert result.success
    assert result.data["width"] == 1388
    assert result.data["height"] == 1040
    assert result.data["format"] == "TIFF"
    assert result.data["ifd_count"] is None
    assert result.requests_made > 0

def test_tiff_parser_sync_with_ifd_count(big_tiff_path):
    reader = open_local_reader(big_tiff_path)
    result = TIFFParser.read_sync(reader, bytes_peek=None, count_ifds=True)
    assert result.success
    assert result.data["width"] == 1388
    assert result.data["height"] == 1040
    assert result.data["format"] == "TIFF"
    assert result.data["ifd_count"] == 1
    assert result.requests_made > 0

def test_parser_registration():
    """Test that TIFF parser is properly registered."""
    from fastheader.core.registry import _REGISTRY
    
    # Test extension registration
    parsers = _REGISTRY._by_ext.get("tif", [])
    assert any(parser_cls.__name__ == "TIFFParser" for _, _, parser_cls in parsers)
    
    parsers = _REGISTRY._by_ext.get("tiff", [])
    assert any(parser_cls.__name__ == "TIFFParser" for _, _, parser_cls in parsers)
