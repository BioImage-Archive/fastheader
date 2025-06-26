
import pytest
import os
from pathlib import Path

from fastheader.io.local import open_local_reader, open_local_reader_async
from fastheader.parsers.png import PNGParser

@pytest.fixture(scope="module")
def tiny_png_path():
    return Path(__file__).parent.parent / "test_data" / "tiny.png"

@pytest.mark.asyncio
async def test_png_parser_async(tiny_png_path):
    reader = await open_local_reader_async(tiny_png_path)
    result = await PNGParser.read(reader, bytes_peek=None)
    assert result.success
    assert result.data["width"] == 10
    assert result.data["height"] == 20
    assert result.data["format"] == "PNG"

def test_png_parser_sync(tiny_png_path):
    reader = open_local_reader(tiny_png_path)
    result = PNGParser.read_sync(reader, bytes_peek=None)
    assert result.success
    assert result.data["width"] == 10
    assert result.data["height"] == 20
    assert result.data["format"] == "PNG"
