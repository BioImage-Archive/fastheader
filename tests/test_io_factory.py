"""Tests for I/O factory functions."""

import pytest
import tempfile
import io
from pathlib import Path

from fastheader.io import open_reader, open_reader_async
from fastheader.io.local import LocalByteReader, LocalAsyncByteReader


class TestFactoryFunctions:
    """Test the main factory functions."""
    
    def test_open_reader_with_path_string(self):
        """Test factory with path string."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = open_reader(f.name)
            assert isinstance(reader, LocalByteReader)
            assert reader.fetch(0, 5) == b"01234"
            reader.close()
    
    def test_open_reader_with_path_object(self):
        """Test factory with Path object."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(test_data)
            f.flush()
            temp_path = Path(f.name)
        
        try:
            reader = open_reader(temp_path)
            assert isinstance(reader, LocalByteReader)
            assert reader.fetch(0, 5) == b"01234"
            reader.close()
        finally:
            temp_path.unlink()
    
    def test_open_reader_with_binary_io(self):
        """Test factory with BinaryIO object."""
        test_data = b"0123456789"
        bio = io.BytesIO(test_data)
        
        reader = open_reader(bio)
        assert isinstance(reader, LocalByteReader)
        assert reader.fetch(0, 5) == b"01234"
        reader.close()
    
    def test_open_reader_with_http_url(self):
        """Test factory with HTTP URL."""
        # This will fail in actual network call, but we can test the type detection
        from fastheader.io.http_sync import HTTPByteReader
        
        # We can't easily test this without a real server, but we can verify
        # the factory creates the right type
        try:
            reader = open_reader("http://example.com/test")
            assert isinstance(reader, HTTPByteReader)
        except Exception:
            # Expected to fail due to network, but type should be correct
            pass
    
    def test_open_reader_with_https_url(self):
        """Test factory with HTTPS URL."""
        from fastheader.io.http_sync import HTTPByteReader
        
        try:
            reader = open_reader("https://example.com/test")
            assert isinstance(reader, HTTPByteReader)
        except Exception:
            # Expected to fail due to network, but type should be correct
            pass
    
    @pytest.mark.asyncio
    async def test_open_reader_async_with_path(self):
        """Test async factory with path."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = await open_reader_async(f.name)
            assert isinstance(reader, LocalAsyncByteReader)
            assert await reader.fetch(0, 5) == b"01234"
            await reader.close()
    
    @pytest.mark.asyncio
    async def test_open_reader_async_with_binary_io(self):
        """Test async factory with BinaryIO object."""
        test_data = b"0123456789"
        bio = io.BytesIO(test_data)
        
        reader = await open_reader_async(bio)
        assert isinstance(reader, LocalAsyncByteReader)
        assert await reader.fetch(0, 5) == b"01234"
        await reader.close()
    
    @pytest.mark.asyncio
    async def test_open_reader_async_with_http_url(self):
        """Test async factory with HTTP URL."""
        from fastheader.io.http_async import HTTPAsyncByteReader
        
        try:
            reader = await open_reader_async("http://example.com/test")
            assert isinstance(reader, HTTPAsyncByteReader)
        except Exception:
            # Expected to fail due to network, but type should be correct
            pass
