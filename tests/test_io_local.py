"""Tests for local file I/O."""

import pytest
import tempfile
from pathlib import Path
import io

from fastheader.io.local import LocalByteReader, LocalAsyncByteReader, open_local_reader, open_local_reader_async


class TestLocalByteReader:
    """Test synchronous local byte reader."""
    
    def test_basic_fetch(self):
        """Test basic fetch operations."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = LocalByteReader(f.name)
            
            # Test various slices
            assert reader.fetch(0, 5) == b"01234"
            assert reader.fetch(5, 5) == b"56789"
            assert reader.fetch(2, 3) == b"234"
            
            # Check bytes_fetched accounting
            assert reader.bytes_fetched == 13  # 5 + 5 + 3
            
            reader.close()
    
    def test_overlapping_slices(self):
        """Test overlapping slice requests."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = LocalByteReader(f.name)
            
            # Overlapping requests
            assert reader.fetch(0, 3) == b"012"
            assert reader.fetch(2, 3) == b"234"
            assert reader.fetch(1, 4) == b"1234"
            
            assert reader.bytes_fetched == 10  # 3 + 3 + 4
            
            reader.close()
    
    def test_binary_io_source(self):
        """Test using BinaryIO as source."""
        test_data = b"0123456789"
        bio = io.BytesIO(test_data)
        
        reader = LocalByteReader(bio)
        
        assert reader.fetch(0, 5) == b"01234"
        assert reader.fetch(5, 5) == b"56789"
        assert reader.bytes_fetched == 10
        
        reader.close()
    
    def test_path_source(self):
        """Test using Path as source."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(test_data)
            f.flush()
            temp_path = Path(f.name)
        
        try:
            reader = LocalByteReader(temp_path)
            
            assert reader.fetch(0, 5) == b"01234"
            assert reader.bytes_fetched == 5
            
            reader.close()
        finally:
            temp_path.unlink()
    
    def test_error_conditions(self):
        """Test error conditions."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = LocalByteReader(f.name)
            
            # Negative start
            with pytest.raises(IOError, match="Start offset cannot be negative"):
                reader.fetch(-1, 5)
            
            # Read past EOF
            with pytest.raises(IOError, match="Not enough data"):
                reader.fetch(5, 10)  # Only 10 bytes total, asking for 10 starting at 5
            
            reader.close()
    
    def test_context_manager(self):
        """Test context manager usage."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            with LocalByteReader(f.name) as reader:
                assert reader.fetch(0, 5) == b"01234"
                assert reader.bytes_fetched == 5
    
    def test_empty_file(self):
        """Test handling of empty files."""
        with tempfile.NamedTemporaryFile() as f:
            # File is empty
            with pytest.raises(IOError, match="Cannot mmap empty file"):
                reader = LocalByteReader(f.name)
                reader.fetch(0, 1)


class TestLocalAsyncByteReader:
    """Test asynchronous local byte reader."""
    
    @pytest.mark.asyncio
    async def test_basic_fetch(self):
        """Test basic async fetch operations."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = LocalAsyncByteReader(f.name)
            
            # Test various slices
            assert await reader.fetch(0, 5) == b"01234"
            assert await reader.fetch(5, 5) == b"56789"
            assert await reader.fetch(2, 3) == b"234"
            
            # Check bytes_fetched accounting
            assert reader.bytes_fetched == 13  # 5 + 5 + 3
            
            await reader.close()
    
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager usage."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            async with LocalAsyncByteReader(f.name) as reader:
                assert await reader.fetch(0, 5) == b"01234"
                assert reader.bytes_fetched == 5


class TestFactoryFunctions:
    """Test factory functions."""
    
    def test_open_local_reader(self):
        """Test sync factory function."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = open_local_reader(f.name)
            assert isinstance(reader, LocalByteReader)
            assert reader.fetch(0, 5) == b"01234"
            reader.close()
    
    @pytest.mark.asyncio
    async def test_open_local_reader_async(self):
        """Test async factory function."""
        test_data = b"0123456789"
        
        with tempfile.NamedTemporaryFile() as f:
            f.write(test_data)
            f.flush()
            
            reader = await open_local_reader_async(f.name)
            assert isinstance(reader, LocalAsyncByteReader)
            assert await reader.fetch(0, 5) == b"01234"
            await reader.close()
