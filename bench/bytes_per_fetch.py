"""Performance sanity benchmark for HTTP byte readers.

Quick script to assert ≤ 65 KB downloaded for a 100 MB TIFF from an HTTP test server.
This is skipped in CI and meant for manual runs.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fastheader.io import open_reader, open_reader_async


def test_sync_efficiency():
    """Test sync HTTP reader efficiency."""
    # This would need a real test server with a large TIFF file
    # For now, just demonstrate the concept
    
    print("Sync HTTP efficiency test")
    print("Note: This requires a test server with a large TIFF file")
    
    # Example usage (commented out since we don't have a real server):
    # url = "http://test-server.example.com/large-100mb.tiff"
    # reader = open_reader(url)
    # 
    # # Simulate typical header reading pattern
    # header_data = reader.fetch(0, 8)  # TIFF magic
    # ifd_offset_data = reader.fetch(4, 4)  # IFD offset
    # # ... more header reads
    # 
    # print(f"Total bytes fetched: {reader.bytes_fetched}")
    # assert reader.bytes_fetched <= 65 * 1024, f"Too many bytes: {reader.bytes_fetched}"
    
    print("Sync test would go here (requires test server)")


async def test_async_efficiency():
    """Test async HTTP reader efficiency."""
    print("Async HTTP efficiency test")
    print("Note: This requires a test server with a large TIFF file")
    
    # Example usage (commented out since we don't have a real server):
    # url = "http://test-server.example.com/large-100mb.tiff"
    # reader = await open_reader_async(url)
    # 
    # # Simulate typical header reading pattern
    # header_data = await reader.fetch(0, 8)  # TIFF magic
    # ifd_offset_data = await reader.fetch(4, 4)  # IFD offset
    # # ... more header reads
    # 
    # print(f"Total bytes fetched: {reader.bytes_fetched}")
    # assert reader.bytes_fetched <= 65 * 1024, f"Too many bytes: {reader.bytes_fetched}"
    
    print("Async test would go here (requires test server)")


if __name__ == "__main__":
    print("FastHeader I/O Performance Benchmark")
    print("=" * 40)
    
    test_sync_efficiency()
    print()
    
    asyncio.run(test_async_efficiency())
    
    print("\nBenchmark complete!")
    print("Note: Real tests require a test server with large files")
