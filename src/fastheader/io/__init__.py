"""I/O layer for FastHeader - delivers exact byte windows to parsers."""

# Re-export these for import convenience
from .base import ByteReader, AsyncByteReader, RangeNotSupportedError
from .local import open_local_reader, open_local_reader_async
from .http_sync import open_http_reader
from .http_async import open_http_reader_async

def open_reader(source):
    """Factory function to create appropriate ByteReader based on source type."""
    from pathlib import Path
    
    if hasattr(source, 'read'):  # BinaryIO
        return open_local_reader(source)
    
    source_str = str(source)
    if source_str.startswith(('http://', 'https://')):
        return open_http_reader(source_str)
    else:
        return open_local_reader(source)

async def open_reader_async(source):
    """Factory function to create appropriate AsyncByteReader based on source type."""
    from pathlib import Path
    
    if hasattr(source, 'read'):  # BinaryIO
        return await open_local_reader_async(source)
    
    source_str = str(source)
    if source_str.startswith(('http://', 'https://')):
        return await open_http_reader_async(source_str)
    else:
        return await open_local_reader_async(source)
