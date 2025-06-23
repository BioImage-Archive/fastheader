#!/usr/bin/env python3
"""Create a minimal valid MRC file for testing."""

import struct
from pathlib import Path

def create_tiny_mrc():
    """Create a minimal 1024-byte MRC file with valid header."""
    header = bytearray(1024)
    
    # Dimensions: 10x20x1 (nx, ny, nz)
    struct.pack_into("<3i", header, 0, 10, 20, 1)
    
    # Mode: 2 (float32)
    struct.pack_into("<i", header, 12, 2)
    
    # Unit cell dimensions in Angstroms: 100.0, 200.0, 10.0
    struct.pack_into("<3f", header, 40, 100.0, 200.0, 10.0)
    
    # Cell angles (ignored but set to 90 degrees)
    struct.pack_into("<3f", header, 52, 90.0, 90.0, 90.0)
    
    # Sampling: mx=10, my=20, mz=1
    struct.pack_into("<3i", header, 92, 10, 20, 1)
    
    # Magic bytes "MAP " at offset 208
    header[208:212] = b"MAP "
    
    # Machine stamp (little endian indicator)
    struct.pack_into("<i", header, 212, 0x00004144)
    
    return bytes(header)

if __name__ == "__main__":
    fixtures_dir = Path(__file__).parent / "fixtures"
    fixtures_dir.mkdir(exist_ok=True)
    
    mrc_data = create_tiny_mrc()
    with open(fixtures_dir / "tiny.mrc", "wb") as f:
        f.write(mrc_data)
    
    print(f"Created tiny.mrc ({len(mrc_data)} bytes)")
