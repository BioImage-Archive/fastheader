FastHeader
==========

A python library and client for extracting file headers quickly, including from remote content. Primarily designed to support image formats.

Design
------

### High-level goals

Goal	Notes
Minimal-byte header reads	HTTP Range + mmap; never full-GET unless file < 10 MB and server rejects ranges
Local & remote	Accept pathlib.Path, file-like objects, or http(s):// URLs
Sync and async	Same semantics; async preferred for remote I/O
Microscopy focus	TIFF, MRC (plus JPEG & PNG for breadth)
Stable CLI/JSON schema	Must remain when ported to Rust

### Data model

For returns:

```
from dataclasses import dataclass

@dataclass
class Result:
    success: bool
    data: dict | None          # flattened header fields
    error: str | None          # message on failure
    bytes_fetched: int         # total bytes read
```

Key	Type	Description
source	str	Original path/URL
format	"JPEG" | "PNG" | "TIFF" | "MRC"	
width / height	int	Pixels
depth	int | None	z-slices (nz for MRC)
dtype	"uint8" …"	NumPy-style
single_voxel_physical_size_x/y/z	float | None	Metres
peek_bytes_b64	str | None	Present when --bytes N; Base64 of first N bytes

### I/O strategy

1.	HEAD → check Accept-Ranges & Content-Length.
2.	If ranged: fetch only needed windows (≤ 32 KB typical, ≤ 64 KB worst TIFF).
3.	If no ranges & size < 10 MB: full GET. Else: fail (error="no-range-large").
4.	Local: mmap.