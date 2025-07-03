from __future__ import annotations
from typing import Dict, Any, Iterable
from .model import Result

# FIXME - this is very MRC specific, should be in the MRC parser
_DTYPE_MAP = {
    0: "int8", 1: "int16", 2: "float32", 6: "uint16",  # used by MRC etc.
    # add more as needed
}


def dtype_from_code(code: int) -> str | None:
    return _DTYPE_MAP.get(code)


def result_asdict(res: Result, *, fields: Iterable[str] | None = None) -> Dict[str, Any]:
    """Return a JSON-serialisable dict (skip None) optionally filtered."""
    if not res.success or res.data is None:
        return {"success": False, "error": res.error, "bytes_fetched": res.bytes_fetched, "requests_made": res.requests_made}
    payload = {k: v for k, v in res.data.items() if v is not None}
    if fields:
        wanted = set(fields)
        payload = {k: v for k, v in payload.items() if k in wanted}
    payload.update({"success": True, "bytes_fetched": res.bytes_fetched, "requests_made": res.requests_made})
    return payload
