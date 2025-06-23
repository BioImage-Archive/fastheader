from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(slots=True)
class Result:
    success: bool
    data: Dict[str, Any] | None
    error: str | None
    bytes_fetched: int         # filled by I/O layer or parser


class UnknownFormatError(RuntimeError):
    """Raised when no parser can be found for a given source."""
    pass


class ParseError(RuntimeError):
    """Raised when a parser encounters an error while parsing."""
    pass
