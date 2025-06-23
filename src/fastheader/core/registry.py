from __future__ import annotations
import bisect
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Type

from .parser_base import HeaderParser
from .model import UnknownFormatError


class ParserRegistry:
    def __init__(self) -> None:
        self._by_ext: Dict[str, List[Type[HeaderParser]]] = defaultdict(list)
        self._parsers: List[tuple[int, Type[HeaderParser]]] = []   # sorted by priority

    # called from HeaderParser.__init_subclass__
    def register(self, parser_cls: Type[HeaderParser]) -> None:
        bisect.insort(self._parsers, (parser_cls.priority, parser_cls))
        for ext in parser_cls.formats:
            self._by_ext[ext].append(parser_cls)

    # --- detection helpers ---
    def _sniff(self, first_kb: bytes) -> Type[HeaderParser] | None:
        for _, p in self._parsers:
            for offset, pat in p.signatures:
                if len(first_kb) >= offset + len(pat):
                    if first_kb[offset : offset + len(pat)] == pat:
                        return p
        return None

    def choose(self, source: str | Path, first_kb: bytes) -> Type[HeaderParser]:
        # 1) magic-number sniff
        parser = self._sniff(first_kb)
        if parser:
            return parser
        # 2) extension hint
        ext = Path(str(source)).suffix.lower().lstrip(".")
        if ext and (lst := self._by_ext.get(ext)):
            return lst[0]                 # first by priority
        raise UnknownFormatError(f"No parser for {source!s}")


# singleton used project-wide
_REGISTRY = ParserRegistry()
