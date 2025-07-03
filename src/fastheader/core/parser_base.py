from abc import ABC, abstractmethod
from typing import ClassVar, Sequence, Tuple
from .model import Result

Signature = Tuple[int, bytes]          # (offset, byte-pattern)


class HeaderParser(ABC):
    # --- required by subclasses ---
    formats: ClassVar[tuple[str, ...]]      # file-extensions (lower, no dot)
    signatures: ClassVar[Sequence[Signature]]  # magic bytes patterns
    priority: ClassVar[int] = 100            # lower = examined earlier

    # --- sync ---
    @classmethod
    @abstractmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        """Read header synchronously from a ByteReader."""
        ...

    # --- async ---
    @classmethod
    @abstractmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None, **kwargs) -> Result:
        """Read header asynchronously from an AsyncByteReader."""
        ...

    # --- registry hook ---
    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        from .registry import _REGISTRY
        _REGISTRY.register(cls)           # noqa: E402
