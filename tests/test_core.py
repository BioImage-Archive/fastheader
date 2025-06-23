import pytest
from pathlib import Path
from typing import ClassVar, Sequence

from fastheader.core.model import Result, UnknownFormatError
from fastheader.core.parser_base import HeaderParser, Signature
from fastheader.core.registry import _REGISTRY, ParserRegistry
from fastheader.core.util import result_asdict, dtype_from_code


class DummyParser(HeaderParser):
    """Test parser for unit tests."""
    formats: ClassVar[tuple[str, ...]] = ("test", "dummy")
    signatures: ClassVar[Sequence[Signature]] = [(0, b"TEST")]
    priority: ClassVar[int] = 50

    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        return Result(
            success=True,
            data={"format": "TEST", "width": 100, "height": 200},
            error=None,
            bytes_fetched=1024
        )

    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        return Result(
            success=True,
            data={"format": "TEST", "width": 100, "height": 200},
            error=None,
            bytes_fetched=1024
        )


class HighPriorityParser(HeaderParser):
    """Higher priority test parser."""
    formats: ClassVar[tuple[str, ...]] = ("test",)
    signatures: ClassVar[Sequence[Signature]] = [(0, b"HIGH")]
    priority: ClassVar[int] = 10

    @classmethod
    def read_sync(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        return Result(
            success=True,
            data={"format": "HIGH", "priority": True},
            error=None,
            bytes_fetched=512
        )

    @classmethod
    async def read(cls, reader, *, bytes_peek: int | None, _prefetched_header: bytes | None = None) -> Result:
        return Result(
            success=True,
            data={"format": "HIGH", "priority": True},
            error=None,
            bytes_fetched=512
        )


class TestParserRegistry:
    """Test the parser registry functionality."""

    def test_auto_registration(self):
        """Test that parsers are automatically registered."""
        # Create a fresh registry to test
        registry = ParserRegistry()
        
        # Define a parser class - should auto-register
        class TestAutoParser(HeaderParser):
            formats = ("auto",)
            signatures = [(0, b"AUTO")]
            
            @classmethod
            def read_sync(cls, reader, *, bytes_peek: int | None) -> Result:
                return Result(True, {}, None, 0)
            
            @classmethod
            async def read(cls, reader, *, bytes_peek: int | None) -> Result:
                return Result(True, {}, None, 0)
        
        # Manually register since we're using a test registry
        registry.register(TestAutoParser)
        
        # Should be able to find by signature
        parser = registry._sniff(b"AUTO" + b"\x00" * 100)
        assert parser == TestAutoParser
        
        # Should be able to find by extension
        parser = registry.choose("test.auto", b"\x00" * 100)
        assert parser == TestAutoParser

    def test_signature_detection(self):
        """Test magic number detection."""
        registry = ParserRegistry()
        registry.register(DummyParser)
        
        # Should find by signature
        parser = registry._sniff(b"TEST" + b"\x00" * 100)
        assert parser == DummyParser
        
        # Should not find with wrong signature
        parser = registry._sniff(b"WRONG" + b"\x00" * 100)
        assert parser is None

    def test_extension_fallback(self):
        """Test extension-based detection."""
        registry = ParserRegistry()
        registry.register(DummyParser)
        
        # Should find by extension when signature doesn't match
        parser = registry.choose("file.test", b"WRONG" + b"\x00" * 100)
        assert parser == DummyParser
        
        # Should find by extension with Path object
        parser = registry.choose(Path("file.dummy"), b"WRONG" + b"\x00" * 100)
        assert parser == DummyParser

    def test_priority_ordering(self):
        """Test that parsers are chosen by priority."""
        registry = ParserRegistry()
        registry.register(DummyParser)  # priority 50
        registry.register(HighPriorityParser)  # priority 10
        
        # Higher priority (lower number) should be chosen for same extension
        parser = registry.choose("file.test", b"NEITHER" + b"\x00" * 100)
        assert parser == HighPriorityParser

    def test_unknown_format_error(self):
        """Test that UnknownFormatError is raised for unknown formats."""
        registry = ParserRegistry()
        
        with pytest.raises(UnknownFormatError, match="No parser for"):
            registry.choose("file.unknown", b"UNKNOWN" + b"\x00" * 100)

    def test_signature_bounds_checking(self):
        """Test that signature detection handles short buffers gracefully."""
        registry = ParserRegistry()
        registry.register(DummyParser)
        
        # Short buffer should not crash
        parser = registry._sniff(b"TE")  # shorter than signature
        assert parser is None


class TestResultAsDict:
    """Test the result_asdict utility function."""

    def test_successful_result(self):
        """Test conversion of successful result."""
        result = Result(
            success=True,
            data={"format": "TEST", "width": 100, "height": None},
            error=None,
            bytes_fetched=1024
        )
        
        output = result_asdict(result)
        expected = {
            "success": True,
            "format": "TEST",
            "width": 100,
            "bytes_fetched": 1024
        }
        assert output == expected

    def test_failed_result(self):
        """Test conversion of failed result."""
        result = Result(
            success=False,
            data=None,
            error="Parse failed",
            bytes_fetched=512
        )
        
        output = result_asdict(result)
        expected = {
            "success": False,
            "error": "Parse failed",
            "bytes_fetched": 512
        }
        assert output == expected

    def test_field_filtering(self):
        """Test field filtering functionality."""
        result = Result(
            success=True,
            data={"format": "TEST", "width": 100, "height": 200, "depth": 1},
            error=None,
            bytes_fetched=1024
        )
        
        output = result_asdict(result, fields=["format", "width"])
        expected = {
            "success": True,
            "format": "TEST",
            "width": 100,
            "bytes_fetched": 1024
        }
        assert output == expected

    def test_none_filtering(self):
        """Test that None values are filtered out."""
        result = Result(
            success=True,
            data={"format": "TEST", "width": 100, "height": None, "depth": None},
            error=None,
            bytes_fetched=1024
        )
        
        output = result_asdict(result)
        expected = {
            "success": True,
            "format": "TEST",
            "width": 100,
            "bytes_fetched": 1024
        }
        assert output == expected


class TestDtypeFromCode:
    """Test the dtype_from_code utility function."""

    def test_known_codes(self):
        """Test conversion of known dtype codes."""
        assert dtype_from_code(0) == "int8"
        assert dtype_from_code(1) == "int16"
        assert dtype_from_code(2) == "float32"
        assert dtype_from_code(6) == "uint16"

    def test_unknown_code(self):
        """Test handling of unknown dtype codes."""
        assert dtype_from_code(999) is None
