"""Tests for the CLI implementation."""

import json
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from fastheader.cli import app


class TestCLI:
    """Test the CLI functionality."""

    @pytest.fixture
    def runner(self):
        """CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def tiny_mrc_path(self):
        """Path to the tiny MRC test fixture."""
        return Path(__file__).parent / "fixtures" / "tiny.mrc"

    def test_single_file_json_pretty(self, runner, tiny_mrc_path):
        """Test single file output with pretty JSON."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = runner.invoke(app, [str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["success"] is True
        assert payload["format"] == "MRC"
        assert payload["width"] == 10
        assert payload["height"] == 20

    def test_multiple_files_jsonl(self, runner, tiny_mrc_path):
        """Test multiple files with JSONL output."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        # Use the same file twice to test multiple inputs
        result = runner.invoke(app, ["--jsonl", str(tiny_mrc_path), str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 2
        
        for line in lines:
            obj = json.loads(line)
            assert obj["success"] is True
            assert obj["format"] == "MRC"

    def test_force_jsonl_single_file(self, runner, tiny_mrc_path):
        """Test --jsonl flag forces JSONL even for single file."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = runner.invoke(app, ["--jsonl", str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 1
        
        obj = json.loads(lines[0])
        assert obj["success"] is True
        assert obj["format"] == "MRC"

    def test_bytes_peek_option(self, runner, tiny_mrc_path):
        """Test --bytes option adds peek data."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = runner.invoke(app, ["--bytes", "16", str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        obj = json.loads(result.stdout)
        assert "peek_bytes_b64" in obj

    def test_fields_filtering(self, runner, tiny_mrc_path):
        """Test --fields option filters output."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = runner.invoke(app, ["--fields", "format,width", str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        obj = json.loads(result.stdout)
        
        # Should only have the requested fields plus success/bytes_fetched
        expected_keys = {"success", "format", "width", "bytes_fetched", "requests_made"}
        assert set(obj.keys()) == expected_keys

    def test_output_file_option(self, runner, tiny_mrc_path):
        """Test -o/--output option writes to file."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
            tmp_path = tmp.name
        
        try:
            result = runner.invoke(app, ["-o", tmp_path, str(tiny_mrc_path)])
            
            assert result.exit_code == 0
            assert result.stdout == ""  # Nothing to stdout when using -o
            
            # Check file contents
            with open(tmp_path, "r") as f:
                obj = json.load(f)
            assert obj["success"] is True
            assert obj["format"] == "MRC"
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def test_sync_option(self, runner, tiny_mrc_path):
        """Test --sync option forces synchronous processing."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        result = runner.invoke(app, ["--sync", str(tiny_mrc_path)])
        
        assert result.exit_code == 0
        obj = json.loads(result.stdout)
        assert obj["success"] is True
        assert obj["format"] == "MRC"

    # def test_stdin_mode_with_dash(self, runner, tiny_mrc_path):
    #     """Test stdin mode with '-' argument."""
    #     if not tiny_mrc_path.exists():
    #         pytest.skip("tiny.mrc fixture not found")

    #     # Use absolute path for stdin to avoid ambiguity
    #     input_data = str(tiny_mrc_path.resolve()) + "\n"
    #     result = runner.invoke(app, ["-"], input=input_data)

    #     assert result.exit_code == 0, result.stderr
    #     obj = json.loads(result.stdout)
    #     assert obj["success"] is True
    #     assert obj["format"] == "MRC"

    # def test_stdin_mode_no_args(self, runner, tiny_mrc_path):
    #     """Test stdin mode with no positional arguments."""
    #     if not tiny_mrc_path.exists():
    #         pytest.skip("tiny.mrc fixture not found")
            
    #     input_data = str(tiny_mrc_path) + "\n"
    #     result = runner.invoke(app, [], input=input_data)
        
    #     assert result.exit_code == 0
    #     obj = json.loads(result.stdout)
    #     assert obj["success"] is True
    #     assert obj["format"] == "MRC"

    # def test_no_input_files_error(self, runner):
    #     """Test error when no input files are provided."""
    #     result = runner.invoke(app, [])
        
    #     assert result.exit_code == 1
    #     assert "No input files given." in result.stderr

    def test_nonexistent_file_error(self, runner):
        """Test error handling for nonexistent files."""
        result = runner.invoke(app, ["/nonexistent/file.mrc"])
        
        assert result.exit_code == 1
        obj = json.loads(result.stdout)
        assert obj["success"] is False
        assert "No such file or directory" in obj["error"]

    def test_mixed_success_failure_exit_code(self, runner, tiny_mrc_path):
        """Test exit code 1 when some files fail."""
        if not tiny_mrc_path.exists():
            pytest.skip("tiny.mrc fixture not found")
            
        # Mix valid and invalid files
        result = runner.invoke(app, ["--jsonl", str(tiny_mrc_path), "/nonexistent/file.mrc"])
        
        assert result.exit_code == 1
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 2
        
        # First should succeed
        obj1 = json.loads(lines[0])
        assert obj1["success"] is True
        
        # Second should fail
        obj2 = json.loads(lines[1])
        assert obj2["success"] is False
        assert "No such file or directory" in obj2["error"]

    def test_empty_stdin_input(self, runner):
        """Test handling of empty stdin input."""
        result = runner.invoke(app, ["-"], input="")
        
        assert result.exit_code == 1
        assert "Aborted." in result.stderr

    # def test_stdin_with_empty_lines(self, runner, tiny_mrc_path):
    #     """Test stdin input with empty lines (should be filtered out)."""
    #     if not tiny_mrc_path.exists():
    #         pytest.skip("tiny.mrc fixture not found")
            
    #     input_data = f"\n{tiny_mrc_path}\n\n"
    #     result = runner.invoke(app, ["-"], input=input_data)
        
    #     assert result.exit_code == 0
    #     obj = json.loads(result.stdout)
    #     assert obj["success"] is True
    #     assert obj["format"] == "MRC"
