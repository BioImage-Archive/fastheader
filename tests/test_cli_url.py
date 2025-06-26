import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from fastheader.cli import app


class TestCLIURL:
    """Test the CLI functionality with remote URLs."""

    @pytest.fixture
    def runner(self):
        """CLI test runner."""
        return CliRunner()

    def test_remote_file(self, runner, httpserver):
        """Test single file output with pretty JSON."""
        fixture_path = Path(__file__).parent.parent / "test_data" / "tiny.png"
        httpserver.expect_request("/tiny.png").respond_with_data(
            fixture_path.read_bytes(), content_type="image/png"
        )
        result = runner.invoke(app, [httpserver.url_for("/tiny.png")])

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["success"] is True
        assert payload["format"] == "PNG"
        assert payload["width"] == 10
        assert payload["height"] == 20
