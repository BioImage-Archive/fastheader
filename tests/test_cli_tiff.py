
import json
from pathlib import Path
from typer.testing import CliRunner

from fastheader.cli import app

runner = CliRunner()

def test_cli_tiff_ifd_count():
    """Test the --count-ifds flag for TIFF files."""
    tiff_path = Path(__file__).parent.parent / "test_data" / "2-Image-Export-02_c1-3.tif"
    result = runner.invoke(app, [str(tiff_path), "--count-ifds"])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["success"]
    assert data["ifd_count"] == 1

def test_cli_tiff_no_ifd_count():
    """Test that IFD count is None by default for TIFF files."""
    tiff_path = Path(__file__).parent.parent / "test_data" / "2-Image-Export-02_c1-3.tif"
    result = runner.invoke(app, [str(tiff_path)])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert data["success"]
    assert "ifd_count" not in data
