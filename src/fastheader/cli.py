
"""CLI implementation for fastheader."""

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import typer

from . import read_header, read_header_sync
from .core.model import Result
from .core.util import result_asdict

app = typer.Typer(add_completion=False, help="Extract header info from files and URLs.")


def iter_sources(files: list[str]) -> list[str]:
    """Get list of sources from files argument or stdin."""
    if files and files != ("-",):
        return list(files)
    # stdin mode
    return [ln.strip() for ln in sys.stdin if ln.strip()]


async def _batch_read(sources: list[str], bytes_peek: Optional[int]) -> list[Result]:
    """Asynchronously read headers from a list of sources."""
    tasks = [read_header(src, bytes_peek=bytes_peek) for src in sources]
    return await asyncio.gather(*tasks)


@app.command()
def main(
    files: list[str] = typer.Argument(None, help="Files or URLs to process, or '-' for stdin"),
    bytes: Optional[int] = typer.Option(None, "--bytes", min=0, help="Peek first N bytes (Base64)"),
    fields: Optional[str] = typer.Option(None, "--fields", help="Comma-separated subset of keys to emit"),
    jsonl: bool = typer.Option(False, "--jsonl", help="Force JSON-lines output"),
    output: Optional[Path] = typer.Option(None, "-o", "--output", help="Write to PATH instead of stdout"),
    sync: bool = typer.Option(False, "--sync", help="Force synchronous I/O"),
):
    """Extract header info from one or many local paths or URLs."""
    sel_fields = set(fields.split(",")) if fields else None
    sources = iter_sources(files)
    
    if not sources:
        typer.echo("No input files given.", err=True)
        raise typer.Exit(code=1)

    # decide sync vs async
    run_sync = sync  # Force sync if requested

    results: list[Result] = []
    if run_sync:
        for src in sources:
            try:
                # Check if src is a URL
                parsed_url = urlparse(src)
                if parsed_url.scheme and parsed_url.netloc:  # It's a URL
                    res = read_header_sync(src, bytes_peek=bytes)
                else:  # It's a local path
                    res = read_header_sync(str(Path(src).resolve()), bytes_peek=bytes)
            except Exception as e:
                res = Result(success=False, data=None, error=str(e), bytes_fetched=0)
            results.append(res)
    else:
        results = asyncio.run(_batch_read(sources, bytes))

    # open output sink
    sink = open(output, "w", encoding="utf-8") if output else sys.stdout
    try:
        # choose output style
        if len(sources) == 1 and not jsonl:
            obj = result_asdict(results[0], fields=sel_fields)
            json.dump(obj, sink, indent=2)
            sink.write("\n")
        else:
            for res in results:
                obj = result_asdict(res, fields=sel_fields)
                sink.write(json.dumps(obj))
                sink.write("\n")
    finally:
        if output:
            sink.close()

    # exit code
    if any(not r.success for r in results):
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

