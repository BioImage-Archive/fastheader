"""CLI implementation for fastheader."""

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import typer

from . import read_header, read_header_sync
from .core.util import result_asdict

app = typer.Typer(add_completion=False, help="Extract header info from files and URLs.")


def iter_sources(files: list[str]) -> list[str]:
    """Get list of sources from files argument or stdin."""
    if files and files != ("-",):
        return list(files)
    # stdin mode
    return [ln.strip() for ln in sys.stdin if ln.strip()]


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
    try:
        loop = asyncio.get_running_loop()
        run_sync = sync  # Force sync if requested
    except RuntimeError:
        run_sync = True  # No loop running, use sync

    results = []
    if run_sync:
        for src in sources:
            res = read_header_sync(src, bytes_peek=bytes)
            results.append(res)
    else:
        async def _batch():
            for src in sources:
                res = await read_header(src, bytes_peek=bytes)
                results.append(res)
        asyncio.run(_batch())

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
