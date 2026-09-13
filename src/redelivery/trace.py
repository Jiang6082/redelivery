"""Atomic, paged export of an append-only audit prefix for TraceScope."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .store import Inbox


def export_trace(inbox: Inbox, destination: Path) -> dict[str, int]:
    destination = destination.resolve()
    if destination == inbox.path or destination.suffix.lower() not in (".jsonl", ".ndjson"):
        raise ValueError("Choose a .jsonl or .ndjson output distinct from the database")
    destination.parent.mkdir(parents=True, exist_ok=True)
    high_water = inbox.audit_high_water()
    after, count = 0, 0
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=destination.parent, delete=False
        ) as stream:
            temporary = stream.name
            while page := inbox.audit_page(after=after, through=high_water):
                for row in page:
                    # Deliberately omit event keys, handler inputs, and results.
                    stream.write(json.dumps({"schema_version": 1, **row}) + "\n")
                    count += 1
                after = int(str(page[-1]["id"]))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
    return {"events": count, "through": high_water}
