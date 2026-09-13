"""Small public job-board adapter. A missing posting never implies a deletion."""

from __future__ import annotations

import hashlib
import json
import re
import urllib.request
from typing import cast

from .store import Inbox, Payload, canonical

MAX_FEED_BYTES = 8 * 1024 * 1024


def board_name(board: str) -> None:
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", board):
        raise ValueError("Board must contain 1 to 80 ASCII letters, digits, underscores or hyphens")


def fetch_board(board: str) -> bytes:
    board_name(board)
    request = urllib.request.Request(
        f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs",
        headers={"User-Agent": "Redelivery/0.2 public-board-reader", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        data = cast(bytes, response.read(MAX_FEED_BYTES + 1))
    if len(data) > MAX_FEED_BYTES:
        raise ValueError("Board response exceeds 8 MiB")
    return data


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2000:
        raise ValueError(f"Invalid {field}")
    return " ".join(value.split())


def parse_board(board: str, data: bytes) -> list[tuple[str, Payload]]:
    """Validate every row before the first enqueue; content changes get a new identity."""
    board_name(board)
    if len(data) > MAX_FEED_BYTES:
        raise ValueError("Board response exceeds 8 MiB")

    def unique_pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result

    try:
        feed = json.loads(data, object_pairs_hook=unique_pairs)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("Invalid board JSON") from exc
    if not isinstance(feed, dict) or not isinstance(feed.get("jobs"), list):
        raise ValueError("Response requires a jobs array")
    rows = feed["jobs"]
    meta = feed.get("meta")
    if (
        len(rows) > 50_000
        or not isinstance(meta, dict)
        or type(meta.get("total")) is not int
        or meta["total"] != len(rows)
    ):
        raise ValueError("Board count is missing, inconsistent, or too large")
    postings: list[tuple[str, Payload]] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] <= 0:
            raise ValueError("Each posting needs a positive integer ID")
        posting_id = cast(int, row["id"])
        if posting_id in seen:
            raise ValueError("Duplicate posting ID in board response")
        seen.add(posting_id)
        location = row.get("location")
        if not isinstance(location, dict):
            raise ValueError("Posting requires a location object")
        payload: Payload = {
            "upstream_id": posting_id,
            "company": _text(row.get("company_name", board), "company"),
            "title": _text(row.get("title"), "title"),
            "location": _text(location.get("name"), "location"),
            "url": _text(row.get("absolute_url"), "absolute_url"),
        }
        digest = hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()
        postings.append((f"{posting_id}:{digest}", payload))
    return postings


def ingest_board(inbox: Inbox, board: str, data: bytes) -> dict[str, int]:
    postings = parse_board(board, data)
    counts = {"accepted": 0, "duplicates": 0}
    for key, payload in postings:
        receipt = inbox.enqueue(f"greenhouse:{board}:v1", key, payload)
        counts["duplicates" if receipt.duplicate else "accepted"] += 1
    return counts
