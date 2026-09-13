"""One-file transactional inbox. Handler computation never runs under a database lock."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias, cast

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
Payload: TypeAlias = dict[str, JSONValue]
MAX_BYTES = 65_536
STATES = ("pending", "leased", "succeeded", "dead")
log = logging.getLogger("redelivery")


class Conflict(ValueError):
    """A producer reused an identity with different content or processing policy."""


class LostLease(RuntimeError):
    """The caller no longer owns a valid assignment."""


@dataclass(frozen=True, slots=True)
class Receipt:
    job_id: int
    duplicate: bool


@dataclass(frozen=True, slots=True)
class Lease:
    job_id: int
    source: str
    key: str
    payload: Payload
    owner: str
    generation: int
    attempt: int
    lease_until: float


def _validate_json(value: object, depth: int = 0) -> None:
    if depth > 32:
        raise ValueError("JSON nesting exceeds 32 levels")
    if value is None or type(value) in (bool, int, str):
        return
    if type(value) is float and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _validate_json(item, depth + 1)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _validate_json(item, depth + 1)
        return
    raise ValueError("Only finite JSON values with string object keys are supported")


def canonical(payload: Payload) -> str:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a JSON object")
    _validate_json(payload)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > MAX_BYTES:
        raise ValueError(f"JSON exceeds {MAX_BYTES} bytes")
    return encoded


def parse_object(text: str) -> Payload:
    if len(text.encode("utf-8")) > MAX_BYTES:
        raise ValueError(f"JSON exceeds {MAX_BYTES} bytes")

    def pairs(items: list[tuple[str, JSONValue]]) -> Payload:
        obj: Payload = {}
        for key, value in items:
            if key in obj:
                raise ValueError("Duplicate JSON object key")
            obj[key] = value
        return obj

    try:
        value = json.loads(text, object_pairs_hook=pairs)
        canonical(value)
    except RecursionError as exc:
        raise ValueError("JSON nesting is too deep") from exc
    return cast(Payload, value)


def label(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        raise ValueError(f"{name} must contain 1 to 200 characters")


def duration(value: float, name: str, maximum: float = 86_400) -> None:
    if isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= maximum:
        raise ValueError(f"{name} must be finite, positive and at most {maximum}")


SCHEMA = [
    """CREATE TABLE jobs (
        id INTEGER PRIMARY KEY,
        source TEXT NOT NULL, event_key TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending','leased','succeeded','dead')),
        generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
        attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
        max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 100),
        available_at REAL NOT NULL, owner TEXT, lease_until REAL,
        created_at REAL NOT NULL, updated_at REAL NOT NULL,
        UNIQUE(source, event_key),
        CHECK((status='leased' AND owner IS NOT NULL AND lease_until IS NOT NULL)
           OR (status!='leased' AND owner IS NULL AND lease_until IS NULL))
    )""",
    "CREATE INDEX pending_jobs ON jobs(available_at, id) WHERE status='pending'",
    "CREATE INDEX expired_jobs ON jobs(lease_until, id) WHERE status='leased'",
    """CREATE TABLE results (
        job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
        generation INTEGER NOT NULL, output TEXT NOT NULL, created_at REAL NOT NULL
    )""",
    """CREATE TABLE transitions (
        id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL REFERENCES jobs(id),
        at REAL NOT NULL, event TEXT NOT NULL, generation INTEGER NOT NULL,
        detail TEXT NOT NULL
    )""",
    "CREATE INDEX job_history ON transitions(job_id, id)",
]


class Inbox:
    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        busy_timeout: float = 5.0,
        retry_base: float = 1.0,
        retry_cap: float = 60.0,
    ) -> None:
        duration(busy_timeout, "busy_timeout", 60)
        duration(retry_base, "retry_base")
        duration(retry_cap, "retry_cap")
        if retry_cap < retry_base:
            raise ValueError("retry_cap must be at least retry_base")
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.busy_timeout = busy_timeout
        self.retry_base = retry_base
        self.retry_cap = retry_cap
        with self._transaction() as db:
            if db.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
                raise ValueError("Use a dedicated rollback-journal database")
            version = int(db.execute("PRAGMA user_version").fetchone()[0])
            if version == 0:
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
                    raise ValueError("Refusing to initialize an unrelated database")
                for statement in SCHEMA:
                    db.execute(statement)
                db.execute("PRAGMA user_version=1")
            elif version != 1:
                raise ValueError(f"Unsupported schema version: {version}")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=self.busy_timeout, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
        except BaseException:
            db.close()
            raise
        return db

    @contextmanager
    def _transaction(self, *, write: bool = True) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _now(self) -> float:
        now = self.clock()
        if not math.isfinite(now):
            raise ValueError("Clock must return finite seconds")
        return now

    @staticmethod
    def _record(
        db: sqlite3.Connection,
        job_id: int,
        now: float,
        event: str,
        generation: int,
        detail: str = "",
    ) -> None:
        db.execute(
            "INSERT INTO transitions(job_id, at, event, generation, detail) VALUES(?,?,?,?,?)",
            (job_id, now, event, generation, detail),
        )

    def enqueue(self, source: str, key: str, payload: Payload, *, max_attempts: int = 3) -> Receipt:
        label(source, "source")
        label(key, "key")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer from 1 to 100")
        encoded = canonical(payload)
        with self._transaction() as db:
            existing = db.execute(
                "SELECT id, payload, max_attempts FROM jobs WHERE source=? AND event_key=?",
                (source, key),
            ).fetchone()
            if existing:
                if existing["payload"] != encoded or existing["max_attempts"] != max_attempts:
                    raise Conflict(
                        "Identity already exists with different content or attempt budget"
                    )
                return Receipt(int(existing["id"]), True)
            now = self._now()
            cursor = db.execute(
                """INSERT INTO jobs(source, event_key, payload, status, max_attempts,
                   available_at, created_at, updated_at) VALUES(?,?,?,'pending',?,?,?,?)""",
                (source, key, encoded, max_attempts, now, now, now),
            )
            job_id = int(cast(int, cursor.lastrowid))
            self._record(db, job_id, now, "enqueued", 0)
        log.info("enqueued", extra={"job_id": job_id})
        return Receipt(job_id, False)

    def claim(self, owner: str, *, lease_seconds: float = 30.0) -> Lease | None:
        label(owner, "owner")
        duration(lease_seconds, "lease_seconds")
        with self._transaction() as db:
            now = self._now()
            # Sweep exhausted leases in a bounded batch. Further claims continue the sweep.
            exhausted = db.execute(
                """SELECT id, generation FROM jobs WHERE status='leased' AND lease_until<=?
                   AND attempts>=max_attempts ORDER BY lease_until, id LIMIT 100""",
                (now,),
            ).fetchall()
            for row in exhausted:
                db.execute(
                    "UPDATE jobs SET status='dead',owner=NULL,lease_until=NULL,updated_at=? "
                    "WHERE id=?",
                    (now, row["id"]),
                )
                self._record(db, row["id"], now, "dead", row["generation"], "lease_exhausted")
            row = db.execute(
                """SELECT * FROM jobs WHERE attempts<max_attempts AND
                   ((status='pending' AND available_at<=?) OR
                    (status='leased' AND lease_until<=?))
                   ORDER BY CASE WHEN status='leased' THEN lease_until ELSE available_at END, id
                   LIMIT 1""",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            generation = int(row["generation"]) + 1
            attempt = int(row["attempts"]) + 1
            until = now + lease_seconds
            db.execute(
                """UPDATE jobs SET status='leased',owner=?,generation=?,attempts=?,lease_until=?,
                   updated_at=? WHERE id=?""",
                (owner, generation, attempt, until, now, row["id"]),
            )
            self._record(
                db,
                row["id"],
                now,
                "reclaimed" if row["status"] == "leased" else "claimed",
                generation,
                owner,
            )
            lease = Lease(
                int(row["id"]),
                str(row["source"]),
                str(row["event_key"]),
                parse_object(row["payload"]),
                owner,
                generation,
                attempt,
                until,
            )
        log.info("claimed", extra={"job_id": lease.job_id, "generation": generation})
        return lease

    @staticmethod
    def _owned(db: sqlite3.Connection, lease: Lease, now: float) -> sqlite3.Row:
        row = db.execute(
            """SELECT * FROM jobs WHERE id=? AND status='leased' AND owner=?
               AND generation=? AND lease_until>?""",
            (lease.job_id, lease.owner, lease.generation, now),
        ).fetchone()
        if row is None:
            raise LostLease("Assignment expired, was replaced, or is already complete")
        return cast(sqlite3.Row, row)

    def complete(self, lease: Lease, result: Payload) -> None:
        encoded = canonical(result)
        with self._transaction() as db:
            now = self._now()
            self._owned(db, lease, now)
            db.execute(
                "INSERT INTO results(job_id,generation,output,created_at) VALUES(?,?,?,?)",
                (lease.job_id, lease.generation, encoded, now),
            )
            db.execute(
                "UPDATE jobs SET status='succeeded',owner=NULL,lease_until=NULL,updated_at=? "
                "WHERE id=?",
                (now, lease.job_id),
            )
            self._record(db, lease.job_id, now, "succeeded", lease.generation)
        log.info("succeeded", extra={"job_id": lease.job_id, "generation": lease.generation})

    def renew(self, lease: Lease, *, lease_seconds: float = 30.0) -> float:
        duration(lease_seconds, "lease_seconds")
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, lease, now)
            until = max(float(row["lease_until"]), now + lease_seconds)
            db.execute(
                "UPDATE jobs SET lease_until=?,updated_at=? WHERE id=?", (until, now, lease.job_id)
            )
            self._record(db, lease.job_id, now, "renewed", lease.generation)
        return until

    def fail(self, lease: Lease, *, code: str = "handler_error") -> str:
        # Codes are operator-defined labels, never raw exception messages.
        if (
            not code
            or len(code) > 64
            or not all(c.isascii() and (c.isalnum() or c == "_") for c in code)
        ):
            raise ValueError("Failure code must use 1 to 64 ASCII letters, digits or underscores")
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, lease, now)
            status = "dead" if row["attempts"] >= row["max_attempts"] else "pending"
            delay = min(self.retry_cap, self.retry_base * 2 ** (int(row["attempts"]) - 1))
            db.execute(
                """UPDATE jobs SET status=?,owner=NULL,lease_until=NULL,available_at=?,updated_at=?
                   WHERE id=?""",
                (status, now + delay, now, lease.job_id),
            )
            self._record(db, lease.job_id, now, status, lease.generation, code)
        log.warning(
            status, extra={"job_id": lease.job_id, "generation": lease.generation, "code": code}
        )
        return status

    def replay(self, job_id: int) -> None:
        with self._transaction() as db:
            now = self._now()
            row = db.execute("SELECT status,generation FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None or row["status"] != "dead":
                raise ValueError("Only an existing dead-letter job can be replayed")
            db.execute(
                "UPDATE jobs SET status='pending',attempts=0,available_at=?,updated_at=? "
                "WHERE id=?",
                (now, now, job_id),
            )
            self._record(db, job_id, now, "replayed", row["generation"])
        log.info("replayed", extra={"job_id": job_id})

    def stats(self) -> dict[str, int]:
        with self._transaction(write=False) as db:
            counts = dict.fromkeys(STATES, 0)
            for row in db.execute("SELECT status,count(*) AS n FROM jobs GROUP BY status"):
                counts[row["status"]] = int(row["n"])
            counts["results"] = int(db.execute("SELECT count(*) FROM results").fetchone()[0])
            return counts

    def get(self, job_id: int) -> Payload | None:
        with self._transaction(write=False) as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                return None
            job = cast(Payload, dict(row))
            job["payload"] = parse_object(str(job["payload"]))
            result = db.execute("SELECT output FROM results WHERE job_id=?", (job_id,)).fetchone()
            job["result"] = parse_object(result[0]) if result else None
            return job

    def history(self, job_id: int, *, after: int = 0, limit: int = 100) -> list[Payload]:
        self._page(after, limit)
        with self._transaction(write=False) as db:
            return [
                cast(Payload, dict(row))
                for row in db.execute(
                    "SELECT * FROM transitions WHERE job_id=? AND id>? ORDER BY id LIMIT ?",
                    (job_id, after, limit),
                )
            ]

    @staticmethod
    def _page(after: int, limit: int) -> None:
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("after must be nonnegative and limit must be 1 to 1000")

    def list_jobs(
        self, *, status: str | None = None, after: int = 0, limit: int = 50
    ) -> list[Payload]:
        self._page(after, limit)
        if status is not None and status not in STATES:
            raise ValueError("Unknown job status")
        with self._transaction(write=False) as db:
            return [
                cast(Payload, dict(row))
                for row in db.execute(
                    """SELECT id,source,event_key,status,generation,attempts,lease_until FROM jobs
                   WHERE id>? AND (? IS NULL OR status=?) ORDER BY id LIMIT ?""",
                    (after, status, status, limit),
                )
            ]
