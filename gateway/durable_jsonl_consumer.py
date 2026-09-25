"""Crash-safe JSONL ingress for externally captured messaging events.

The bridge's HTTP ``/messages`` route is deliberately not used here.  That
route drains an unauthenticated, in-memory, capped queue.  This consumer reads
the bridge's append-first durable JSONL store, stages every complete record in
its own SQLite inbox, and advances a committed cursor only after the inbox
transaction commits.

Production processing requires two independent declarations: ``pa.enabled``
in the consumer's Hermes config and the root-owned processing gate passed to
the daemon.  Unless both are true, the daemon does not open the source JSONL
and does not advance either the source cursor or inbox state.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import yaml


CURSOR_VERSION = 1
INBOX_SCHEMA_VERSION = 5


def _cron_ticker(stop_event: threading.Event, *, interval_seconds: float = 60.0) -> None:
    """Run Hermes's existing cron tick without blocking capture processing.

    The consumer is Christopher's durable process.  Keeping this tiny loop
    here gives it the normal scheduler behaviour without another daemon; the
    scheduler's own file lock remains the cross-process duplicate guard.
    """
    from cron.scheduler import tick

    while not stop_event.is_set():
        try:
            tick(verbose=False)
        except Exception:
            # A failed scheduled job/tick must not stop capture.  Scheduler
            # state and job output retain the actual failure for delivery.
            import logging
            logging.getLogger(__name__).exception("Christopher cron ticker failed")
        stop_event.wait(interval_seconds)


def _start_cron_ticker(*, interval_seconds: float = 60.0) -> tuple[threading.Event, threading.Thread]:
    """Start the bounded background tick owned by one consumer lifetime."""
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_cron_ticker,
        args=(stop_event,),
        kwargs={"interval_seconds": interval_seconds},
        name="christopher-cron-ticker",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def _stop_cron_ticker(stop_event: threading.Event, thread: threading.Thread) -> None:
    """Stop and join the consumer-owned ticker during every exit path."""
    stop_event.set()
    thread.join(timeout=5)


def _parse_ingress_timestamp(value: Any) -> datetime:
    """Normalize bridge timestamps without guessing the timezone.

    The capture bridge has emitted both unix seconds/milliseconds and ISO-8601
    strings.  Naive strings are refused because a bounded production replay
    must not silently move its cutoff with the host timezone.
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000.0
        return datetime.fromtimestamp(number, tz=timezone.utc)
    text = str(value or "").strip()
    if not text:
        raise ConsumerError("inbox record has no ingress timestamp")
    if text.replace(".", "", 1).isdigit():
        return _parse_ingress_timestamp(float(text))
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConsumerError(f"invalid ingress timestamp: {text!r}") from exc
    if parsed.tzinfo is None:
        raise ConsumerError("naive ingress timestamp is forbidden")
    return parsed.astimezone(timezone.utc)


def _record_ingress_timestamp(record: "InboxRecord") -> datetime:
    for key in ("timestamp", "ingressTimestamp", "ingress_ts", "receivedAt"):
        if key in record.raw:
            return _parse_ingress_timestamp(record.raw[key])
    raise ConsumerError(f"inbox record {record.message_id} has no ingress timestamp")


def _secret_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _env_file_value(path: Path, name: str) -> str:
    if not path.is_file():
        raise ConsumerError(f"canonical service-token source is missing: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == name:
            return value.strip().strip('"').strip("'")
    raise ConsumerError(f"canonical service-token source has no {name}")


def assert_service_token_hash(
    canonical_env: Path, env_name: str, *, environ: Mapping[str, str] | None = None
) -> str:
    canonical = _env_file_value(canonical_env, env_name)
    current = (environ if environ is not None else os.environ).get(env_name, "")
    if not canonical or not current:
        raise ConsumerError("service-token hash guard refused: token absent")
    canonical_hash = _secret_hash(canonical)
    current_hash = _secret_hash(current)
    if current_hash != canonical_hash:
        raise ConsumerError("service-token hash guard refused: running process mismatch")
    return canonical_hash


class ConsumerError(RuntimeError):
    """Fail-closed consumer contract violation."""


class MediaRetentionError(ConsumerError):
    """Retryable failure before a claimed event reaches the model."""


class ItemMediaRetentionError(MediaRetentionError):
    """Per-event permanent failure eligible for bounded quarantine retries."""


class SourceEvidenceProjectionError(ConsumerError):
    """Retryable failure binding claimed source rows before business writes."""


class PermanentMediaRefusal(ConsumerError):
    """Terminal safety refusal that may reach the model as a refused attachment."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    data = (json.dumps(dict(payload), sort_keys=True, indent=2) + "\n").encode("utf-8")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


@dataclass(frozen=True)
class SourceCursor:
    version: int
    source_path: str
    source_device: int
    source_inode: int
    initial_offset: int
    offset: int
    initialized_at: str
    updated_at: str
    last_message_id: str | None = None

    @classmethod
    def from_path(cls, path: Path) -> "SourceCursor":
        raw = json.loads(path.read_text(encoding="utf-8"))
        cursor = cls(
            version=int(raw["version"]),
            source_path=str(raw["source_path"]),
            source_device=int(raw["source_device"]),
            source_inode=int(raw["source_inode"]),
            initial_offset=int(raw.get("initial_offset", raw["offset"])),
            offset=int(raw["offset"]),
            initialized_at=str(raw["initialized_at"]),
            updated_at=str(raw["updated_at"]),
            last_message_id=(
                str(raw["last_message_id"]) if raw.get("last_message_id") else None
            ),
        )
        if cursor.version != CURSOR_VERSION:
            raise ConsumerError(
                f"unsupported cursor version {cursor.version}; expected {CURSOR_VERSION}"
            )
        if cursor.offset < 0 or cursor.initial_offset < 0:
            raise ConsumerError("cursor offsets cannot be negative")
        if cursor.offset < cursor.initial_offset:
            raise ConsumerError("cursor offset cannot precede its initial boundary")
        return cursor


def initialize_cursor(
    source: Path, cursor_path: Path, *, position: str
) -> SourceCursor:
    source = source.resolve()
    if position not in {"start", "end"}:
        raise ConsumerError("initial cursor position must be start or end")
    stat = source.stat()
    now = _utc_now()
    initial_offset = int(stat.st_size if position == "end" else 0)
    cursor = SourceCursor(
        version=CURSOR_VERSION,
        source_path=str(source),
        source_device=int(stat.st_dev),
        source_inode=int(stat.st_ino),
        initial_offset=initial_offset,
        offset=initial_offset,
        initialized_at=now,
        updated_at=now,
        last_message_id=None,
    )
    if cursor_path.exists():
        existing = SourceCursor.from_path(cursor_path)
        if existing.source_path != cursor.source_path:
            raise ConsumerError(
                f"cursor already belongs to {existing.source_path}, not {cursor.source_path}"
            )
        return existing
    _atomic_write_json(cursor_path, asdict(cursor))
    return cursor


class SingletonLock:
    """Process singleton guard backed by a non-blocking flock."""

    def __init__(self, path: Path):
        self.path = path
        self._handle: Any = None

    def __enter__(self) -> "SingletonLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._handle.close()
            self._handle = None
            raise ConsumerError(
                f"consumer singleton already holds {self.path}"
            ) from exc
        self._handle.seek(0)
        self._handle.truncate()
        self._handle.write(f"{os.getpid()}\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


class SharedActivityLock:
    """Shared release barrier held while Christopher owns an inbox claim.

    The consumer takes a shared advisory lock before it changes a row to
    ``processing`` and retains it until the row is terminal again.  The small
    Christopher release executor takes the same file exclusively and refuses
    rather than restarting a live investigation.  This is deliberately only a
    barrier, not another queue or deployment state machine.
    """

    def __init__(self, path: Path | None):
        self.path = path
        self._handle: Any = None

    def __enter__(self) -> "SharedActivityLock":
        if self.path is None:
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_SH)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None


def _bridge_item_ref(value: Any) -> Mapping[str, Any]:
    """Resolve the durable bridge item or a shallow append-envelope wrapper."""
    if not isinstance(value, Mapping):
        raise ConsumerError("durable JSONL record is not an object")
    candidates = [value]
    # "normalized" is the live capture-bridge envelope (whatsapp_capture_event
    # records in events.jsonl nest the bridge item there); the flat shape and
    # the other wrapper keys cover fixtures and replay corpora. Missing
    # "normalized" put the live consumer in a poison-record crash loop on the
    # first real capture record at activation (2026-07-21, first-light).
    for key in ("normalized", "message", "event", "payload", "data"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
    for candidate in candidates:
        if candidate.get("messageId") and candidate.get("chatId"):
            return candidate
    raise ConsumerError("durable JSONL record has no bridge messageId/chatId item")


def _bridge_item(value: Any) -> dict[str, Any]:
    """Return an isolated durable bridge item for normalization/persistence."""
    return dict(_bridge_item_ref(value))


def _declared_document_mime(value: Any) -> str:
    """Recover the provider-declared document MIME before normalizing the event."""
    if not isinstance(value, Mapping):
        return ""
    document = value.get("documentMessage")
    if isinstance(document, Mapping):
        return str(
            document.get("mimetype")
            or document.get("mimeType")
            or document.get("contentType")
            or ""
        ).split(";", 1)[0].strip().lower()
    for nested in value.values():
        if isinstance(nested, Mapping):
            mime = _declared_document_mime(nested)
            if mime:
                return mime
    return ""


@dataclass(frozen=True)
class InboxRecord:
    seq: int
    message_id: str
    chat_id: str
    start_offset: int
    end_offset: int
    raw: dict[str, Any]
    # The immutable capture wrapper, rather than the normalized bridge item.
    # Systems source ingestion must receive this exact document so reply
    # lineage and capture provenance cannot be reconstructed or lost here.
    source_envelope: dict[str, Any] | None = None
    retention_state: str | None = None
    retention_quarantined: bool = False


@dataclass(frozen=True)
class ManagementDocumentEventConfig:
    """The narrow, opt-in transport contract for document outbox events.

    This is intentionally separate from WhatsApp capture configuration.  A
    document entry is an internal Systems event, not a forged inbound WhatsApp
    message, so it has its own endpoint, cursor and destination declaration.
    """

    api_url: str
    chat_id: str
    token_env: str


def _management_document_delivery_key(*, entry_id: str, chat_id: str) -> str:
    """One at-most-once delivery claim per entry and destination chat."""
    return f"human-resolution:{entry_id}:destination:{chat_id}"


def _is_management_document_delivery_key(
    delivery_key: str, *, entry_id: str, chat_id: str
) -> bool:
    """Accept the destination-scoped key and the previous single-destination key."""
    return delivery_key in {
        _management_document_delivery_key(entry_id=entry_id, chat_id=chat_id),
        f"human-resolution:{entry_id}",
    }


def _initial_retention_state(item: Mapping[str, Any]) -> str:
    """Classify media that needs content validation before model ingress."""
    coarse = str(item.get("mediaType") or item.get("mimeType") or "")
    if coarse.split("/", 1)[0].strip().lower() == "image":
        return "pending"
    values = item.get("mediaUrls") or item.get("media") or item.get("mediaPaths") or []
    declared_mimes = item.get("mediaMimes") or []
    if isinstance(declared_mimes, (str, bytes)):
        declared_mimes = [declared_mimes]
    if isinstance(values, Mapping):
        values = [values]
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes)):
        for index, value in enumerate(values):
            mime = ""
            if isinstance(value, Mapping):
                mime = str(
                    value.get("mime") or value.get("mimeType")
                    or value.get("contentType") or ""
                )
            elif index < len(declared_mimes):
                mime = str(declared_mimes[index] or "")
            if mime.split("/", 1)[0].strip().lower() == "image":
                return "pending"
            suffix = Path(
                str(
                    value.get("path")
                    or value.get("filePath")
                    or value.get("localPath")
                    or value.get("url")
                    or ""
                )
                if isinstance(value, Mapping)
                else str(value or "")
            ).suffix.lower()
            if suffix in {
                ".xlsx",
                ".csv",
                ".xlsm",
                ".xltm",
                ".pdf",
                ".docx",
                ".docm",
                ".dotm",
            }:
                return "pending"
    return "bypassed"


class DurableInbox:
    """Consumer-owned durable inbox and source cursor staging ledger."""

    def __init__(self, db_path: Path, *, read_only: bool = False):
        self.db_path = db_path
        self.read_only = read_only
        if read_only:
            if not self.db_path.is_file():
                raise ConsumerError(f"read-only inbox is missing: {self.db_path}")
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_schema()

    def connect(self) -> sqlite3.Connection:
        if self.read_only:
            conn = sqlite3.connect(
                f"file:{self.db_path}?mode=ro", uri=True, timeout=30
            )
            conn.execute("PRAGMA query_only=ON")
        else:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingress_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reply_deliveries (
                    delivery_key TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    reply_to_message_id TEXT,
                    correlation_json TEXT,
                    status TEXT NOT NULL
                        CHECK (status IN ('delivered','undelivered')),
                    bridge_message_id TEXT,
                    provider_outcome TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ingress_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL UNIQUE,
                    chat_id TEXT NOT NULL,
                    source_device INTEGER NOT NULL,
                    source_inode INTEGER NOT NULL,
                    start_offset INTEGER NOT NULL,
                    end_offset INTEGER NOT NULL,
                    raw_json TEXT NOT NULL,
                    source_envelope_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','completed','skipped','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    pa_turn_id TEXT,
                    last_error TEXT,
                    retained_media_count INTEGER NOT NULL DEFAULT 0,
                    retention_failures INTEGER NOT NULL DEFAULT 0,
                    retention_attempts INTEGER NOT NULL DEFAULT 0,
                    retention_quarantine_attempts INTEGER NOT NULL DEFAULT 0,
                    retention_state TEXT NOT NULL DEFAULT 'pending'
                        CHECK (retention_state IN ('pending','complete','bypassed','held')),
                    retention_last_error TEXT,
                    retention_updated_at TEXT,
                    projection_state TEXT NOT NULL DEFAULT 'pending'
                        CHECK (projection_state IN ('pending','complete','held')),
                    projection_attempts INTEGER NOT NULL DEFAULT 0,
                    projection_last_error TEXT,
                    projection_updated_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ingress_events_status_seq_idx
                    ON ingress_events(status, seq);
                CREATE TABLE IF NOT EXISTS media_retention_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ingress_seq INTEGER NOT NULL,
                    attempt INTEGER NOT NULL,
                    error TEXT NOT NULL,
                    quarantine_eligible INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(ingress_seq, attempt),
                    FOREIGN KEY(ingress_seq) REFERENCES ingress_events(seq)
                );
                CREATE TABLE IF NOT EXISTS media_retention_quarantine (
                    ingress_seq INTEGER PRIMARY KEY,
                    message_id TEXT NOT NULL UNIQUE,
                    chat_id TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    failure_history_json TEXT NOT NULL,
                    terminal_error TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'quarantined'
                        CHECK (status IN ('quarantined')),
                    quarantined_at TEXT NOT NULL,
                    FOREIGN KEY(ingress_seq) REFERENCES ingress_events(seq)
                );
                CREATE INDEX IF NOT EXISTS media_retention_quarantine_status_idx
                    ON media_retention_quarantine(status, quarantined_at);
                """
            )
            # Existing consumer DBs predate durable provider-outcome detail.
            reply_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(reply_deliveries)")
            }
            if "provider_outcome" not in reply_columns:
                conn.execute("ALTER TABLE reply_deliveries ADD COLUMN provider_outcome TEXT")
            if "correlation_json" not in reply_columns:
                conn.execute("ALTER TABLE reply_deliveries ADD COLUMN correlation_json TEXT")
            ingress_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(ingress_events)")
            }
            if "source_envelope_json" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN source_envelope_json TEXT"
                )
                conn.execute(
                    "UPDATE ingress_events SET source_envelope_json=raw_json "
                    "WHERE source_envelope_json IS NULL"
                )
            if "retained_media_count" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retained_media_count "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "retention_failures" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retention_failures "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "retention_attempts" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retention_attempts "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            # Quarantine accounting starts with this release.  The lifetime
            # retention_attempts counter predates quarantine and must never
            # make a legacy held row terminal on its first post-deploy retry.
            if "retention_quarantine_attempts" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN "
                    "retention_quarantine_attempts INTEGER NOT NULL DEFAULT 0"
                )
            failure_columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(media_retention_failures)")
            }
            if "quarantine_eligible" not in failure_columns:
                conn.execute(
                    "ALTER TABLE media_retention_failures ADD COLUMN "
                    "quarantine_eligible INTEGER NOT NULL DEFAULT 0"
                )
            retention_state_added = "retention_state" not in ingress_columns
            if retention_state_added:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retention_state "
                    "TEXT NOT NULL DEFAULT 'pending'"
                )
            if "retention_last_error" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retention_last_error TEXT"
                )
            if "retention_updated_at" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN retention_updated_at TEXT"
                )
            if "projection_state" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN projection_state "
                    "TEXT NOT NULL DEFAULT 'pending'"
                )
            if "projection_attempts" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN projection_attempts "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "projection_last_error" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN projection_last_error TEXT"
                )
            if "projection_updated_at" not in ingress_columns:
                conn.execute(
                    "ALTER TABLE ingress_events ADD COLUMN projection_updated_at TEXT"
                )
            if retention_state_added:
                rows = conn.execute(
                    "SELECT seq,raw_json,retained_media_count FROM ingress_events"
                ).fetchall()
                for row in rows:
                    if int(row["retained_media_count"] or 0) > 0:
                        conn.execute(
                            "UPDATE ingress_events SET retention_state='complete',"
                            "retention_updated_at=? WHERE seq=?",
                            (_utc_now(), int(row["seq"])),
                        )
                        continue
                    try:
                        item = _bridge_item(json.loads(row["raw_json"]))
                        state = _initial_retention_state(item)
                    except (ConsumerError, TypeError, ValueError):
                        state = "pending"
                    if state == "bypassed":
                        conn.execute(
                            "UPDATE ingress_events SET retention_state='bypassed',"
                            "retention_updated_at=? WHERE seq=?",
                            (_utc_now(), int(row["seq"])),
                        )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ingress_events_retention_queue_idx "
                "ON ingress_events(status,retention_state,seq)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ingress_events_projection_queue_idx "
                "ON ingress_events(projection_state,projection_updated_at,seq)"
            )
            conn.execute(
                "INSERT INTO ingress_meta(key,value) VALUES('schema_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(INBOX_SCHEMA_VERSION),),
            )

    def stage_from_source(
        self,
        source: Path,
        cursor_path: Path,
        *,
        max_records: int = 100,
    ) -> int:
        """Stage complete JSONL records, then advance the source cursor.

        DB commit happens before the cursor write.  A crash between them only
        re-reads already-unique message IDs; it cannot lose a record.
        """
        source = source.resolve()
        if not cursor_path.exists():
            raise ConsumerError("source cursor is missing; initialize it explicitly")
        cursor = SourceCursor.from_path(cursor_path)
        if cursor.source_path != str(source):
            raise ConsumerError(
                f"cursor source mismatch: {cursor.source_path} != {source}"
            )
        stat = source.stat()
        if (int(stat.st_dev), int(stat.st_ino)) != (
            cursor.source_device,
            cursor.source_inode,
        ):
            raise ConsumerError(
                "source JSONL inode changed; explicit rotation recovery required"
            )
        if cursor.offset > stat.st_size:
            raise ConsumerError("source JSONL truncated below committed cursor")

        staged: list[tuple[int, int, dict[str, Any], dict[str, Any]]] = []
        last_message_id = cursor.last_message_id
        with source.open("rb") as handle:
            handle.seek(cursor.offset)
            for _ in range(max(1, max_records)):
                start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                end = handle.tell()
                if not line.endswith(b"\n"):
                    break
                try:
                    decoded = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ConsumerError(
                        f"invalid JSONL at byte {start}: {exc.msg}"
                    ) from exc
                item = _bridge_item(decoded)
                declared_document_mime = _declared_document_mime(decoded)
                if declared_document_mime and len(item.get("mediaUrls") or []) == 1:
                    item["mediaMimes"] = [declared_document_mime]
                message_id = str(item["messageId"])
                staged.append((start, end, item, decoded))
                last_message_id = message_id

        if not staged:
            return 0

        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for start, end, item, source_envelope in staged:
                conn.execute(
                    """
                    INSERT INTO ingress_events(
                        message_id,chat_id,source_device,source_inode,
                        start_offset,end_offset,raw_json,source_envelope_json,
                        status,retention_state,retention_updated_at,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,'pending',?,?,?,?)
                    ON CONFLICT(message_id) DO NOTHING
                    """,
                    (
                        str(item["messageId"]),
                        str(item["chatId"]),
                        cursor.source_device,
                        cursor.source_inode,
                        start,
                        end,
                        json.dumps(item, sort_keys=True, separators=(",", ":")),
                        json.dumps(source_envelope, sort_keys=True, separators=(",", ":")),
                        _initial_retention_state(item),
                        now,
                        now,
                        now,
                    ),
                )
            conn.commit()

        updated = SourceCursor(
            version=cursor.version,
            source_path=cursor.source_path,
            source_device=cursor.source_device,
            source_inode=cursor.source_inode,
            initial_offset=cursor.initial_offset,
            offset=staged[-1][1],
            initialized_at=cursor.initialized_at,
            updated_at=now,
            last_message_id=last_message_id,
        )
        _atomic_write_json(cursor_path, asdict(updated))
        return len(staged)

    def pending(
        self,
        *,
        limit: int = 10,
        priority_chats: frozenset[str] | set[str] | None = None,
    ) -> list[InboxRecord]:
        """Pending records, priority chats first, then source order.

        Management chats jump the drain queue (2026-07-21): a fresh mgmt
        message must not wait behind a thousand-record site backlog for its
        reply. Per-chat ordering stays strictly by seq within each class, and
        chats are independent sessions, so cross-chat reordering is safe.
        """
        priority = sorted(priority_chats or ())
        placeholders = ",".join("?" for _ in priority)
        priority_clause = (
            f"(CASE WHEN chat_id IN ({placeholders}) THEN 0 ELSE 1 END), "
            if priority
            else ""
        )
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "retention_state "
                "FROM ingress_events WHERE status='pending' "
                f"ORDER BY {priority_clause}seq LIMIT ?",
                (*priority, max(1, limit)),
            ).fetchall()
        return [
            InboxRecord(
                seq=int(row["seq"]),
                message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=json.loads(row["raw_json"]),
                retention_state=str(row["retention_state"]),
            )
            for row in rows
        ]

    def pending_chat_batches(
        self,
        *,
        batch_size: int,
        priority_chats: frozenset[str] | set[str] | None = None,
        exclude_chats: frozenset[str] | set[str] | None = None,
        priority_quiet_seconds: float = 0.0,
        now: datetime | None = None,
    ) -> tuple[list[tuple[str, list[InboxRecord]]], list[tuple[str, list[InboxRecord]]]]:
        """Return FIFO batches grouped by chat, split into management/site lanes.

        A chat appears at most once.  Rows stay pending until the scheduler has
        capacity and claims that chat's batch, so no unrelated chat can own or
        strand them.  Ordering is oldest pending row per chat, then row seq.
        """
        size = max(1, int(batch_size))
        priority = set(priority_chats or ())
        excluded = set(exclude_chats or ())
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT e.seq,e.message_id,e.chat_id,e.start_offset,e.end_offset,"
                "e.raw_json,e.retention_state,e.created_at,"
                "q.ingress_seq IS NOT NULL AS retention_quarantined "
                "FROM ingress_events e LEFT JOIN media_retention_quarantine q "
                "ON q.ingress_seq=e.seq AND q.status='quarantined' "
                "WHERE e.status='pending' "
                "AND e.retention_state IN ('complete','bypassed') ORDER BY e.seq"
            ).fetchall()
        grouped: dict[str, list[InboxRecord]] = {}
        latest_created_at: dict[str, datetime] = {}
        for row in rows:
            chat_id = str(row["chat_id"])
            if chat_id in excluded:
                continue
            created_at = datetime.fromisoformat(str(row["created_at"]).replace("Z", "+00:00"))
            latest_created_at[chat_id] = max(
                latest_created_at.get(chat_id, created_at), created_at
            )
            batch = grouped.setdefault(chat_id, [])
            if len(batch) >= size:
                continue
            batch.append(
                InboxRecord(
                    seq=int(row["seq"]),
                    message_id=str(row["message_id"]),
                    chat_id=chat_id,
                    start_offset=int(row["start_offset"]),
                    end_offset=int(row["end_offset"]),
                    raw=json.loads(row["raw_json"]),
                    retention_state=str(row["retention_state"]),
                    retention_quarantined=bool(row["retention_quarantined"]),
                )
            )
        ordered = sorted(grouped.items(), key=lambda item: item[1][0].seq)
        quiet_seconds = max(0.0, float(priority_quiet_seconds))
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        management = [
            item for item in ordered
            if item[0] in priority and (
                quiet_seconds <= 0
                or (current - latest_created_at[item[0]].astimezone(timezone.utc)).total_seconds()
                >= quiet_seconds
            )
        ]
        site = [item for item in ordered if item[0] not in priority]
        return management, site

    def pending_chat_batch(self, chat_id: str, *, batch_size: int) -> list[InboxRecord]:
        """Return one retained, pending FIFO batch for an already-active chat."""
        management, _ = self.pending_chat_batches(
            batch_size=batch_size,
            priority_chats={chat_id},
        )
        return management[0][1] if management else []

    def bounded_window(
        self, *, chat_ids: Sequence[str], cutoff: datetime
    ) -> list[InboxRecord]:
        """Return the exact existing-inbox window, FIFO, with no claims."""
        wanted = frozenset(str(value) for value in chat_ids)
        if not wanted:
            raise ConsumerError("bounded replay requires at least one chat id")
        if cutoff.tzinfo is None:
            raise ConsumerError("bounded replay cutoff must be timezone-aware")
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "retention_state "
                "FROM ingress_events ORDER BY seq"
            ).fetchall()
        selected: list[InboxRecord] = []
        for row in rows:
            if str(row["chat_id"]) not in wanted:
                continue
            record = InboxRecord(
                seq=int(row["seq"]), message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]), start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]), raw=json.loads(row["raw_json"]),
                retention_state=str(row["retention_state"]),
            )
            if _record_ingress_timestamp(record) >= cutoff.astimezone(timezone.utc):
                selected.append(record)
        return selected

    def message_id_selection(self, message_ids: Sequence[str]) -> list[InboxRecord]:
        """Return an exact existing-inbox message-id set, FIFO, with no claims."""
        wanted = tuple(dict.fromkeys(str(value).strip() for value in message_ids if str(value).strip()))
        if not wanted:
            raise ConsumerError("bounded replay message-id selection is empty")
        placeholders = ",".join("?" for _ in wanted)
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "retention_state "
                f"FROM ingress_events WHERE message_id IN ({placeholders}) ORDER BY seq",
                wanted,
            ).fetchall()
        found = {str(row["message_id"]) for row in rows}
        missing = [value for value in wanted if value not in found]
        if missing:
            raise ConsumerError(
                "bounded replay message ids missing from inbox: " + ",".join(missing[:10])
            )
        if len(rows) != len(wanted):
            raise ConsumerError("bounded replay message-id selection is not one-to-one")
        return [
            InboxRecord(
                seq=int(row["seq"]), message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]), start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]), raw=json.loads(row["raw_json"]),
                retention_state=str(row["retention_state"]),
            )
            for row in rows
        ]

    def requeue_selected_for_readjudication(
        self,
        records: Sequence[InboxRecord],
        *,
        before_image_path: Path,
        run_id: str,
        dry_run: bool,
    ) -> dict[str, Any]:
        """CAS-reset an exact selected set to pending with a durable before-image."""
        if not records:
            raise ConsumerError("readjudication reset requires selected records")
        seqs = [record.seq for record in records]
        placeholders = ",".join("?" for _ in seqs)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM ingress_events WHERE seq IN ({placeholders}) "
                "ORDER BY seq",
                seqs,
            ).fetchall()
        before = [dict(row) for row in rows]
        if len(before) != len(records):
            raise ConsumerError("readjudication before-image denominator mismatch")
        processing = [row["message_id"] for row in before if row["status"] == "processing"]
        if processing:
            raise ConsumerError(
                "readjudication refuses selected processing rows: "
                + ",".join(str(value) for value in processing[:10])
            )
        image = {
            "artifact_type": "tgg_readjudication_inbox_before_image",
            "run_id": run_id,
            "created_at": _utc_now(),
            "selected_count": len(before),
            "rows": before,
        }
        if not dry_run:
            _atomic_write_json(before_image_path, image)
            now = _utc_now()
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                changed = 0
                for row in before:
                    result = conn.execute(
                        "UPDATE ingress_events SET status='pending',pa_turn_id=NULL,"
                        "last_error=?,updated_at=? WHERE seq=? AND status=?",
                        (
                            f"readjudication:{run_id}", now,
                            int(row["seq"]), str(row["status"]),
                        ),
                    )
                    changed += int(result.rowcount)
                if changed != len(before):
                    conn.rollback()
                    raise ConsumerError(
                        "readjudication CAS reset mismatch: "
                        f"expected={len(before)} changed={changed}"
                    )
                conn.commit()
        return {
            "selected": len(before),
            "status_before": dict(Counter(str(row["status"]) for row in before)),
            "cas_updated": 0 if dry_run else len(before),
            "before_image": str(before_image_path),
            "dry_run": dry_run,
        }

    def window_statuses(self, records: Sequence[InboxRecord]) -> dict[str, str]:
        if not records:
            return {}
        seqs = [record.seq for record in records]
        placeholders = ",".join("?" for _ in seqs)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT seq,status FROM ingress_events WHERE seq IN ({placeholders})",
                seqs,
            ).fetchall()
        return {str(row["seq"]): str(row["status"]) for row in rows}

    def reconcile_window_processing(
        self, records: Sequence[InboxRecord], state_db: Path, *, dry_run: bool
    ) -> dict[str, Any]:
        """Reconcile only selected processing rows, optionally without writes."""
        turn_by_message = _completed_turn_refs(state_db)
        statuses = self.window_statuses(records)
        processing = [r for r in records if statuses.get(str(r.seq)) == "processing"]
        completed = [r for r in processing if r.message_id in turn_by_message]
        requeued = [r for r in processing if r.message_id not in turn_by_message]
        before = self.total()
        if not dry_run and processing:
            now = _utc_now()
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                for record in completed:
                    conn.execute(
                        "UPDATE ingress_events SET status='completed',pa_turn_id=?,"
                        "last_error=NULL,updated_at=? WHERE seq=? AND status='processing'",
                        (turn_by_message[record.message_id], now, record.seq),
                    )
                for record in requeued:
                    conn.execute(
                        "UPDATE ingress_events SET status='pending',"
                        "last_error='bounded-orphan-requeued',updated_at=? "
                        "WHERE seq=? AND status='processing'", (now, record.seq),
                    )
                after = int(conn.execute("SELECT COUNT(*) FROM ingress_events").fetchone()[0])
                if after != before:
                    raise ConsumerError("bounded reconciliation violated row conservation")
                conn.commit()
        after = self.total()
        if after != before:
            raise ConsumerError("bounded reconciliation violated row conservation")
        predicted = dict(statuses)
        for record in completed:
            predicted[str(record.seq)] = "completed"
        for record in requeued:
            predicted[str(record.seq)] = "pending"
        unresolved = [r.message_id for r in records if predicted.get(str(r.seq)) == "processing"]
        if unresolved:
            raise ConsumerError("bounded reconciliation left unresolved processing rows")
        return {
            "completed": len(completed), "requeued": len(requeued),
            "processing_before": len(processing), "unresolved": unresolved,
            "row_total_before": before, "row_total_after": after,
            "predicted_statuses": predicted,
        }

    def newest_pending_for_chats(
        self, chats: frozenset[str] | set[str]
    ) -> list[InboxRecord]:
        """Return one newest pending record from the selected chats."""
        selected = sorted(chats)
        if not selected:
            return []
        placeholders = ",".join("?" for _ in selected)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "retention_state "
                "FROM ingress_events WHERE status='pending' "
                f"AND chat_id IN ({placeholders}) ORDER BY seq DESC LIMIT 1",
                tuple(selected),
            ).fetchone()
        if row is None:
            return []
        return [
            InboxRecord(
                seq=int(row["seq"]),
                message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=json.loads(row["raw_json"]),
                retention_state=str(row["retention_state"]),
            )
        ]

    def claim(self, records: Sequence[InboxRecord]) -> None:
        if not records:
            return
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                changed = conn.execute(
                    "UPDATE ingress_events SET status='processing', attempts=attempts+1, "
                    "updated_at=? WHERE seq=? AND status='pending'",
                    (now, record.seq),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(f"inbox record {record.seq} was not pending")
            conn.commit()

    def finish(
        self,
        records: Sequence[InboxRecord],
        *,
        status: str,
        pa_turn_id: str | None = None,
        error: str | None = None,
    ) -> None:
        if status not in {"completed", "skipped", "failed"}:
            raise ConsumerError(f"invalid inbox finish status {status}")
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                changed = conn.execute(
                    "UPDATE ingress_events SET status=?, pa_turn_id=?, last_error=?, "
                    "updated_at=? WHERE seq=? AND status='processing'",
                    (status, pa_turn_id, (error or "")[:2000] or None, now, record.seq),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(
                        f"inbox record {record.seq} was not processing at finish"
                    )
            conn.commit()

    def requeue(self, records: Sequence[InboxRecord], *, reason: str) -> None:
        """Return an owned batch to pending after graceful interruption."""
        if not records:
            return
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                changed = conn.execute(
                    "UPDATE ingress_events SET status='pending', last_error=?, updated_at=? "
                    "WHERE seq=? AND status='processing'",
                    ((reason or "interrupted")[:2000], now, record.seq),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(
                        f"inbox record {record.seq} was not processing at requeue"
                    )
            conn.commit()

    def finish_processed_batch(
        self,
        records: Sequence[InboxRecord],
        *,
        turn_for_message: Mapping[str, str],
    ) -> None:
        """Atomically terminal every record in a processor-evidenced batch."""
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for record in records:
                turn_id = turn_for_message.get(record.message_id)
                status = "completed" if turn_id else "skipped"
                changed = conn.execute(
                    "UPDATE ingress_events SET status=?,pa_turn_id=?,last_error=NULL,"
                    "updated_at=? WHERE seq=? AND status='processing'",
                    (status, turn_id, now, record.seq),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(
                        f"inbox record {record.seq} was not processing at batch finish"
                    )
            conn.commit()

    def reconcile_orphan_processing(self, state_db: Path) -> dict[str, int]:
        """Recover prior-process claims using successful pa_turn evidence.

        Completed turn refs are authoritative.  A processing row referenced by
        such a turn becomes completed with that turn id; every other orphan is
        returned to pending.  Nothing is deleted and the row total must remain
        identical across the transaction.
        """
        turn_by_message: dict[str, str] = {}
        if state_db.exists():
            conn = sqlite3.connect(state_db)
            conn.row_factory = sqlite3.Row
            try:
                tables = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "pa_turns" in tables:
                    rows = conn.execute(
                        "SELECT turn_id,message_refs_json FROM pa_turns "
                        "WHERE turn_status='completed' AND error_json IS NULL "
                        "ORDER BY completed_at"
                    ).fetchall()
                    for row in rows:
                        try:
                            refs = json.loads(row["message_refs_json"] or "[]")
                        except (TypeError, ValueError):
                            refs = []
                        for ref in refs:
                            if ref:
                                turn_by_message[str(ref)] = str(row["turn_id"])
            finally:
                conn.close()

        now = _utc_now()
        completed = 0
        requeued = 0
        with self.connect() as conn:
            before = int(conn.execute("SELECT COUNT(*) FROM ingress_events").fetchone()[0])
            processing = conn.execute(
                "SELECT seq,message_id FROM ingress_events WHERE status='processing'"
            ).fetchall()
            conn.execute("BEGIN IMMEDIATE")
            for row in processing:
                turn_id = turn_by_message.get(str(row["message_id"]))
                if turn_id:
                    conn.execute(
                        "UPDATE ingress_events SET status='completed',pa_turn_id=?,"
                        "last_error=NULL,updated_at=? WHERE seq=? AND status='processing'",
                        (turn_id, now, int(row["seq"])),
                    )
                    completed += 1
                else:
                    conn.execute(
                        "UPDATE ingress_events SET status='pending',"
                        "last_error='startup-orphan-requeued',updated_at=? "
                        "WHERE seq=? AND status='processing'",
                        (now, int(row["seq"])),
                    )
                    requeued += 1
            after = int(conn.execute("SELECT COUNT(*) FROM ingress_events").fetchone()[0])
            if after != before:
                raise ConsumerError(
                    f"inbox conservation failed during recovery: before={before} after={after}"
                )
            conn.commit()
        return {"completed": completed, "requeued": requeued, "total": completed + requeued}

    def counts(self) -> dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT status,COUNT(*) AS n FROM ingress_events GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["n"]) for row in rows}

    def management_document_cursor(self) -> tuple[int, str] | None:
        """Return the exclusive Systems document-entry cursor, if initialized.

        It lives in the existing durable consumer metadata store rather than a
        second queue table.  The tuple is Systems' canonical ascending order;
        it is advanced only after this process has reached a terminal local
        delivery outcome for that entry.
        """
        with self.connect() as conn:
            created = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='management_document_cursor_created_at'"
            ).fetchone()
            entry_id = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='management_document_cursor_id'"
            ).fetchone()
        if created is None and entry_id is None:
            return None
        if created is None or entry_id is None:
            raise ConsumerError("management document cursor is incomplete")
        try:
            created_at = int(str(created[0]))
        except (TypeError, ValueError) as exc:
            raise ConsumerError("management document cursor created_at is invalid") from exc
        value = str(entry_id[0]).strip()
        if created_at < 0 or not value:
            raise ConsumerError("management document cursor is invalid")
        return created_at, value

    def advance_management_document_cursor(self, *, created_at: int, entry_id: str) -> None:
        """CAS-advance the event cursor after an at-most-once terminal outcome."""
        if created_at < 0 or not entry_id.strip():
            raise ConsumerError("management document cursor advance is invalid")
        current = self.management_document_cursor()
        candidate = (int(created_at), str(entry_id))
        if current is not None and candidate < current:
            raise ConsumerError("management document cursor cannot move backwards")
        if current == candidate:
            return
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            observed_created = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='management_document_cursor_created_at'"
            ).fetchone()
            observed_id = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='management_document_cursor_id'"
            ).fetchone()
            observed = (
                (int(str(observed_created[0])), str(observed_id[0]))
                if observed_created is not None and observed_id is not None
                else None
            )
            if observed != current:
                conn.rollback()
                raise ConsumerError("management document cursor changed concurrently")
            conn.execute(
                "INSERT INTO ingress_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("management_document_cursor_created_at", str(candidate[0])),
            )
            conn.execute(
                "INSERT INTO ingress_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                ("management_document_cursor_id", candidate[1]),
            )
            conn.commit()

    def reply_delivery_status(self, delivery_key: str) -> str | None:
        """Read a prior durable claim for crash recovery, without retrying it."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT status FROM reply_deliveries WHERE delivery_key=?", (delivery_key,)
            ).fetchone()
        return str(row[0]) if row is not None else None

    def reply_delivery_correlation(self, delivery_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT correlation_json FROM reply_deliveries WHERE delivery_key=?",
                (delivery_key,),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        try:
            value = json.loads(row[0])
        except (TypeError, ValueError) as exc:
            raise ConsumerError("reply delivery correlation is unreadable") from exc
        if not isinstance(value, Mapping):
            raise ConsumerError("reply delivery correlation is invalid")
        return dict(value)

    def initial_management_document_notice(
        self, *, chat_id: str, document_id: str
    ) -> dict[str, str] | None:
        """Return this destination's confirmed document-notice quote anchor.

        The first notice is scoped to both document and destination.  A test
        management notice is durable history, but must never become the quote
        anchor for the first production-management notice.
        """
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT delivery_key,bridge_message_id,correlation_json FROM reply_deliveries "
                "WHERE chat_id=? AND status='delivered' AND bridge_message_id IS NOT NULL "
                "AND delivery_key GLOB 'human-resolution:*' ORDER BY created_at,delivery_key",
                (chat_id,),
            ).fetchall()
        for row in rows:
            try:
                correlation = json.loads(row["correlation_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if not isinstance(correlation, Mapping):
                continue
            if (
                correlation.get("document_id") == document_id
                and (
                    correlation.get("notice_role") == "initial_notice"
                    # Pre-PA-83 rows did not carry the explicit role.  They
                    # remain valid anchors in their own destination only.
                    or correlation.get("entry_kind") == "initial_default"
                )
                and str(correlation.get("notice_body") or "").strip()
            ):
                return {
                    "delivery_key": str(row["delivery_key"]),
                    "message_id": str(row["bridge_message_id"]),
                    "body": str(correlation["notice_body"]),
                }
        return None

    def management_document_delivery_status(
        self, *, entry_id: str, chat_id: str
    ) -> str | None:
        """Read a terminal notice claim for one entry in one destination.

        New claims encode their destination in the key.  The correlation scan
        also recognises pre-PA-83 claims so upgrading cannot replay an already
        terminal notice into the same chat.
        """
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT delivery_key,status,correlation_json FROM reply_deliveries "
                "WHERE chat_id=? AND delivery_key GLOB 'human-resolution:*' "
                "ORDER BY created_at,delivery_key",
                (chat_id,),
            ).fetchall()
        for row in rows:
            if _is_management_document_delivery_key(
                str(row["delivery_key"]), entry_id=entry_id, chat_id=chat_id
            ):
                return str(row["status"])
            try:
                correlation = json.loads(row["correlation_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if isinstance(correlation, Mapping) and str(correlation.get("entry_id") or "") == entry_id:
                return str(row["status"])
        return None

    def retention_candidates(
        self, *, limit: int, retry_interval_seconds: float = 0
    ) -> list[InboxRecord]:
        """Bounded pending-business work, with spaced retries behind new rows."""
        retry_interval = max(0.0, float(retry_interval_seconds))
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "retention_state "
                "FROM ingress_events WHERE status='pending' "
                "AND retention_state IN ('pending','held') "
                "AND (retention_state='pending' OR retention_updated_at IS NULL "
                "OR julianday(retention_updated_at) <= "
                "julianday('now', ?)) "
                "ORDER BY CASE retention_state WHEN 'pending' THEN 0 ELSE 1 END, "
                "COALESCE(retention_updated_at,created_at),seq LIMIT ?",
                (f"-{retry_interval:.6f} seconds", max(1, int(limit))),
            ).fetchall()
        return [
            InboxRecord(
                seq=int(row["seq"]),
                message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=json.loads(row["raw_json"]),
                retention_state=str(row["retention_state"]),
            )
            for row in rows
        ]

    def source_projection_candidates(
        self, *, limit: int, retry_interval_seconds: float = 0, retry_cap: int = 1
    ) -> list[InboxRecord]:
        """Return capture events awaiting independent Systems projection.

        This deliberately does not inspect ``status`` or ``retention_state``.
        Projection is a capture concern: a held Systems write must not block
        later source staging, attachment retention, or a business/model turn.
        """
        retry_interval = max(0.0, float(retry_interval_seconds))
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json,"
                "source_envelope_json,retention_state FROM ingress_events "
                "WHERE projection_state IN ('pending','held') "
                "AND (projection_state='pending' OR projection_attempts < ?) "
                "AND (projection_state='pending' OR projection_updated_at IS NULL "
                "OR julianday(projection_updated_at) <= julianday('now', ?)) "
                "ORDER BY CASE projection_state WHEN 'pending' THEN 0 ELSE 1 END, "
                "COALESCE(projection_updated_at,created_at),seq LIMIT ?",
                (max(1, int(retry_cap)), f"-{retry_interval:.6f} seconds", max(1, int(limit))),
            ).fetchall()
        return [
            InboxRecord(
                seq=int(row["seq"]),
                message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=json.loads(row["raw_json"]),
                source_envelope=json.loads(row["source_envelope_json"]),
                retention_state=str(row["retention_state"]),
            )
            for row in rows
        ]

    def record_source_projection(
        self,
        record: InboxRecord,
        *,
        error: str | None = None,
        retry_cap: int = 1,
    ) -> dict[str, Any]:
        """Persist a projection success or bounded retryable hold.

        A Systems commit can succeed immediately before this local update.  In
        that crash window the event remains pending and is intentionally sent
        again; Systems owns message-id idempotency.  No business row is ever
        claimed or terminalled by this method.
        """
        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT projection_state,projection_attempts FROM ingress_events WHERE seq=?",
                (record.seq,),
            ).fetchone()
            if row is None:
                raise ConsumerError(f"inbox record {record.seq} disappeared")
            state = str(row["projection_state"])
            if state == "complete":
                conn.commit()
                return {"complete": True, "already_complete": True}
            attempts = int(row["projection_attempts"] or 0) + 1
            if error is None:
                changed = conn.execute(
                    "UPDATE ingress_events SET projection_state='complete',"
                    "projection_attempts=?,projection_last_error=NULL,projection_updated_at=? "
                    "WHERE seq=? AND projection_state IN ('pending','held')",
                    (attempts, now, record.seq),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(
                        f"inbox record {record.seq} could not record projection success"
                    )
                conn.commit()
                return {"complete": True, "attempt": attempts}
            cap = max(1, int(retry_cap))
            # ``held`` is durable operator-visible debt after every failed
            # attempt.  At the cap it remains held but is no longer selected
            # automatically; an operator/restart can explicitly re-arm it.
            changed = conn.execute(
                "UPDATE ingress_events SET projection_state='held',"
                "projection_attempts=?,projection_last_error=?,projection_updated_at=? "
                "WHERE seq=? AND projection_state IN ('pending','held')",
                (attempts, str(error)[:2000], now, record.seq),
            ).rowcount
            if changed != 1:
                raise ConsumerError(
                    f"inbox record {record.seq} could not record projection failure"
                )
            conn.commit()
            return {"complete": False, "attempt": attempts, "retry_cap": cap,
                    "terminal_hold": attempts >= cap}

    def source_projection_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT projection_state,COUNT(*) AS n FROM ingress_events "
                "GROUP BY projection_state"
            ).fetchall()
        counts = {str(row["projection_state"]): int(row["n"]) for row in rows}
        return {
            "source_projection_pending": counts.get("pending", 0),
            "source_projection_complete": counts.get("complete", 0),
            "source_projection_held": counts.get("held", 0),
        }

    def source_projection_last_error(self) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT projection_last_error FROM ingress_events "
                "WHERE projection_state='held' ORDER BY projection_updated_at DESC,seq DESC "
                "LIMIT 1"
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    def retention_result(self, record: InboxRecord) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT e.retention_state,e.retained_media_count,"
                "q.status AS quarantine_status FROM ingress_events e "
                "LEFT JOIN media_retention_quarantine q ON q.ingress_seq=e.seq "
                "WHERE e.seq=?",
                (record.seq,),
            ).fetchone()
        if row is None:
            raise ConsumerError(f"inbox record {record.seq} disappeared")
        state = str(row["retention_state"])
        if state not in {"complete", "bypassed"}:
            return None
        return {
            "retained": int(row["retained_media_count"]),
            "bytes": 0,
            "operation": state == "complete",
            "durable": True,
            "quarantined": row["quarantine_status"] == "quarantined",
        }

    def record_retention(
        self,
        record: InboxRecord,
        *,
        retained: int | None = None,
        bypassed: bool = False,
        refusal: str | None = None,
        error: str | None = None,
        retry_cap: int | None = None,
        quarantine_eligible: bool = False,
    ) -> dict[str, Any]:
        """Persist one attempt and atomically give up after the configured cap.

        Retryable mandatory-media failures retain an append-only failure history.
        At the cap the full raw event and that history are copied to quarantine,
        while the inbox row becomes ``bypassed`` so FIFO business processing can
        continue without deleting or rewriting the original event.
        """
        now = _utc_now()
        with self.connect() as conn:
            if error is not None:
                cap = max(1, int(retry_cap or 1))
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT message_id,chat_id,raw_json,source_envelope_json,"
                    "retention_state,retention_attempts,"
                    "retention_quarantine_attempts FROM ingress_events WHERE seq=?",
                    (record.seq,),
                ).fetchone()
                if row is None:
                    raise ConsumerError(f"inbox record {record.seq} disappeared")
                if str(row["retention_state"]) in {"complete", "bypassed"}:
                    conn.commit()
                    return {"quarantined": False, "durable": True}
                attempt = int(row["retention_attempts"] or 0) + 1
                quarantine_attempt = int(row["retention_quarantine_attempts"] or 0)
                if quarantine_eligible:
                    quarantine_attempt += 1
                failure = (error or "retention failed")[:2000]
                conn.execute(
                    "INSERT INTO media_retention_failures("
                    "ingress_seq,attempt,error,quarantine_eligible,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (record.seq, attempt, failure, int(quarantine_eligible), now),
                )
                quarantined = quarantine_eligible and quarantine_attempt >= cap
                state = "bypassed" if quarantined else "held"
                changed = conn.execute(
                    "UPDATE ingress_events SET retention_state=?,"
                    "retention_attempts=?,retention_quarantine_attempts=?,"
                    "retention_failures=retention_failures+1,"
                    "retention_last_error=?,retention_updated_at=? "
                    "WHERE seq=? AND retention_state IN ('pending','held')",
                    (
                        state,
                        attempt,
                        quarantine_attempt,
                        failure,
                        now,
                        record.seq,
                    ),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(
                        f"inbox record {record.seq} could not record retention failure"
                    )
                if quarantined:
                    history = [
                        dict(history_row)
                        for history_row in conn.execute(
                            "SELECT attempt,error,quarantine_eligible,created_at FROM "
                            "media_retention_failures WHERE ingress_seq=? ORDER BY attempt",
                            (record.seq,),
                        ).fetchall()
                    ]
                    conn.execute(
                        "INSERT INTO media_retention_quarantine("
                        "ingress_seq,message_id,chat_id,raw_json,"
                        "failure_history_json,terminal_error,status,quarantined_at) "
                        "VALUES(?,?,?,?,?,?,'quarantined',?) "
                        "ON CONFLICT(ingress_seq) DO NOTHING",
                        (
                            record.seq,
                            str(row["message_id"]),
                            str(row["chat_id"]),
                            str(row["source_envelope_json"] or row["raw_json"]),
                            json.dumps(history, sort_keys=True, separators=(",", ":")),
                            failure,
                            now,
                        ),
                    )
                conn.commit()
                return {
                    "quarantined": quarantined,
                    "attempt": attempt,
                    "quarantine_attempt": quarantine_attempt,
                    "retry_cap": cap,
                }
            state = "bypassed" if bypassed else "complete"
            changed = conn.execute(
                "UPDATE ingress_events SET retention_state=?,"
                "retained_media_count=?,retention_attempts=retention_attempts+1,"
                "retention_last_error=?,retention_updated_at=? "
                "WHERE seq=? AND retention_state IN ('pending','held')",
                (
                    state,
                    max(0, int(retained or 0)),
                    (refusal or "")[:2000] if refusal else None,
                    now,
                    record.seq,
                ),
            ).rowcount
            if changed == 0:
                row = conn.execute(
                    "SELECT retention_state FROM ingress_events WHERE seq=?",
                    (record.seq,),
                ).fetchone()
                if row is None or str(row[0]) not in {"complete", "bypassed"}:
                    raise ConsumerError(
                        f"inbox record {record.seq} could not record retention outcome"
                    )
        return {"quarantined": False}

    def retention_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(retained_media_count),0),"
                "COALESCE(SUM(retention_failures),0),"
                "COALESCE(SUM(retention_attempts),0),"
                "COALESCE(SUM(CASE WHEN status='pending' "
                "AND retention_state='pending' THEN 1 ELSE 0 END),0),"
                "COALESCE(SUM(CASE WHEN retention_state='complete' THEN 1 ELSE 0 END),0),"
                "COALESCE(SUM(CASE WHEN retention_state='bypassed' THEN 1 ELSE 0 END),0),"
                "COALESCE(SUM(CASE WHEN status IN ('pending','processing') "
                "AND retention_state='held' THEN 1 ELSE 0 END),0) "
                "FROM ingress_events"
            ).fetchone()
            quarantine_row = conn.execute(
                "SELECT COUNT(*) FROM media_retention_quarantine "
                "WHERE status='quarantined'"
            ).fetchone()
        return {
            "retention_total": int(row[0]),
            "retention_failures": int(row[1]),
            "retention_attempts": int(row[2]),
            "retention_pending": int(row[3]),
            "retention_complete": int(row[4]),
            "retention_bypassed": int(row[5]),
            "retention_held": int(row[6]),
            "retention_quarantined": int(quarantine_row[0]),
        }

    def retention_quarantine_status(self) -> dict[str, int]:
        """Queryable terminal give-up population by durable status."""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT status,COUNT(*) AS n FROM media_retention_quarantine "
                "GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["n"]) for row in rows}

    def retention_quarantine_message_ids(self) -> list[str]:
        """Stable row identities for operator-visible terminal retention debt."""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT message_id FROM media_retention_quarantine "
                "WHERE status='quarantined' ORDER BY quarantined_at,ingress_seq"
            ).fetchall()
        return [str(row["message_id"]) for row in rows]

    def retention_last_error(self) -> str | None:
        """Newest unresolved retention hold, independent across chat lanes."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT retention_last_error FROM ingress_events "
                "WHERE status IN ('pending','processing') "
                "AND retention_state='held' "
                "ORDER BY retention_updated_at DESC, seq DESC LIMIT 1"
            ).fetchone()
        return str(row[0]) if row else None

    def total(self) -> int:
        return sum(self.counts().values())

    def assert_and_record_conservation(self) -> int:
        """Persist a monotonic row-total high-water mark; deletion hard-aborts."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            current = int(conn.execute("SELECT COUNT(*) FROM ingress_events").fetchone()[0])
            row = conn.execute(
                "SELECT value FROM ingress_meta WHERE key='state_total_high_water'"
            ).fetchone()
            previous = int(row[0]) if row else 0
            if current < previous:
                raise ConsumerError(
                    "inbox conservation hard-abort: "
                    f"recorded_total={previous} current_total={current}"
                )
            conn.execute(
                "INSERT INTO ingress_meta(key,value) VALUES('state_total_high_water',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(current),),
            )
            conn.commit()
        return current

    def oldest_processing_updated_at(self) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT MIN(updated_at) FROM ingress_events WHERE status='processing'"
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    def claim_reply_delivery(
        self,
        delivery_key: str,
        *,
        chat_id: str,
        reply_to_message_id: str | None,
        correlation: Mapping[str, Any] | None = None,
    ) -> bool:
        """Durably claim a reply delivery BEFORE the send (at-most-once).

        Returns True when this call claimed the key. A crash between send and
        the outcome update leaves the claim row behind, which permanently
        refuses a re-send — undelivered-and-loud over retry-into-spam.
        """
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO reply_deliveries(
                    delivery_key,chat_id,reply_to_message_id,correlation_json,status,error,created_at
                ) VALUES(?,?,?,?, 'undelivered','claimed-in-flight',?)
                """,
                (
                    delivery_key,
                    chat_id,
                    reply_to_message_id,
                    json.dumps(dict(correlation), sort_keys=True, separators=(",", ":"))
                    if correlation is not None else None,
                    _utc_now(),
                ),
            )
            return cursor.rowcount == 1

    def management_document_correlation(self, record: InboxRecord) -> dict[str, Any] | None:
        """Resolve only an authentic quoted management reply to a sent notice.

        A bare acknowledgement has no document binding and remains an ordinary
        management message.  This lookup supplies context to Christopher; it
        never classifies the reply body or writes source/case evidence.
        """
        item = _bridge_item(record.raw)
        if bool(item.get("fromMe")):
            return None
        reply_to = item.get("replyTo")
        reply_to_id = reply_to.get("messageId") if isinstance(reply_to, Mapping) else None
        quoted = str(
            item.get("quotedMessageId")
            or item.get("replyToMessageId")
            or item.get("reply_to_message_id")
            or reply_to_id
            or ""
        ).strip()
        if not quoted:
            return None
        with self.connect() as conn:
            row = conn.execute(
                "SELECT delivery_key,bridge_message_id,correlation_json FROM reply_deliveries "
                "WHERE chat_id=? AND bridge_message_id=? AND status='delivered' "
                "AND delivery_key GLOB 'human-resolution:*'",
                (record.chat_id, quoted),
            ).fetchone()
        if row is None:
            return None
        try:
            stored = json.loads(row["correlation_json"] or "{}")
        except (TypeError, ValueError):
            raise ConsumerError("human-resolution delivery correlation is unreadable") from None
        if not isinstance(stored, Mapping):
            raise ConsumerError("human-resolution delivery correlation is invalid")
        document_id = str(stored.get("document_id") or "").strip()
        entry_id = str(stored.get("entry_id") or "").strip()
        if not document_id or not entry_id:
            raise ConsumerError("human-resolution delivery correlation is incomplete")
        if not _is_management_document_delivery_key(
            str(row["delivery_key"]), entry_id=entry_id, chat_id=record.chat_id
        ):
            raise ConsumerError("human-resolution delivery correlation key mismatch")
        correlation = {
            "document_id": document_id,
            "document_entry_id": entry_id,
            "outbound_notice_id": str(row["bridge_message_id"]),
            "reply_message_id": record.message_id,
            "confidence": "quoted_outbound_notice_exact",
        }
        canary_projection = stored.get("pa75_canary_projection")
        if canary_projection is not None:
            if not isinstance(canary_projection, Mapping):
                raise ConsumerError("human-resolution canary projection is invalid")
            correlation["pa75_canary_projection"] = dict(canary_projection)
        return correlation

    def record_reply_delivery(
        self,
        delivery_key: str,
        *,
        status: str,
        bridge_message_id: str | None = None,
        provider_outcome: str | None = None,
        error: str | None = None,
    ) -> None:
        if status not in {"delivered", "undelivered"}:
            raise ValueError(f"invalid reply delivery status {status!r}")
        with self.connect() as conn:
            conn.execute(
                "UPDATE reply_deliveries SET status=?, bridge_message_id=?, "
                "provider_outcome=?, error=? "
                "WHERE delivery_key=?",
                (status, bridge_message_id, provider_outcome, error, delivery_key),
            )


def _completed_turn_refs(state_db: Path) -> dict[str, str]:
    refs: dict[str, str] = {}
    if not state_db.exists():
        return refs
    conn = sqlite3.connect(state_db)
    conn.row_factory = sqlite3.Row
    try:
        tables = {str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "pa_turns" not in tables:
            return refs
        for row in conn.execute(
            "SELECT turn_id,message_refs_json FROM pa_turns "
            "WHERE turn_status='completed' AND error_json IS NULL ORDER BY completed_at"
        ):
            try:
                values = json.loads(row["message_refs_json"] or "[]")
            except (TypeError, ValueError):
                values = []
            for value in values:
                if value:
                    refs[str(value)] = str(row["turn_id"])
        return refs
    finally:
        conn.close()


def processing_enabled(config_path: Path) -> bool:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    return bool(pa.get("enabled")) if isinstance(pa, dict) else False


def _retention_config(config_path: Path) -> dict[str, Any] | None:
    """Return the opt-in generic media-retention contract.

    TGG supplies the values, but the consumer only understands roots and a
    configured business-operation name.  Client/case semantics stay behind
    that operation.
    """
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, Mapping) else None
    raw = pa.get("media_retention") if isinstance(pa, Mapping) else None
    if not isinstance(raw, Mapping) or raw.get("enabled") is not True:
        return None
    root_value = str(raw.get("media_root") or raw.get("root") or "").strip()
    root = Path(root_value).expanduser()
    source_roots = raw.get("source_roots") or raw.get("allowed_source_roots") or []
    operation = str(raw.get("operation") or raw.get("retention_operation") or "")
    ref_prefix = str(raw.get("media_ref_prefix") or "/media").strip().rstrip("/")
    if not root_value:
        raise MediaRetentionError("media retention root is not configured")
    if isinstance(source_roots, (str, bytes)):
        source_roots = [source_roots]
    if not isinstance(source_roots, Sequence) or not source_roots:
        raise MediaRetentionError("media retention source_roots are not configured")
    if not operation:
        raise MediaRetentionError("media retention operation is not configured")
    if (
        ref_prefix != "/media"
        and (
            not ref_prefix.startswith("/media/")
            or any(part in {"", ".", ".."} for part in ref_prefix.split("/")[2:])
        )
    ):
        raise MediaRetentionError("media retention ref prefix is invalid")
    return {
        "root": root.resolve(),
        "source_roots": tuple(Path(str(p)).expanduser().resolve() for p in source_roots),
        "operation": operation,
        "ref_prefix": ref_prefix,
        "min_free_bytes": (
            int(raw["min_free_bytes"])
            if raw.get("min_free_bytes") is not None
            else None
        ),
        "min_free_percent": (
            float(raw["min_free_percent"])
            if "min_free_percent" in raw
            else None
        ),
        "max_attempts": max(1, int(raw.get("max_attempts", 5))),
        "retry_interval_seconds": max(
            1.0, float(raw.get("retry_interval_seconds", 60))
        ),
    }


def _source_projection_config(config_path: Path) -> dict[str, Any] | None:
    """Return the independent capture-to-Systems projection contract.

    It is intentionally separate from ``pa.enabled``.  Turning model/business
    processing off is not permission to let the authoritative source ledger
    become stale.
    """
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, Mapping) else None
    raw = pa.get("source_projection") if isinstance(pa, Mapping) else None
    if not isinstance(raw, Mapping) or raw.get("enabled") is not True:
        return None
    operation = str(raw.get("operation") or "tgg_whatsapp_source_ingest").strip()
    if operation != "tgg_whatsapp_source_ingest":
        raise ConsumerError("source projection operation must be tgg_whatsapp_source_ingest")
    chat_ids = raw.get("chat_ids")
    if (
        not isinstance(chat_ids, list) or not chat_ids
        or any(not isinstance(value, str) or not value.strip() for value in chat_ids)
        or len(chat_ids) != len(set(chat_ids))
    ):
        raise ConsumerError("source projection chat_ids must be a non-empty unique list")
    return {
        "operation": operation,
        "chat_ids": frozenset(chat_ids),
        "max_attempts": max(1, int(raw.get("max_attempts", 5))),
        "retry_interval_seconds": max(
            1.0, float(raw.get("retry_interval_seconds", 15))
        ),
    }


def _project_source_envelope(
    config_path: Path, *, operation: str, source_envelope: Mapping[str, Any]
) -> dict[str, Any]:
    """Idempotently project one untouched durable capture document to Systems."""
    from types import SimpleNamespace

    from agent.pa_constitution import configured_constitution, resolve_context
    from tools.pa_business_tools import execute_business_operation, load_business_bridge_config

    # Validate against the persisted source document, not a reconstructed
    # bridge item.  Systems receives those same bytes as the operation payload.
    item = _bridge_item_ref(source_envelope)
    config_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    resolved = resolve_context(
        config_data,
        {"source": {"platform": "whatsapp", "chat_id": str(item.get("chatId") or "")}},
    )
    constitution = (
        resolved.constitution if resolved is not None else configured_constitution(config_data)
    )
    if constitution is None:
        raise ConsumerError("source projection could not resolve constitution")
    internal_context = SimpleNamespace(
        constitution=constitution,
        job_brief=None,
        job_type="whatsapp_source_projection_internal",
    )
    bridge = load_business_bridge_config(config_data, pa_context=internal_context)
    result = execute_business_operation(
        bridge, operation=operation, payload=dict(source_envelope)
    )
    if not isinstance(result, Mapping) or result.get("ok") is not True:
        details = ""
        if isinstance(result, Mapping):
            error = result.get("error")
            if isinstance(error, Mapping):
                details = str(error.get("message") or error.get("code") or "")
            elif error:
                details = str(error)
        raise ConsumerError(
            "source projection Systems operation failed" + (f": {details}" if details else "")
        )
    return dict(result)


def project_pending_source_events(
    inbox: DurableInbox, *, config_path: Path, limit: int
) -> dict[str, int]:
    """Drain a bounded independent projection outbox without touching model work."""
    config = _source_projection_config(config_path)
    if config is None:
        return {"attempted": 0, "complete": 0, "held": 0, "skipped": 0, "disabled": 1}
    summary = {"attempted": 0, "complete": 0, "held": 0, "skipped": 0, "disabled": 0}
    records = inbox.source_projection_candidates(
        limit=limit,
        retry_interval_seconds=config["retry_interval_seconds"],
        retry_cap=config["max_attempts"],
    )
    for record in records:
        summary["attempted"] += 1
        try:
            if record.chat_id not in config["chat_ids"]:
                inbox.record_source_projection(record, retry_cap=config["max_attempts"])
                summary["skipped"] += 1
                continue
            envelope = record.source_envelope
            if not isinstance(envelope, Mapping):
                raise ConsumerError("source projection record lacks stored source envelope")
            # Refuse an accidental mismatch before an external side effect.
            item = _bridge_item_ref(envelope)
            if (
                str(item.get("messageId") or "") != record.message_id
                or str(item.get("chatId") or "") != record.chat_id
            ):
                raise ConsumerError("source projection envelope identity diverges from inbox")
            _project_source_envelope(
                config_path,
                operation=str(config["operation"]),
                source_envelope=envelope,
            )
            inbox.record_source_projection(record, retry_cap=config["max_attempts"])
            summary["complete"] += 1
        except Exception as exc:
            result = inbox.record_source_projection(
                record,
                error=f"source-projection-retry: {exc}",
                retry_cap=config["max_attempts"],
            )
            summary["held"] += 1
            if result.get("terminal_hold"):
                print(
                    "source projection retry cap reached: "
                    f"message={record.message_id} error={exc}",
                    file=sys.stderr,
                )
    return summary


def _media_root_metrics(
    config_path: Path, *, inspect: bool, count_root: bool = True
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "media_root_count": 0,
        "media_root_bytes": 0,
        "media_volume_free_percent": None,
        "media_volume_free_bytes": None,
        "media_volume_total_bytes": None,
    }
    config = _retention_config(config_path)
    if config is None or not inspect:
        return metrics
    root: Path = config["root"]
    volume_path = root
    while not volume_path.exists() and volume_path != volume_path.parent:
        volume_path = volume_path.parent
    try:
        usage = shutil.disk_usage(volume_path)
    except OSError as exc:
        raise MediaRetentionError(f"media volume is not measurable: {exc}") from exc
    free_percent = (usage.free / usage.total * 100) if usage.total else 0.0
    metrics["media_volume_free_percent"] = round(free_percent, 3)
    metrics["media_volume_free_bytes"] = int(usage.free)
    metrics["media_volume_total_bytes"] = int(usage.total)
    if root.exists() and count_root:
        if not root.is_dir():
            raise MediaRetentionError("configured media root is not a directory")
        for directory, _, filenames in os.walk(root, followlinks=False):
            for filename in filenames:
                candidate = Path(directory) / filename
                if candidate.is_file():
                    metrics["media_root_count"] += 1
                    metrics["media_root_bytes"] += candidate.stat().st_size
    return metrics


def _retention_status(
    inbox: DurableInbox, config_path: Path, *, inspect_media: bool
) -> dict[str, Any]:
    return {
        **inbox.retention_counts(),
        **_media_root_metrics(config_path, inspect=inspect_media),
        "retention_quarantine_status": inbox.retention_quarantine_status(),
        "retention_quarantine_message_ids": (
            inbox.retention_quarantine_message_ids()
        ),
        "retention_hold": inbox.retention_last_error(),
    }


def _assert_media_headroom(config_path: Path, status: Mapping[str, Any]) -> None:
    config = _retention_config(config_path)
    if config is None:
        return
    free_bytes = status.get("media_volume_free_bytes")
    if free_bytes is None:
        raise MediaRetentionError("media volume free space is unknown")
    minimum_bytes = config.get("min_free_bytes")
    if minimum_bytes is not None:
        if int(minimum_bytes) < 0:
            raise MediaRetentionError("media retention min_free_bytes must be non-negative")
        if int(free_bytes) >= int(minimum_bytes):
            return
        raise MediaRetentionError(
            "media volume free space below configured absolute reserve: "
            f"{int(free_bytes)} B < {int(minimum_bytes)} B"
        )
    legacy_minimum = config.get("min_free_percent")
    if legacy_minimum is None:
        return
    free_percent = status.get("media_volume_free_percent")
    if free_percent is None:
        raise MediaRetentionError("media volume free percentage is unknown")
    if float(free_percent) < float(legacy_minimum):
        raise MediaRetentionError(
            "media volume free space below configured percentage floor: "
            f"{float(free_percent):.3f}% < {float(legacy_minimum):.3f}%"
        )


_IMAGE_SIGNATURES: tuple[tuple[str, str, bytes, int], ...] = (
    ("image/jpeg", "jpg", b"\xff\xd8\xff", 0),
    ("image/png", "png", b"\x89PNG\r\n\x1a\n", 0),
    ("image/gif", "gif", b"GIF8", 0),
    ("image/webp", "webp", b"WEBP", 8),
)


def _validated_image_type(path: Path, declared: str | None) -> tuple[str, str]:
    with path.open("rb") as handle:
        prefix = handle.read(16)
    detected = next(
        ((mime, ext) for mime, ext, signature, offset in _IMAGE_SIGNATURES
         if prefix[offset:offset + len(signature)] == signature),
        None,
    )
    if detected is None:
        raise ItemMediaRetentionError(
            f"retention source is not a supported image: {path.name}"
        )
    mime, ext = detected
    declared = str(declared or "").split(";", 1)[0].strip().lower()
    if declared and "/" in declared and declared != mime:
        # image/jpg is a widespread non-standard spelling for image/jpeg.
        if not (declared == "image/jpg" and mime == "image/jpeg"):
            raise ItemMediaRetentionError(
                f"PROVENANCE_DIVERGENCE: declared MIME {declared} != {mime}"
            )
    return mime, ext


def _validated_captured_media_type(path: Path) -> tuple[str, str, str | None]:
    """Classify retained outbound media without weakening image validation."""
    try:
        mime, _ = _validated_image_type(path, None)
        return "image", mime, None
    except MediaRetentionError as image_error:
        if path.suffix.lower() not in {".xlsx", ".zip"}:
            raise image_error
    with path.open("rb") as handle:
        if handle.read(4) != b"PK\x03\x04":
            raise MediaRetentionError(
                f"retained document is not a supported zip container: {path.name}"
            )
    if path.suffix.lower() == ".zip":
        return "document", "application/zip", path.name
    return (
        "document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        path.name,
    )


def _contained_existing_file(value: Any, roots: Sequence[Path]) -> Path:
    if isinstance(value, Mapping):
        value = value.get("path") or value.get("filePath") or value.get("localPath") or value.get("url")
    text = str(value or "")
    if text.startswith("file://"):
        text = text[7:]
    try:
        candidate = Path(text).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ItemMediaRetentionError(f"media source is unavailable: {exc}") from exc
    if not candidate.is_file() or not any(candidate.is_relative_to(root) for root in roots):
        raise ItemMediaRetentionError(
            "media source escapes configured roots or is not a file"
        )
    return candidate


def _event_media(item: Mapping[str, Any]) -> list[tuple[int, Any, str | None]]:
    values = item.get("mediaUrls") or item.get("media") or item.get("mediaPaths") or []
    if isinstance(values, (str, bytes, Mapping)):
        values = [values]
    if not isinstance(values, Sequence):
        raise ItemMediaRetentionError("event media collection is not a list")
    result: list[tuple[int, Any, str | None]] = []
    event_mime = item.get("mediaType") or item.get("mimeType")
    event_kind = str(event_mime or "").split("/", 1)[0].strip().lower()
    declared_mimes = item.get("mediaMimes") or []
    if isinstance(declared_mimes, (str, bytes)):
        declared_mimes = [declared_mimes]
    for index, value in enumerate(values):
        mime = (
            value.get("mime") or value.get("mimeType") or value.get("contentType")
            if isinstance(value, Mapping)
            else (
                declared_mimes[index]
                if index < len(declared_mimes)
                else event_mime
            )
        )
        if not mime and event_kind == "image":
            mime = event_mime
        declared = str(mime) if mime else None
        kind = str(declared or "").split("/", 1)[0].strip().lower()
        if kind != "image":
            continue
        result.append((index, value, declared))
    return result


def _event_retainable_documents(
    item: Mapping[str, Any],
) -> list[tuple[int, Any, str]]:
    """Return retainable documents with their provider-declared MIME."""
    values = item.get("mediaUrls") or item.get("media") or item.get("mediaPaths") or []
    if isinstance(values, (str, bytes, Mapping)):
        values = [values]
    if not isinstance(values, Sequence):
        raise ItemMediaRetentionError("event media collection is not a list")
    declared_mimes = item.get("mediaMimes") or []
    if isinstance(declared_mimes, (str, bytes)):
        declared_mimes = [declared_mimes]
    result: list[tuple[int, Any, str]] = []
    for index, value in enumerate(values):
        raw_path = (
            value.get("path")
            or value.get("filePath")
            or value.get("localPath")
            or value.get("url")
            if isinstance(value, Mapping)
            else value
        )
        suffix = Path(str(raw_path or "")).suffix.lower()
        if suffix not in {
            ".xlsx",
            ".csv",
            ".xlsm",
            ".xltm",
            ".pdf",
            ".docx",
            ".docm",
            ".dotm",
        }:
            continue
        declared = ""
        if isinstance(value, Mapping):
            declared = str(
                value.get("mime")
                or value.get("mimeType")
                or value.get("contentType")
                or ""
            )
        if not declared and index < len(declared_mimes):
            declared = str(declared_mimes[index] or "")
        if not declared:
            raise PermanentMediaRefusal(
                "PROVENANCE_DIVERGENCE: document has no provider-declared MIME"
            )
        result.append((index, value, declared))
    return result


def _converge_retained_media(
    config_path: Path, *, operation: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    from types import SimpleNamespace

    from agent.pa_constitution import configured_constitution, resolve_context
    from tools.pa_business_tools import execute_business_operation, load_business_bridge_config

    config_data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    resolved = resolve_context(
        config_data,
        {
            "source": {
                "platform": "whatsapp",
                "chat_id": str(payload.get("chat_jid") or ""),
            }
        },
    )
    constitution = (
        resolved.constitution
        if resolved is not None
        else configured_constitution(config_data)
    )
    if constitution is None:
        raise MediaRetentionError("media retention could not resolve constitution")
    # Retention is pre-model ingress infrastructure, not a model-callable job
    # operation. Reuse the resolved tenant/auth/operation registry while
    # deliberately avoiding job-brief allow/deny scope (ops correctly denies
    # the model from invoking this internal write itself).
    internal_context = SimpleNamespace(
        constitution=constitution,
        job_brief=None,
        job_type="media_retention_internal",
    )
    bridge = load_business_bridge_config(config_data, pa_context=internal_context)
    result = execute_business_operation(bridge, operation=operation, payload=payload)
    data = result.get("data") if isinstance(result, Mapping) else None
    if (
        not isinstance(result, Mapping)
        or result.get("ok") is not True
        or not isinstance(data, Mapping)
        or "ledgerChanged" not in data
        or "observationsChanged" not in data
    ):
        if isinstance(result, Mapping):
            details: list[str] = []
            if result.get("status_code") is not None:
                details.append(f"status_code={result['status_code']}")
            error = result.get("error")
            if isinstance(error, Mapping):
                if error.get("code"):
                    details.append(f"code={error['code']}")
                if error.get("message"):
                    details.append(f"message={error['message']}")
            if details:
                raise MediaRetentionError(
                    "media retention convergence failed: " + " ".join(details)
                )
        raise MediaRetentionError(
            "media retention convergence returned an invalid Systems envelope"
        )
    return dict(result)


def _retention_identity(
    record: InboxRecord, item: Mapping[str, Any]
) -> tuple[str, str, str, str]:
    """Return the stable retention identity and filename prefix for a record."""
    chat_id = str(item.get("chatId") or record.chat_id)
    message_id = str(item.get("messageId") or record.message_id)
    if chat_id != record.chat_id or message_id != record.message_id:
        raise ItemMediaRetentionError(
            "PROVENANCE_DIVERGENCE: inbox/event identity mismatch"
        )
    identity_digest = hashlib.sha256(
        (chat_id + "\0" + message_id).encode("utf-8")
    ).hexdigest()
    source_key = f"whatsapp-capture-v1:{identity_digest}"
    filename_prefix = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:24]
    return chat_id, message_id, source_key, filename_prefix


def _document_retention_kind(path: Any) -> str:
    if isinstance(path, Mapping):
        path = (
            path.get("path")
            or path.get("filePath")
            or path.get("localPath")
            or path.get("url")
        )
    extension = Path(str(path or "")).suffix.lower()
    return "spreadsheet" if extension in {".xlsx", ".csv"} else "document"


def _retained_document_name(
    filename_prefix: str,
    retention_kind: str,
    ordinal: int,
    digest: str,
    extension: str,
) -> str:
    return (
        f"{filename_prefix}_{retention_kind}_{ordinal}_"
        f"{digest[:24]}{extension}"
    )


def _retained_document_glob(
    root: Path, filename_prefix: str, retention_kind: str, ordinal: int
) -> list[Path]:
    return sorted(root.glob(f"{filename_prefix}_{retention_kind}_{ordinal}_*"))


def _retain_record_media_impl(
    record: InboxRecord, *, config_path: Path
) -> dict[str, Any]:
    """Retain one event's documents/images and converge ledger entries.

    Files land before the idempotent operation.  Therefore a crash after the
    rename or operation is safe to replay, while changed bytes/MIME at the
    same source ordinal fail closed.
    """
    config = _retention_config(config_path)
    if config is None:
        return {"retained": 0, "bytes": 0, "operation": False}
    item = _bridge_item(record.raw)
    chat_id, message_id, source_key, filename_prefix = _retention_identity(
        record, item
    )
    root: Path = config["root"]
    documents = _event_retainable_documents(item)
    retained_documents = 0
    document_bytes = 0
    retained_spreadsheets = 0
    retained: list[dict[str, Any]] = []
    if documents:
        from tools.pa_business_tools import validate_retainable_document

        root.mkdir(parents=True, exist_ok=True, mode=0o750)
        _assert_media_headroom(
            config_path,
            _media_root_metrics(config_path, inspect=True, count_root=False),
        )
        for ordinal, raw_path, declared_mime in documents:
            source = _contained_existing_file(raw_path, config["source_roots"])
            try:
                validate_retainable_document(
                    source, declared_mime=declared_mime
                )
            except ValueError as exc:
                raise PermanentMediaRefusal(str(exc)) from exc
            content = source.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            extension = source.suffix.lower()
            retention_kind = _document_retention_kind(source)
            target = (
                root
                / _retained_document_name(
                    filename_prefix,
                    retention_kind,
                    ordinal,
                    digest,
                    extension,
                )
            ).resolve()
            if not target.is_relative_to(root):
                raise ItemMediaRetentionError(
                    "derived document retention target escapes configured root"
                )
            ordinal_candidates = _retained_document_glob(
                root, filename_prefix, "spreadsheet", ordinal
            ) + _retained_document_glob(
                root, filename_prefix, "document", ordinal
            )
            if ordinal_candidates and target not in ordinal_candidates:
                raise ItemMediaRetentionError(
                    "PROVENANCE_DIVERGENCE: retained document ordinal "
                    f"{ordinal} changed"
                )
            if target.exists():
                if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
                    raise ItemMediaRetentionError(
                        "PROVENANCE_DIVERGENCE: retained document ordinal "
                        f"{ordinal} changed"
                    )
            else:
                tmp = root / f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(content)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(tmp, target)
                    directory_fd = os.open(root, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    with contextlib.suppress(FileNotFoundError):
                        tmp.unlink()
            retained_documents += 1
            document_bytes += len(content)
            retained_spreadsheets += int(retention_kind == "spreadsheet")
            retained.append({
                "source_key": source_key,
                "media_ordinal": ordinal,
                "digest": digest,
                "mime": declared_mime,
                "ref": f"{config['ref_prefix']}/{target.name}",
            })
    media = _event_media(item)
    if not media:
        if retained_documents:
            total_bytes = document_bytes
        else:
            coarse_kind = str(
                item.get("mediaType") or item.get("mimeType") or ""
            ).split("/", 1)[0].strip().lower()
            if coarse_kind != "image":
                return {"retained": 0, "bytes": 0, "operation": False}
            if item.get("hasMedia") is True:
                raise ItemMediaRetentionError(
                    "mandatory inbound media has no resolvable capture path"
                )
            return {"retained": 0, "bytes": 0, "operation": False}
    else:
        root.mkdir(parents=True, exist_ok=True, mode=0o750)
        _assert_media_headroom(
            config_path,
            _media_root_metrics(config_path, inspect=True, count_root=False),
        )
        total_bytes = document_bytes
    for ordinal, raw_path, declared_mime in media:
        source = _contained_existing_file(raw_path, config["source_roots"])
        mime, ext = _validated_image_type(source, declared_mime)
        content = source.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        target = (root / f"{filename_prefix}_{ordinal}.{ext}").resolve()
        if not target.is_relative_to(root):
            raise ItemMediaRetentionError(
                "derived retention target escapes configured root"
            )
        ordinal_candidates = list(root.glob(f"{filename_prefix}_{ordinal}.*"))
        if ordinal_candidates and target not in ordinal_candidates:
            raise ItemMediaRetentionError(
                f"PROVENANCE_DIVERGENCE: retained ordinal {ordinal} MIME changed"
            )
        if target.exists():
            existing = target.read_bytes()
            existing_mime, _ = _validated_image_type(target, mime)
            if hashlib.sha256(existing).hexdigest() != digest or existing_mime != mime:
                raise ItemMediaRetentionError(
                    f"PROVENANCE_DIVERGENCE: retained ordinal {ordinal} changed"
                )
        else:
            tmp = root / f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp, target)
                directory_fd = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    tmp.unlink()
        total_bytes += len(content)
        retained.append({
            "source_key": source_key,
            "media_ordinal": ordinal,
            "digest": digest,
            "mime": mime,
            "ref": f"{config['ref_prefix']}/{target.name}",
        })

    try:
        _converge_retained_media(
            config_path,
            operation=config["operation"],
            payload={
                "message_id": message_id,
                "source_key": source_key,
                "chat_jid": chat_id,
                "media": retained,
            },
        )
    except Exception as exc:
        if isinstance(exc, MediaRetentionError):
            raise
        raise MediaRetentionError(f"media retention convergence failed: {exc}") from exc
    return {
        "retained": len(retained),
        "bytes": total_bytes,
        "operation": True,
        **(
            {"validated_documents": len(documents)}
            if retained_documents != retained_spreadsheets
            else {}
        ),
        **(
            {"validated_spreadsheets": retained_spreadsheets}
            if retained_spreadsheets
            else {}
        ),
    }


def retain_record_media(record: InboxRecord, *, config_path: Path) -> dict[str, Any]:
    """Normalize fallible retention I/O to the retryable retention class."""
    try:
        return _retain_record_media_impl(record, config_path=config_path)
    except MediaRetentionError:
        raise
    except OSError as exc:
        raise MediaRetentionError(f"media retention I/O failed: {exc}") from exc


def ensure_record_media_retained(
    inbox: DurableInbox, record: InboxRecord, *, config_path: Path
) -> dict[str, Any]:
    """Retain once, with a durable result shared by capture and claim paths."""
    durable = inbox.retention_result(record)
    if durable is not None:
        return durable
    try:
        result = retain_record_media(record, config_path=config_path)
    except PermanentMediaRefusal as exc:
        refusal = f"media-refused: {exc}"
        inbox.record_retention(record, bypassed=True, refusal=refusal)
        return {
            "retained": 0,
            "bytes": 0,
            "operation": False,
            "refused": True,
            "reason": str(exc),
        }
    except MediaRetentionError as exc:
        config = _retention_config(config_path)
        retry_cap = int(config["max_attempts"]) if config is not None else 5
        outcome = inbox.record_retention(
            record,
            error=f"media-retention-retry: {exc}",
            retry_cap=retry_cap,
            quarantine_eligible=isinstance(exc, ItemMediaRetentionError),
        )
        if outcome.get("quarantined"):
            return {
                "retained": 0,
                "bytes": 0,
                "operation": False,
                "quarantined": True,
                "reason": str(exc),
                "attempt": int(outcome["attempt"]),
                "quarantine_attempt": int(outcome["quarantine_attempt"]),
                "retry_cap": int(outcome["retry_cap"]),
            }
        raise
    inbox.record_retention(
        record,
        retained=int(result["retained"]),
        bypassed=not bool(result.get("operation") or int(result["retained"]) > 0),
    )
    return result


async def retain_pending_media(
    inbox: DurableInbox, *, config_path: Path, limit: int
) -> dict[str, int]:
    """Bounded capture-lane retention independent of business processing."""
    summary = {
        "examined": 0,
        "retained": 0,
        "bypassed": 0,
        "held": 0,
        "quarantined": 0,
    }
    config = _retention_config(config_path)
    retry_interval = (
        float(config["retry_interval_seconds"]) if config is not None else 60.0
    )
    for record in inbox.retention_candidates(
        limit=limit, retry_interval_seconds=retry_interval
    ):
        summary["examined"] += 1
        try:
            result = await asyncio.to_thread(
                ensure_record_media_retained,
                inbox,
                record,
                config_path=config_path,
            )
        except MediaRetentionError as exc:
            summary["held"] += 1
            print(
                "media retention HELD/PENDING: "
                f"chat={record.chat_id} message={record.message_id} error={exc}",
                file=sys.stderr,
            )
            continue
        if result.get("quarantined"):
            summary["quarantined"] += 1
            print(
                "media retention QUARANTINED: "
                f"chat={record.chat_id} message={record.message_id} "
                f"reason={result.get('reason')}",
                file=sys.stderr,
            )
        elif result.get("operation") or int(result["retained"]) > 0:
            summary["retained"] += int(result["retained"])
        else:
            summary["bypassed"] += 1
    return summary


def retain_claimed_media(records: Sequence[InboxRecord], *, config_path: Path) -> dict[str, int]:
    totals = {"retained": 0, "bytes": 0, "events": 0}
    for record in records:
        result = retain_record_media(record, config_path=config_path)
        totals["retained"] += int(result["retained"])
        totals["bytes"] += int(result["bytes"])
        totals["events"] += int(bool(result["retained"]))
    return totals


def processing_gate_state(gate_path: Path) -> dict[str, Any]:
    if not gate_path.is_file():
        raise ConsumerError(f"production processing gate is missing: {gate_path}")
    try:
        state = json.loads(gate_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ConsumerError(f"production processing gate is unreadable: {exc}") from exc
    if not isinstance(state, dict) or state.get("version") != 1:
        raise ConsumerError("production processing gate must be a version-1 object")
    if state.get("enabled") not in {True, False}:
        raise ConsumerError("production processing gate enabled must be boolean")
    generation = state.get("generation")
    if not isinstance(generation, int) or generation < 0:
        raise ConsumerError(
            "production processing gate generation must be non-negative"
        )
    return state


def configured_engine(config_path: Path) -> tuple[str, str]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    model = data.get("model") if isinstance(data, dict) else None
    if not isinstance(model, dict):
        raise ConsumerError("config.model must be a mapping")
    provider = str(model.get("provider") or "")
    selected = str(model.get("default") or model.get("model") or "")
    if not provider or not selected:
        raise ConsumerError("config model provider/default is incomplete")
    return provider, selected


def _turn_row(
    state_db: Path,
    *,
    replay_run_id: str | None = None,
    started_after: float | None = None,
) -> sqlite3.Row | None:
    if not state_db.exists():
        return None
    conn = sqlite3.connect(state_db)
    conn.row_factory = sqlite3.Row
    try:
        if replay_run_id:
            return conn.execute(
                "SELECT * FROM pa_turns WHERE replay_run_id=? "
                "ORDER BY started_at DESC LIMIT 1",
                (replay_run_id,),
            ).fetchone()
        return conn.execute(
            "SELECT * FROM pa_turns WHERE started_at>=? "
            "ORDER BY started_at DESC LIMIT 1",
            (float(started_after or 0),),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def _assert_completed_turn(
    row: sqlite3.Row | None,
    *,
    provider: str,
    model: str,
    require_response: bool,
) -> str:
    if row is None:
        raise ConsumerError("Hermes produced no pa_turn record")
    if str(row["turn_status"]) != "completed":
        raise ConsumerError(
            f"Hermes pa_turn {row['turn_id']} status={row['turn_status']}"
        )
    if str(row["provider"]) != provider or str(row["model"]) != model:
        raise ConsumerError(
            f"Hermes pa_turn engine mismatch: {row['provider']}/{row['model']}"
        )
    if require_response:
        envelope = json.loads(row["raw_turn_envelope_json"] or "{}")
        response = str(envelope.get("final_response") or "").strip()
        if not response:
            raise ConsumerError("Hermes pa_turn completed without a provider response")
    return str(row["turn_id"])


async def process_replay_records(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    state_db: Path,
    run_id: str,
) -> dict[str, Any]:
    """Process fixture records through Hermes replay with outbound captured."""
    from gateway.config import load_gateway_config
    from gateway.replay import ReplayPlan
    from gateway.run import GatewayRunner

    provider, model = configured_engine(config_path)
    runner = GatewayRunner(load_gateway_config())
    result = await runner.replay(
        ReplayPlan(
            platform="whatsapp",
            messages=tuple(record.raw for record in records),
            run_id=run_id,
            attempt_id=f"attempt-{uuid.uuid4().hex[:12]}",
            delivery_mode="capture",
            bypass_require_mention=True,
            bypass_auth=True,
            source_path="fixture-only-durable-jsonl-consumer",
        )
    )
    row = _turn_row(state_db, replay_run_id=run_id)
    turn_id = _assert_completed_turn(
        row,
        provider=provider,
        model=model,
        require_response=True,
    )
    return {
        "turn_id": turn_id,
        "provider": provider,
        "model": model,
        "processed": int(result.processed),
        "outbound_captured": len(result.outbound),
        # Fixture callers must inspect the captured payload, not only its
        # count.  The replay path is outbound-disabled; carrying these
        # envelopes forward lets consumer-layer checks prove attachment and
        # exception-receipt content without opening a real delivery path.
        "captured_outbound": [dict(entry) for entry in result.outbound],
        "blocked_commands": len(result.blocked_commands),
    }


def _turn_rows(state_db: Path, *, replay_run_id: str) -> list[sqlite3.Row]:
    if not state_db.exists():
        return []
    conn = sqlite3.connect(state_db)
    conn.row_factory = sqlite3.Row
    try:
        return list(
            conn.execute(
                "SELECT * FROM pa_turns WHERE replay_run_id=? ORDER BY started_at",
                (replay_run_id,),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def _sandbox_dataset_for_retention_root(
    config_path: Path, retention_root: Path
) -> str | None:
    from tools.python_sandbox_paths import is_python_sandbox_dataset_name

    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    sandbox = data.get("python_sandbox") if isinstance(data, Mapping) else None
    datasets = sandbox.get("datasets") if isinstance(sandbox, Mapping) else None
    if not isinstance(datasets, Mapping):
        return None
    matches: list[str] = []
    for raw_name, spec in datasets.items():
        if not isinstance(spec, Mapping) or spec.get("type") != "path":
            continue
        raw_path = spec.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        if Path(raw_path).expanduser().resolve() == retention_root:
            name = str(raw_name)
            if not is_python_sandbox_dataset_name(name):
                raise MediaRetentionError(
                    f"python_sandbox dataset name is invalid: {name!r}"
                )
            matches.append(name)
    if len(matches) > 1:
        raise MediaRetentionError(
            "multiple python_sandbox datasets map the media retention root"
        )
    return matches[0] if matches else None


def _mutable_bridge_item(value: dict[str, Any]) -> dict[str, Any]:
    item = _bridge_item_ref(value)
    if not isinstance(item, dict):
        raise ConsumerError("copied durable bridge item is not mutable")
    return item


def _replay_messages_with_retained_documents(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    management_document_correlations: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[dict[str, Any], ...]:
    """Copy records and append retention evidence or quarantine warnings."""
    config = _retention_config(config_path)
    root: Path | None = config["root"] if config is not None else None
    dataset_name = (
        _sandbox_dataset_for_retention_root(config_path, root)
        if root is not None
        else None
    )
    python_sandbox_dataset_path = None
    if dataset_name is not None:
        from tools.python_sandbox_paths import (
            python_sandbox_dataset_path as _python_sandbox_dataset_path,
        )

        python_sandbox_dataset_path = _python_sandbox_dataset_path

    messages: list[dict[str, Any]] = []
    for record in records:
        message = copy.deepcopy(record.raw)
        messages.append(message)
        item = _mutable_bridge_item(message)
        correlation = (management_document_correlations or {}).get(record.message_id)
        if correlation is not None:
            # This is turn-local correlation metadata derived from an
            # authenticated WhatsApp quote and our own delivery receipt. It
            # is not capture provenance, is never projected to Systems, and
            # does not decide what the human's language means.
            existing_context = item.get("_hermes_pa_context")
            context = dict(existing_context) if isinstance(existing_context, Mapping) else {}
            context["management_document_correlation"] = dict(correlation)
            item["_hermes_pa_context"] = context
        # The durable consumer enters Hermes through the replay adapter.  That
        # adapter only exposes PA turn metadata from these private bridge
        # fields; retaining ``metadata`` on the raw capture record is not
        # enough.  Copy the structured nightly assignment only after the
        # consumer's reserved-chat/sender/role validation has accepted it.
        # This lets the same-session completion gate identify the batch/chat
        # without trusting trigger prose or allowing ordinary WhatsApp rows to
        # inject PA context.
        metadata = item.get("metadata")
        if (
            isinstance(metadata, Mapping)
            and metadata.get("job_type") == "tgg_nightly_whatsapp"
            and _priority_direct_trigger(record, config_path)
        ):
            item["_hermes_pa_job_type"] = "tgg_nightly_whatsapp"
            context = {
                "job_type": "tgg_nightly_whatsapp",
                "nightly_batch_id": metadata.get("nightly_batch_id"),
                "nightly_role": metadata.get("nightly_role"),
                "authoritative_chat_id": metadata.get("authoritative_chat_id"),
            }
            if metadata.get("continuous_interval") is True:
                context["continuous_interval"] = True
                context["continuous_contract"] = metadata.get("continuous_contract")
            item["_hermes_pa_context"] = context
        if record.retention_quarantined:
            warning = (
                "[Attachment unavailable: retention quarantine for message "
                f"{record.message_id}. Do not infer or claim the attachment's contents.]"
            )
            body = str(item.get("body") or item.get("text") or "").rstrip()
            item["body"] = (body + "\n\n" if body else "") + warning
            continue
        if (
            record.retention_state != "complete"
            or root is None
            or dataset_name is None
            or python_sandbox_dataset_path is None
        ):
            continue
        documents = _event_retainable_documents(item)
        if not documents:
            continue
        _, _, _, filename_prefix = _retention_identity(record, item)
        annotations: list[str] = []
        for ordinal, raw_path, _declared_mime in documents:
            retention_kind = _document_retention_kind(raw_path)
            candidates = [
                path
                for path in _retained_document_glob(
                    root, filename_prefix, retention_kind, ordinal
                )
                if path.is_file()
            ]
            if len(candidates) > 1:
                raise MediaRetentionError(
                    "PROVENANCE_DIVERGENCE: multiple retained documents for "
                    f"ordinal {ordinal}"
                )
            if not candidates:
                continue
            annotations.append(
                "[Attachment retained for analysis in python_sandbox: "
                f"{python_sandbox_dataset_path(dataset_name) / candidates[0].name}]"
            )
        if not annotations:
            continue
        body = str(item.get("body") or item.get("text") or "").rstrip()
        item["body"] = (body + "\n\n" if body else "") + "\n".join(annotations)
    return tuple(messages)


async def process_live_records(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    state_db: Path,
    persistent_session: bool,
    runner: Any | None = None,
    defer_provider_errors: bool = False,
    management_document_correlations: Mapping[str, Mapping[str, Any]] | None = None,
    capture_business_writes_for_test_management: bool = False,
    replay_messages: Sequence[Mapping[str, Any]] | None = None,
    replay_session_key_override: str | None = None,
    replay_internal_message_ids: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Process live durable records through the replay orchestrator.

    This is the eval-proven machinery (turn bundling, passive-debounce parity,
    per-run turn attribution) rather than a hand-rolled per-message path:

    - ``delivery_mode="capture"``: agent responses are captured, never sent —
      outbound remains impossible from this path regardless of trust rung.
    - ``bypass_auth=True``: the observer/ingest scope is governed by the chat
      allowlist + constitution selectors, NOT per-user pairing. Site workers
      are unpaired by design; routing their group messages through the paired
      -user auth gate silently dropped every event and produced the
      2026-07-21 first-light no-turn crash loop.
    - Outcome classification (activation ruling): a message a turn cites is
      turn-produced (completed); a message the pipeline legitimately held —
      mention-gated, debounced into a later bundle, deduped, non-content —
      is consumed-no-turn (skipped), NOT a failure. Only genuine errors
      (turn failed, engine mismatch, orchestrator exception) raise and mark
      the batch failed.
    """
    from gateway.config import load_gateway_config
    from gateway.replay import ReplayPlan
    from gateway.run import GatewayRunner

    provider, model = configured_engine(config_path)
    business_write_mode = "apply"
    if capture_business_writes_for_test_management:
        # This is the only PA-75 selector.  It is evaluated by the durable
        # runtime from its already-authenticated records, not by Christopher,
        # a replay file, or an operation payload.  The literal test chat is an
        # independent guard against a canary invoking the real group.
        test_chat = "120363426509183563@g.us"
        record_chats = {record.chat_id for record in records}
        if record_chats != {test_chat} or test_chat not in _management_selector_chats(config_path):
            raise ConsumerError("capture-only mode requires the approved test management selector")
        business_write_mode = "capture"
    # Production passes one long-lived runner into every per-chat task.  The
    # optional construction path preserves isolated callers and fixtures.
    runner = runner or GatewayRunner(load_gateway_config())
    run_id = f"live-drain-{uuid.uuid4().hex[:12]}"
    replay_plan = ReplayPlan(
        platform="whatsapp",
        messages=(
            tuple(dict(message) for message in replay_messages)
            if replay_messages is not None
            else _replay_messages_with_retained_documents(
                records,
                config_path=config_path,
                management_document_correlations=management_document_correlations,
            )
        ),
        run_id=run_id,
        attempt_id=f"attempt-{uuid.uuid4().hex[:12]}",
        delivery_mode="capture",
        bypass_require_mention=True,
        bypass_auth=True,
        live_business_writes=True,
        business_write_mode=business_write_mode,
        source_path="durable-jsonl-consumer-live",
        # Ordinary live drain is one ongoing conversation per chat. Bounded
        # backplay is recovery/diagnostic replay and remains isolated.
        # A persisted conversation cannot safely carry its engine identity
        # across a provider/model switch. Include that identity in the live
        # namespace so ordinary restarts retain context, while an intentional
        # engine change starts a fresh session instead of producing an audit
        # mismatch and suppressing reply delivery.
        replay_namespace=(
            persistent_chat_namespace(provider, model)
            if persistent_session
            else None
        ),
        session_key_override=replay_session_key_override,
        internal_message_ids=replay_internal_message_ids,
    )
    try:
        result = await runner.replay(replay_plan)
    except Exception as exc:
        if not defer_provider_errors:
            raise
        captured = [
            dict(entry)
            for entry in (getattr(exc, "replay_outbound", None) or [])
            if isinstance(entry, Mapping)
        ]
        return {
            "provider": provider,
            "model": model,
            "processed": 0,
            "handled": [],
            "captured_outbound": captured,
            "captured_business_mutations": [
                dict(entry)
                for entry in (getattr(exc, "replay_captured_business_mutations", None) or [])
                if isinstance(entry, Mapping)
            ],
            "provider_errors": [f"{type(exc).__name__}: {exc}"],
            "outbound_sent": 0,
            "submitted_message_ids": [record.message_id for record in records],
        }
    handled: list[dict[str, Any]] = []
    provider_errors: list[str] = []
    for row in _turn_rows(state_db, replay_run_id=run_id):
        try:
            turn_id = _assert_completed_turn(
                row,
                provider=provider,
                model=model,
                require_response=False,
            )
        except ConsumerError as exc:
            detail = str(exc)
            try:
                error_payload = json.loads(row["error_json"] or "null")
            except (TypeError, ValueError, KeyError, IndexError):
                error_payload = None
            if error_payload:
                detail = f"{detail}: {json.dumps(error_payload, sort_keys=True)}"
            provider_errors.append(detail)
            continue
        try:
            refs = json.loads(row["message_refs_json"] or "[]")
        except (TypeError, ValueError, KeyError, IndexError):
            refs = []
        ids = [str(ref) for ref in refs if ref]
        handled.append({"message_ids": ids, "turn_id": turn_id})
    captured_outbound = [dict(entry) for entry in result.outbound]
    captured_business_mutations = [
        dict(entry)
        for entry in (getattr(result, "captured_business_mutations", None) or [])
    ]
    if not handled and not provider_errors:
        captured_error = _captured_provider_error(captured_outbound)
        if captured_error:
            provider_errors.append(captured_error)
    if provider_errors and not defer_provider_errors:
        raise ConsumerError(provider_errors[0])
    return {
        "provider": provider,
        "model": model,
        "processed": int(result.processed or 0),
        "handled": handled,
        "captured_outbound": captured_outbound,
        "captured_business_mutations": captured_business_mutations,
        "provider_errors": provider_errors,
        "outbound_sent": 0,
        "submitted_message_ids": [record.message_id for record in records],
    }


def persistent_chat_namespace(provider: str, model: str) -> str:
    """Session namespace of the ordinary live drain: one conversation per chat."""
    return f"agent:live-drain:persistent-chat:{provider}:{model}"


def management_conversation_session_key(
    config_path: Path, *, platform: str, chat_id: str
) -> str | None:
    """Stable session key of one configured Management chat's live conversation.

    This is the key the ordinary live drain gives that chat's turns, so it
    also owns the chat's persistent sandbox workspace (PA-129) and survives
    compaction.  A scheduled job created from that chat uses it to work in
    the same folder (PA-203).  Returns ``None`` when the chat is not a
    configured WhatsApp Management chat, when its ordinary turns run in
    isolated nightly sessions, or when per-participant group sessions mean no
    single chat-level conversation exists.
    """
    chat = str(chat_id or "").strip()
    if str(platform or "").strip().lower() != "whatsapp" or not chat:
        return None
    if chat not in _management_selector_chats(config_path):
        return None
    if chat in _nightly_selector_chats(config_path):
        return None
    from gateway.config import Platform, load_gateway_config
    from gateway.replay import namespace_session_key
    from gateway.session import SessionSource, build_session_key

    gateway_config = load_gateway_config()
    group_sessions_per_user = bool(
        getattr(gateway_config, "group_sessions_per_user", True)
    )
    is_group = chat.endswith("@g.us")
    if is_group and group_sessions_per_user:
        return None
    base_key = build_session_key(
        SessionSource(
            platform=Platform.WHATSAPP,
            chat_id=chat,
            chat_type="group" if is_group else "dm",
        ),
        group_sessions_per_user=group_sessions_per_user,
        thread_sessions_per_user=bool(
            getattr(gateway_config, "thread_sessions_per_user", False)
        ),
    )
    provider, model = configured_engine(config_path)
    return namespace_session_key(base_key, persistent_chat_namespace(provider, model))


def _management_selector_chats(config_path: Path) -> frozenset[str]:
    """WhatsApp chats bound to the tgg_management selector class.

    The reply-delivery decision keys on the SELECTOR of the inbound chat
    (teren 2026-07-21 12:00 ruling) — never on prose or model output.
    Ingest/site-selector chats are structurally absent from this set, so
    their captured responses can never deliver.
    """
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    constitution_raw = str((pa or {}).get("constitution_path") or "")
    if not constitution_raw:
        return frozenset()
    constitution_path = Path(constitution_raw)
    if not constitution_path.is_file():
        return frozenset()
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8")) or {}
    chats: set[str] = set()
    for selector in constitution.get("selectors") or []:
        if not isinstance(selector, Mapping):
            continue
        match = selector.get("match") or {}
        if (
            selector.get("job_type") == "tgg_management"
            and match.get("source.platform") == "whatsapp"
            and match.get("source.chat_id")
        ):
            chats.add(str(match.get("source.chat_id")))
    return frozenset(chats)


def _priority_selector_chats(config_path: Path) -> frozenset[str]:
    """Internal and management chats that keep reserved consumer capacity.

    Management remains the only reply-delivery class.  The separate nightly
    selector is included only for scheduling while site ingestion is paused.
    """
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    constitution_raw = str((pa or {}).get("constitution_path") or "")
    if not constitution_raw:
        return frozenset()
    constitution_path = Path(constitution_raw)
    if not constitution_path.is_file():
        return frozenset()
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8")) or {}
    chats: set[str] = set()
    for selector in constitution.get("selectors") or []:
        if not isinstance(selector, Mapping):
            continue
        match = selector.get("match") or {}
        if (
            selector.get("job_type") in {"tgg_management", "tgg_nightly_whatsapp"}
            and match.get("source.platform") == "whatsapp"
            and match.get("source.chat_id")
        ):
            chats.add(str(match.get("source.chat_id")))
    return frozenset(chats)


def _nightly_selector_chats(config_path: Path) -> frozenset[str]:
    """Internal WhatsApp chats bound to isolated nightly analyzer sessions."""
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    constitution_raw = str((pa or {}).get("constitution_path") or "")
    if not constitution_raw:
        return frozenset()
    constitution_path = Path(constitution_raw)
    if not constitution_path.is_file():
        return frozenset()
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8")) or {}
    chats: set[str] = set()
    for selector in constitution.get("selectors") or []:
        if not isinstance(selector, Mapping):
            continue
        match = selector.get("match") or {}
        if (
            selector.get("job_type") == "tgg_nightly_whatsapp"
            and match.get("source.platform") == "whatsapp"
            and match.get("source.chat_id")
        ):
            chats.add(str(match.get("source.chat_id")))
    return frozenset(chats)


def _management_quiet_seconds(config_path: Path) -> float:
    """Configured trailing-quiet window before a management turn starts."""
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    constitution_raw = str((pa or {}).get("constitution_path") or "")
    if not constitution_raw:
        return 0.0
    constitution_path = Path(constitution_raw)
    if not constitution_path.is_file():
        return 0.0
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8")) or {}
    briefs = constitution.get("job_briefs") if isinstance(constitution, Mapping) else None
    management = briefs.get("tgg_management") if isinstance(briefs, Mapping) else None
    raw = management.get("debounce_addressed_ms") if isinstance(management, Mapping) else None
    try:
        return max(0.0, float(raw) / 1000.0) if raw is not None else 0.0
    except (TypeError, ValueError):
        raise ConsumerError("tgg_management debounce_addressed_ms is invalid") from None


def _management_requires_direct_trigger(config_path: Path, chat_id: str) -> bool:
    """Read the management brief gate without widening other priority chats."""
    if chat_id not in _management_selector_chats(config_path):
        return True
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    pa = data.get("pa") if isinstance(data, dict) else None
    constitution_raw = str((pa or {}).get("constitution_path") or "")
    if not constitution_raw:
        return True
    constitution_path = Path(constitution_raw)
    if not constitution_path.is_file():
        return True
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8")) or {}
    briefs = constitution.get("job_briefs") if isinstance(constitution, Mapping) else None
    management = briefs.get("tgg_management") if isinstance(briefs, Mapping) else None
    raw = management.get("require_mention") if isinstance(management, Mapping) else None
    if raw is None:
        return True
    if not isinstance(raw, bool):
        raise ConsumerError("tgg_management require_mention must be boolean")
    return raw


def _normalize_whatsapp_id(value: Any) -> str:
    normalized = str(value or "").strip()
    if ":" in normalized and "@" in normalized:
        normalized = normalized.replace(":", "@", 1)
    return normalized


def _management_direct_trigger(record: InboxRecord) -> bool:
    """True only for an explicit mention/command or a reply quoting Christopher."""
    item = _bridge_item(record.raw)
    if bool(item.get("fromMe")):
        return False
    body = str(item.get("body") or item.get("text") or "").strip()
    if body.startswith("/"):
        return True
    bot_ids = {
        normalized
        for value in (item.get("botIds") or [])
        if (normalized := _normalize_whatsapp_id(value))
    }
    mentioned_ids = {
        normalized
        for value in (item.get("mentionedIds") or [])
        if (normalized := _normalize_whatsapp_id(value))
    }
    if bot_ids & mentioned_ids:
        return True
    lower_body = body.lower()
    if any(
        (bare := bot_id.split("@", 1)[0].lower())
        and f"@{bare}" in lower_body
        for bot_id in bot_ids
    ):
        return True
    if item.get("quotedFromBot"):
        return True
    quoted_participant = _normalize_whatsapp_id(item.get("quotedParticipant"))
    return bool(quoted_participant and quoted_participant in bot_ids)


def _priority_direct_trigger(record: InboxRecord, config_path: Path) -> bool:
    """Accept normal management triggers or a structured internal nightly event.

    The nightly event body is model input, not authentication material.  Its
    routing identity is the reserved source chat plus the structured
    job/batch/role assignment emitted by Christopher's launcher.
    """
    if _management_direct_trigger(record):
        return True
    item = _bridge_item(record.raw)
    if bool(item.get("fromMe")) or str(item.get("senderId") or "") != "system@internal":
        return False
    nightly_chats = _priority_selector_chats(config_path) - _management_selector_chats(config_path)
    if record.chat_id not in nightly_chats:
        return False
    metadata = item.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("job_type") != "tgg_nightly_whatsapp":
        return False
    batch_id = str(metadata.get("nightly_batch_id") or "")
    role = str(metadata.get("nightly_role") or "")
    authoritative = metadata.get("authoritative_chat_id")
    assignments: dict[str, tuple[str, str | None]] = {
        "900000000000000001@g.us": ("amk", "120363421424519051@g.us"),
        "900000000000000002@g.us": ("hg", "120363422582425366@g.us"),
        "900000000000000003@g.us": ("pg", "120363423568509280@g.us"),
        "900000000000000004@g.us": ("sk", "120363403845802098@g.us"),
        "900000000000000005@g.us": ("rental", "120363421153247095@g.us"),
        "900000000000000006@g.us": ("backend", "120363404682000990@g.us"),
        "900000000000000007@g.us": ("consolidator", None),
    }
    expected = assignments.get(record.chat_id)
    if (
        expected is None
        or (role, authoritative) != expected
        or not re.fullmatch(r"nightly:\d{4}-\d{2}-\d{2}:[0-9a-f]{12}", batch_id)
    ):
        return False
    return True


def _continuous_interval_trigger(record: InboxRecord, config_path: Path) -> bool:
    """Admit persistence only for a fully validated internal PA-26 trigger."""
    if not _priority_direct_trigger(record, config_path):
        return False
    item = _bridge_item_ref(record.raw)
    metadata = item.get("metadata") if isinstance(item, Mapping) else None
    return bool(
        isinstance(metadata, Mapping)
        and metadata.get("continuous_interval") is True
        and metadata.get("continuous_contract") == "tgg-christopher-continuous-interval/v1"
    )


def _continuous_interval_batch(records: Sequence[InboxRecord], config_path: Path) -> bool:
    return bool(records) and all(_continuous_interval_trigger(record, config_path) for record in records)


def _same_session_steering_allowed(
    active: Sequence[InboxRecord], followups: Sequence[InboxRecord], config_path: Path,
) -> bool:
    if not active or not followups:
        return False
    chat_id = active[0].chat_id
    if chat_id not in _nightly_selector_chats(config_path):
        return True
    return _continuous_interval_batch(active, config_path) == _continuous_interval_batch(
        followups, config_path,
    )


def _parse_captured_send(entry: Mapping[str, Any]) -> dict[str, Any] | None:
    """Extract (chat_id, content, reply_to) from a captured adapter send.

    Only plain text ``send`` calls are deliverable; media/confirm/clarify
    kinds stay capture-only.
    """
    if not isinstance(entry, Mapping) or entry.get("kind") != "send":
        return None
    args = list(entry.get("args") or [])
    kwargs = dict(entry.get("kwargs") or {})
    chat_id = str(kwargs.get("chat_id") or (args[0] if len(args) > 0 else "") or "")
    content = str(kwargs.get("content") or (args[1] if len(args) > 1 else "") or "")
    reply_to = kwargs.get("reply_to") if kwargs.get("reply_to") else (
        args[2] if len(args) > 2 else None
    )
    if not chat_id or not content.strip():
        return None
    return {
        "chat_id": chat_id,
        "content": content,
        "reply_to": str(reply_to) if reply_to else None,
        "workspace_owner": str(entry.get("workspace_owner") or "").strip() or None,
    }


_CAPTURED_IMAGE_MEDIA_RE = re.compile(
    r"MEDIA:\s*(?P<path>(?:(?:file://)?(?:~/|/)\S+|work/\S+))",
    re.IGNORECASE,
)


def _expand_captured_send(send: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Turn streamed ``MEDIA:`` directives into native captured image sends.

    Streaming records the assistant's final chunk as a plain ``send`` before
    the normal adapter post-processing can always emit ``send_multiple_images``.
    Treat the captured body as the same gateway response surface: extract local
    image directives, attach the remaining text as the first image caption, and
    never forward the private filesystem path as chat text.  The downstream
    retained-root and image-signature checks remain the authority boundary.
    """
    content = str(send.get("content") or "")
    matches = list(_CAPTURED_IMAGE_MEDIA_RE.finditer(content))
    if not matches:
        return [dict(send)]
    cleaned = _CAPTURED_IMAGE_MEDIA_RE.sub("", content).strip()
    expanded: list[dict[str, Any]] = []
    for ordinal, match in enumerate(matches):
        expanded.append(
            {
                "send_kind": "media",
                "chat_id": str(send.get("chat_id") or ""),
                "path": match.group("path"),
                "caption": cleaned if ordinal == 0 and cleaned else None,
                # Keep the answer on every expanded item so delivery can send
                # it separately if any attachment fails.  This field is never
                # forwarded to the media bridge payload.
                "response_text": cleaned or None,
                "reply_to": send.get("reply_to"),
                "ordinal": ordinal,
                "workspace_owner": send.get("workspace_owner"),
            }
        )
    return expanded


def _resolve_captured_media_path(
    raw_path: Any,
    retention: Mapping[str, Any] | None,
    *,
    workspace_owner: str | None = None,
) -> Path:
    """Resolve a captured local path or configured opaque media reference.

    Business tools expose retained files using ``media_ref_prefix`` rather
    than leaking the host filesystem root.  Convert only an exact
    ``<configured-prefix>/<single-basename>`` reference.  Absolute local paths
    remain supported, but both forms terminate at the same retained-root and
    regular-file checks in the caller.
    """
    from urllib.parse import unquote, urlsplit

    text = str(raw_path or "").strip()
    if not text or any(marker in text for marker in ("?", "#", "\r", "\n")):
        raise MediaRetentionError("captured media reference is invalid")
    if text.startswith(("work/", "/work/")):
        from tools.python_sandbox_tool import resolve_sandbox_work_path

        try:
            return resolve_sandbox_work_path(text, str(workspace_owner or ""))
        except ValueError as exc:
            raise MediaRetentionError(str(exc)) from exc
    if text.startswith("file://"):
        parsed = urlsplit(text)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise MediaRetentionError("captured media file URI is invalid")
        text = unquote(parsed.path)

    if retention is None:
        raise MediaRetentionError("retained media is not configured")
    ref_prefix = str(retention["ref_prefix"])
    prefix = f"{ref_prefix}/"
    if text.startswith(prefix):
        basename = text[len(prefix):]
        if (
            not basename
            or basename in {".", ".."}
            or "/" in basename
            or "\\" in basename
            or "%" in basename
            or Path(basename).name != basename
        ):
            raise MediaRetentionError("captured media reference is not opaque")
        candidate = Path(retention["root"]) / basename
    else:
        candidate = Path(text).expanduser()

    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise MediaRetentionError("captured media file is unavailable") from exc
    root = Path(retention["root"])
    if not resolved.is_file() or not resolved.is_relative_to(root):
        raise MediaRetentionError("captured media path escapes retained-media root")
    return resolved


def _keep_sent_file_in_workspace(
    path: Path, *, chat_id: str, message_id: str = ""
) -> None:
    """Keep a file Christopher sent in the agent workspace (PA-203 round 2).

    A sandbox file is already there and is only logged; a retained-media or
    other local file is copied in, so a later turn in any conversation can
    open exactly what was sent.  Never fails the delivery.
    """
    try:
        from tools.python_sandbox_tool import record_workspace_file

        record_workspace_file(
            path,
            direction="sent",
            folder="sent",
            details={"chat_id": chat_id, "message_id": message_id},
        )
    except Exception as exc:
        print(
            f"sent file was not kept in the agent workspace: chat={chat_id} error={exc}",
            file=sys.stderr,
        )


def _recent_valid_sandbox_file_paths(
    messages: Sequence[Mapping[str, Any]],
    *,
    workspace_owner: str,
) -> tuple[str, ...]:
    """Return exact paths from the newest sandbox result that has usable files."""
    for message in reversed(messages):
        if not isinstance(message, Mapping) or message.get("role") != "tool":
            continue
        content = message.get("content")
        if not isinstance(content, str):
            continue
        try:
            payload = json.loads(content)
        except (TypeError, ValueError):
            continue
        files = payload.get("files") if isinstance(payload, Mapping) else None
        if not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
            continue
        exact_paths: list[str] = []
        for item in files:
            if not isinstance(item, Mapping):
                continue
            path = str(item.get("path") or "").strip()
            if not path or path in exact_paths:
                continue
            try:
                resolved = _resolve_captured_media_path(
                    path,
                    None,
                    workspace_owner=workspace_owner,
                )
                _validated_captured_media_type(resolved)
            except (OSError, MediaRetentionError):
                continue
            exact_paths.append(path)
        if exact_paths:
            return tuple(exact_paths)
    return ()


def validate_management_attachment_response(
    response_text: str,
    messages: Sequence[Mapping[str, Any]],
    *,
    retention: Mapping[str, Any] | None,
    workspace_owner: str,
) -> str | None:
    """Validate proposed Management attachments before replay captures a send.

    A returned string is a runtime-generated correction observation for the
    model, not a client message.  ``None`` accepts the response unchanged.
    """
    proposed = str(response_text or "")
    raw_directives = re.findall(r"(?im)\bMEDIA:\s*([^\s]+)", proposed)
    if not raw_directives:
        return None

    expanded = _expand_captured_send(
        {
            "send_kind": "text",
            "chat_id": "validation-only",
            "content": proposed,
            "reply_to": None,
            "workspace_owner": workspace_owner,
        }
    )
    media_sends = [
        item for item in expanded if item.get("send_kind") == "media"
    ]
    refusal: str | None = None
    if len(media_sends) != len(raw_directives):
        refusal = "the MEDIA directive is not a supported attachment reference"
    else:
        for send in media_sends:
            try:
                path = _resolve_captured_media_path(
                    send.get("path"),
                    retention,
                    workspace_owner=workspace_owner,
                )
                _validated_captured_media_type(path)
            except (OSError, MediaRetentionError) as exc:
                refusal = str(exc)
                break
    if refusal is None:
        return None

    exact_paths = _recent_valid_sandbox_file_paths(
        messages,
        workspace_owner=workspace_owner,
    )
    if exact_paths:
        rendered = ", ".join(f"`{path}`" for path in exact_paths)
        exact_hint = (
            "The newest usable sandbox result returned these exact "
            f"files[].path values: {rendered}. Copy the intended value unchanged. "
        )
    else:
        exact_hint = (
            "Use an exact files[].path value returned by the current sandbox "
            "result. If no such value is available, explain that the attachment "
            "could not be sent. "
        )
    return (
        "[Runtime attachment validation — not a client message] The proposed "
        f"attachment was rejected before sending because {refusal}. "
        f"{exact_hint}Do not construct or infer a /media/... reference. "
        "Produce the corrected final response now."
    )


def build_management_attachment_response_validator(
    *,
    config_path: Path,
    workspace_owner: str,
):
    """Build the exact pre-send validator used by the Management response loop."""
    retention = _retention_config(config_path)

    def _validate(
        response_text: str,
        messages: Sequence[Mapping[str, Any]],
    ) -> str | None:
        return validate_management_attachment_response(
            response_text,
            messages,
            retention=retention,
            workspace_owner=workspace_owner,
        )

    return _validate


def _captured_provider_error(
    captured_outbound: Sequence[Mapping[str, Any]],
) -> str | None:
    """Return a provider/model error notice captured instead of a PA turn."""
    markers = (
        "authenticationerror",
        "authorizationerror",
        "missing authentication header",
        "http 401",
        "http 403",
        "provider authentication",
        "provider-auth",
        "provider error",
        "model error",
        "model resolution",
        "unable to resolve model",
        "model not found",
        "no provider configured",
    )
    for entry in captured_outbound:
        parsed = _parse_captured_send(entry)
        if parsed is None:
            continue
        body = str(parsed["content"]).strip()
        lowered = body.lower()
        if any(marker in lowered for marker in markers):
            return body
    return None


def _captured_audit_entries(
    captured_outbound: Sequence[Mapping[str, Any]],
    *,
    batch: Sequence[InboxRecord],
    handled: Sequence[Mapping[str, Any]],
    start_index: int,
) -> list[dict[str, Any]]:
    """Normalize captured bodies while retaining the adapter's raw metadata."""
    batch_message_ids = [record.message_id for record in batch]
    batch_chat_ids = sorted({record.chat_id for record in batch})
    turn_ids = sorted(
        {
            str(group.get("turn_id"))
            for group in handled
            if group.get("turn_id")
        }
    )
    normalized: list[dict[str, Any]] = []
    for offset, entry in enumerate(captured_outbound):
        parsed = _parse_captured_send(entry)
        raw = dict(entry)
        normalized.append(
            {
                "capture_index": start_index + offset,
                "kind": str(entry.get("kind") or ""),
                "chat_id": (
                    str(parsed["chat_id"])
                    if parsed is not None
                    else str(entry.get("chat_id") or "") or None
                ),
                "batch_chat_ids": batch_chat_ids,
                "message_ids": batch_message_ids,
                "turn_ids": turn_ids,
                "reply_to": parsed.get("reply_to") if parsed is not None else None,
                "body": parsed.get("content") if parsed is not None else None,
                "raw": raw,
            }
        )
    return normalized


def _parse_captured_media(entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Expand captured native media calls into one bounded item per file."""
    if not isinstance(entry, Mapping):
        return []
    kind = str(entry.get("kind") or "")
    if kind not in {"send_image_file", "send_multiple_images", "send_document"}:
        return []
    args = list(entry.get("args") or [])
    kwargs = dict(entry.get("kwargs") or {})
    chat_id = str(kwargs.get("chat_id") or (args[0] if args else "") or "")
    reply_to = kwargs.get("reply_to")
    if reply_to is None:
        if kind == "send_image_file" and len(args) > 3:
            reply_to = args[3]
        elif kind == "send_document" and len(args) > 4:
            reply_to = args[4]
    if not chat_id:
        return []
    if kind == "send_document":
        path = kwargs.get("file_path") or (args[1] if len(args) > 1 else None)
        caption = kwargs.get("caption") or (args[2] if len(args) > 2 else None)
        values = [(path, caption)] if path else []
    elif kind == "send_image_file":
        path = kwargs.get("image_path") or (args[1] if len(args) > 1 else None)
        caption = kwargs.get("caption") or (args[2] if len(args) > 2 else None)
        values: Sequence[Any] = [(path, caption)] if path else []
    else:
        values = kwargs.get("images") or (args[1] if len(args) > 1 else [])
        if isinstance(values, (str, bytes, Mapping)):
            values = [values]
    parsed: list[dict[str, Any]] = []
    for ordinal, value in enumerate(values if isinstance(values, Sequence) else []):
        if isinstance(value, Mapping):
            path = value.get("image_path") or value.get("path") or value.get("url")
            caption = value.get("caption") or value.get("alt_text")
        elif isinstance(value, (list, tuple)):
            path = value[0] if value else None
            caption = value[1] if len(value) > 1 else None
        else:
            path, caption = value, None
        text = str(path or "")
        if text.startswith("file://"):
            from urllib.parse import unquote
            text = unquote(text[7:])
        if text:
            parsed.append({
                "send_kind": "media",
                "chat_id": chat_id,
                "path": text,
                "caption": str(caption) if caption else None,
                "reply_to": str(reply_to) if reply_to else None,
                "ordinal": ordinal,
                "workspace_owner": str(entry.get("workspace_owner") or "").strip() or None,
            })
    return parsed


def _timestamp_epoch_seconds(value: Any) -> float | None:
    """Normalize bridge/gate timestamp shapes to epoch seconds."""
    if isinstance(value, Mapping):
        value = value.get("low") or value.get("value") or value.get("seconds")
    if value is None or value == "":
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    # WhatsApp exports may use milliseconds while the replay corpus commonly
    # uses seconds.  Normalize before comparing with the ISO activation gate.
    if numeric >= 100_000_000_000:
        numeric /= 1000
    return numeric


def _fresh_management_chats(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    gate_changed_at: str,
) -> frozenset[str]:
    """Return post-activation management chats in this exact processing pick."""
    gate_epoch = _timestamp_epoch_seconds(gate_changed_at)
    if gate_epoch is None:
        raise ConsumerError("processing gate changed_at is not a valid timestamp")
    management_chats = _management_selector_chats(config_path)
    fresh: set[str] = set()
    for record in records:
        if record.chat_id not in management_chats:
            continue
        try:
            item = _bridge_item(record.raw)
        except ConsumerError:
            continue
        timestamp = _timestamp_epoch_seconds(item.get("timestamp"))
        if timestamp is not None and timestamp >= gate_epoch:
            fresh.add(record.chat_id)
    return frozenset(fresh)


def _post_typing_presence(chat_id: str, presence: str) -> bool:
    """Best-effort presence update through the same local bridge."""
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    bridge_url = os.environ.get("TGG_REPLY_BRIDGE_URL", "http://127.0.0.1:3011").rstrip("/")
    request = Request(
        f"{bridge_url}/typing",
        data=json.dumps({"chatId": chat_id, "presence": presence}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            status_code = int(getattr(response, "status", 0) or 0)
            payload = json.loads(response.read() or b"{}")
        if status_code == 200 and payload.get("success") is True:
            return True
        print(
            "typing presence outcome NOT confirmed: "
            f"chat={chat_id} presence={presence} http={status_code} "
            f"payload={json.dumps(payload)[:200]}",
            file=sys.stderr,
        )
    except HTTPError as exc:
        print(
            "typing presence REFUSED/FAILED at bridge: "
            f"chat={chat_id} presence={presence} http={exc.code}",
            file=sys.stderr,
        )
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        print(
            f"typing presence FAILED: chat={chat_id} presence={presence} error={exc}",
            file=sys.stderr,
        )
    return False


@contextlib.asynccontextmanager
async def _management_typing_presence(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    gate_changed_at: str,
    reassert_seconds: float = 6,
):
    """Keep fresh management chats composing until processing+delivery ends."""
    chats = _fresh_management_chats(
        records, config_path=config_path, gate_changed_at=gate_changed_at
    )
    if not chats:
        yield
        return
    for chat_id in chats:
        await asyncio.to_thread(_post_typing_presence, chat_id, "composing")
    stop = asyncio.Event()

    async def reassert() -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=reassert_seconds)
                return
            except asyncio.TimeoutError:
                for chat_id in chats:
                    await asyncio.to_thread(
                        _post_typing_presence, chat_id, "composing"
                    )

    task = asyncio.create_task(reassert())
    try:
        yield
    finally:
        stop.set()
        await task
        for chat_id in chats:
            await asyncio.to_thread(_post_typing_presence, chat_id, "paused")


def deliver_management_replies(
    inbox: DurableInbox,
    *,
    config_path: Path,
    captured_outbound: Sequence[Mapping[str, Any]],
    batch_records: Sequence[InboxRecord],
    gate_changed_at: str,
    handled_groups: Sequence[Mapping[str, Any]],
    continuation: Mapping[str, Any] | None = None,
    completion: Mapping[str, Any] | None = None,
) -> dict[str, int]:
    """Deliver mgmt-selector responses through the rung-gated bridge.

    Contract (teren 2026-07-21 12:00 ruling):
    - delivery keys on the inbound chat's selector class: management only;
      site/ingest responses stay capture-only forever.
    - only anchors cited by completed turns from THIS processing result and
      whose ingress timestamp is at/after the current activation gate may
      deliver; backlog-context output remains captured but suppressed.
    - every send goes through the bridge POST /send — the 4-layer outbound
      stack (policy, authority, lease, guarded transport) is THE enforcement;
      no allowlist logic is duplicated here and a bridge refusal is final.
    - at-most-once per turn response: a durable claim precedes the send; a
      failure logs loudly and records undelivered — never retries.
    """
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    if continuation is not None:
        continuation_keys = {"list_id", "original_message_id", "internal_message_id"}
        if set(continuation) != continuation_keys or not re.fullmatch(
            r"case-list-[0-9]{14}-[a-f0-9]{10}", str(continuation.get("list_id") or "")
        ) or not continuation.get("original_message_id"):
            raise ConsumerError("management continuation delivery identity invalid")
        if "internal_message_id" in continuation and not re.fullmatch(
            r"management-continuation:[A-Za-z0-9-]{1,128}",
            str(continuation.get("internal_message_id") or ""),
        ):
            raise ConsumerError("management continuation internal turn identity invalid")
    if completion is not None:
        # PA-203 round 2: a background-completion turn replies to the original
        # request under its own delivery key, so the original turn's earlier
        # reply to the same message cannot suppress it.  Files may be sent.
        if set(completion) != {"completion_id", "original_message_id", "internal_message_id"} or not (
            completion.get("completion_id") and completion.get("original_message_id")
        ) or str(completion.get("internal_message_id") or "") != (
            f"background-completion:{completion['completion_id']}"
        ):
            raise ConsumerError("background completion delivery identity invalid")
    anchored = continuation if continuation is not None else completion

    summary = {"delivered": 0, "undelivered": 0, "suppressed": 0, "duplicate": 0}
    sends: list[dict[str, Any]] = []
    for entry in captured_outbound:
        parsed = _parse_captured_send(entry)
        if parsed is not None:
            sends.extend(_expand_captured_send(parsed))
    for entry in captured_outbound:
        sends.extend(_parse_captured_media(entry))
    if not sends:
        return summary
    gate_epoch = _timestamp_epoch_seconds(gate_changed_at)
    if gate_epoch is None:
        raise ConsumerError("processing gate changed_at is not a valid timestamp")
    management_chats = _management_selector_chats(config_path)
    bridge_url = os.environ.get("TGG_REPLY_BRIDGE_URL", "http://127.0.0.1:3011").rstrip("/")
    newest_message_by_chat: dict[str, str] = {}
    records_by_id: dict[str, InboxRecord] = {}
    for record in batch_records:
        newest_message_by_chat[record.chat_id] = record.message_id
        records_by_id[record.message_id] = record
    handled_message_ids = {
        str(message_id)
        for group in handled_groups
        for message_id in (group.get("message_ids") or [])
        if message_id
    }
    media_failures: dict[tuple[str, str], dict[str, Any]] = {}
    media_caption_delivered: set[tuple[str, str]] = set()

    def note_media_failure(
        send: Mapping[str, Any], chat_id: str, anchor: str, anchor_item: Mapping[str, Any]
    ) -> None:
        key = (chat_id, anchor)
        entry = media_failures.setdefault(
            key,
            {
                "response_text": str(send.get("response_text") or "").strip(),
                "anchor_item": dict(anchor_item),
                "failure_note": str(send.get("failure_note") or "").strip(),
            },
        )
        if not entry.get("response_text") and send.get("response_text"):
            entry["response_text"] = str(send["response_text"]).strip()

    for send in sends:
        chat_id = send["chat_id"]
        if continuation is not None and send.get("send_kind", "text") != "text":
            summary["suppressed"] += 1
            continue
        if chat_id not in management_chats:
            summary["suppressed"] += 1
            continue
        anchor = send["reply_to"] or newest_message_by_chat.get(chat_id)
        if anchored is not None:
            anchor = str(anchored["original_message_id"])
        if anchor and "+" in str(anchor) and anchor not in handled_message_ids:
            # A multi-message WhatsApp turn bundle carries a synthetic
            # composite id ("id1+id2+..."; platforms/whatsapp.py join). The
            # components were handled individually, so the composite can
            # never match the allow-set and the reply was silently
            # suppressed (2026-08-04 incident: every nrefs>=2 turn
            # composed-without-send). Resolve to the newest component that
            # was actually handled in this batch.
            for component in reversed(str(anchor).split("+")):
                if component in handled_message_ids and component in records_by_id:
                    anchor = component
                    break
        anchor_record = records_by_id.get(str(anchor)) if anchor else None
        try:
            anchor_item = _bridge_item(anchor_record.raw) if anchor_record else None
        except ConsumerError:
            anchor_item = None
        anchor_epoch = _timestamp_epoch_seconds(
            anchor_item.get("timestamp") if anchor_item else None
        )
        handled_anchor = (
            str(anchored["internal_message_id"])
            if anchored is not None and anchored.get("internal_message_id")
            else anchor
        )
        if (
            not anchor
            or handled_anchor not in handled_message_ids
            or anchor_record is None
            or anchor_record.chat_id != chat_id
            or anchor_epoch is None
            or (anchored is None and anchor_epoch < gate_epoch)
        ):
            summary["suppressed"] += 1
            continue
        # Cross-source contract: WhatsApp-native inbound identity only.
        # Export/backfill and live capture may render body/quote/media text
        # differently; content hashing would silently turn those rendering
        # differences into distinct sends.  Once a reply is claimed for an
        # inbound WA message, no second rendering/model response may send it
        # again.  ``anchor`` is the source-native messageId, not replay-1.
        if send.get("send_kind", "text") == "media":
            retention = _retention_config(config_path)
            try:
                media_path = _resolve_captured_media_path(
                    send["path"],
                    retention,
                    workspace_owner=send.get("workspace_owner"),
                )
                media_type, media_mime, media_file_name = (
                    _validated_captured_media_type(media_path)
                )
                media_identity = hashlib.sha256(media_path.read_bytes()).hexdigest()
            except (OSError, MediaRetentionError) as exc:
                requested_reference = str(send.get("path") or "")
                if not requested_reference.startswith(("work/", "/work/")):
                    summary["suppressed"] += 1
                    continue
                refusal_identity = json.dumps(
                    {
                        "chat_id": chat_id,
                        "anchor": str(anchor),
                        "ordinal": int(send.get("ordinal") or 0),
                        "requested_reference": requested_reference,
                        "workspace_owner": str(send.get("workspace_owner") or ""),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                refusal_digest = hashlib.sha256(refusal_identity.encode()).hexdigest()
                delivery_key = f"media-refused::{chat_id}::{anchor}::{refusal_digest}"
                if not inbox.claim_reply_delivery(
                    delivery_key,
                    chat_id=chat_id,
                    reply_to_message_id=anchor,
                ):
                    summary["duplicate"] += 1
                    continue
                inbox.record_reply_delivery(
                    delivery_key,
                    status="undelivered",
                    error=f"sandbox attachment refused: {exc}"[:300],
                )
                summary["undelivered"] += 1
                failed_send = dict(send)
                failed_send["failure_note"] = (
                    "I couldn't send the attachment. Use the exact files[].path "
                    "returned by the sandbox, or an existing /media/... reference."
                )
                note_media_failure(failed_send, chat_id, str(anchor), anchor_item)
                continue
            delivery_key = (
                f"media::{chat_id}::{anchor}::{media_identity}::{send['ordinal']}"
            )
        else:
            delivery_key = f"{chat_id}::{anchor or 'no-anchor'}"
        if continuation is not None:
            delivery_key = f"continuation::{continuation['list_id']}::{delivery_key}"
        elif completion is not None:
            delivery_key = f"completion::{completion['completion_id']}::{delivery_key}"
        if not inbox.claim_reply_delivery(
            delivery_key, chat_id=chat_id, reply_to_message_id=anchor,
            **({"correlation": dict(anchored)} if anchored is not None else {}),
        ):
            summary["duplicate"] += 1
            continue
        # The bridge renders the visible quote from these fields; a bare
        # messageId makes it embed a placeholder body with no participant,
        # which phones display as a phantom "You / [message]" quote and
        # WhatsApp Web may refuse to render at all (2026-08-06 incident).
        anchor_body = str(anchor_item.get("body") or "").strip()
        if not anchor_body:
            anchor_media = str(anchor_item.get("mediaType") or "").strip()
            anchor_body = f"[{anchor_media}]" if anchor_media else ""
        reply_to_payload: dict[str, Any] = {"messageId": anchor}
        if anchor_item.get("senderId"):
            reply_to_payload["participant"] = str(anchor_item["senderId"])
        if anchor_body:
            reply_to_payload["body"] = anchor_body[:1024]
        if anchor_item.get("fromMe"):
            reply_to_payload["fromMe"] = True
        body_payload: dict[str, Any] = {
                "chatId": chat_id,
                "replyTo": reply_to_payload,
        }
        endpoint = "send"
        if send.get("send_kind", "text") == "media":
            endpoint = "send-media"
            body_payload.update({
                "filePath": str(media_path),
                "mediaType": media_type,
            })
            if media_file_name:
                body_payload["fileName"] = media_file_name
            if send.get("caption"):
                body_payload["caption"] = send["caption"]
            # The bridge's media route currently consumes a scalar native id.
            body_payload["replyTo"] = anchor
        else:
            body_payload["message"] = send["content"]
        body = json.dumps(body_payload).encode()
        request = Request(
            f"{bridge_url}/{endpoint}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                status_code = int(getattr(response, "status", 0) or 0)
                payload = json.loads(response.read() or b"{}")
            # Strict success only (codex round): the bridge signals
            # indeterminate sends as HTTP 202 {"outcome":"unknown",
            # "retrySafe":false} — urllib does not raise for 202, and an
            # unknown outcome must NEVER be recorded delivered (nor retried:
            # the claim stays consumed).
            if status_code == 200 and payload.get("success") is True:
                inbox.record_reply_delivery(
                    delivery_key,
                    status="delivered",
                    bridge_message_id=str(payload.get("messageId") or ""),
                    provider_outcome=str(payload.get("outcome") or "delivered"),
                )
                summary["delivered"] += 1
                if send.get("send_kind", "text") == "media":
                    # Off the delivery loop: keeping the file waits for the
                    # workspace lock, which a sandbox run holds while it runs.
                    threading.Thread(
                        target=_keep_sent_file_in_workspace,
                        args=(media_path,),
                        kwargs={
                            "chat_id": chat_id,
                            "message_id": str(payload.get("messageId") or ""),
                        },
                        name="pa203-keep-sent-file",
                    ).start()
                if send.get("send_kind", "text") == "media" and send.get("caption"):
                    media_caption_delivered.add((chat_id, str(anchor)))
            else:
                print(
                    "reply delivery outcome NOT confirmed: "
                    f"chat={chat_id} anchor={anchor} http={status_code} "
                    f"payload={json.dumps(payload)[:200]}",
                    file=sys.stderr,
                )
                inbox.record_reply_delivery(
                    delivery_key,
                    status="undelivered",
                    provider_outcome=str(payload.get("outcome") or "unconfirmed"),
                    error=f"http-{status_code}-unconfirmed: {json.dumps(payload)[:200]}",
                )
                summary["undelivered"] += 1
                if (
                    send.get("send_kind", "text") == "media"
                    and status_code >= 400
                ):
                    note_media_failure(send, chat_id, str(anchor), anchor_item)
        except HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode(errors="replace")[:300]
            except Exception:
                pass
            print(
                "reply delivery REFUSED/FAILED at bridge: "
                f"chat={chat_id} anchor={anchor} http={exc.code} detail={detail}",
                file=sys.stderr,
            )
            inbox.record_reply_delivery(
                delivery_key,
                status="undelivered",
                error=f"http-{exc.code}: {detail}",
            )
            summary["undelivered"] += 1
            if send.get("send_kind", "text") == "media":
                note_media_failure(send, chat_id, str(anchor), anchor_item)
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            print(
                f"reply delivery FAILED (transport): chat={chat_id} anchor={anchor} error={exc}",
                file=sys.stderr,
            )
            inbox.record_reply_delivery(
                delivery_key, status="undelivered", error=str(exc)[:300]
            )
            summary["undelivered"] += 1

    # A media response previously carried the prose only as the first image's
    # caption.  If that image was unreadable or the bridge rejected any image,
    # the operator could receive no answer at all.  Send one separately claimed
    # text fallback per response.  The original media claim remains consumed;
    # neither the attachment nor fallback is retried after an uncertain outcome.
    for (chat_id, anchor), failure in media_failures.items():
        fallback_key = f"media-fallback::{chat_id}::{anchor}"
        if not inbox.claim_reply_delivery(
            fallback_key, chat_id=chat_id, reply_to_message_id=anchor
        ):
            summary["duplicate"] += 1
            continue
        answer = str(failure.get("response_text") or "").strip()
        note = str(failure.get("failure_note") or "").strip() or (
            "I couldn't send one or more of the selected images."
        )
        if (chat_id, anchor) in media_caption_delivered:
            message = note
        else:
            message = f"{answer}\n\n{note}" if answer else note
        item = failure.get("anchor_item") or {}
        anchor_body = str(item.get("body") or "").strip()
        if not anchor_body:
            anchor_media = str(item.get("mediaType") or "").strip()
            anchor_body = f"[{anchor_media}]" if anchor_media else ""
        reply_to_payload: dict[str, Any] = {"messageId": anchor}
        if item.get("senderId"):
            reply_to_payload["participant"] = str(item["senderId"])
        if anchor_body:
            reply_to_payload["body"] = anchor_body[:1024]
        if item.get("fromMe"):
            reply_to_payload["fromMe"] = True
        request = Request(
            f"{bridge_url}/send",
            data=json.dumps({
                "chatId": chat_id,
                "replyTo": reply_to_payload,
                "message": message,
            }).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                status_code = int(getattr(response, "status", 0) or 0)
                payload = json.loads(response.read() or b"{}")
            if status_code == 200 and payload.get("success") is True:
                inbox.record_reply_delivery(
                    fallback_key,
                    status="delivered",
                    bridge_message_id=str(payload.get("messageId") or ""),
                    provider_outcome=str(payload.get("outcome") or "delivered"),
                )
                summary["delivered"] += 1
            else:
                inbox.record_reply_delivery(
                    fallback_key,
                    status="undelivered",
                    provider_outcome=str(payload.get("outcome") or "unconfirmed"),
                    error=f"http-{status_code}-unconfirmed: {json.dumps(payload)[:200]}",
                )
                summary["undelivered"] += 1
        except HTTPError as exc:
            inbox.record_reply_delivery(
                fallback_key, status="undelivered", error=f"http-{exc.code}"
            )
            summary["undelivered"] += 1
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            inbox.record_reply_delivery(
                fallback_key, status="undelivered", error=str(exc)[:300]
            )
            summary["undelivered"] += 1
    return summary


def _management_document_event_config(config_path: Path) -> ManagementDocumentEventConfig | None:
    """Load the explicit opt-in document-outbox transport declaration.

    Runtime deployment owns these values because the destination is operational
    authority, not something Christopher may choose from an event body.  Keep
    the feature entirely dormant until all required values are present.
    """
    api_url = os.environ.get("TGG_MANAGEMENT_DOCUMENT_API_URL", "").strip().rstrip("/")
    chat_id = os.environ.get("TGG_MANAGEMENT_DOCUMENT_CHAT_ID", "").strip()
    token_env = os.environ.get(
        "TGG_MANAGEMENT_DOCUMENT_TOKEN_ENV", "CHRISTOPHER_TGG_PS_SERVICE_TOKEN"
    ).strip()
    if not api_url and not chat_id:
        return None
    if not api_url or not chat_id or not token_env:
        raise ConsumerError(
            "management document event transport requires API URL, chat ID and token env"
        )
    if not api_url.startswith(("http://", "https://")):
        raise ConsumerError("management document event API URL must be HTTP(S)")
    if chat_id not in _management_selector_chats(config_path):
        raise ConsumerError(
            "management document event destination is not a WhatsApp management selector"
        )
    if not os.environ.get(token_env, "").strip():
        raise ConsumerError("management document event service token is absent")
    return ManagementDocumentEventConfig(
        api_url=api_url, chat_id=chat_id, token_env=token_env,
    )


def _management_document_entries(
    config: ManagementDocumentEventConfig, cursor: tuple[int, str] | None, *, limit: int = 100
) -> list[dict[str, Any]]:
    """Read PA-73's typed, exclusive document-outbox page.

    This is deliberately a Systems operator read, never a capture read.  The
    resulting values are used only to make a transient internal turn prompt;
    they are not inserted into ``ingress_events`` or source-evidence tables.
    """
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    query: dict[str, str] = {"limit": str(max(1, min(500, int(limit))))}
    if cursor is not None:
        query.update({"after_created_at": str(cursor[0]), "after_id": cursor[1]})
    request = Request(
        f"{config.api_url}/api/operator/human-resolution-document-entries?{urlencode(query)}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {os.environ[config.token_env]}",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=30) as response:
            status_code = int(getattr(response, "status", 0) or 0)
            payload = json.loads(response.read() or b"{}")
    except HTTPError as exc:
        raise ConsumerError(f"management document event poll HTTP {exc.code}") from exc
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        raise ConsumerError(f"management document event poll failed: {exc}") from exc
    if status_code != 200 or not isinstance(payload, Mapping):
        raise ConsumerError("management document event poll returned an invalid envelope")
    data = payload.get("data", payload)
    if not isinstance(data, Mapping) or data.get("contract") != "tgg-human-resolution-document-entry/v1":
        raise ConsumerError("management document event poll returned the wrong contract")
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ConsumerError("management document event poll entries are invalid")
    result: list[dict[str, Any]] = []
    previous = cursor
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise ConsumerError("management document event entry is not an object")
        try:
            created_at = int(raw["createdAt"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ConsumerError("management document event entry has invalid createdAt") from exc
        entry_id = str(raw.get("id") or "").strip()
        record_id = str(raw.get("recordId") or "").strip()
        kind = str(raw.get("entryKind") or "").strip()
        if created_at < 0 or not entry_id or not record_id or not kind:
            raise ConsumerError("management document event entry identity is incomplete")
        if not isinstance(raw.get("body"), Mapping) or not isinstance(raw.get("effects"), list):
            raise ConsumerError("management document event entry has no decision-ready brief")
        identity = (created_at, entry_id)
        if previous is not None and identity <= previous:
            raise ConsumerError("management document event page is not exclusive ascending")
        previous = identity
        result.append(dict(raw))
    return result


def _internal_management_document_message(
    entry: Mapping[str, Any], *, chat_id: str, canary_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create transient decision-ready input without WhatsApp provenance.

    This is deliberately a new turn in the persistent management conversation,
    but it receives the producing turn's body/effects directly.  It should use
    that brief and the chat's existing context to phrase the question, rather
    than re-reading a document or repeating source investigation just to
    recreate work that has already been completed.
    """
    entry_id = str(entry["id"])
    record_id = str(entry["recordId"])
    created_at = int(entry["createdAt"])
    entry_kind = str(entry["entryKind"])
    instruction = (
        "A new Christopher human-resolution document needs a management notice. "
        if entry_kind == "initial_default" else
        "A Christopher human-resolution document has a new lifecycle entry and needs a management update. "
    )
    metadata: dict[str, Any] = {
        "contract": "tgg_management_document_event/v1",
        "entry_id": entry_id,
        "record_id": record_id,
        "entry_kind": str(entry["entryKind"]),
        "document_url": f"/tgg/human-resolutions/{record_id}",
        "decision_ready_brief": {
            "document_id": record_id,
            "entry_id": entry_id,
            "entry_kind": entry_kind,
            "body": dict(entry.get("body") or {}),
            "effects": list(entry.get("effects") or []),
        },
    }
    if canary_projection is not None:
        metadata["pa75_canary_projection"] = dict(canary_projection)
        visible_projection = {
            "entry": dict(canary_projection.get("entry") or {}),
            "source_record_projection_sha256": canary_projection.get("source_record_projection_sha256"),
            "event_sha256": canary_projection.get("event_sha256"),
        }
        instruction += (
            " This is the approved PA-75 typed-canary projection; its authored entry, "
            "cited evidence and source-record hash are included below. Use that immutable "
            "projection together with fresh case reads; do not claim it is a live Systems entry."
            "\n\nTyped-canary projection:\n"
            + json.dumps(visible_projection, ensure_ascii=False, sort_keys=True)
        )
    return {
        "messageId": f"human-resolution-document-entry:{entry_id}",
        "chatId": chat_id,
        # This is an internal typed event, but it must enter the real WhatsApp
        # adapter as the configured management *group*.  Omitting this made
        # the adapter treat system@internal as a direct-message sender and
        # reject it under the DM allowlist before the management selector ran.
        "isGroup": True,
        "chatName": "TGG Management",
        "senderId": "system@internal",
        "timestamp": created_at,
        "body": (
            instruction +
            "The producing Christopher turn's decision-ready brief is below. "
            "Use it together with the existing management-chat conversation to write one natural, "
            "first-person message for management. Do not retrieve the document or investigate source "
            "history merely to recreate this brief; use document tools only when a genuinely new or missing "
            "fact is needed. Do not treat this internal event as WhatsApp evidence and do not make a case change."
            "\n\nDecision-ready brief:\n"
            + json.dumps(metadata["decision_ready_brief"], ensure_ascii=False, sort_keys=True)
        ),
        "metadata": metadata,
    }


def _normalize_management_document_captured_outbound(
    entry: Mapping[str, Any],
    captured_outbound: Sequence[Mapping[str, Any]],
    *,
    fresh_initial_notice: bool | None = None,
) -> list[dict[str, Any]]:
    """Remove the synthetic quote only from a destination's first notice.

    A destination may first encounter a later lifecycle entry after another
    destination already received the original initial entry.  That first
    destination-local notice must be fresh and unquoted, then becomes the
    anchor for later close-outs in that destination.
    """
    if fresh_initial_notice is None:
        # Compatibility for direct callers: real drain callers pass the
        # destination-specific fact explicitly.
        fresh_initial_notice = str(entry.get("entryKind") or "") == "initial_default"
    if not fresh_initial_notice:
        return [dict(item) for item in captured_outbound]
    synthetic_anchor = f"human-resolution-document-entry:{str(entry['id'])}"
    normalized: list[dict[str, Any]] = []
    for raw in captured_outbound:
        item = dict(raw)
        kwargs = dict(item.get("kwargs") or {})
        args = list(item.get("args") or [])
        if kwargs.get("reply_to") == synthetic_anchor:
            kwargs.pop("reply_to")
            item["kwargs"] = kwargs
        elif len(args) > 2 and args[2] == synthetic_anchor:
            item["args"] = args[:2] + args[3:]
        normalized.append(item)
    return normalized


async def _run_management_document_turn(
    entry: Mapping[str, Any],
    *,
    config_path: Path,
    destination_chat_id: str,
    runner: Any,
    canary_projection: Mapping[str, Any] | None = None,
    fresh_initial_notice: bool = True,
) -> list[dict[str, Any]]:
    """Run the existing management selector in its persistent namespace.

    The sole input is an in-memory typed event.  It never reaches capture,
    source projection or the business compiler as a WhatsApp message.
    """
    from gateway.replay import ReplayPlan

    provider, model = configured_engine(config_path)
    result = await runner.replay(
        ReplayPlan(
            platform="whatsapp",
            messages=(_internal_management_document_message(
                entry, chat_id=destination_chat_id, canary_projection=canary_projection,
            ),),
            run_id=f"management-document-{uuid.uuid4().hex[:12]}",
            attempt_id=f"attempt-{uuid.uuid4().hex[:12]}",
            delivery_mode="capture",
            bypass_require_mention=True,
            bypass_auth=True,
            live_business_writes=False,
            source_path="tgg-management-document-event",
            replay_namespace=f"agent:live-drain:persistent-chat:{provider}:{model}",
        )
    )
    captured = [dict(item) for item in result.outbound if isinstance(item, Mapping)]
    return _normalize_management_document_captured_outbound(
        entry, captured, fresh_initial_notice=fresh_initial_notice,
    )


def _deliver_management_document_notice(
    inbox: DurableInbox,
    *,
    config: ManagementDocumentEventConfig,
    entry: Mapping[str, Any],
    captured_outbound: Sequence[Mapping[str, Any]],
    canary_projection: Mapping[str, Any] | None = None,
) -> str:
    """Make one durable, proactive attempt for a document lifecycle entry.

    WhatsApp has no transaction/idempotency key.  The local claim is therefore
    intentionally at-most-once: a delivered, refused, failed or unknown bridge
    outcome is terminal and the cursor may advance.  A prior claim after a
    process crash is never sent again.
    """
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen

    entry_id = str(entry["id"])
    document_id = str(entry["recordId"])
    entry_kind = str(entry["entryKind"])
    key = _management_document_delivery_key(entry_id=entry_id, chat_id=config.chat_id)
    previous = inbox.management_document_delivery_status(
        entry_id=entry_id, chat_id=config.chat_id,
    )
    if previous is not None:
        return previous
    correlation = {
        "document_id": document_id,
        "entry_id": entry_id,
        "entry_kind": entry_kind,
    }
    if canary_projection is not None:
        correlation["pa75_canary_projection"] = dict(canary_projection)
    initial_notice = inbox.initial_management_document_notice(
        chat_id=config.chat_id, document_id=document_id,
    )
    # An old test-chat notice cannot anchor production.  The first notice in a
    # new destination is deliberately fresh even when its lifecycle entry is
    # an amendment/closure, then becomes that destination's quote anchor.
    fresh_initial_notice = initial_notice is None
    correlation["notice_role"] = "initial_notice" if fresh_initial_notice else "lifecycle_notice"
    sends = [
        parsed for raw in captured_outbound
        if (parsed := _parse_captured_send(raw)) is not None
        and parsed["chat_id"] == config.chat_id
    ]
    expected_reply_to = initial_notice["message_id"] if initial_notice else None
    # Christopher is replying to the transient lifecycle event presented to
    # the ordinary WhatsApp adapter, so the captured transport anchor must be
    # that exact event.  The real provider message ID is deliberately not
    # model-visible; the bridge attaches the already-delivered initial notice
    # below after this current-event anchor has been validated.
    captured_reply_to = (
        None if fresh_initial_notice else f"human-resolution-document-entry:{entry_id}"
    )
    if (
        len(sends) != 1
        or sends[0].get("reply_to") != captured_reply_to
    ):
        raise ConsumerError(
            "management document turn emitted an invalid lifecycle notice anchor"
        )
    # The authored initial text is known before any bridge I/O. Store it in
    # the same durable pre-send claim as the idempotency key so a crash after a
    # confirmed provider send cannot leave future lifecycle entries unable to
    # build their honest quote context.
    if fresh_initial_notice:
        correlation["notice_body"] = sends[0]["content"]
    if not inbox.claim_reply_delivery(
        key,
        chat_id=config.chat_id,
        reply_to_message_id=expected_reply_to,
        correlation=correlation,
    ):
        return inbox.reply_delivery_status(key) or "undelivered"
    payload: dict[str, Any] = {"chatId": config.chat_id, "message": sends[0]["content"]}
    if initial_notice is not None:
        # Quote only the actual previously delivered Christopher notice. The
        # lifecycle event is internal and is never rewritten as an inbound
        # WhatsApp message merely to satisfy the bridge's quote shape.
        payload["replyTo"] = {
            "messageId": initial_notice["message_id"],
            "body": initial_notice["body"][:1024],
            "fromMe": True,
        }
    request = Request(
        f"{os.environ.get('TGG_REPLY_BRIDGE_URL', 'http://127.0.0.1:3011').rstrip('/')}/send",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            status_code = int(getattr(response, "status", 0) or 0)
            payload = json.loads(response.read() or b"{}")
        if status_code == 200 and payload.get("success") is True:
            inbox.record_reply_delivery(
                key, status="delivered", bridge_message_id=str(payload.get("messageId") or ""),
                provider_outcome=str(payload.get("outcome") or "delivered"),
            )
            return "delivered"
        inbox.record_reply_delivery(
            key, status="undelivered", provider_outcome=str(payload.get("outcome") or "unconfirmed"),
            error=f"http-{status_code}-unconfirmed: {json.dumps(payload)[:200]}",
        )
    except HTTPError as exc:
        inbox.record_reply_delivery(key, status="undelivered", error=f"http-{exc.code}")
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        inbox.record_reply_delivery(key, status="undelivered", error=str(exc)[:300])
    return "undelivered"


async def process_management_document_events(
    inbox: DurableInbox, *, config_path: Path, runner: Any
) -> dict[str, int]:
    """Drain PA-73 initial entries in strict source order through the consumer."""
    config = _management_document_event_config(config_path)
    summary = {"examined": 0, "delivered": 0, "undelivered": 0, "skipped": 0}
    if config is None:
        return summary
    cursor = inbox.management_document_cursor()
    for entry in await asyncio.to_thread(_management_document_entries, config, cursor):
        summary["examined"] += 1
        created_at, entry_id = int(entry["createdAt"]), str(entry["id"])
        # A crash can land after bridge confirmation but before the local
        # cursor write. The durable delivery row is then terminal: advance
        # without reconstructing a model turn or lifecycle notice.
        prior = inbox.management_document_delivery_status(
            entry_id=entry_id, chat_id=config.chat_id,
        )
        if prior is not None:
            terminal = prior
        else:
            fresh_initial_notice = inbox.initial_management_document_notice(
                chat_id=config.chat_id, document_id=str(entry["recordId"]),
            ) is None
            outcome = await _run_management_document_turn(
                entry,
                config_path=config_path,
                destination_chat_id=config.chat_id,
                runner=runner,
                fresh_initial_notice=fresh_initial_notice,
            )
            terminal = _deliver_management_document_notice(
                inbox, config=config, entry=entry, captured_outbound=outcome,
            )
        summary[terminal] += 1
        inbox.advance_management_document_cursor(created_at=created_at, entry_id=entry_id)
        cursor = (created_at, entry_id)
    return summary


def _validated_pa75_canary_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the durable projection again before a reply can select capture."""
    if value.get("contract") != "tgg-pa75-typed-canary-projection/v1":
        raise ConsumerError("management document canary projection contract is invalid")
    event_id = str(value.get("event_id") or "")
    event_sha = str(value.get("event_sha256") or "")
    entry = value.get("entry")
    source = value.get("source_record_projection")
    source_sha = str(value.get("source_record_projection_sha256") or "")
    event_payload = value.get("event_payload")
    if (
        not re.fullmatch(r"pa75-canary:[a-z0-9][a-z0-9-]{0,127}", event_id)
        or not isinstance(entry, Mapping)
        or not re.fullmatch(r"pa75-canary-entry:[a-z0-9][a-z0-9-]{0,127}", str(entry.get("id") or ""))
        or not isinstance(entry.get("body"), Mapping)
        or not entry.get("body")
        or not isinstance(entry.get("effects"), list)
        or not isinstance(source, Mapping)
        or not source
    ):
        raise ConsumerError("management document canary projection is incomplete")
    actual_source_sha = hashlib.sha256(
        (json.dumps(dict(source), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    if source_sha != actual_source_sha:
        raise ConsumerError("management document canary projection source hash mismatch")
    if (
        not isinstance(event_payload, Mapping)
        or event_payload.get("contract") != "tgg-pa75-typed-canary-event/v1"
        or event_payload.get("id") != event_id
        or event_payload.get("destination_chat_id") != "120363426509183563@g.us"
        or event_payload.get("entry") != dict(entry)
        or event_payload.get("source_record_projection") != dict(source)
        or event_payload.get("source_record_projection_sha256") != source_sha
    ):
        raise ConsumerError("management document canary projection event payload is invalid")
    unsigned = dict(event_payload)
    actual_event_sha = hashlib.sha256(
        (json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    if event_sha != actual_event_sha:
        raise ConsumerError("management document canary projection event hash mismatch")
    return dict(value)


def _pa75_capture_reply_batch(
    records: Sequence[InboxRecord],
    correlations: Mapping[str, Mapping[str, Any]],
) -> bool:
    """Select capture only for authentic quotes of our stored canary notice."""
    if not records or {record.chat_id for record in records} != {"120363426509183563@g.us"}:
        return False
    if set(correlations) != {record.message_id for record in records}:
        return False
    try:
        for correlation in correlations.values():
            projection = correlation.get("pa75_canary_projection")
            if not isinstance(projection, Mapping):
                return False
            _validated_pa75_canary_projection(projection)
    except ConsumerError:
        return False
    return True


def _write_immutable_json_receipt(path: Path, value: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    """Write one canonical receipt, accepting only an identical retry.

    O_EXCL is intentional: these receipts are evidence, never a mutable status
    file.  A same-path mismatch is a failed-closed collision rather than an
    opportunity to overwrite an earlier capture.
    """
    payload = (json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        try:
            existing = path.read_bytes()
        except OSError as exc:
            raise ConsumerError(f"cannot read existing {label} receipt: {path}") from exc
        if existing != payload:
            raise ConsumerError(f"{label} receipt collision is divergent: {path}")
        return dict(value)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        # The receipt is not usable unless it is fully durable.  Remove only
        # the O_EXCL file this invocation created, never a pre-existing one.
        with contextlib.suppress(OSError):
            path.unlink()
        raise
    return dict(value)


def _pa75_capture_receipt(
    *,
    records: Sequence[InboxRecord],
    correlations: Mapping[str, Mapping[str, Any]],
    result: Mapping[str, Any],
    config_path: Path,
) -> Path:
    """Persist the canary's raw mutation evidence before terminal inbox state."""
    configured_dir = os.environ.get("TGG_PA75_CAPTURE_RECEIPT_DIR", "").strip()
    if not configured_dir:
        raise ConsumerError("PA-75 canary capture requires TGG_PA75_CAPTURE_RECEIPT_DIR")
    message_ids = sorted(record.message_id for record in records)
    correlation_rows: list[dict[str, str]] = []
    for message_id in message_ids:
        correlation = correlations.get(message_id)
        projection = correlation.get("pa75_canary_projection") if isinstance(correlation, Mapping) else None
        if not isinstance(projection, Mapping):
            raise ConsumerError("PA-75 canary capture lost its authenticated correlation")
        correlation_rows.append({
            "message_id": message_id,
            "event_id": str(projection["event_id"]),
            "event_sha256": str(projection["event_sha256"]),
        })
    identity = {
        "chat_id": records[0].chat_id,
        "message_ids": message_ids,
        "correlations": correlation_rows,
    }
    receipt_key = hashlib.sha256(
        (json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    provider, model = configured_engine(config_path)
    receipt = {
        "contract": "tgg-pa75-capture-mutation-receipt/v1",
        "receipt_key_sha256": receipt_key,
        **identity,
        "provider": provider,
        "model": model,
        "handled": [dict(item) for item in (result.get("handled") or []) if isinstance(item, Mapping)],
        "captured_business_mutations": [
            dict(item) for item in (result.get("captured_business_mutations") or []) if isinstance(item, Mapping)
        ],
    }
    receipt_path = Path(configured_dir).expanduser().resolve() / f"{receipt_key}.json"
    _write_immutable_json_receipt(receipt_path, receipt, label="PA-75 capture")
    return receipt_path


async def process_management_document_canary_event(
    inbox: DurableInbox,
    *,
    config_path: Path,
    runner: Any,
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """Exercise PA-74's real notice path from one immutable PA-75 projection.

    This deliberately does *not* insert a document entry, poll Systems, append
    a WhatsApp capture row, or advance the production document cursor.  The
    caller supplies a read-only projection of one source record.  That keeps
    the live chat proof honest while Tier 1 separately proves the real source
    transaction produces an outbox entry.
    """
    if event.get("contract") != "tgg-pa75-typed-canary-event/v1":
        raise ConsumerError("management document canary event has the wrong contract")
    event_id = str(event.get("id") or "").strip()
    if not re.fullmatch(r"pa75-canary:[a-z0-9][a-z0-9-]{0,127}", event_id):
        raise ConsumerError("management document canary event ID is invalid")
    if str(event.get("destination_chat_id") or "") != "120363426509183563@g.us":
        raise ConsumerError("management document canary destination is not the approved test chat")
    entry = event.get("entry")
    if not isinstance(entry, Mapping):
        raise ConsumerError("management document canary entry is missing")
    required = ("id", "recordId", "createdAt", "entryKind")
    if any(not str(entry.get(field) or "").strip() for field in required):
        raise ConsumerError("management document canary entry identity is incomplete")
    entry_id = str(entry["id"])
    if not re.fullmatch(r"pa75-canary-entry:[a-z0-9][a-z0-9-]{0,127}", entry_id):
        raise ConsumerError("management document canary entry ID must use the canary namespace")
    if str(entry.get("entryKind")) not in {"initial_default", "amendment", "closure"}:
        raise ConsumerError("management document canary entry kind is invalid")
    if not isinstance(entry.get("body"), Mapping) or not entry["body"] or not isinstance(entry.get("effects"), list):
        raise ConsumerError("management document canary entry projection is incomplete")
    source_projection = event.get("source_record_projection")
    source_sha = str(event.get("source_record_projection_sha256") or "")
    if not isinstance(source_projection, Mapping) or not source_projection or not re.fullmatch(r"[0-9a-f]{64}", source_sha):
        raise ConsumerError("management document canary source projection is invalid")
    actual_source_sha = hashlib.sha256(
        (json.dumps(dict(source_projection), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    if actual_source_sha != source_sha:
        raise ConsumerError("management document canary source projection hash mismatch")
    supplied_event_sha = str(event.get("event_sha256") or "")
    unsigned_event = {key: value for key, value in event.items() if key != "event_sha256"}
    actual_event_sha = hashlib.sha256(
        (json.dumps(unsigned_event, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    if supplied_event_sha != actual_event_sha:
        raise ConsumerError("management document canary event hash mismatch")
    projection = {
        "contract": "tgg-pa75-typed-canary-projection/v1",
        "event_id": event_id,
        "event_sha256": supplied_event_sha,
        "entry": dict(entry),
        "source_record_projection": dict(source_projection),
        "source_record_projection_sha256": source_sha,
        "event_payload": unsigned_event,
    }
    _validated_pa75_canary_projection(projection)
    config = _management_document_event_config(config_path)
    if config is None or config.chat_id != "120363426509183563@g.us":
        raise ConsumerError("management document canary transport is not bound to the approved test chat")
    outbound = await _run_management_document_turn(
        entry, config_path=config_path, destination_chat_id=config.chat_id, runner=runner,
        canary_projection=projection,
    )
    outcome = _deliver_management_document_notice(
        inbox, config=config, entry=entry, captured_outbound=outbound,
        canary_projection=projection,
    )
    return {
        "contract": "tgg-pa75-typed-canary-event-receipt/v1",
        "canary_event_id": event_id,
        "canary_event_sha256": supplied_event_sha,
        "record_id": str(entry["recordId"]),
        "entry_id": str(entry["id"]),
        "destination_chat_id": config.chat_id,
        "delivery_outcome": outcome,
        "outbound_count": len(outbound),
    }


def _loaded_catalog_status() -> Mapping[str, Any]:
    """What this process has actually loaded: plugins and registered tools.

    Read-only view of the in-process plugin manager and tool registry, so a
    release readback can check the running service rather than a fresh
    process.  It never triggers plugin discovery itself.
    """
    try:
        from hermes_cli.plugins import get_plugin_manager
        from tools.registry import registry

        manager = get_plugin_manager()
        return {
            "pid": os.getpid(),
            "plugins_discovered": bool(getattr(manager, "_discovered", False)),
            "plugins": sorted(
                name
                for name, plugin in getattr(manager, "_plugins", {}).items()
                if getattr(plugin, "enabled", False)
            ),
            "tools": sorted(getattr(registry, "_tools", {})),
        }
    except Exception as exc:  # status must never fail the consumer
        return {"pid": os.getpid(), "error": f"{type(exc).__name__}: {exc}"[:300]}


def _write_status(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_json(
        path,
        {
            "version": 1,
            "updated_at": _utc_now(),
            **dict(payload),
            "loaded_catalog": _loaded_catalog_status(),
        },
    )


def _management_continuation_status(
    session_db: Any | None, config: Any | None
) -> Mapping[str, Any]:
    """Return typed mailbox failures for the daemon's existing status file."""
    if session_db is None or config is None:
        return {"failed_count": 0, "latest_error": None}
    from gateway.management_continuation_consumer import failed_continuation_status

    return failed_continuation_status(session_db, config)


@contextlib.contextmanager
def _runtime_config_context(config_path: Path | None):
    """Bind config loading to the explicit Hermes runtime home."""
    if config_path is None:
        yield
        return
    resolved = config_path.resolve()
    if resolved.name != "config.yaml":
        raise ConsumerError(
            "explicit Hermes runtime config must be named config.yaml: "
            f"{resolved}"
        )
    previous = os.environ.get("HERMES_HOME")
    os.environ["HERMES_HOME"] = str(resolved.parent)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = previous


def _new_gateway_runner(config_path: Path | None = None) -> Any:
    from gateway.config import load_gateway_config
    from gateway.run import GatewayRunner

    with _runtime_config_context(config_path):
        return GatewayRunner(load_gateway_config())


async def run_management_document_canary(args: argparse.Namespace) -> int:
    """Run one file-backed PA-75 event through the real notice path only."""
    event_path = Path(args.event).resolve()
    receipt_path = Path(args.receipt).resolve()
    try:
        event = json.loads(event_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConsumerError(f"cannot read structured PA-75 canary event: {event_path}") from exc
    if not isinstance(event, Mapping):
        raise ConsumerError("PA-75 canary event must be a JSON object")

    # A retry must not open another model turn or attempt another bridge send.
    # The event SHA binds every signed event field; a different event at this
    # immutable receipt location is explicitly refused.
    if receipt_path.exists():
        try:
            existing = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ConsumerError(f"cannot read existing PA-75 canary receipt: {receipt_path}") from exc
        entry = event.get("entry")
        expected = {
            "contract": "tgg-pa75-typed-canary-event-receipt/v1",
            "canary_event_id": event.get("id"),
            "canary_event_sha256": event.get("event_sha256"),
            "record_id": entry.get("recordId") if isinstance(entry, Mapping) else None,
            "entry_id": entry.get("id") if isinstance(entry, Mapping) else None,
            "destination_chat_id": event.get("destination_chat_id"),
        }
        terminal_outcome = existing.get("delivery_outcome") if isinstance(existing, Mapping) else None
        outbound_count = existing.get("outbound_count") if isinstance(existing, Mapping) else None
        if (
            isinstance(existing, Mapping)
            and all(existing.get(key) == value for key, value in expected.items())
            and terminal_outcome in {"delivered", "undelivered", "unknown"}
            and isinstance(outbound_count, int)
            and not isinstance(outbound_count, bool)
            and outbound_count >= 0
        ):
            print(json.dumps(dict(existing), sort_keys=True))
            return 0
        raise ConsumerError(f"PA-75 canary receipt collision is divergent: {receipt_path}")

    inbox = DurableInbox(Path(args.inbox).resolve())
    config_path = Path(args.config).resolve()
    with SharedActivityLock(Path(args.activity_lock_file).resolve() if args.activity_lock_file else None):
        runner = _new_gateway_runner(config_path)
        receipt = await process_management_document_canary_event(
            inbox, config_path=config_path, runner=runner, event=event,
        )
        _write_immutable_json_receipt(receipt_path, receipt, label="PA-75 canary event")
    print(json.dumps(receipt, sort_keys=True))
    return 0


async def _process_claimed_chat_batch_unlocked(
    inbox: DurableInbox,
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    state_db: Path,
    case_db: Path,
    source_before_image_dir: Path,
    gate_changed_at: str,
    runner: Any,
    direct_trigger_required: bool = False,
    allow_active_steering: bool = False,
    steering_poll_seconds: float = 0.5,
    steering_batch_size: int = 25,
    persistent_session: bool = True,
) -> None:
    """Claim and process exactly one chat batch through the shared runner."""
    if not records:
        return
    chat_ids = {record.chat_id for record in records}
    if len(chat_ids) != 1:
        raise ConsumerError(f"scheduler produced a mixed-chat batch: {sorted(chat_ids)}")
    inbox.claim(records)
    if direct_trigger_required and not any(
        _priority_direct_trigger(record, config_path) for record in records
    ):
        # Retain the reason: a consumed-no-turn row must not look like an
        # unexplained model failure during nightly recovery.
        inbox.finish(
            records,
            status="skipped",
            error="PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED",
        )
        return
    try:
        # Capture-lane retention normally terminals while the row is pending.
        # This claimed-chat path remains the idempotent safety net: Hermes sees
        # either retained media, an explicit permanent refusal, or an explicit
        # quarantine warning; it never silently assumes attachment contents.
        for record in records:
            await asyncio.to_thread(
                ensure_record_media_retained,
                inbox,
                record,
                config_path=config_path,
            )
        # Systems validates business-write citations against bridge_message_log.
        # Project the exact claimed source rows before Hermes can run the model
        # and emit a business write.  The shared helper preserves the bounded
        # backplay contract: message-id idempotency, identity verification, and
        # a durable complete before-image.
        projection_run_id = (
            f"live-source-{records[0].seq}-{uuid.uuid4().hex[:12]}"
        )
        before_image_path = (
            source_before_image_dir / f"{projection_run_id}.json"
        )
        try:
            await asyncio.to_thread(
                _inject_bounded_source_evidence,
                case_db,
                records,
                before_image_path=before_image_path,
                run_id=projection_run_id,
                dry_run=False,
            )
        except Exception as exc:
            raise SourceEvidenceProjectionError(
                f"source-evidence-projection-held: {exc}"
            ) from exc
        # A reply quote can deterministically locate one of Christopher's own
        # prior document notices.  The metadata is only supplied to the model
        # turn; the capture event remains the same source record and ordinary
        # unquoted management messages receive no correlation at all.
        management_document_correlations = {
            record.message_id: correlation
            for record in records
            if record.chat_id in _management_selector_chats(config_path)
            if (correlation := inbox.management_document_correlation(record)) is not None
        }
        capture_canary_reply = _pa75_capture_reply_batch(
            records, management_document_correlations,
        )
        async with _management_typing_presence(
            records,
            config_path=config_path,
            gate_changed_at=gate_changed_at,
        ):
            processing = asyncio.create_task(
                process_live_records(
                    records,
                    config_path=config_path,
                    state_db=state_db,
                    persistent_session=persistent_session,
                    runner=runner,
                    management_document_correlations=management_document_correlations,
                    capture_business_writes_for_test_management=capture_canary_reply,
                )
            )
            if allow_active_steering:
                while not processing.done():
                    await asyncio.sleep(max(0.05, steering_poll_seconds))
                    if processing.done():
                        break
                    followups = inbox.pending_chat_batch(
                        records[0].chat_id,
                        batch_size=steering_batch_size,
                    )
                    if not followups:
                        continue
                    # A continuous interval and the full nightly fallback use
                    # the same reserved pseudo-chat but intentionally different
                    # session lifetimes. Never steer one mode into the other's
                    # active turn; leave it pending for the scheduler to launch
                    # with its own persistent/fresh decision after this turn.
                    if not _same_session_steering_allowed(records, followups, config_path):
                        continue
                    # The shared runner is still executing the addressed turn.
                    # With busy_input_mode=steer this second replay injects the
                    # follow-up after the active tool boundary; it never cancels
                    # file generation or another in-flight tool.
                    try:
                        await _process_claimed_chat_batch_unlocked(
                            inbox,
                            followups,
                            config_path=config_path,
                            state_db=state_db,
                            case_db=case_db,
                            source_before_image_dir=source_before_image_dir,
                            gate_changed_at=gate_changed_at,
                            runner=runner,
                            direct_trigger_required=False,
                            allow_active_steering=False,
                            persistent_session=persistent_session,
                        )
                    except Exception as exc:
                        print(
                            "active management steer FAILED "
                            f"chat={records[0].chat_id}: {exc}",
                            file=sys.stderr,
                        )
            result = await processing
        submitted = {str(value) for value in result.get("submitted_message_ids") or []}
        expected = {record.message_id for record in records}
        if submitted != expected:
            raise ConsumerError(
                "processor evidence did not conserve claimed messages: "
                f"claimed={len(expected)} evidenced={len(submitted)}"
            )
        turn_for_message: dict[str, str] = {}
        for group in result.get("handled") or []:
            turn_id = str(group.get("turn_id") or "")
            if not turn_id:
                continue
            for message_id in group.get("message_ids") or []:
                turn_for_message[str(message_id)] = turn_id
        unknown = set(turn_for_message) - expected
        if unknown:
            raise ConsumerError(
                f"turn evidence referenced messages outside claimed chat batch: {sorted(unknown)}"
            )
        if capture_canary_reply:
            # This receipt is the canary's durable raw-mutation boundary.  It
            # must exist before the claimed WhatsApp rows become terminal; a
            # storage failure therefore follows the existing failed/requeue
            # path instead of falsely recording a successful capture.
            _pa75_capture_receipt(
                records=records,
                correlations=management_document_correlations,
                result=result,
                config_path=config_path,
            )
        # One transaction: a multi-turn batch can never land partially
        # terminal if the process exits between turn groups.
        inbox.finish_processed_batch(records, turn_for_message=turn_for_message)
    except asyncio.CancelledError:
        inbox.requeue(records, reason="graceful-cancellation")
        raise
    except MediaRetentionError as exc:
        inbox.requeue(records, reason=f"media-retention-retry: {exc}")
        raise
    except SourceEvidenceProjectionError as exc:
        inbox.requeue(records, reason=f"source-evidence-projection-retry: {exc}")
        raise
    except Exception as exc:
        inbox.finish(records, status="failed", error=str(exc))
        raise

    # Delivery follows durable ingest completion.  It cannot change inbox
    # terminal state, retry the batch, or consume site-lane capacity.
    try:
        delivery = deliver_management_replies(
            inbox,
            config_path=config_path,
            captured_outbound=result.get("captured_outbound") or [],
            batch_records=records,
            gate_changed_at=gate_changed_at,
            handled_groups=result.get("handled") or [],
        )
        if delivery.get("delivered") or delivery.get("undelivered"):
            print(f"reply deliveries: {delivery}", file=sys.stderr)
    except Exception as exc:
        print(
            f"reply delivery machinery FAILED (ingest unaffected): {exc}",
            file=sys.stderr,
        )


async def _process_claimed_chat_batch(
    inbox: DurableInbox,
    records: Sequence[InboxRecord],
    *,
    activity_lock_file: Path | None = None,
    **kwargs: Any,
) -> None:
    """Process one batch while making a release/claim race impossible.

    The lock starts before ``inbox.claim``; taking it only after the claim
    leaves the exact check-then-restart race this seam exists to prevent.
    """
    with SharedActivityLock(activity_lock_file):
        await _process_claimed_chat_batch_unlocked(inbox, records, **kwargs)


async def _process_claimed_management_continuation(
    *,
    row: Mapping[str, Any],
    envelope: Any,
    session_db: Any,
    continuation_config: Any,
    inbox: DurableInbox,
    config_path: Path,
    state_db: Path,
    gate_changed_at: str,
    runner: Any,
    activity_lock_file: Path | None,
) -> None:
    """Run one typed mailbox row under the ordinary management activity lock."""
    from gateway.management_continuation_consumer import (
        process_claimed_management_continuation,
    )

    with SharedActivityLock(activity_lock_file):
        mailbox_id = ""
        claimed = False
        try:
            mailbox_id = str(row.get("id") or "")
            claimed = session_db.claim_session_mailbox(
                mailbox_id,
                include_body_contract="management-list-continuation/v1",
            )
            if not claimed:
                return
            await process_claimed_management_continuation(
                row=row,
                envelope=envelope,
                session_db=session_db,
                inbox=inbox,
                config=continuation_config,
                config_path=config_path,
                state_db=state_db,
                gate_changed_at=gate_changed_at,
                runner=runner,
            )
        except asyncio.CancelledError:
            if claimed:
                session_db.fail_session_mailbox(
                    mailbox_id, "CONTINUATION_CANCELLED_OUTCOME_UNKNOWN"
                )
            raise
        except Exception as exc:
            # The mailbox implementation recorded the terminal failure.  Do
            # not let one uncertain provider outcome stop unrelated chats.
            print(f"management continuation FAILED: {exc}", file=sys.stderr)


async def _process_background_completion(
    *,
    row: Mapping[str, Any],
    store: Any,
    inbox: DurableInbox,
    config_path: Path,
    state_db: Path,
    gate_changed_at: str,
    runner: Any,
    activity_lock_file: Path | None,
) -> None:
    """Run one claimed background completion under the ordinary activity lock."""
    from gateway.pa_background_completions import process_claimed_completion

    with SharedActivityLock(activity_lock_file):
        try:
            outcome = await process_claimed_completion(
                row=row,
                store=store,
                inbox=inbox,
                config_path=config_path,
                state_db=state_db,
                gate_changed_at=gate_changed_at,
                runner=runner,
            )
            print(f"background completion turn: {outcome}", file=sys.stderr)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The store kept the row pending (or failed after its retry limit).
            print(f"background completion FAILED: {exc}", file=sys.stderr)


async def run_consumer(args: argparse.Namespace) -> int:
    config_path = Path(args.config).resolve()
    source = Path(args.source).resolve()
    cursor = Path(args.cursor).resolve()
    inbox = DurableInbox(Path(args.inbox).resolve())
    status_path = Path(args.status_file).resolve()
    gate_path = Path(args.processing_gate).resolve()
    state_db = Path(args.state_db).resolve()
    case_db = Path(args.case_db).resolve()
    activity_lock_file = (
        Path(args.activity_lock_file).resolve()
        if getattr(args, "activity_lock_file", None)
        else None
    )
    source_before_image_dir = Path(args.source_before_image_dir).resolve()
    site_concurrency = max(1, int(getattr(args, "site_concurrency", 4)))
    chat_batch_size = max(1, int(getattr(args, "chat_batch_size", 25)))
    retention_batch_size = max(1, int(getattr(args, "retention_batch_size", 25)))
    management_quiet_seconds = _management_quiet_seconds(config_path)

    with SingletonLock(Path(args.lock_file).resolve()):
        recovery = inbox.reconcile_orphan_processing(state_db)
        expected_total = inbox.assert_and_record_conservation()
        # PA-203 round 2: this process, not the gateway watcher, owns the
        # return path of background work started from its turns.
        from gateway import pa_background_completions as background

        completion_store, completion_recovery, restore_completion_sink = (
            background.start_for_consumer(inbox.db_path, state_db)
        )
        runner: Any | None = None
        continuation_session_db: Any | None = None
        continuation_config: Any | None = None
        from gateway.management_continuation_consumer import (
            load_management_continuation_config,
            terminalize_prior_process_continuations,
        )
        continuation_config = load_management_continuation_config(
            config_path, inbox_db=inbox.db_path, state_db=state_db
        )
        if continuation_config is not None:
            from hermes_state import SessionDB
            continuation_session_db = SessionDB(db_path=state_db)
            terminalize_prior_process_continuations(
                continuation_session_db, continuation_config
            )
        tasks: dict[str, asyncio.Task[None]] = {}
        lanes: dict[str, str] = {}
        source_projection_holds: dict[str, str] = {}
        cron_stop, cron_thread = _start_cron_ticker()
        try:
            while True:
                # Keep the existing opt-in configuration reload behaviour.
                # Orphan terminalization is deliberately startup-only, before
                # this process can own a typed claim.
                continuation_config = load_management_continuation_config(
                    config_path, inbox_db=inbox.db_path, state_db=state_db
                )
                if continuation_config is not None and continuation_session_db is None:
                    from hermes_state import SessionDB
                    continuation_session_db = SessionDB(db_path=state_db)
                continuation_status = _management_continuation_status(
                    continuation_session_db, continuation_config
                )
                done_chats = [chat_id for chat_id, task in tasks.items() if task.done()]
                for chat_id in done_chats:
                    task = tasks.pop(chat_id)
                    lanes.pop(chat_id, None)
                    try:
                        await task
                        source_projection_holds.pop(chat_id, None)
                    except MediaRetentionError as exc:
                        # This chat's claimed rows are already pending again.
                        # Keep the other chat lanes and the daemon alive; the
                        # durable status exposes the hold until a retry clears it.
                        print(
                            f"media retention HELD/PENDING: chat={chat_id} error={exc}",
                            file=sys.stderr,
                        )
                    except SourceEvidenceProjectionError as exc:
                        source_projection_holds[chat_id] = str(exc)
                        print(
                            "source evidence projection HELD/PENDING: "
                            f"chat={chat_id} error={exc}",
                            file=sys.stderr,
                        )

                config_enabled = processing_enabled(config_path)
                gate = processing_gate_state(gate_path)
                gate_enabled = gate["enabled"] is True
                projection_enabled = _source_projection_config(config_path) is not None
                # Keep every finished background result durable, even in standby.
                background.maintain(completion_store)

                # Capture staging and its Systems projection are independent
                # from the business/model gate.  A paused Christopher must not
                # make the authoritative ledger fall behind raw capture.
                staged_total = 0
                projection_cycle = {"attempted": 0, "complete": 0, "held": 0, "skipped": 0, "disabled": 1}
                if projection_enabled:
                    before_stage = inbox.total()
                    staged = inbox.stage_from_source(
                        source, cursor, max_records=args.max_records
                    )
                    staged_total = staged
                    while staged >= args.max_records:
                        staged = inbox.stage_from_source(
                            source, cursor, max_records=args.max_records
                        )
                        staged_total += staged
                    after_stage = inbox.total()
                    if after_stage < before_stage:
                        raise ConsumerError(
                            "inbox conservation failed during capture staging: "
                            f"before={before_stage} after={after_stage}"
                        )
                    expected_total += after_stage - before_stage
                    if inbox.assert_and_record_conservation() != expected_total:
                        raise ConsumerError(
                            "inbox conservation hard-abort after capture staging: "
                            f"expected={expected_total} actual={inbox.total()}"
                        )
                    projection_cycle = await asyncio.to_thread(
                        project_pending_source_events,
                        inbox,
                        config_path=config_path,
                        limit=max(1, int(getattr(args, "source_projection_batch_size", 100))),
                    )
                if not (config_enabled and gate_enabled):
                    if tasks:
                        for task in tasks.values():
                            task.cancel()
                        await asyncio.gather(*tasks.values(), return_exceptions=True)
                        tasks.clear()
                        lanes.clear()
                    counts = inbox.counts()
                    if sum(counts.values()) != expected_total:
                        raise ConsumerError(
                            "inbox conservation failed in standby: "
                            f"expected={expected_total} actual={sum(counts.values())}"
                        )
                    _write_status(
                        status_path,
                        {
                            **_retention_status(
                                inbox, config_path, inspect_media=False
                            ),
                            "management_continuation": continuation_status,
                            **inbox.source_projection_counts(),
                            "source_projection_cycle": projection_cycle,
                            "source_projection_hold": inbox.source_projection_last_error(),
                            "state": "standby",
                            "processing_enabled": False,
                            "config_enabled": config_enabled,
                            "gate_enabled": gate_enabled,
                            "gate_generation": int(gate["generation"]),
                            "gate_change_run_id": gate.get("change_run_id"),
                            "pid": os.getpid(),
                            "source_opened": projection_enabled,
                            "cursor_advanced": staged_total > 0,
                            "scheduler_mode": "per-chat-parallel",
                            "claim_stale_seconds": 1800,
                            "site_concurrency": site_concurrency,
                            "chat_batch_size": chat_batch_size,
                            "management_quiet_seconds": management_quiet_seconds,
                            "retention_batch_size": retention_batch_size,
                            "active_management_chats": [],
                            "active_site_chats": [],
                            "oldest_active_claim": inbox.oldest_processing_updated_at(),
                            "state_total": expected_total,
                            "startup_recovery": recovery,
                            "background_completions": {
                                **completion_store.counts(),
                                "startup_recovery": completion_recovery,
                            },
                            "inbox": counts,
                        },
                    )
                    if args.once:
                        return 0
                    await asyncio.sleep(args.poll_seconds)
                    continue

                retention_status = _retention_status(
                    inbox, config_path, inspect_media=True
                )
                try:
                    _assert_media_headroom(config_path, retention_status)
                except MediaRetentionError as exc:
                    _write_status(
                        status_path,
                        {
                            **retention_status,
                            "management_continuation": continuation_status,
                            **inbox.source_projection_counts(),
                            "source_projection_cycle": projection_cycle,
                            "source_capture_projection_hold": inbox.source_projection_last_error(),
                            "state": "held",
                            "processing_enabled": False,
                            "config_enabled": True,
                            "gate_enabled": True,
                            "gate_generation": int(gate["generation"]),
                            "pid": os.getpid(),
                            "source_opened": projection_enabled,
                            "cursor_advanced": staged_total > 0,
                            "retention_hold": str(exc),
                            "inbox": inbox.counts(),
                        },
                    )
                    if args.once:
                        return 0
                    await asyncio.sleep(args.poll_seconds)
                    continue
                # Preserve the existing safety ordering for deployments that
                # have not enabled the independent source projector: a media
                # volume hold must not advance their business cursor.  Once
                # projection is configured, it is intentionally independent
                # and has already staged above.
                if not projection_enabled:
                    before_stage = inbox.total()
                    staged = inbox.stage_from_source(
                        source, cursor, max_records=args.max_records
                    )
                    staged_total = staged
                    while staged >= args.max_records:
                        staged = inbox.stage_from_source(
                            source, cursor, max_records=args.max_records
                        )
                        staged_total += staged
                    after_stage = inbox.total()
                    if after_stage < before_stage:
                        raise ConsumerError(
                            "inbox conservation failed during staging: "
                            f"before={before_stage} after={after_stage}"
                        )
                    expected_total += after_stage - before_stage
                    if inbox.assert_and_record_conservation() != expected_total:
                        raise ConsumerError(
                            "inbox conservation hard-abort after staging: "
                            f"expected={expected_total} actual={inbox.total()}"
                        )
                _write_status(
                    status_path,
                    {
                        **retention_status,
                        "management_continuation": continuation_status,
                        **inbox.source_projection_counts(),
                        "source_projection_cycle": projection_cycle,
                        "source_capture_projection_hold": inbox.source_projection_last_error(),
                        "state": (
                            "held-pending"
                            if inbox.retention_last_error()
                            or source_projection_holds
                            else "running"
                        ),
                        "processing_enabled": True,
                        "config_enabled": True,
                        "gate_enabled": True,
                        "gate_generation": int(gate["generation"]),
                        "gate_change_run_id": gate.get("change_run_id"),
                        "pid": os.getpid(),
                        "source_opened": True,
                        "cursor_advanced": staged_total > 0,
                        "scheduler_mode": "per-chat-parallel",
                        "claim_stale_seconds": 1800,
                        "site_concurrency": site_concurrency,
                        "chat_batch_size": chat_batch_size,
                        "management_quiet_seconds": management_quiet_seconds,
                        "retention_batch_size": retention_batch_size,
                        "source_projection_hold": (
                            next(iter(source_projection_holds.values()), None)
                        ),
                        "source_projection_held_chats": sorted(
                            source_projection_holds
                        ),
                        "active_management_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "management"
                        ),
                        "active_site_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "site"
                        ),
                        "oldest_active_claim": inbox.oldest_processing_updated_at(),
                        "state_total": expected_total,
                        "startup_recovery": recovery,
                        "background_completions": {
                            **completion_store.counts(),
                            "startup_recovery": completion_recovery,
                        },
                        "inbox": inbox.counts(),
                    },
                )

                # Retention is a capture-lane concern, not a model/business
                # concern.  It runs before demo-pause lane selection and never
                # claims or terminals the business row.
                retention_cycle = await retain_pending_media(
                    inbox,
                    config_path=config_path,
                    limit=retention_batch_size,
                )

                try:
                    priority_chats = _priority_selector_chats(config_path)
                    nightly_chats = _nightly_selector_chats(config_path)
                except Exception:
                    priority_chats = frozenset()
                    nightly_chats = frozenset()
                demo_management_only = os.environ.get(
                    "TGG_DEMO_MANAGEMENT_ONLY", ""
                ).strip().lower() in {"1", "true", "yes", "on"}
                management_batches, site_batches = inbox.pending_chat_batches(
                    batch_size=chat_batch_size,
                    priority_chats=priority_chats,
                    priority_quiet_seconds=management_quiet_seconds,
                    # A source-projection failure is an operator-visible hold,
                    # not a 2-second retry loop against the same broken
                    # binding.  Restarting the daemon re-arms pending rows
                    # after the case-db/schema fault has been repaired.
                    exclude_chats=set(tasks) | set(source_projection_holds),
                )
                continuation_candidate: tuple[Mapping[str, Any], Any] | None = None
                if continuation_config is not None:
                    from gateway.management_continuation_consumer import next_pending_continuation
                    continuation_candidate = next_pending_continuation(
                        continuation_session_db, continuation_config
                    )
                gate_changed_at = str(gate.get("changed_at") or "")
                active_site = sum(1 for lane in lanes.values() if lane == "site")
                available_site = max(0, site_concurrency - active_site)
                selected_site_batches = (
                    [] if demo_management_only else site_batches[:available_site]
                )
                document_event_config = _management_document_event_config(config_path)
                if (
                    management_batches
                    or selected_site_batches
                    or document_event_config is not None
                    or continuation_candidate is not None
                ) and runner is None:
                    runner = _new_gateway_runner(config_path)

                # PA-74's source-fired document outbox is deliberately
                # adjacent to, not inside, WhatsApp ingress.  It reuses this
                # durable consumer's runner and outbound ledger, but never
                # creates a capture row, source projection, or business-model
                # write from the internal event itself.
                document_event_cycle = {"examined": 0, "delivered": 0, "undelivered": 0, "skipped": 0}
                if document_event_config is not None:
                    document_event_cycle = await process_management_document_events(
                        inbox, config_path=config_path, runner=runner
                    )

                # Reserved management capacity: these tasks never acquire or
                # wait for a site slot.  One task per chat preserves FIFO.
                for chat_id, records in management_batches:
                    tasks[chat_id] = asyncio.create_task(
                        _process_claimed_chat_batch(
                            inbox,
                            records,
                            activity_lock_file=activity_lock_file,
                            config_path=config_path,
                            state_db=state_db,
                            gate_changed_at=gate_changed_at,
                            runner=runner,
                            case_db=case_db,
                            source_before_image_dir=source_before_image_dir,
                            direct_trigger_required=_management_requires_direct_trigger(
                                config_path, chat_id
                            ),
                            allow_active_steering=True,
                            persistent_session=(
                                _continuous_interval_batch(records, config_path)
                                or chat_id not in nightly_chats
                            ),
                        )
                    )
                    lanes[chat_id] = "management"

                # A normal captured management task always wins the chat lane.
                # Leaving the mailbox pending is the only safe response while
                # that original session is busy; no second model turn starts.
                if (
                    continuation_candidate is not None
                    and continuation_config.management_chat_id not in tasks
                    and runner is not None
                ):
                    row, envelope = continuation_candidate
                    chat_id = continuation_config.management_chat_id
                    tasks[chat_id] = asyncio.create_task(
                        _process_claimed_management_continuation(
                            row=row,
                            envelope=envelope,
                            session_db=continuation_session_db,
                            continuation_config=continuation_config,
                            inbox=inbox,
                            config_path=config_path,
                            state_db=state_db,
                            gate_changed_at=gate_changed_at,
                            runner=runner,
                            activity_lock_file=activity_lock_file,
                        )
                    )
                    lanes[chat_id] = "management"

                # A finished background result is one ordinary turn in its
                # conversation, taken only when that chat is otherwise idle.
                completion_row = completion_store.next_pending(exclude_chats=set(tasks))
                if completion_row is not None and runner is None:
                    runner = _new_gateway_runner(config_path)
                if completion_row is not None and completion_store.claim(
                    str(completion_row["completion_id"])
                ):
                    tasks[completion_row["chat_id"]] = asyncio.create_task(
                        _process_background_completion(
                            row=completion_row,
                            store=completion_store,
                            inbox=inbox,
                            config_path=config_path,
                            state_db=state_db,
                            gate_changed_at=gate_changed_at,
                            runner=runner,
                            activity_lock_file=activity_lock_file,
                        )
                    )
                    lanes[completion_row["chat_id"]] = "management"

                if not demo_management_only:
                    for chat_id, records in selected_site_batches:
                        tasks[chat_id] = asyncio.create_task(
                            _process_claimed_chat_batch(
                                inbox,
                                records,
                                activity_lock_file=activity_lock_file,
                                config_path=config_path,
                                state_db=state_db,
                                gate_changed_at=gate_changed_at,
                                runner=runner,
                                case_db=case_db,
                                source_before_image_dir=source_before_image_dir,
                            )
                        )
                        lanes[chat_id] = "site"

                if args.once:
                    if tasks:
                        task_items = list(tasks.items())
                        outcomes = await asyncio.gather(
                            *(task for _, task in task_items), return_exceptions=True
                        )
                        for (chat_id, _), outcome in zip(task_items, outcomes):
                            if isinstance(outcome, MediaRetentionError):
                                print(
                                    f"media retention HELD/PENDING: {outcome}",
                                    file=sys.stderr,
                                )
                            elif isinstance(outcome, SourceEvidenceProjectionError):
                                source_projection_holds[chat_id] = str(outcome)
                                print(
                                    "source evidence projection HELD/PENDING: "
                                    f"chat={chat_id} error={outcome}",
                                    file=sys.stderr,
                                )
                            elif isinstance(outcome, BaseException):
                                raise outcome
                            else:
                                source_projection_holds.pop(chat_id, None)
                        tasks.clear()
                        lanes.clear()
                    counts = inbox.counts()
                    if sum(counts.values()) != expected_total:
                        raise ConsumerError(
                            "inbox conservation failed at once boundary: "
                            f"expected={expected_total} actual={sum(counts.values())}"
                        )
                    _write_status(
                        status_path,
                        {
                            **_retention_status(
                                inbox, config_path, inspect_media=True
                            ),
                            "management_continuation": continuation_status,
                            **inbox.source_projection_counts(),
                            "source_projection_cycle": projection_cycle,
                            "source_capture_projection_hold": inbox.source_projection_last_error(),
                            "state": (
                                "held-pending"
                                if inbox.retention_last_error()
                                or source_projection_holds
                                else "running"
                            ),
                            "processing_enabled": True,
                            "config_enabled": True,
                            "gate_enabled": True,
                            "gate_generation": int(gate["generation"]),
                            "gate_change_run_id": gate.get("change_run_id"),
                            "pid": os.getpid(),
                            "staged": staged_total,
                            "scheduler_mode": "per-chat-parallel",
                            "claim_stale_seconds": 1800,
                            "site_concurrency": site_concurrency,
                            "chat_batch_size": chat_batch_size,
                            "retention_batch_size": retention_batch_size,
                            "retention_cycle": retention_cycle,
                            "source_projection_hold": (
                                next(iter(source_projection_holds.values()), None)
                            ),
                            "source_projection_held_chats": sorted(
                                source_projection_holds
                            ),
                            "active_management_chats": [],
                            "active_site_chats": [],
                            "oldest_active_claim": inbox.oldest_processing_updated_at(),
                            "state_total": expected_total,
                            "startup_recovery": recovery,
                            "background_completions": {
                                **completion_store.counts(),
                                "startup_recovery": completion_recovery,
                            },
                            "inbox": counts,
                        },
                    )
                    return 0

                counts = inbox.counts()
                if sum(counts.values()) != expected_total:
                    raise ConsumerError(
                        "inbox conservation failed: "
                        f"expected={expected_total} actual={sum(counts.values())}"
                    )
                _write_status(
                    status_path,
                    {
                        **_retention_status(
                            inbox, config_path, inspect_media=True
                        ),
                        "management_continuation": continuation_status,
                        **inbox.source_projection_counts(),
                        "source_projection_cycle": projection_cycle,
                        "source_capture_projection_hold": inbox.source_projection_last_error(),
                        "state": (
                            "held-pending"
                            if inbox.retention_last_error()
                            or source_projection_holds
                            else "running"
                        ),
                        "processing_enabled": True,
                        "config_enabled": True,
                        "gate_enabled": True,
                        "gate_generation": int(gate["generation"]),
                        "gate_change_run_id": gate.get("change_run_id"),
                        "pid": os.getpid(),
                        "staged": staged_total,
                        "scheduler_mode": "per-chat-parallel",
                        "claim_stale_seconds": 1800,
                        "site_concurrency": site_concurrency,
                        "chat_batch_size": chat_batch_size,
                        "retention_batch_size": retention_batch_size,
                        "retention_cycle": retention_cycle,
                        "source_projection_hold": (
                            next(iter(source_projection_holds.values()), None)
                        ),
                        "source_projection_held_chats": sorted(
                            source_projection_holds
                        ),
                        "active_management_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "management"
                        ),
                        "active_site_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "site"
                        ),
                        "oldest_active_claim": inbox.oldest_processing_updated_at(),
                        "state_total": expected_total,
                        "startup_recovery": recovery,
                        "background_completions": {
                            **completion_store.counts(),
                            "startup_recovery": completion_recovery,
                        },
                        "inbox": counts,
                    },
                )
                await asyncio.sleep(args.poll_seconds)
        except asyncio.CancelledError:
            for task in tasks.values():
                task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            if inbox.total() != expected_total:
                raise ConsumerError(
                    "inbox conservation failed during graceful cancellation"
                )
            raise
        finally:
            _stop_cron_ticker(cron_stop, cron_thread)
            restore_completion_sink()
            if continuation_session_db is not None:
                continuation_session_db.close()


async def run_fixture(args: argparse.Namespace) -> int:
    test_root = Path(args.test_root).resolve()
    source = Path(args.source).resolve()
    cursor = Path(args.cursor).resolve()
    inbox_path = Path(args.inbox).resolve()
    config_path = Path(args.config).resolve()
    state_db = Path(args.state_db).resolve()
    for path in (source, cursor, inbox_path, config_path, state_db):
        if path != test_root and test_root not in path.parents:
            raise ConsumerError(f"fixture path escapes test root: {path}")
    if "/var/lib/tgg-capture" in str(source):
        raise ConsumerError("fixture mode refuses the live capture store")
    if not processing_enabled(config_path):
        raise ConsumerError("fixture config must explicitly enable PA inside test root")

    inbox = DurableInbox(inbox_path)
    initialize_cursor(source, cursor, position="start")
    staged = inbox.stage_from_source(source, cursor, max_records=args.max_records)
    records = inbox.pending(limit=args.max_records)
    if not records:
        raise ConsumerError("fixture source staged no records")
    inbox.claim(records)
    try:
        result = await process_replay_records(
            records,
            config_path=config_path,
            state_db=state_db,
            run_id=args.run_id,
        )
        inbox.finish(records, status="completed", pa_turn_id=result["turn_id"])
    except Exception as exc:
        inbox.finish(records, status="failed", error=str(exc))
        raise
    report = {
        "ok": True,
        "mode": "fixture-only",
        "staged": staged,
        "inbox": inbox.counts(),
        "result": result,
    }
    _atomic_write_json(Path(args.report).resolve(), report)
    print(json.dumps(report, sort_keys=True))
    return 0


def _window_counts(
    records: Sequence[InboxRecord], statuses: Mapping[str, str]
) -> dict[str, Any]:
    per_chat: dict[str, dict[str, int]] = {}
    totals: dict[str, int] = {}
    for record in records:
        status = statuses.get(str(record.seq), "missing")
        per_chat.setdefault(record.chat_id, {})[status] = (
            per_chat.setdefault(record.chat_id, {}).get(status, 0) + 1
        )
        totals[status] = totals.get(status, 0) + 1
    return {"total": len(records), "per_chat": per_chat, "statuses": totals}


def assert_bounded_selection(
    records: Sequence[InboxRecord], *, chat_ids: Sequence[str], cutoff: datetime,
    expected_total: int,
) -> None:
    allowed = frozenset(chat_ids)
    if len(allowed) != 4:
        raise ConsumerError("bounded replay requires exactly four unique chat ids")
    if len({record.message_id for record in records}) != len(records):
        raise ConsumerError("bounded replay selection contains duplicate message ids")
    if len(records) != expected_total:
        raise ConsumerError(
            "bounded replay denominator mismatch: "
            f"expected={expected_total} selected={len(records)}"
        )
    for record in records:
        if record.chat_id not in allowed or _record_ingress_timestamp(record) < cutoff:
            raise ConsumerError(
                f"bounded replay selected out-of-window row {record.message_id}"
            )


def assert_message_id_selection(
    records: Sequence[InboxRecord], *, expected_message_ids: Sequence[str],
    expected_total: int,
) -> None:
    expected = tuple(dict.fromkeys(str(value).strip() for value in expected_message_ids if str(value).strip()))
    actual = tuple(record.message_id for record in records)
    if len(actual) != expected_total:
        raise ConsumerError(
            "bounded replay denominator mismatch: "
            f"expected={expected_total} selected={len(actual)}"
        )
    if len(set(actual)) != len(actual):
        raise ConsumerError("bounded replay selection contains duplicate message ids")
    if set(actual) != set(expected):
        raise ConsumerError("bounded replay exact message-id selection mismatch")


def _read_id_file(path: Path) -> list[str]:
    if not path.is_file():
        raise ConsumerError(f"bounded replay id file is missing: {path}")
    values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return list(dict.fromkeys(value for value in values if value and not value.startswith("#")))


def _read_message_groups(
    path: Path, *, records: Sequence[InboxRecord]
) -> list[tuple[str, list[InboxRecord]]]:
    """Load a prior-turn grouping without changing the selected message set."""
    if not path.is_file():
        raise ConsumerError(f"bounded replay message-group file is missing: {path}")
    by_id = {record.message_id: record for record in records}
    groups: list[tuple[str, list[InboxRecord]]] = []
    seen: set[str] = set()
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ConsumerError(
                f"bounded replay message-group JSON invalid at line {line_no}"
            ) from exc
        ids = [str(value).strip() for value in payload.get("message_ids") or []]
        if not ids:
            raise ConsumerError(f"bounded replay message-group {line_no} is empty")
        duplicate = [value for value in ids if value in seen]
        if duplicate:
            raise ConsumerError(
                f"bounded replay message-group duplicates message id {duplicate[0]}"
            )
        missing = [value for value in ids if value not in by_id]
        if missing:
            raise ConsumerError(
                f"bounded replay message-group references unselected id {missing[0]}"
            )
        batch = [by_id[value] for value in ids]
        chat_ids = {record.chat_id for record in batch}
        declared_chat = str(payload.get("chat_id") or "")
        if len(chat_ids) != 1 or (declared_chat and declared_chat not in chat_ids):
            raise ConsumerError(
                f"bounded replay message-group {line_no} crosses or misstates chat"
            )
        groups.append((next(iter(chat_ids)), batch))
        seen.update(ids)
    if seen != set(by_id):
        omitted = sorted(set(by_id) - seen)
        raise ConsumerError(
            "bounded replay message-group partition omitted selected ids: "
            + ",".join(omitted[:10])
        )
    return groups


def assert_no_window_orphans(statuses: Mapping[str, str]) -> None:
    remaining = sum(1 for value in statuses.values() if value == "processing")
    if remaining:
        raise ConsumerError(
            f"bounded replay reconciliation left {remaining} processing/orphan rows"
        )


def _sqlite_table_counts(path: Path) -> dict[str, int]:
    if not path.is_file():
        raise ConsumerError(f"case database is missing: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = [str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )]
        return {
            table: int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in tables
        }
    finally:
        conn.close()


def _business_audit_cursor(path: Path) -> dict[str, int]:
    """Return a structural cursor for audited Systems mutations."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ps_audit_log'"
        ).fetchone()
        if not exists:
            return {"row_count": 0, "max_rowid": 0}
        row = conn.execute(
            "SELECT COUNT(*),COALESCE(MAX(rowid),0) FROM ps_audit_log"
        ).fetchone()
        return {"row_count": int(row[0]), "max_rowid": int(row[1])}
    finally:
        conn.close()


def _business_audit_delta(path: Path, *, after_rowid: int) -> list[dict[str, Any]]:
    """Read only structural fields for Systems mutations after a cursor."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ps_audit_log'"
        ).fetchone()
        if not exists:
            return []
        rows = conn.execute(
            "SELECT rowid AS audit_rowid,id,action,target_kind,target_id,source_surface,ts "
            "FROM ps_audit_log WHERE rowid>? ORDER BY rowid",
            (int(after_rowid),),
        ).fetchall()
        return [
            {
                "rowid": int(row["audit_rowid"]),
                "id": str(row["id"]),
                "action": str(row["action"]),
                "target_kind": str(row["target_kind"]),
                "target_id": str(row["target_id"]),
                "source_surface": str(row["source_surface"]),
                "ts": str(row["ts"]),
            }
            for row in rows
        ]
    finally:
        conn.close()


def _inject_bounded_source_evidence(
    case_db: Path,
    records: Sequence[InboxRecord],
    *,
    before_image_path: Path,
    run_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Make exact bounded inbox rows citable by the Systems job-number gate.

    The capture inbox is the durable source for these rows, while Systems' citation
    gate reads ``bridge_message_log``.  This message-id-scoped convergence copies
    only the selected source documents and records the complete pre-mutation image.
    Existing identical source refs are preserved; divergent identities fail closed.
    """
    if not case_db.is_file():
        raise ConsumerError(f"case database is missing: {case_db}")
    conn = sqlite3.connect(f"file:{case_db}?mode=rw", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        columns = [
            str(row[1])
            for row in conn.execute("PRAGMA table_info(bridge_message_log)")
        ]
        required = {
            "local_id", "source", "source_ref", "chat_jid", "chat_name",
            "zone", "channel_type", "sender_id", "from_me", "ts", "sgt",
            "text", "message_kind", "has_media", "media_refs", "quoted_text",
            "reply_to_source_ref", "raw_json",
        }
        if not required.issubset(columns):
            raise ConsumerError("bridge_message_log schema cannot accept source evidence")

        refs = [record.message_id for record in records]
        if len(refs) != len(set(refs)):
            raise ConsumerError("bounded source evidence contains duplicate message ids")
        existing: dict[str, dict[str, Any]] = {}
        if refs:
            placeholders = ",".join("?" for _ in refs)
            for row in conn.execute(
                f"SELECT * FROM bridge_message_log WHERE source_ref IN ({placeholders})",
                refs,
            ):
                existing[str(row["source_ref"])] = dict(row)
        _atomic_write_json(
            before_image_path,
            {
                "artifact_type": "tgg_bounded_source_evidence_before_image",
                "run_id": run_id,
                "created_at": _utc_now(),
                "selected_message_ids": refs,
                "existing_rows": list(existing.values()),
            },
        )

        rows: list[tuple[Any, ...]] = []
        skipped = 0
        sgt_zone = ZoneInfo("Asia/Singapore")
        for record in records:
            item = _bridge_item(record.raw)
            raw_message_id = str(item.get("messageId") or "")
            raw_chat_id = str(item.get("chatId") or "")
            if raw_message_id != record.message_id or raw_chat_id != record.chat_id:
                raise ConsumerError(
                    "bounded source evidence identity diverges from inbox selection"
                )
            raw_json = json.dumps(record.raw, ensure_ascii=False, sort_keys=True)
            prior = existing.get(record.message_id)
            if prior is not None:
                if str(prior.get("chat_jid")) != record.chat_id:
                    raise ConsumerError(
                        f"source evidence conflict for {record.message_id}"
                    )
                skipped += 1
                continue
            timestamp = int(_record_ingress_timestamp(record).timestamp())
            sgt = datetime.fromtimestamp(timestamp, tz=sgt_zone).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            media_refs = item.get("mediaUrls") or item.get("media") or []
            if isinstance(media_refs, (str, bytes, Mapping)):
                media_refs = [media_refs]
            rows.append(
                (
                    "whatsapp-capture",
                    record.message_id,
                    record.chat_id,
                    str(item.get("chatName") or record.chat_id),
                    "",
                    "group" if bool(item.get("isGroup", record.chat_id.endswith("@g.us"))) else "direct",
                    str(item.get("senderId") or "") or None,
                    int(bool(item.get("fromMe"))),
                    timestamp,
                    sgt,
                    str(item.get("body") or item.get("text") or ""),
                    str(item.get("mediaType") or ("media" if item.get("hasMedia") else "text")),
                    int(bool(item.get("hasMedia") or media_refs)),
                    json.dumps(list(media_refs), ensure_ascii=False, sort_keys=True),
                    str(item.get("quotedText") or ""),
                    str(item.get("quotedMessageId") or "") or None,
                    raw_json,
                )
            )
        if not dry_run and rows:
            with conn:
                conn.executemany(
                    """
                    INSERT INTO bridge_message_log
                      (source,source_ref,chat_jid,chat_name,zone,channel_type,
                       sender_id,from_me,ts,sgt,text,message_kind,has_media,
                       media_refs,quoted_text,reply_to_source_ref,raw_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
        return {
            "selected": len(records),
            "inserted": 0 if dry_run else len(rows),
            "would_insert": len(rows),
            "already_present": skipped,
            "before_image": str(before_image_path),
            "dry_run": dry_run,
        }
    finally:
        conn.close()


def _read_int_id_file(path: Path) -> list[int]:
    values = _read_id_file(path)
    try:
        ids = [int(value) for value in values]
    except ValueError as exc:
        raise ConsumerError(f"draft id file contains a non-integer: {path}") from exc
    if any(value <= 0 for value in ids):
        raise ConsumerError(f"draft id file contains a non-positive id: {path}")
    return ids


def _transition_readjudication_drafts(
    case_db: Path,
    *,
    readjudicated_ids: Sequence[int],
    pending_manager_ids: Sequence[int],
    manager_chat_id: str,
    before_image_path: Path,
    run_id: str,
    dry_run: bool,
    source_state: str = "draft",
) -> dict[str, Any]:
    """Close stale drafts or hold them for the manager, with row-level audit."""
    readjudicated = tuple(dict.fromkeys(int(value) for value in readjudicated_ids))
    pending_manager = tuple(dict.fromkeys(int(value) for value in pending_manager_ids))
    if set(readjudicated) & set(pending_manager):
        raise ConsumerError("readjudicated and pending-manager draft ids overlap")
    all_ids = readjudicated + pending_manager
    if not all_ids:
        return {"readjudicated": 0, "pending_manager": 0, "dry_run": dry_run}
    if not manager_chat_id.strip():
        raise ConsumerError("manager chat id is required for pending-manager drafts")

    uri = f"file:{case_db}?mode=ro" if dry_run else str(case_db)
    conn = sqlite3.connect(uri, uri=dry_run)
    conn.row_factory = sqlite3.Row
    try:
        placeholders = ",".join("?" for _ in all_ids)
        rows = conn.execute(
            f"SELECT * FROM draft_outbound WHERE id IN ({placeholders}) ORDER BY id",
            all_ids,
        ).fetchall()
        if len(rows) != len(all_ids):
            found = {int(row["id"]) for row in rows}
            raise ConsumerError(
                f"draft transition denominator mismatch: missing={sorted(set(all_ids) - found)}"
            )
        wrong_state = [
            int(row["id"]) for row in rows if str(row["state"]) != source_state
        ]
        if wrong_state:
            raise ConsumerError(
                f"draft transition refuses rows outside {source_state}: {wrong_state}"
            )
        before = [dict(row) for row in rows]
        before_payload = {
            "version": 1,
            "run_id": run_id,
            "case_db": str(case_db),
            "captured_at": _utc_now(),
            "draft_outbound": before,
            "planned": {
                "readjudicated": list(readjudicated),
                "pending_manager": list(pending_manager),
                "manager_chat_id": manager_chat_id,
            },
        }
        if dry_run:
            return {
                "readjudicated": len(readjudicated),
                "pending_manager": len(pending_manager),
                "dry_run": True,
            }

        _atomic_write_json(before_image_path, before_payload)
        now = int(time.time())
        before_by_id = {int(row["id"]): dict(row) for row in rows}
        with conn:
            for draft_id in readjudicated:
                changed = conn.execute(
                    "UPDATE draft_outbound SET state='readjudicated',recipient=NULL,updated_at=? "
                    "WHERE id=? AND state=?",
                    (now, draft_id, source_state),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(f"draft {draft_id} readjudication CAS failed")
                after = dict(
                    conn.execute(
                        "SELECT * FROM draft_outbound WHERE id=?", (draft_id,)
                    ).fetchone()
                )
                conn.execute(
                    "INSERT INTO ps_audit_log "
                    "(tenant_slug,actor_kind,actor,action,target_kind,target_id,"
                    "before_json,after_json,source_surface,summary,ts) "
                    "VALUES ('tgg','agent','christopher','clarification.readjudicated',"
                    "'draft_outbound',?,?,?,?,?,?)",
                    (
                        str(draft_id),
                        json.dumps(before_by_id[draft_id], sort_keys=True),
                        json.dumps(after, sort_keys=True),
                        f"bounded-backplay:{run_id}",
                        "stale clarification closed after amended-constitution readjudication",
                        now,
                    ),
                )
            for draft_id in pending_manager:
                changed = conn.execute(
                    "UPDATE draft_outbound SET state='pending_manager',recipient=?,updated_at=? "
                    "WHERE id=? AND state=?",
                    (manager_chat_id, now, draft_id, source_state),
                ).rowcount
                if changed != 1:
                    raise ConsumerError(f"draft {draft_id} pending-manager CAS failed")
                after = dict(
                    conn.execute(
                        "SELECT * FROM draft_outbound WHERE id=?", (draft_id,)
                    ).fetchone()
                )
                conn.execute(
                    "INSERT INTO ps_audit_log "
                    "(tenant_slug,actor_kind,actor,action,target_kind,target_id,"
                    "before_json,after_json,source_surface,summary,ts) "
                    "VALUES ('tgg','agent','christopher','clarification.pending_manager',"
                    "'draft_outbound',?,?,?,?,?,?)",
                    (
                        str(draft_id),
                        json.dumps(before_by_id[draft_id], sort_keys=True),
                        json.dumps(after, sort_keys=True),
                        f"bounded-backplay:{run_id}",
                        "excluded from auto-create; awaiting manager clarification",
                        now,
                    ),
                )
        return {
            "readjudicated": len(readjudicated),
            "pending_manager": len(pending_manager),
            "dry_run": False,
            "before_image": str(before_image_path),
        }
    finally:
        conn.close()


async def run_bounded_backplay(args: argparse.Namespace) -> int:
    """One-shot, capture-only execution over an existing inbox window."""
    dry_run = bool(args.dry_run)
    inbox_path = Path(args.inbox).resolve()
    state_db = Path(args.state_db).resolve()
    case_db = Path(args.case_db).resolve()
    config_path = Path(args.config).resolve()
    audit_path = Path(args.audit).resolve()
    chat_ids = tuple(dict.fromkeys(str(value) for value in (args.chat_id or ())))
    message_id_file = getattr(args, "message_id_file", None)
    exact_message_ids = (
        _read_id_file(Path(message_id_file).resolve()) if message_id_file else []
    )
    readjudicated_draft_file = getattr(args, "readjudicated_draft_id_file", None)
    pending_manager_draft_file = getattr(args, "pending_manager_draft_id_file", None)
    message_group_file = getattr(args, "message_group_file", None)
    readjudicated_draft_ids = (
        _read_int_id_file(Path(readjudicated_draft_file).resolve())
        if readjudicated_draft_file
        else []
    )
    pending_manager_draft_ids = (
        _read_int_id_file(Path(pending_manager_draft_file).resolve())
        if pending_manager_draft_file
        else []
    )
    cutoff = _parse_ingress_timestamp(args.cutoff) if args.cutoff else None
    expected_total = int(args.expected_total)
    batch_size = max(1, int(args.batch_size))
    run_id = str(args.run_id or f"bounded-{uuid.uuid4().hex[:12]}")

    lock_context = (
        contextlib.nullcontext()
        if dry_run
        else SingletonLock(Path(args.lock_file).resolve())
    )
    with lock_context:
        # A write-mode DurableInbox initializes schema metadata.  Construct it
        # only after exclusivity is held so an ordinary consumer holding the
        # same lock sees zero state change from a refused bounded run.
        inbox = DurableInbox(inbox_path, read_only=dry_run)
        if exact_message_ids:
            selected = inbox.message_id_selection(exact_message_ids)
        else:
            if cutoff is None:
                raise ConsumerError("bounded replay cutoff is required for chat-window mode")
            selected = inbox.bounded_window(chat_ids=chat_ids, cutoff=cutoff)
        statuses_before = inbox.window_statuses(selected)
        message_groups = (
            _read_message_groups(Path(message_group_file).resolve(), records=selected)
            if message_group_file
            else []
        )
        preflight = {
            "run_id": run_id,
            "mode": "dry-run" if dry_run else "capture-execute",
            "window": {
                "selection_mode": "message-id-file" if exact_message_ids else "chat-cutoff",
                "chat_ids": list(chat_ids),
                "cutoff": cutoff.isoformat() if cutoff is not None else None,
                "message_id_file": str(Path(message_id_file).resolve()) if message_id_file else None,
                "message_group_file": str(Path(message_group_file).resolve()) if message_group_file else None,
                "message_group_count": len(message_groups),
                "selected_message_ids": [record.message_id for record in selected],
            },
            "selection": _window_counts(selected, statuses_before),
        }
        # Deliberately emitted before reconciliation or any claim.
        print(json.dumps({"bounded_backplay_preclaim": preflight}, sort_keys=True))

        failures: list[str] = []
        processed: list[str] = []
        mutations: list[dict[str, Any]] = []
        audit: dict[str, Any] = {
            **preflight,
            "started_at": _utc_now(),
            "batch_size": batch_size,
            "failures": failures,
            "zero_real_sends": True,
            "outbound_sent": 0,
            "processed_message_ids": processed,
            "captured_outbound": 0,
            "captured_outbound_entries": [],
            "mutations": mutations,
        }
        case_before: dict[str, int] | None = None
        business_cursor_before: dict[str, int] | None = None
        row_total_before: int | None = None
        captured = 0
        try:
            if exact_message_ids:
                assert_message_id_selection(
                    selected,
                    expected_message_ids=exact_message_ids,
                    expected_total=expected_total,
                )
            else:
                assert_bounded_selection(
                    selected,
                    chat_ids=chat_ids,
                    cutoff=cutoff,
                    expected_total=expected_total,
                )
            audit["service_token_hash"] = assert_service_token_hash(
                Path(args.canonical_env).resolve(), args.service_token_env
            )
            case_before = _sqlite_table_counts(case_db)
            business_cursor_before = _business_audit_cursor(case_db)
            audit["case_counts_before"] = case_before
            audit["business_audit_before"] = business_cursor_before
            row_total_before = inbox.total()

            if getattr(args, "inject_source_evidence", False):
                source_before_image = getattr(args, "source_before_image", None)
                if not source_before_image:
                    raise ConsumerError(
                        "--source-before-image is required with --inject-source-evidence"
                    )
                audit["source_evidence_injection"] = _inject_bounded_source_evidence(
                    case_db,
                    selected,
                    before_image_path=Path(source_before_image).resolve(),
                    run_id=run_id,
                    dry_run=dry_run,
                )

            if readjudicated_draft_ids or pending_manager_draft_ids:
                draft_before_image = getattr(args, "draft_before_image", None)
                if not draft_before_image:
                    raise ConsumerError(
                        "--draft-before-image is required with draft transitions"
                    )
                audit["draft_transition_preview"] = _transition_readjudication_drafts(
                    case_db,
                    readjudicated_ids=readjudicated_draft_ids,
                    pending_manager_ids=pending_manager_draft_ids,
                    manager_chat_id=str(getattr(args, "manager_chat_id", "") or ""),
                    before_image_path=Path(draft_before_image).resolve(),
                    run_id=run_id,
                    dry_run=True,
                    source_state=str(getattr(args, "draft_source_state", "draft")),
                )

            if getattr(args, "requeue_selected", False):
                before_image = getattr(args, "before_image", None)
                if not before_image:
                    raise ConsumerError(
                        "--before-image is required with --requeue-selected"
                    )
                audit["selected_reset"] = inbox.requeue_selected_for_readjudication(
                    selected,
                    before_image_path=Path(before_image).resolve(),
                    run_id=run_id,
                    dry_run=dry_run,
                )

            reconciliation = inbox.reconcile_window_processing(
                selected, state_db, dry_run=dry_run
            )
            assert_no_window_orphans(reconciliation["predicted_statuses"])
            audit["reconciliation"] = {
                key: value
                for key, value in reconciliation.items()
                if key != "predicted_statuses"
            }

            if not dry_run:
                statuses = inbox.window_statuses(selected)
                pending = [
                    record
                    for record in selected
                    if statuses.get(str(record.seq)) == "pending"
                ]
                if message_groups:
                    pending_ids = {record.message_id for record in pending}
                    ordered_batches = [
                        (chat_id, [record for record in group if record.message_id in pending_ids])
                        for chat_id, group in message_groups
                    ]
                    ordered_batches = [item for item in ordered_batches if item[1]]
                else:
                    grouped: dict[str, list[InboxRecord]] = {}
                    for record in pending:
                        grouped.setdefault(record.chat_id, []).append(record)
                    ordered_batches = []
                    for chat_id, chat_records in sorted(
                        grouped.items(), key=lambda item: item[1][0].seq
                    ):
                        ordered_batches.extend(
                            (chat_id, chat_records[start : start + batch_size])
                            for start in range(0, len(chat_records), batch_size)
                        )
                with _runtime_config_context(config_path):
                    runner = _new_gateway_runner(config_path)
                    for chat_id, batch in ordered_batches:
                            inbox.claim(batch)
                            try:
                                result = await process_live_records(
                                    batch,
                                    config_path=config_path,
                                    state_db=state_db,
                                    persistent_session=False,
                                    runner=runner,
                                    defer_provider_errors=True,
                                )
                                batch_captures = _captured_audit_entries(
                                    result.get("captured_outbound") or [],
                                    batch=batch,
                                    handled=result.get("handled") or [],
                                    start_index=captured,
                                )
                                audit["captured_outbound_entries"].extend(batch_captures)
                                captured += len(batch_captures)
                                audit["captured_outbound"] = captured
                                provider_errors = [
                                    str(value)
                                    for value in result.get("provider_errors") or []
                                    if str(value).strip()
                                ]
                                if not provider_errors and not result.get("handled"):
                                    captured_error = _captured_provider_error(
                                        result.get("captured_outbound") or []
                                    )
                                    if captured_error:
                                        provider_errors.append(captured_error)
                                if provider_errors:
                                    raise ConsumerError(provider_errors[0])
                                submitted = {
                                    str(value)
                                    for value in result.get("submitted_message_ids") or []
                                }
                                expected = {record.message_id for record in batch}
                                if submitted != expected:
                                    raise ConsumerError(
                                        "bounded processor evidence mismatch"
                                    )
                                if int(result.get("outbound_sent") or 0) != 0:
                                    raise ConsumerError(
                                        "capture-only invariant violated: outbound sent"
                                    )
                                turn_for_message: dict[str, str] = {}
                                for group in result.get("handled") or []:
                                    for message_id in group.get("message_ids") or []:
                                        turn_for_message[str(message_id)] = str(
                                            group.get("turn_id") or ""
                                        )
                                if set(turn_for_message) - expected:
                                    raise ConsumerError(
                                        "bounded turn evidence escaped selected batch"
                                    )
                                inbox.finish_processed_batch(
                                    batch, turn_for_message=turn_for_message
                                )
                                processed.extend(record.message_id for record in batch)
                                mutations.append(
                                    {
                                        "status": "completed",
                                        "chat_id": chat_id,
                                        "message_ids": [
                                            record.message_id for record in batch
                                        ],
                                        "completed": len(turn_for_message),
                                        "skipped": len(batch) - len(turn_for_message),
                                        "captured_outbound": len(
                                            result.get("captured_outbound") or []
                                        ),
                                    }
                                )
                            except Exception as exc:
                                inbox.requeue(batch, reason=f"bounded-retry: {exc}")
                                mutations.append(
                                    {
                                        "status": "retryable",
                                        "chat_id": chat_id,
                                        "message_ids": [
                                            record.message_id for record in batch
                                        ],
                                        "error_class": type(exc).__name__,
                                        "error": str(exc),
                                    }
                                )
                                raise

            if readjudicated_draft_ids or pending_manager_draft_ids:
                audit["draft_transitions"] = _transition_readjudication_drafts(
                    case_db,
                    readjudicated_ids=readjudicated_draft_ids,
                    pending_manager_ids=pending_manager_draft_ids,
                    manager_chat_id=str(args.manager_chat_id or ""),
                    before_image_path=Path(args.draft_before_image).resolve(),
                    run_id=run_id,
                    dry_run=dry_run,
                    source_state=str(getattr(args, "draft_source_state", "draft")),
                )

            case_after = _sqlite_table_counts(case_db)
            row_total_after = inbox.total()
            if row_total_after != row_total_before:
                raise ConsumerError(
                    "bounded replay conservation failed: "
                    f"before={row_total_before} after={row_total_after}"
                )
            audit.update(
                {
                    "captured_outbound": captured,
                    "case_counts_after": case_after,
                    "case_count_delta": {
                        table: case_after.get(table, 0) - case_before.get(table, 0)
                        for table in sorted(set(case_before) | set(case_after))
                    },
                    "conservation": {
                        "inbox_rows_before": row_total_before,
                        "inbox_rows_after": row_total_after,
                        "preserved": True,
                    },
                    "business_mutations": _business_audit_delta(
                        case_db,
                        after_rowid=business_cursor_before["max_rowid"],
                    ),
                    "completed_at": _utc_now(),
                    "ok": True,
                }
            )
            _atomic_write_json(audit_path, audit)
            print(json.dumps(audit, sort_keys=True))
            return 0
        except Exception as exc:
            failures.append(type(exc).__name__)
            audit["error"] = str(exc)
            audit["captured_outbound"] = captured
            if case_before is not None:
                case_after = _sqlite_table_counts(case_db)
                audit["case_counts_after"] = case_after
                audit["case_count_delta"] = {
                    table: case_after.get(table, 0) - case_before.get(table, 0)
                    for table in sorted(set(case_before) | set(case_after))
                }
            if business_cursor_before is not None:
                audit["business_mutations"] = _business_audit_delta(
                    case_db,
                    after_rowid=business_cursor_before["max_rowid"],
                )
            if row_total_before is not None:
                row_total_after = inbox.total()
                audit["conservation"] = {
                    "inbox_rows_before": row_total_before,
                    "inbox_rows_after": row_total_after,
                    "preserved": row_total_after == row_total_before,
                }
                if row_total_after != row_total_before:
                    failures.append("inbox-row-conservation-failed")
            audit.update({"ok": False, "completed_at": _utc_now()})
            _atomic_write_json(audit_path, audit)
            if isinstance(exc, ConsumerError):
                raise
            raise ConsumerError(
                f"bounded replay failed: {type(exc).__name__}"
            ) from exc

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init-cursor", help="Create the source cursor once")
    init.add_argument("--source", required=True)
    init.add_argument("--cursor", required=True)
    init.add_argument("--position", choices=("start", "end"), required=True)

    run = sub.add_parser("run", help="Run the production standby/consumer loop")
    run.add_argument("--source", required=True)
    run.add_argument("--cursor", required=True)
    run.add_argument("--inbox", required=True)
    run.add_argument("--config", required=True)
    run.add_argument("--state-db", required=True)
    run.add_argument("--case-db", required=True)
    run.add_argument("--source-before-image-dir", required=True)
    run.add_argument("--processing-gate", required=True)
    run.add_argument("--lock-file", required=True)
    run.add_argument(
        "--activity-lock-file",
        help="shared with standalone release executor; held before an inbox claim",
    )
    run.add_argument("--status-file", required=True)
    run.add_argument("--poll-seconds", type=float, default=2.0)
    run.add_argument("--max-records", type=int, default=100)
    run.add_argument("--site-concurrency", type=int, default=4)
    run.add_argument("--chat-batch-size", type=int, default=25)
    run.add_argument("--retention-batch-size", type=int, default=25)
    run.add_argument("--source-projection-batch-size", type=int, default=100)
    run.add_argument("--once", action="store_true")

    fixture = sub.add_parser(
        "fixture", help="Run the same ingress path against isolated fixture state"
    )
    fixture.add_argument("--test-root", required=True)
    fixture.add_argument("--source", required=True)
    fixture.add_argument("--cursor", required=True)
    fixture.add_argument("--inbox", required=True)
    fixture.add_argument("--config", required=True)
    fixture.add_argument("--state-db", required=True)
    fixture.add_argument("--report", required=True)
    fixture.add_argument("--run-id", required=True)
    fixture.add_argument("--max-records", type=int, default=10)

    bounded = sub.add_parser(
        "bounded-backplay", help="Run one capture-only existing-inbox window"
    )
    bounded.add_argument("--inbox", required=True)
    bounded.add_argument("--config", required=True)
    bounded.add_argument("--state-db", required=True)
    bounded.add_argument("--case-db", required=True)
    bounded.add_argument("--canonical-env", required=True)
    bounded.add_argument(
        "--service-token-env", default="CHRISTOPHER_TGG_PS_SERVICE_TOKEN"
    )
    bounded.add_argument("--chat-id", action="append")
    bounded.add_argument("--cutoff")
    bounded.add_argument("--message-id-file")
    bounded.add_argument("--message-group-file")
    bounded.add_argument("--requeue-selected", action="store_true")
    bounded.add_argument("--before-image")
    bounded.add_argument("--inject-source-evidence", action="store_true")
    bounded.add_argument("--source-before-image")
    bounded.add_argument("--readjudicated-draft-id-file")
    bounded.add_argument("--pending-manager-draft-id-file")
    bounded.add_argument("--manager-chat-id")
    bounded.add_argument(
        "--draft-source-state", choices=("draft", "pending_manager"), default="draft"
    )
    bounded.add_argument("--draft-before-image")
    bounded.add_argument("--expected-total", type=int, required=True)
    bounded.add_argument("--batch-size", type=int, default=25)
    bounded.add_argument("--audit", required=True)
    bounded.add_argument("--lock-file", required=True)
    bounded.add_argument("--run-id")
    bounded.add_argument("--dry-run", action="store_true")

    canary = sub.add_parser(
        "management-document-canary",
        help="Run one typed PA-75 test-chat event without polling or cursor advance",
    )
    canary.add_argument("--event", required=True, help="immutable typed event JSON file")
    canary.add_argument("--inbox", required=True)
    canary.add_argument("--config", required=True)
    canary.add_argument("--receipt", required=True, help="immutable canary receipt path")
    canary.add_argument(
        "--activity-lock-file",
        help="shared with release executor while the real GatewayRunner owns the canary",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "init-cursor":
            cursor = initialize_cursor(
                Path(args.source), Path(args.cursor), position=args.position
            )
            print(json.dumps(asdict(cursor), sort_keys=True))
            return 0
        if args.command == "run":
            return asyncio.run(run_consumer(args))
        if args.command == "fixture":
            return asyncio.run(run_fixture(args))
        if args.command == "bounded-backplay":
            return asyncio.run(run_bounded_backplay(args))
        if args.command == "management-document-canary":
            return asyncio.run(run_management_document_canary(args))
        raise ConsumerError(f"unknown command {args.command}")
    except ConsumerError as exc:
        print(f"consumer error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
