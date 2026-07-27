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
import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
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
INBOX_SCHEMA_VERSION = 2


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


def _bridge_item(value: Any) -> dict[str, Any]:
    """Accept the durable bridge item or a shallow append-envelope wrapper."""
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
            return dict(candidate)
    raise ConsumerError("durable JSONL record has no bridge messageId/chatId item")


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
            if suffix in {".xlsx", ".csv", ".xlsm", ".xltm"}:
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
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','processing','completed','skipped','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    pa_turn_id TEXT,
                    last_error TEXT,
                    retained_media_count INTEGER NOT NULL DEFAULT 0,
                    retention_failures INTEGER NOT NULL DEFAULT 0,
                    retention_attempts INTEGER NOT NULL DEFAULT 0,
                    retention_state TEXT NOT NULL DEFAULT 'pending'
                        CHECK (retention_state IN ('pending','complete','bypassed','held')),
                    retention_last_error TEXT,
                    retention_updated_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ingress_events_status_seq_idx
                    ON ingress_events(status, seq);
                """
            )
            # Existing consumer DBs predate durable provider-outcome detail.
            reply_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(reply_deliveries)")
            }
            if "provider_outcome" not in reply_columns:
                conn.execute("ALTER TABLE reply_deliveries ADD COLUMN provider_outcome TEXT")
            ingress_columns = {
                str(row[1]) for row in conn.execute("PRAGMA table_info(ingress_events)")
            }
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

        staged: list[tuple[int, int, dict[str, Any]]] = []
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
                staged.append((start, end, item))
                last_message_id = message_id

        if not staged:
            return 0

        now = _utc_now()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for start, end, item in staged:
                conn.execute(
                    """
                    INSERT INTO ingress_events(
                        message_id,chat_id,source_device,source_inode,
                        start_offset,end_offset,raw_json,status,retention_state,
                        retention_updated_at,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,'pending',?,?,?,?)
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
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
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
            )
            for row in rows
        ]

    def pending_chat_batches(
        self,
        *,
        batch_size: int,
        priority_chats: frozenset[str] | set[str] | None = None,
        exclude_chats: frozenset[str] | set[str] | None = None,
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
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
                "FROM ingress_events WHERE status='pending' "
                "AND retention_state IN ('complete','bypassed') ORDER BY seq"
            ).fetchall()
        grouped: dict[str, list[InboxRecord]] = {}
        for row in rows:
            chat_id = str(row["chat_id"])
            if chat_id in excluded:
                continue
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
                )
            )
        ordered = sorted(grouped.items(), key=lambda item: item[1][0].seq)
        management = [item for item in ordered if item[0] in priority]
        site = [item for item in ordered if item[0] not in priority]
        return management, site

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
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
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
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
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
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
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

    def retention_candidates(self, *, limit: int) -> list[InboxRecord]:
        """Bounded pending-business work, with new rows ahead of retries."""
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT seq,message_id,chat_id,start_offset,end_offset,raw_json "
                "FROM ingress_events WHERE status='pending' "
                "AND retention_state IN ('pending','held') "
                "ORDER BY CASE retention_state WHEN 'pending' THEN 0 ELSE 1 END, "
                "COALESCE(retention_updated_at,created_at),seq LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        return [
            InboxRecord(
                seq=int(row["seq"]),
                message_id=str(row["message_id"]),
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=json.loads(row["raw_json"]),
            )
            for row in rows
        ]

    def retention_result(self, record: InboxRecord) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT retention_state,retained_media_count "
                "FROM ingress_events WHERE seq=?",
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
        }

    def record_retention(
        self,
        record: InboxRecord,
        *,
        retained: int | None = None,
        bypassed: bool = False,
        refusal: str | None = None,
        error: str | None = None,
    ) -> None:
        now = _utc_now()
        with self.connect() as conn:
            if error is not None:
                conn.execute(
                    "UPDATE ingress_events SET retention_state='held',"
                    "retention_attempts=retention_attempts+1,"
                    "retention_failures=retention_failures+1,"
                    "retention_last_error=?,retention_updated_at=? "
                    "WHERE seq=? AND retention_state IN ('pending','held')",
                    ((error or "retention failed")[:2000], now, record.seq),
                )
                return
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
        return {
            "retention_total": int(row[0]),
            "retention_failures": int(row[1]),
            "retention_attempts": int(row[2]),
            "retention_pending": int(row[3]),
            "retention_complete": int(row[4]),
            "retention_bypassed": int(row[5]),
            "retention_held": int(row[6]),
        }

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
        self, delivery_key: str, *, chat_id: str, reply_to_message_id: str | None
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
                    delivery_key,chat_id,reply_to_message_id,status,error,created_at
                ) VALUES(?,?,?,'undelivered','claimed-in-flight',?)
                """,
                (delivery_key, chat_id, reply_to_message_id, _utc_now()),
            )
            return cursor.rowcount == 1

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
        "min_free_percent": float(raw.get("min_free_percent", 20)),
    }


def _media_root_metrics(
    config_path: Path, *, inspect: bool, count_root: bool = True
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "media_root_count": 0,
        "media_root_bytes": 0,
        "media_volume_free_percent": None,
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
        "retention_hold": inbox.retention_last_error(),
    }


def _assert_media_headroom(config_path: Path, status: Mapping[str, Any]) -> None:
    config = _retention_config(config_path)
    if config is None:
        return
    free = status.get("media_volume_free_percent")
    if free is None:
        raise MediaRetentionError("media volume free space is unknown")
    if float(free) < float(config["min_free_percent"]):
        raise MediaRetentionError(
            "media volume free space below configured floor: "
            f"{float(free):.3f}% < {config['min_free_percent']:.3f}%"
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
        raise MediaRetentionError(f"retention source is not a supported image: {path.name}")
    mime, ext = detected
    declared = str(declared or "").split(";", 1)[0].strip().lower()
    if declared and "/" in declared and declared != mime:
        # image/jpg is a widespread non-standard spelling for image/jpeg.
        if not (declared == "image/jpg" and mime == "image/jpeg"):
            raise MediaRetentionError(
                f"PROVENANCE_DIVERGENCE: declared MIME {declared} != {mime}"
            )
    return mime, ext


def _contained_existing_file(value: Any, roots: Sequence[Path]) -> Path:
    if isinstance(value, Mapping):
        value = value.get("path") or value.get("filePath") or value.get("localPath") or value.get("url")
    text = str(value or "")
    if text.startswith("file://"):
        text = text[7:]
    try:
        candidate = Path(text).expanduser().resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise MediaRetentionError(f"media source is unavailable: {exc}") from exc
    if not candidate.is_file() or not any(candidate.is_relative_to(root) for root in roots):
        raise MediaRetentionError("media source escapes configured roots or is not a file")
    return candidate


def _event_media(item: Mapping[str, Any]) -> list[tuple[Any, str | None]]:
    values = item.get("mediaUrls") or item.get("media") or item.get("mediaPaths") or []
    if isinstance(values, (str, bytes, Mapping)):
        values = [values]
    if not isinstance(values, Sequence):
        raise MediaRetentionError("event media collection is not a list")
    result: list[tuple[Any, str | None]] = []
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
        result.append((value, declared))
    return result


def _event_spreadsheets(item: Mapping[str, Any]) -> list[tuple[Any, str]]:
    """Return spreadsheet-like documents with their provider-declared MIME."""
    values = item.get("mediaUrls") or item.get("media") or item.get("mediaPaths") or []
    if isinstance(values, (str, bytes, Mapping)):
        values = [values]
    if not isinstance(values, Sequence):
        raise MediaRetentionError("event media collection is not a list")
    declared_mimes = item.get("mediaMimes") or []
    if isinstance(declared_mimes, (str, bytes)):
        declared_mimes = [declared_mimes]
    result: list[tuple[Any, str]] = []
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
        if suffix not in {".xlsx", ".csv", ".xlsm", ".xltm"}:
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
                "PROVENANCE_DIVERGENCE: spreadsheet has no provider-declared MIME"
            )
        result.append((value, declared))
    return result


def _converge_retained_media(
    config_path: Path, *, operation: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    from types import SimpleNamespace

    from agent.pa_constitution import resolve_context
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
    if resolved is None:
        raise MediaRetentionError("media retention could not resolve client context")
    # Retention is pre-model ingress infrastructure, not a model-callable job
    # operation. Reuse the resolved tenant/auth/operation registry while
    # deliberately avoiding job-brief allow/deny scope (ops correctly denies
    # the model from invoking this internal write itself).
    internal_context = SimpleNamespace(
        constitution=resolved.constitution,
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
        raise MediaRetentionError(
            "media retention convergence returned an invalid Systems envelope"
        )
    return dict(result)


def _retain_record_media_impl(
    record: InboxRecord, *, config_path: Path
) -> dict[str, Any]:
    """Retain one event's images and converge its configured system ledger.

    Files land before the idempotent operation.  Therefore a crash after the
    rename or operation is safe to replay, while changed bytes/MIME at the
    same source ordinal fail closed.
    """
    config = _retention_config(config_path)
    if config is None:
        return {"retained": 0, "bytes": 0, "operation": False}
    item = _bridge_item(record.raw)
    spreadsheets = _event_spreadsheets(item)
    if spreadsheets:
        from tools.pa_business_tools import validate_tgg_spreadsheet

        for raw_path, declared_mime in spreadsheets:
            source = _contained_existing_file(raw_path, config["source_roots"])
            try:
                validate_tgg_spreadsheet(source, declared_mime=declared_mime)
            except ValueError as exc:
                raise PermanentMediaRefusal(str(exc)) from exc
        if not _event_media(item):
            return {
                "retained": 0,
                "bytes": 0,
                "operation": False,
                "validated_spreadsheets": len(spreadsheets),
            }
    media = _event_media(item)
    if not media:
        coarse_kind = str(
            item.get("mediaType") or item.get("mimeType") or ""
        ).split("/", 1)[0].strip().lower()
        if coarse_kind != "image":
            return {"retained": 0, "bytes": 0, "operation": False}
        if item.get("hasMedia") is True:
            raise MediaRetentionError(
                "mandatory inbound media has no resolvable capture path"
            )
        return {"retained": 0, "bytes": 0, "operation": False}
    chat_id = str(item.get("chatId") or record.chat_id)
    message_id = str(item.get("messageId") or record.message_id)
    if chat_id != record.chat_id or message_id != record.message_id:
        raise MediaRetentionError("PROVENANCE_DIVERGENCE: inbox/event identity mismatch")
    identity_digest = hashlib.sha256(
        (chat_id + "\0" + message_id).encode("utf-8")
    ).hexdigest()
    source_key = f"whatsapp-capture-v1:{identity_digest}"
    filename_prefix = hashlib.sha256(source_key.encode("utf-8")).hexdigest()[:24]
    root: Path = config["root"]
    root.mkdir(parents=True, exist_ok=True, mode=0o750)
    _assert_media_headroom(
        config_path,
        _media_root_metrics(config_path, inspect=True, count_root=False),
    )
    retained: list[dict[str, Any]] = []
    total_bytes = 0
    for ordinal, (raw_path, declared_mime) in enumerate(media):
        source = _contained_existing_file(raw_path, config["source_roots"])
        mime, ext = _validated_image_type(source, declared_mime)
        content = source.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        target = (root / f"{filename_prefix}_{ordinal}.{ext}").resolve()
        if not target.is_relative_to(root):
            raise MediaRetentionError("derived retention target escapes configured root")
        ordinal_candidates = list(root.glob(f"{filename_prefix}_{ordinal}.*"))
        if ordinal_candidates and target not in ordinal_candidates:
            raise MediaRetentionError(
                f"PROVENANCE_DIVERGENCE: retained ordinal {ordinal} MIME changed"
            )
        if target.exists():
            existing = target.read_bytes()
            existing_mime, _ = _validated_image_type(target, mime)
            if hashlib.sha256(existing).hexdigest() != digest or existing_mime != mime:
                raise MediaRetentionError(
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
    return {"retained": len(retained), "bytes": total_bytes, "operation": True}


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
        inbox.record_retention(record, error=f"media-retention-retry: {exc}")
        raise
    inbox.record_retention(
        record,
        retained=int(result["retained"]),
        bypassed=not bool(result.get("operation")),
    )
    return result


async def retain_pending_media(
    inbox: DurableInbox, *, config_path: Path, limit: int
) -> dict[str, int]:
    """Bounded capture-lane retention independent of business processing."""
    summary = {"examined": 0, "retained": 0, "bypassed": 0, "held": 0}
    for record in inbox.retention_candidates(limit=limit):
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
        if result.get("operation"):
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


async def process_live_records(
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    state_db: Path,
    runner: Any | None = None,
    defer_provider_errors: bool = False,
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
    # Production passes one long-lived runner into every per-chat task.  The
    # optional construction path preserves isolated callers and fixtures.
    runner = runner or GatewayRunner(load_gateway_config())
    run_id = f"live-drain-{uuid.uuid4().hex[:12]}"
    persistent_scope = os.environ.get(
        "TGG_PERSISTENT_CHAT_SESSION_SCOPE", "management"
    ).strip().lower()
    if persistent_scope not in {"off", "management", "all"}:
        raise ConsumerError(
            "TGG_PERSISTENT_CHAT_SESSION_SCOPE must be off, management, or all"
        )
    management_chats = _management_selector_chats(config_path)
    persistent_batch = persistent_scope == "all" or (
        persistent_scope == "management"
        and bool(records)
        and all(record.chat_id in management_chats for record in records)
    )
    replay_namespace = (
        "agent:live-drain:persistent-chat" if persistent_batch else None
    )
    replay_plan = ReplayPlan(
        platform="whatsapp",
        messages=tuple(record.raw for record in records),
        run_id=run_id,
        attempt_id=f"attempt-{uuid.uuid4().hex[:12]}",
        delivery_mode="capture",
        bypass_require_mention=True,
        bypass_auth=True,
        live_business_writes=True,
        source_path="durable-jsonl-consumer-live",
        # Every chat is one ongoing conversation. The stable prefix plus
        # SessionStore's existing platform/chat suffix yields one session
        # per chat. Rollout scope is management-only for the demo; setting
        # TGG_PERSISTENT_CHAT_SESSION_SCOPE=all extends the same mechanism
        # to site chats after backlog/autocompact validation.
        replay_namespace=replay_namespace,
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
        "provider_errors": provider_errors,
        "outbound_sent": 0,
        "submitted_message_ids": [record.message_id for record in records],
    }


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
    }


_CAPTURED_IMAGE_MEDIA_RE = re.compile(
    r"MEDIA:\s*(?P<path>(?:file://)?(?:~/|/)\S+)",
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
                "reply_to": send.get("reply_to"),
                "ordinal": ordinal,
            }
        )
    return expanded


def _resolve_captured_media_path(
    raw_path: Any, retention: Mapping[str, Any]
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
    if text.startswith("file://"):
        parsed = urlsplit(text)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise MediaRetentionError("captured media file URI is invalid")
        text = unquote(parsed.path)

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
    """Expand captured native image calls into one bounded item per file."""
    if not isinstance(entry, Mapping):
        return []
    kind = str(entry.get("kind") or "")
    if kind not in {"send_image_file", "send_multiple_images"}:
        return []
    args = list(entry.get("args") or [])
    kwargs = dict(entry.get("kwargs") or {})
    chat_id = str(kwargs.get("chat_id") or (args[0] if args else "") or "")
    reply_to = kwargs.get("reply_to")
    if reply_to is None and kind == "send_image_file" and len(args) > 3:
        reply_to = args[3]
    if not chat_id:
        return []
    if kind == "send_image_file":
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
    reassert_seconds: float = 20,
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
    for send in sends:
        chat_id = send["chat_id"]
        if chat_id not in management_chats:
            summary["suppressed"] += 1
            continue
        anchor = send["reply_to"] or newest_message_by_chat.get(chat_id)
        anchor_record = records_by_id.get(str(anchor)) if anchor else None
        try:
            anchor_item = _bridge_item(anchor_record.raw) if anchor_record else None
        except ConsumerError:
            anchor_item = None
        anchor_epoch = _timestamp_epoch_seconds(
            anchor_item.get("timestamp") if anchor_item else None
        )
        if (
            not anchor
            or anchor not in handled_message_ids
            or anchor_record is None
            or anchor_record.chat_id != chat_id
            or anchor_epoch is None
            or anchor_epoch < gate_epoch
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
            if retention is None:
                summary["suppressed"] += 1
                continue
            try:
                media_path = _resolve_captured_media_path(send["path"], retention)
                media_mime, _ = _validated_image_type(media_path, None)
                media_identity = hashlib.sha256(media_path.read_bytes()).hexdigest()
            except (OSError, MediaRetentionError):
                summary["suppressed"] += 1
                continue
            delivery_key = (
                f"media::{chat_id}::{anchor}::{media_identity}::{send['ordinal']}"
            )
        else:
            delivery_key = f"{chat_id}::{anchor or 'no-anchor'}"
        if not inbox.claim_reply_delivery(
            delivery_key, chat_id=chat_id, reply_to_message_id=anchor
        ):
            summary["duplicate"] += 1
            continue
        body_payload: dict[str, Any] = {
                "chatId": chat_id,
                "replyTo": {"messageId": anchor},
        }
        endpoint = "send"
        if send.get("send_kind", "text") == "media":
            endpoint = "send-media"
            body_payload.update({
                "filePath": str(media_path),
                "mediaType": "image",
            })
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
        except (URLError, TimeoutError, OSError, ValueError) as exc:
            print(
                f"reply delivery FAILED (transport): chat={chat_id} anchor={anchor} error={exc}",
                file=sys.stderr,
            )
            inbox.record_reply_delivery(
                delivery_key, status="undelivered", error=str(exc)[:300]
            )
            summary["undelivered"] += 1
    return summary


def _write_status(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_json(path, {"version": 1, "updated_at": _utc_now(), **dict(payload)})


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


async def _process_claimed_chat_batch(
    inbox: DurableInbox,
    records: Sequence[InboxRecord],
    *,
    config_path: Path,
    state_db: Path,
    gate_changed_at: str,
    runner: Any,
) -> None:
    """Claim and process exactly one chat batch through the shared runner."""
    if not records:
        return
    chat_ids = {record.chat_id for record in records}
    if len(chat_ids) != 1:
        raise ConsumerError(f"scheduler produced a mixed-chat batch: {sorted(chat_ids)}")
    inbox.claim(records)
    try:
        # Capture-lane retention normally completed while the row was pending.
        # This claimed-chat path remains the idempotent safety net: a row can
        # never reach Hermes until its durable retention outcome is complete.
        for record in records:
            await asyncio.to_thread(
                ensure_record_media_retained,
                inbox,
                record,
                config_path=config_path,
            )
        async with _management_typing_presence(
            records,
            config_path=config_path,
            gate_changed_at=gate_changed_at,
        ):
            result = await process_live_records(
                records,
                config_path=config_path,
                state_db=state_db,
                runner=runner,
            )
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
        # One transaction: a multi-turn batch can never land partially
        # terminal if the process exits between turn groups.
        inbox.finish_processed_batch(records, turn_for_message=turn_for_message)
    except asyncio.CancelledError:
        inbox.requeue(records, reason="graceful-cancellation")
        raise
    except MediaRetentionError as exc:
        inbox.requeue(records, reason=f"media-retention-retry: {exc}")
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


async def run_consumer(args: argparse.Namespace) -> int:
    config_path = Path(args.config).resolve()
    source = Path(args.source).resolve()
    cursor = Path(args.cursor).resolve()
    inbox = DurableInbox(Path(args.inbox).resolve())
    status_path = Path(args.status_file).resolve()
    gate_path = Path(args.processing_gate).resolve()
    state_db = Path(args.state_db).resolve()
    site_concurrency = max(1, int(getattr(args, "site_concurrency", 4)))
    chat_batch_size = max(1, int(getattr(args, "chat_batch_size", 25)))
    retention_batch_size = max(1, int(getattr(args, "retention_batch_size", 25)))

    with SingletonLock(Path(args.lock_file).resolve()):
        recovery = inbox.reconcile_orphan_processing(state_db)
        expected_total = inbox.assert_and_record_conservation()
        runner: Any | None = None
        tasks: dict[str, asyncio.Task[None]] = {}
        lanes: dict[str, str] = {}
        try:
            while True:
                done_chats = [chat_id for chat_id, task in tasks.items() if task.done()]
                for chat_id in done_chats:
                    task = tasks.pop(chat_id)
                    lanes.pop(chat_id, None)
                    try:
                        await task
                    except MediaRetentionError as exc:
                        # This chat's claimed rows are already pending again.
                        # Keep the other chat lanes and the daemon alive; the
                        # durable status exposes the hold until a retry clears it.
                        print(
                            f"media retention HELD/PENDING: chat={chat_id} error={exc}",
                            file=sys.stderr,
                        )

                config_enabled = processing_enabled(config_path)
                gate = processing_gate_state(gate_path)
                gate_enabled = gate["enabled"] is True
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
                            "state": "standby",
                            "processing_enabled": False,
                            "config_enabled": config_enabled,
                            "gate_enabled": gate_enabled,
                            "gate_generation": int(gate["generation"]),
                            "gate_change_run_id": gate.get("change_run_id"),
                            "pid": os.getpid(),
                            "source_opened": False,
                            "cursor_advanced": False,
                            "scheduler_mode": "per-chat-parallel",
                            "claim_stale_seconds": 1800,
                            "site_concurrency": site_concurrency,
                            "chat_batch_size": chat_batch_size,
                            "retention_batch_size": retention_batch_size,
                            "active_management_chats": [],
                            "active_site_chats": [],
                            "oldest_active_claim": inbox.oldest_processing_updated_at(),
                            "state_total": expected_total,
                            "startup_recovery": recovery,
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
                            "state": "held",
                            "processing_enabled": False,
                            "config_enabled": True,
                            "gate_enabled": True,
                            "gate_generation": int(gate["generation"]),
                            "pid": os.getpid(),
                            "source_opened": False,
                            "cursor_advanced": False,
                            "retention_hold": str(exc),
                            "inbox": inbox.counts(),
                        },
                    )
                    raise
                _write_status(
                    status_path,
                    {
                        **retention_status,
                        "state": (
                            "held-pending" if inbox.retention_last_error() else "running"
                        ),
                        "processing_enabled": True,
                        "config_enabled": True,
                        "gate_enabled": True,
                        "gate_generation": int(gate["generation"]),
                        "gate_change_run_id": gate.get("change_run_id"),
                        "pid": os.getpid(),
                        "source_opened": True,
                        "cursor_advanced": False,
                        "scheduler_mode": "per-chat-parallel",
                        "claim_stale_seconds": 1800,
                        "site_concurrency": site_concurrency,
                        "chat_batch_size": chat_batch_size,
                        "retention_batch_size": retention_batch_size,
                        "active_management_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "management"
                        ),
                        "active_site_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "site"
                        ),
                        "oldest_active_claim": inbox.oldest_processing_updated_at(),
                        "state_total": expected_total,
                        "startup_recovery": recovery,
                        "inbox": inbox.counts(),
                    },
                )

                before_stage = inbox.total()
                staged = inbox.stage_from_source(
                    source, cursor, max_records=args.max_records
                )
                while staged >= args.max_records:
                    staged = inbox.stage_from_source(
                        source, cursor, max_records=args.max_records
                    )
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

                # Retention is a capture-lane concern, not a model/business
                # concern.  It runs before demo-pause lane selection and never
                # claims or terminals the business row.
                retention_cycle = await retain_pending_media(
                    inbox,
                    config_path=config_path,
                    limit=retention_batch_size,
                )

                try:
                    priority_chats = _management_selector_chats(config_path)
                except Exception:
                    priority_chats = frozenset()
                demo_management_only = os.environ.get(
                    "TGG_DEMO_MANAGEMENT_ONLY", ""
                ).strip().lower() in {"1", "true", "yes", "on"}
                management_batches, site_batches = inbox.pending_chat_batches(
                    batch_size=chat_batch_size,
                    priority_chats=priority_chats,
                    exclude_chats=set(tasks),
                )
                gate_changed_at = str(gate.get("changed_at") or "")
                active_site = sum(1 for lane in lanes.values() if lane == "site")
                available_site = max(0, site_concurrency - active_site)
                selected_site_batches = (
                    [] if demo_management_only else site_batches[:available_site]
                )
                if (management_batches or selected_site_batches) and runner is None:
                    runner = _new_gateway_runner()

                # Reserved management capacity: these tasks never acquire or
                # wait for a site slot.  One task per chat preserves FIFO.
                for chat_id, records in management_batches:
                    tasks[chat_id] = asyncio.create_task(
                        _process_claimed_chat_batch(
                            inbox,
                            records,
                            config_path=config_path,
                            state_db=state_db,
                            gate_changed_at=gate_changed_at,
                            runner=runner,
                        )
                    )
                    lanes[chat_id] = "management"

                if not demo_management_only:
                    for chat_id, records in selected_site_batches:
                        tasks[chat_id] = asyncio.create_task(
                            _process_claimed_chat_batch(
                                inbox,
                                records,
                                config_path=config_path,
                                state_db=state_db,
                                gate_changed_at=gate_changed_at,
                                runner=runner,
                            )
                        )
                        lanes[chat_id] = "site"

                if args.once:
                    if tasks:
                        outcomes = await asyncio.gather(
                            *tasks.values(), return_exceptions=True
                        )
                        for outcome in outcomes:
                            if isinstance(outcome, MediaRetentionError):
                                print(
                                    f"media retention HELD/PENDING: {outcome}",
                                    file=sys.stderr,
                                )
                            elif isinstance(outcome, BaseException):
                                raise outcome
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
                            "state": (
                                "held-pending" if inbox.retention_last_error() else "running"
                            ),
                            "processing_enabled": True,
                            "config_enabled": True,
                            "gate_enabled": True,
                            "gate_generation": int(gate["generation"]),
                            "gate_change_run_id": gate.get("change_run_id"),
                            "pid": os.getpid(),
                            "staged": after_stage - before_stage,
                            "scheduler_mode": "per-chat-parallel",
                            "claim_stale_seconds": 1800,
                            "site_concurrency": site_concurrency,
                            "chat_batch_size": chat_batch_size,
                            "retention_batch_size": retention_batch_size,
                            "retention_cycle": retention_cycle,
                            "active_management_chats": [],
                            "active_site_chats": [],
                            "oldest_active_claim": inbox.oldest_processing_updated_at(),
                            "state_total": expected_total,
                            "startup_recovery": recovery,
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
                        "state": (
                            "held-pending" if inbox.retention_last_error() else "running"
                        ),
                        "processing_enabled": True,
                        "config_enabled": True,
                        "gate_enabled": True,
                        "gate_generation": int(gate["generation"]),
                        "gate_change_run_id": gate.get("change_run_id"),
                        "pid": os.getpid(),
                        "staged": after_stage - before_stage,
                        "scheduler_mode": "per-chat-parallel",
                        "claim_stale_seconds": 1800,
                        "site_concurrency": site_concurrency,
                        "chat_batch_size": chat_batch_size,
                        "retention_batch_size": retention_batch_size,
                        "retention_cycle": retention_cycle,
                        "active_management_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "management"
                        ),
                        "active_site_chats": sorted(
                            chat for chat, lane in lanes.items() if lane == "site"
                        ),
                        "oldest_active_claim": inbox.oldest_processing_updated_at(),
                        "state_total": expected_total,
                        "startup_recovery": recovery,
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
    conn = sqlite3.connect(case_db)
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
    run.add_argument("--processing-gate", required=True)
    run.add_argument("--lock-file", required=True)
    run.add_argument("--status-file", required=True)
    run.add_argument("--poll-seconds", type=float, default=2.0)
    run.add_argument("--max-records", type=int, default=100)
    run.add_argument("--site-concurrency", type=int, default=4)
    run.add_argument("--chat-batch-size", type=int, default=25)
    run.add_argument("--retention-batch-size", type=int, default=25)
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
        raise ConsumerError(f"unknown command {args.command}")
    except ConsumerError as exc:
        print(f"consumer error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
