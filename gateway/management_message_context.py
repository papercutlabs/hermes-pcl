"""Select retained passive Management messages for a later addressed turn.

This module belongs to the durable consumer adapter.  It only reads the
adapter's existing inbox and PA turn evidence; it does not claim records,
change terminal accounting, or open a model turn.  The selected records are
passed through the existing trusted ``process_live_records(replay_messages=)``
seam, which is also used by the Management continuation adapter.
"""

from __future__ import annotations

import copy
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


PASSIVE_SKIP_REASON = "PRIORITY_DIRECT_TRIGGER_NOT_RECOGNIZED"


@dataclass(frozen=True)
class ContextRecord:
    """Read-only projection of an already-terminal durable inbox row."""

    seq: int
    message_id: str
    chat_id: str
    start_offset: int
    end_offset: int
    raw: dict[str, Any]
    retention_state: str | None = None
    retention_quarantined: bool = False
    # Set by DurableInbox.with_retention_view, as for a claimed row (PA-203).
    retention_attempts: int = 0
    retention_file_error: str | None = None


def _bridge_item(value: Mapping[str, Any]) -> Mapping[str, Any]:
    candidates = [value]
    for key in ("normalized", "message", "event", "payload", "data"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested)
    for candidate in candidates:
        if candidate.get("messageId") and candidate.get("chatId"):
            return candidate
    raise ValueError("durable inbox row has no bridge message identity")


def _completed_message_refs(state_db: Path, *, chat_id: str) -> set[str]:
    """Return refs already supplied to a completed turn in this chat."""
    if not state_db.is_file():
        return set()
    conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "pa_turns" not in tables:
            return set()
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(pa_turns)")
        }
        if not {"message_refs_json", "turn_status"}.issubset(columns):
            return set()
        sql = (
            "SELECT message_refs_json FROM pa_turns "
            "WHERE turn_status='completed'"
        )
        params: tuple[Any, ...] = ()
        if "chat_id" in columns:
            sql += " AND chat_id=?"
            params = (chat_id,)
        refs: set[str] = set()
        for row in conn.execute(sql, params):
            try:
                values = json.loads(row[0] or "[]")
            except (TypeError, ValueError):
                continue
            if isinstance(values, list):
                refs.update(str(value) for value in values if value)
        return refs
    finally:
        conn.close()


def previous_skipped_context(
    *,
    inbox_db: Path,
    state_db: Path,
    current_records: Sequence[Any],
    limit: int = 25,
) -> tuple[ContextRecord, ...]:
    """Return trailing passive context since the prior handled boundary.

    Selection is same-chat and source-ordered.  It stops at the prior handled
    boundary or at a record with a different terminal reason.  A completed
    turn reference is the durable marker that a skipped row has already been
    supplied as context.  The caller supplies the existing durable-drain batch
    limit; this adapter adds no separate queue or scheduling width.
    """
    if not current_records:
        return ()
    chats = {str(record.chat_id) for record in current_records}
    if len(chats) != 1:
        raise ValueError("context selection requires one current chat")
    bounded_limit = max(0, int(limit))
    if bounded_limit <= 0:
        return ()

    ordered_current = sorted(current_records, key=lambda record: int(record.seq))
    before_seq = int(ordered_current[0].seq)
    chat_id = next(iter(chats))
    already_used = _completed_message_refs(state_db, chat_id=chat_id)

    conn = sqlite3.connect(f"file:{inbox_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT e.seq,e.message_id,e.chat_id,e.start_offset,e.end_offset,"
            "e.raw_json,e.status,e.last_error,e.retention_state,"
            "q.ingress_seq IS NOT NULL AS retention_quarantined "
            "FROM ingress_events e LEFT JOIN media_retention_quarantine q "
            "ON q.ingress_seq=e.seq AND q.status='quarantined' "
            "WHERE e.chat_id=? AND e.seq<? ORDER BY e.seq DESC LIMIT ?",
            (chat_id, before_seq, bounded_limit + 1),
        ).fetchall()
    finally:
        conn.close()

    selected: list[ContextRecord] = []
    for row in rows:
        message_id = str(row["message_id"])
        if (
            row["status"] != "skipped"
            or row["last_error"] != PASSIVE_SKIP_REASON
            or row["retention_state"] not in {"complete", "bypassed"}
            or message_id in already_used
        ):
            break
        raw = json.loads(row["raw_json"])
        selected.append(
            ContextRecord(
                seq=int(row["seq"]),
                message_id=message_id,
                chat_id=str(row["chat_id"]),
                start_offset=int(row["start_offset"]),
                end_offset=int(row["end_offset"]),
                raw=raw,
                retention_state=str(row["retention_state"]),
                retention_quarantined=bool(row["retention_quarantined"]),
            )
        )
        if len(selected) >= bounded_limit:
            break
    return tuple(reversed(selected))


def contextual_replay_message(
    rendered_messages: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    """Fold ordered context and its trigger into one native replay event.

    Keeping the addressed message's original timestamp avoids changing the
    conversation chronology.  Folding happens above ReplayPlan so even
    context older than WhatsApp's coalescing debounce cannot open a separate
    model turn.  Source message ids and attachment paths remain explicit on
    the single event.
    """
    if not rendered_messages:
        return ()
    items = [_bridge_item(message) for message in rendered_messages]
    chat_ids = {str(item.get("chatId") or "") for item in items}
    if len(chat_ids) != 1 or "" in chat_ids:
        raise ValueError("contextual replay requires one identified chat")

    result = copy.deepcopy(dict(items[-1]))
    source_ids: list[str] = []
    media_urls: list[Any] = []
    media_mimes: list[str] = []
    lines = [f"WhatsApp context bundle ({len(items)} messages)", ""]
    bot_ids: list[str] = []
    mentioned_ids: list[str] = []

    for index, item in enumerate(items, start=1):
        message_id = str(item.get("messageId") or "")
        if not message_id:
            raise ValueError("contextual replay message has no message id")
        source_ids.append(message_id)
        timestamp = item.get("_pa_local_time") or item.get("timestamp")
        sender = item.get("senderName") or item.get("senderId")
        prefix = f"{index}. "
        if timestamp:
            prefix += f"[{timestamp}] "
        if sender:
            prefix += f"{sender}: "
        body = str(item.get("body") or item.get("text") or "")
        if not body and item.get("hasMedia"):
            body = "[media received]"
        lines.append((prefix + body).rstrip())
        urls = item.get("mediaUrls") or []
        if isinstance(urls, (str, bytes, Mapping)):
            urls = [urls]
        declared = item.get("mediaMimes") or []
        if isinstance(declared, (str, bytes)):
            declared = [declared]
        for media_index, url in enumerate(urls if isinstance(urls, Sequence) else []):
            media_urls.append(copy.deepcopy(url))
            mime = (
                str(declared[media_index])
                if media_index < len(declared) and declared[media_index]
                else str(item.get("mediaType") or "")
            )
            media_mimes.append(mime)
        if urls:
            lines.append(f"   attachments: {len(urls)}")
        lines.append(f"   source_message_id: {message_id}")
        for value in item.get("botIds") or []:
            if str(value) not in bot_ids:
                bot_ids.append(str(value))
        for value in item.get("mentionedIds") or []:
            if str(value) not in mentioned_ids:
                mentioned_ids.append(str(value))

    result["body"] = "\n".join(lines)
    result["sourceMessageIds"] = source_ids
    result["botIds"] = bot_ids
    result["mentionedIds"] = mentioned_ids
    result["mediaUrls"] = media_urls
    result["mediaMimes"] = media_mimes
    result["hasMedia"] = bool(media_urls)
    if media_urls:
        kinds = {mime.split("/", 1)[0].lower() for mime in media_mimes if mime}
        result["mediaType"] = (
            next(iter(kinds)) if len(kinds) == 1 else "document"
        )
    return (result,)
