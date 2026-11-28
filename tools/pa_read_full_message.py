"""``read_full_message`` — the full text of a carried message cut during compaction.

When compaction carries the conversation word for word (PA-648,
agent.pa_carried_conversation), a message longer than the configured limit
keeps only its start and end, with a note naming where the whole message is
stored: ``read_full_message session=<id> message=<row id>``.  This tool
returns that one stored message, exactly as it was stored.

Read-only and narrow by construction:

* the state database is opened read-only (``mode=ro``);
* the row is fetched by its primary key, and the lineage by the sessions
  primary key — no scans;
* only a conversation row is returned — a person's (user-role) message or
  the agent's own text reply (assistant-role, no tool calls) — and only from
  the calling session or one of its compaction ancestors (the same chat's
  lineage).  Tool output, tool calls and anything else are refused.

``session_search`` does not serve this: it returns model-written summaries,
not the exact text.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, List, Mapping, Optional

from tools.registry import registry, tool_error

_MAX_LINEAGE = 100

READ_FULL_MESSAGE_SCHEMA = {
    "name": "read_full_message",
    "description": (
        "Return the full, exact text of one earlier message in this chat (a "
        "person's message or your own reply), by the session and message id named "
        "in a cut note such as "
        "'[cut 10,900 characters ...; full text: read_full_message session=<id> "
        "message=<id>]'. Use it when you need the part of a long message that was cut."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "session_id": {
                "type": "string",
                "description": "The session id from the cut note.",
            },
            "message_id": {
                "type": "integer",
                "description": "The message id from the cut note.",
            },
        },
        "required": ["session_id", "message_id"],
    },
}


def _db_path() -> Path:
    from hermes_constants import get_hermes_home

    return get_hermes_home() / "state.db"


def _connect_read_only(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _lineage(conn: sqlite3.Connection, session_id: str) -> List[str]:
    chain: List[str] = []
    current: Optional[str] = session_id
    while current and current not in chain and len(chain) < _MAX_LINEAGE:
        chain.append(current)
        row = conn.execute(
            "SELECT parent_session_id FROM sessions WHERE id = ?", (current,)
        ).fetchone()
        current = row["parent_session_id"] if row is not None else None
    return chain


def read_full_message(
    session_id: Any,
    message_id: Any,
    *,
    current_session_id: Optional[str],
    db_path: Optional[Path] = None,
) -> str:
    requested_session = str(session_id or "").strip()
    try:
        requested_id = int(message_id)
    except (TypeError, ValueError):
        return tool_error("message_id must be the number from the cut note", success=False)
    if not requested_session:
        return tool_error("session_id is required", success=False)
    caller = str(current_session_id or "").strip()
    if not caller:
        return tool_error("no current session to check the request against", success=False)

    path = db_path or _db_path()
    if not Path(path).exists():
        return tool_error("message store unavailable", success=False)
    try:
        conn = _connect_read_only(Path(path))
    except sqlite3.Error as exc:
        return tool_error(f"message store unavailable: {exc}", success=False)
    try:
        if requested_session not in _lineage(conn, caller):
            return tool_error(
                "refused: that session is not part of this chat's conversation",
                success=False,
            )
        row = conn.execute(
            "SELECT id, session_id, role, content, tool_calls, timestamp FROM messages WHERE id = ?",
            (requested_id,),
        ).fetchone()
    except sqlite3.Error as exc:
        return tool_error(f"message store read failed: {exc}", success=False)
    finally:
        conn.close()

    if row is None or row["session_id"] != requested_session:
        return tool_error("no such message in that session", success=False)
    is_reply = row["role"] == "assistant" and str(row["tool_calls"] or "").strip() in ("", "[]")
    if row["role"] != "user" and not is_reply:
        return tool_error(
            "refused: only a person's message or your own text reply can be read this way",
            success=False,
        )
    content = row["content"]
    try:
        from hermes_state import SessionDB

        content = SessionDB._decode_content(content)
    except Exception:
        pass
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False)
    return json.dumps(
        {
            "session_id": row["session_id"],
            "message_id": row["id"],
            "role": row["role"],
            "sent_at": row["timestamp"],
            "content": content,
        },
        ensure_ascii=False,
    )


def _handle_read_full_message(args: Mapping[str, Any], **kwargs: Any) -> str:
    return read_full_message(
        args.get("session_id"),
        args.get("message_id"),
        current_session_id=kwargs.get("session_id"),
    )


registry.register(
    name="read_full_message",
    toolset="pa-message-recall",
    schema=READ_FULL_MESSAGE_SCHEMA,
    handler=_handle_read_full_message,
    emoji="📜",
)
