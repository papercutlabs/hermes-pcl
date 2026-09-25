"""Durable return path for background work in Christopher's durable consumer.

PA-203 round 2 (Teren, 25 Sep 2026).  Background work, a ``delegate_task``
with ``background=true`` or a workflow run, finishes after the turn that
started it has ended.  Its completion is pushed onto the in-memory
``process_registry.completion_queue``, which only the gateway's
``_async_delegation_watcher`` drains, and the durable JSONL consumer never
starts that watcher.  This module is the consumer's own drain:

1. **Dispatch is durable.**  The consumer registers a sink with
   ``tools.async_delegation``; each background dispatch is written as a
   ``running`` row in the consumer's inbox database *before* the helper
   starts, with its chat, conversation, the request message IDs and a time
   limit.  A dispatch that cannot be written is refused, not started.
2. **Completion is durable.**  The consumer loop drains the queue; each
   completion turns its row ``pending`` with the result, in one transaction.
3. **A restart loses no result.**  At start-up a row still ``running``
   belongs to a helper the restart killed: it becomes a ``pending``
   completion whose outcome says the helper was interrupted.  A workflow run
   is instead resumed, once processing is enabled.  A finished turn's reply
   is stored (``delivering``) before any send, so delivery is retried after a
   failure or restart; each send is claimed first, so none repeats.  One
   window remains: a turn that finished just before a restart, before its
   reply was stored, is recorded ``failed`` rather than re-run, because a
   re-run could repeat the turn's writes.
4. **Failure and time limits still complete.**  A helper that raises already
   completes with an error; one still running past its time limit is
   interrupted and completes as ``timeout``.
5. **Each completion is one ordinary turn** in the originating conversation.
   The turn input carries the original request messages verbatim and the
   result.  Its reply quotes the original request and is delivered through
   the ordinary management delivery path under its own delivery key.

Nothing here checks what the turn says or whether it sent anything.
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

COMPLETION_SCHEMA = """
CREATE TABLE IF NOT EXISTS background_completions (
    completion_id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('delegation','workflow')),
    chat_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    origin_json TEXT NOT NULL,
    task_json TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('running','pending','processing','delivering','completed','failed')),
    outcome TEXT,
    result_json TEXT,
    outbound_json TEXT,
    handled_json TEXT,
    deadline_at REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    turn_id TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS background_completions_status_idx
    ON background_completions(status, created_at);
"""

INTERNAL_PREFIX = "background-completion:"
MAX_RESULT_CHARS = 60000
MAX_TURN_ATTEMPTS = 3
_COMPLETION_EVENT_TYPES = {"async_delegation", "workflow_run"}


class CompletionError(RuntimeError):
    """A background completion could not be recorded or turned into a turn."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def internal_message_id(completion_id: str) -> str:
    return f"{INTERNAL_PREFIX}{completion_id}"


class CompletionStore:
    """The ``background_completions`` table in the consumer's inbox database."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        with self._connect() as conn:
            conn.executescript(COMPLETION_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.row_factory = sqlite3.Row
        return conn

    # ── writes ──────────────────────────────────────────────────────────
    def record_dispatch(self, record: Mapping[str, Any], *, source: str = "delegation") -> None:
        """Write a ``running`` row before the background work starts.

        Refuses (raises) when the work has no conversation to return to.
        """
        origin = dict(record.get("origin") or {})
        chat_id = str(origin.get("chat_id") or "").strip()
        session_key = str(origin.get("session_key") or "").strip()
        if not chat_id or not session_key:
            raise CompletionError("no originating chat and conversation")
        source = str(record.get("source") or source)
        # Work started inside a completion turn answers the same original
        # request: record Krisleen's inbound message ids, not the internal
        # completion id the turn was driven by (r2d replay s3).
        started_from = [str(v) for v in origin.get("source_message_ids") or [] if v]
        origin["source_message_ids"] = self.original_source_ids(started_from)
        if started_from != origin["source_message_ids"]:
            origin["started_from_message_ids"] = started_from
        completion_id = str(record.get("delegation_id") or record.get("run_id") or "").strip()
        if not completion_id:
            raise CompletionError("background work has no identity")
        task = {
            key: record.get(key)
            for key in ("goal", "context", "role", "model", "toolsets", "title")
            if record.get(key) is not None
        }
        now = _now()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO background_completions(completion_id,source,chat_id,session_key,"
                "origin_json,task_json,status,deadline_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,'running',?,?,?)",
                (
                    completion_id, source, chat_id, session_key,
                    json.dumps(origin, ensure_ascii=False, sort_keys=True),
                    json.dumps(task, ensure_ascii=False, sort_keys=True, default=str),
                    record.get("deadline_at"), now, now,
                ),
            )

    def record_completion(self, completion_id: str, *, outcome: str, result: Mapping[str, Any]) -> bool:
        """Turn a ``running`` row ``pending`` with its result.

        Returns False when there is no running row (already completed by a
        time limit or a restart, or dispatched outside this consumer).
        """
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE background_completions SET status='pending',outcome=?,result_json=?,"
                "updated_at=? WHERE completion_id=? AND status='running'",
                (
                    outcome,
                    json.dumps(dict(result), ensure_ascii=False, sort_keys=True, default=str),
                    _now(), completion_id,
                ),
            )
            return cur.rowcount == 1

    def expire_overdue(self, *, now: float | None = None) -> list[str]:
        """Complete every running row past its time limit as ``timeout``."""
        now = time.time() if now is None else now
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT completion_id,deadline_at FROM background_completions "
                "WHERE status='running' AND deadline_at IS NOT NULL AND deadline_at < ?",
                (now,),
            ).fetchall()
        expired = []
        for row in rows:
            if self.record_completion(
                row["completion_id"],
                outcome="timeout",
                result={
                    "status": "timeout",
                    "error": "The background work did not finish within its time limit and was stopped.",
                },
            ):
                expired.append(str(row["completion_id"]))
        return expired

    def recover_after_restart(
        self,
        completed_turn_refs: Mapping[str, str],
        *,
        keep_running: set[str] | frozenset[str] = frozenset(),
    ) -> dict[str, int]:
        """Start-up only: nothing a previous process owned is lost.

        ``keep_running`` names work that resumes in this process (workflow
        runs); everything else still running died with the old process.
        """
        summary = {"interrupted": 0, "processing_completed": 0, "processing_retried": 0,
                   "resumed": len(keep_running)}
        now = _now()
        with self._connect() as conn:
            for row in conn.execute(
                "SELECT completion_id FROM background_completions WHERE status='running'"
            ).fetchall():
                if row["completion_id"] in keep_running:
                    continue
                conn.execute(
                    "UPDATE background_completions SET status='pending',outcome='interrupted',"
                    "result_json=?,updated_at=? WHERE completion_id=?",
                    (
                        json.dumps({
                            "status": "interrupted",
                            "error": "The service restarted before this background work finished; its result was lost and it did not complete.",
                        }),
                        now, row["completion_id"],
                    ),
                )
                summary["interrupted"] += 1
            for row in conn.execute(
                "SELECT completion_id FROM background_completions WHERE status='processing'"
            ).fetchall():
                turn_id = completed_turn_refs.get(internal_message_id(row["completion_id"]))
                if turn_id:
                    # The turn finished but its reply was never handed to
                    # delivery.  Re-running the turn could repeat its writes,
                    # so the loss is recorded rather than hidden (review item 9).
                    conn.execute(
                        "UPDATE background_completions SET status='failed',turn_id=?,updated_at=?,"
                        "last_error='the completion turn finished but the process stopped before "
                        "its reply was handed to delivery' WHERE completion_id=?",
                        (turn_id, now, row["completion_id"]),
                    )
                    summary["processing_completed"] += 1
                else:
                    conn.execute(
                        "UPDATE background_completions SET status='pending',updated_at=?,"
                        "last_error='prior process stopped before the completion turn finished' "
                        "WHERE completion_id=?",
                        (now, row["completion_id"]),
                    )
                    summary["processing_retried"] += 1
        return summary

    def claim(self, completion_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE background_completions SET status='processing',attempts=attempts+1,"
                "updated_at=? WHERE completion_id=? AND status='pending'",
                (_now(), completion_id),
            )
            if cur.rowcount == 1:
                return True
            # A reply already handed to delivery is retried as delivery only.
            cur = conn.execute(
                "UPDATE background_completions SET attempts=attempts+1,updated_at=? "
                "WHERE completion_id=? AND status='delivering'",
                (_now(), completion_id),
            )
            return cur.rowcount == 1

    def hand_to_delivery(
        self, completion_id: str, *, turn_id: str | None,
        captured_outbound: Sequence[Mapping[str, Any]], handled: Sequence[Mapping[str, Any]],
    ) -> None:
        """Keep the finished turn's reply durably before any send is tried."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE background_completions SET status='delivering',turn_id=?,outbound_json=?,"
                "handled_json=?,last_error=NULL,updated_at=? WHERE completion_id=?",
                (
                    turn_id,
                    json.dumps([dict(x) for x in captured_outbound], ensure_ascii=False, default=str),
                    json.dumps([dict(x) for x in handled], ensure_ascii=False, default=str),
                    _now(), completion_id,
                ),
            )

    def finish(self, completion_id: str, *, turn_id: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE background_completions SET status='completed',turn_id=?,last_error=NULL,"
                "updated_at=? WHERE completion_id=?",
                (turn_id, _now(), completion_id),
            )

    def release(self, completion_id: str, *, error: str) -> str:
        """Return a failed attempt for retry, or fail it after the limit."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT attempts,status FROM background_completions WHERE completion_id=?",
                (completion_id,),
            ).fetchone()
            terminal = row is not None and int(row["attempts"]) >= MAX_TURN_ATTEMPTS
            retry = "delivering" if row is not None and row["status"] == "delivering" else "pending"
            status = "failed" if terminal else retry
            conn.execute(
                "UPDATE background_completions SET status=?,last_error=?,updated_at=? "
                "WHERE completion_id=?",
                (status, error[:1000], _now(), completion_id),
            )
            return status

    # ── reads ───────────────────────────────────────────────────────────
    def original_source_ids(self, message_ids: Sequence[str]) -> list[str]:
        """Follow internal completion ids back to the original inbound ids.

        A completion turn is driven by ``background-completion:<id>``; work it
        starts inherits that completion's source, at any depth of nesting.
        Composite ``id1+id2`` references are split.  Order is kept.
        """
        resolved: list[str] = []
        seen: set[str] = set()
        queue = [str(value) for value in message_ids if value]
        while queue:
            for part in queue.pop(0).split("+"):
                part = part.strip()
                if not part or part in seen:
                    continue
                seen.add(part)
                if part.startswith(INTERNAL_PREFIX):
                    parent = self.get(part[len(INTERNAL_PREFIX):])
                    if parent is not None:
                        parent_origin = json.loads(parent.get("origin_json") or "{}")
                        queue.extend(str(v) for v in parent_origin.get("source_message_ids") or [] if v)
                    continue
                resolved.append(part)
        return resolved

    def running(self, *, source: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM background_completions WHERE status='running'"
        params: tuple[Any, ...] = ()
        if source:
            query += " AND source=?"
            params = (source,)
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(query, params)]

    def get(self, completion_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM background_completions WHERE completion_id=?", (completion_id,)
            ).fetchone()
        return dict(row) if row else None

    def next_pending(self, *, exclude_chats: set[str] | frozenset[str] = frozenset()) -> dict[str, Any] | None:
        with self._connect() as conn:
            for row in conn.execute(
                "SELECT * FROM background_completions WHERE status IN ('pending','delivering') "
                "ORDER BY created_at"
            ):
                if row["chat_id"] not in exclude_chats:
                    return dict(row)
        return None

    def counts(self) -> dict[str, int]:
        counts = {name: 0 for name in ("running", "pending", "processing", "delivering", "completed", "failed")}
        with self._connect() as conn:
            for row in conn.execute(
                "SELECT status,COUNT(*) AS n FROM background_completions GROUP BY status"
            ):
                counts[str(row["status"])] = int(row["n"])
        return counts


class ConsumerCompletionSink:
    """The sink registered with ``tools.async_delegation`` by the consumer."""

    def __init__(self, store: CompletionStore):
        self.store = store

    def record_dispatch(self, record: Mapping[str, Any]) -> None:
        self.store.record_dispatch(record, source=str(record.get("source") or "delegation"))


def install_sink(store: CompletionStore) -> Any:
    """Register the consumer's sink; returns the previous one for restoring."""
    from tools.async_delegation import set_durable_sink

    return set_durable_sink(ConsumerCompletionSink(store))


def start_for_consumer(inbox_db: Path, state_db: Path) -> tuple[CompletionStore, dict[str, Any], Any]:
    """Consumer start-up: recover, register the sink, resume workflow runs.

    Returns the store, a recovery summary for the status file, and a function
    that unregisters the sink at shutdown.
    """
    from gateway.durable_jsonl_consumer import _completed_turn_refs
    from tools.async_delegation import set_durable_sink

    store = CompletionStore(inbox_db)
    try:
        from tools.pa_workflow_tool import prepare_workflow_recovery
    except ImportError:  # pragma: no cover - workflow adapter absent
        prepare_workflow_recovery = None
    # Results already queued in this process are kept before anything
    # still running is treated as lost.
    drain_completion_queue(store)
    keep: set[str] = set()
    if prepare_workflow_recovery is not None:
        keep = set(prepare_workflow_recovery(store))
    summary = dict(store.recover_after_restart(_completed_turn_refs(state_db), keep_running=keep))
    previous = install_sink(store)
    # Workflow runs resume only while processing is enabled: set_processing()
    # does it on the first enabled loop (review item 8).

    def restore() -> None:
        set_durable_sink(previous)

    return store, summary, restore


def maintain(store: CompletionStore) -> dict[str, int]:
    """Each consumer loop: drain the queue, then apply time limits."""
    recorded = drain_completion_queue(store)
    expired = store.expire_overdue()
    if expired:
        from tools.async_delegation import interrupt_delegation
        from tools.pa_workflow_tool import stop_workflow

        for completion_id in expired:
            # A helper is interrupted; a workflow run stops before its next
            # helper and ends failed (review item 8 and B3).
            if not interrupt_delegation(completion_id):
                stop_workflow(completion_id, "timeout")
    return {"recorded": recorded, "expired": len(expired)}


def set_processing(store: CompletionStore, enabled: bool) -> dict[str, int]:
    """Each consumer loop: workflow runs follow Christopher's pause state.

    Paused: every active run stops before its next helper and stays
    resumable.  Enabled: every unfinished run with a waiting completion that
    is not already executing here resumes from its stored results.
    """
    from tools.pa_workflow_tool import (
        active_workflow_ids, pause_all_workflows, resume_workflow_runs,
    )

    if not enabled:
        return {"paused": pause_all_workflows(), "resumed": 0}
    active = active_workflow_ids()
    waiting = [
        str(row["completion_id"]) for row in store.running(source="workflow")
        if str(row["completion_id"]) not in active
    ]
    return {"paused": 0, "resumed": len(resume_workflow_runs(waiting)) if waiting else 0}


def drain_completion_queue(store: CompletionStore, completion_queue: Any = None) -> int:
    """Move queued background completions into the durable store.

    Events this consumer does not own are put back.  Returns how many were
    recorded.
    """
    if completion_queue is None:
        from tools.process_registry import process_registry

        completion_queue = process_registry.completion_queue
    mine: list[dict[str, Any]] = []
    requeue: list[Any] = []
    while not completion_queue.empty():
        try:
            evt = completion_queue.get_nowait()
        except Exception:
            break
        if isinstance(evt, dict) and evt.get("type") in _COMPLETION_EVENT_TYPES:
            mine.append(evt)
        else:
            requeue.append(evt)
    for evt in requeue:
        completion_queue.put(evt)
    recorded = 0
    for evt in mine:
        completion_id = str(evt.get("delegation_id") or evt.get("run_id") or "")
        outcome = str(evt.get("status") or "completed")
        result = {
            key: evt.get(key)
            for key in (
                "status", "summary", "error", "output", "counts", "api_calls",
                "duration_seconds", "exit_reason", "run_id",
            )
            if evt.get(key) is not None
        }
        if store.record_completion(completion_id, outcome=outcome, result=result):
            recorded += 1
    return recorded


def _clip(text: str, limit: int = MAX_RESULT_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n[... {len(text) - limit} more characters not shown]"


_OUTCOME_WORDS = {
    "completed": "finished",
    "error": "failed",
    "failed": "failed",
    "timeout": "timed out",
    "interrupted": "was interrupted",
}


def completion_event(
    row: Mapping[str, Any], originals: Sequence[Any]
) -> tuple[str, dict[str, Any]]:
    """Build the internal turn input: original request verbatim plus the result."""
    from gateway.durable_jsonl_consumer import _bridge_item

    completion_id = str(row["completion_id"])
    message_id = internal_message_id(completion_id)
    task = json.loads(row.get("task_json") or "{}")
    result = json.loads(row.get("result_json") or "{}")
    items = [_bridge_item(record.raw) for record in originals]
    request_lines = []
    for item in items:
        who = str(item.get("senderName") or item.get("senderId") or "unknown sender")
        stamp = item.get("timestamp")
        request_lines.append(
            f"From {who} at {stamp}, message {item.get('messageId')}:\n\"\"\"\n"
            f"{item.get('body') or ''}\n\"\"\""
        )
    outcome = str(row.get("outcome") or result.get("status") or "completed")
    if row.get("source") == "workflow":
        label = f"Workflow run {result.get('run_id') or completion_id}"
        if task.get("title"):
            label += f" ({task['title']})"
        payload = {
            key: result.get(key)
            for key in ("counts", "output", "error")
            if result.get(key) is not None
        }
        result_text = json.dumps(payload, ensure_ascii=False, indent=1, default=str)
    else:
        label = f"Background helper {completion_id}: {task.get('goal') or ''}"
        result_text = str(result.get("summary") or result.get("error") or "(no result text)")
        if result.get("summary") and result.get("error"):
            result_text += f"\nError: {result['error']}"
    body = (
        "[Internal: background work you started in this conversation has "
        f"{_OUTCOME_WORDS.get(outcome, outcome)}. This is not a new client message.]\n\n"
        "The request it was started for, verbatim:\n"
        + ("\n\n".join(request_lines) or "(the original request message is not available)")
        + f"\n\n{label}\nOutcome: {outcome}\nResult:\n{_clip(result_text)}\n\n"
        "Finish the original request with this result."
    )
    first = items[0] if items else {}
    event = {
        "messageId": message_id,
        "chatId": str(row["chat_id"]),
        "chatName": first.get("chatName"),
        "isGroup": bool(first.get("isGroup", True)),
        "senderId": "system@internal",
        "senderName": "Christopher background completion",
        "body": body,
        "timestamp": int(time.time()),
        "fromMe": False,
        **({"quotedMessageId": first.get("messageId")} if first.get("messageId") else {}),
    }
    return message_id, event


def _session_key_override(row: Mapping[str, Any], config_path: Path) -> str | None:
    """The originating conversation, when it is still in the live namespace."""
    from gateway.durable_jsonl_consumer import persistent_chat_namespace

    namespace = persistent_chat_namespace()
    key = str(row.get("session_key") or "")
    return key if key.startswith(namespace + ":") else None


def _inbox_records(inbox: Any, message_ids: Sequence[str]) -> list[Any]:
    """The ingress records among ``message_ids``; ids not in the inbox are skipped."""
    records: list[Any] = []
    for message_id in dict.fromkeys(message_ids):
        try:
            records.extend(inbox.message_id_selection([message_id]))
        except Exception:
            continue
    return records


async def process_claimed_completion(
    *,
    row: Mapping[str, Any],
    store: CompletionStore,
    inbox: Any,
    config_path: Path,
    state_db: Path,
    gate_changed_at: str,
    runner: Any,
) -> dict[str, Any]:
    """Run one claimed completion as a turn in its conversation, then deliver."""
    from gateway import durable_jsonl_consumer as durable

    completion_id = str(row["completion_id"])
    try:
        origin = json.loads(row.get("origin_json") or "{}")
        ids = [str(value) for value in origin.get("source_message_ids") or [] if value]
        # Follow any internal completion id back to the original request, and
        # never require a synthetic id to exist in the ingress inbox.
        flat = store.original_source_ids(ids)
        originals = [
            record for record in _inbox_records(inbox, flat)
            if record.chat_id == row["chat_id"]
        ]
        if not originals:
            raise CompletionError("the original request is not in this consumer's inbox")
        internal_id = internal_message_id(completion_id)
        current = store.get(completion_id) or dict(row)
        if current.get("status") == "delivering":
            # The turn already finished; only its stored reply is retried.
            # Delivery claims each send before trying it, so nothing repeats.
            captured_outbound = json.loads(current.get("outbound_json") or "[]")
            handled = json.loads(current.get("handled_json") or "[]")
            turn_id = current.get("turn_id")
            return await _deliver_stored(
                store=store, durable=durable, inbox=inbox, config_path=config_path,
                completion_id=completion_id, originals=originals, internal_id=internal_id,
                captured_outbound=captured_outbound, handled=handled, turn_id=turn_id,
                gate_changed_at=gate_changed_at,
            )
        internal_id, event = completion_event(row, originals)
        override = _session_key_override(row, config_path)
        if override is None:
            # Engine changed since dispatch: speak as the requester so the
            # completion lands in their conversation in the current namespace.
            from gateway.durable_jsonl_consumer import _bridge_item

            requester = _bridge_item(originals[-1].raw)
            event["senderId"] = requester.get("senderId") or event["senderId"]
            event["senderName"] = requester.get("senderName") or event["senderName"]
        result = await durable.process_live_records(
            originals,
            config_path=config_path,
            state_db=state_db,
            persistent_session=True,
            runner=runner,
            replay_messages=(event,),
            replay_session_key_override=override,
            replay_internal_message_ids=(internal_id,),
        )
        handled = [
            dict(group) for group in result.get("handled") or []
            if internal_id in {str(value) for value in group.get("message_ids") or []}
        ]
        if result.get("provider_errors") or len(handled) != 1:
            raise CompletionError(
                "the completion turn did not complete: "
                + "; ".join(str(e) for e in result.get("provider_errors") or [])
            )
        turn_id = str(handled[0].get("turn_id") or "") or None
        captured_outbound = [dict(x) for x in result.get("captured_outbound") or []]
        # The reply is kept durably before any send is tried (review item 9).
        store.hand_to_delivery(
            completion_id, turn_id=turn_id, captured_outbound=captured_outbound, handled=handled,
        )
    except BaseException as exc:
        store.release(completion_id, error=str(exc) or type(exc).__name__)
        raise
    return await _deliver_stored(
        store=store, durable=durable, inbox=inbox, config_path=config_path,
        completion_id=completion_id, originals=originals, internal_id=internal_id,
        captured_outbound=captured_outbound, handled=handled, turn_id=turn_id,
        gate_changed_at=gate_changed_at,
    )


async def _deliver_stored(
    *, store: CompletionStore, durable: Any, inbox: Any, config_path: Path,
    completion_id: str, originals: Sequence[Any], internal_id: str,
    captured_outbound: Sequence[Mapping[str, Any]], handled: Sequence[Mapping[str, Any]],
    turn_id: str | None, gate_changed_at: str,
) -> dict[str, Any]:
    """Hand a completion's reply to delivery; complete the row once it returns.

    Delivery follows the ordinary at-most-once contract: each send is claimed
    durably before it is tried, and a claimed send is never tried again.  If
    delivery raises or the process stops first, the row stays ``delivering``
    and the next loop retries only the sends not yet claimed.
    """
    try:
        delivery = durable.deliver_management_replies(
            inbox,
            config_path=config_path,
            captured_outbound=list(captured_outbound),
            batch_records=list(originals),
            gate_changed_at=gate_changed_at,
            handled_groups=list(handled),
            completion={
                "completion_id": completion_id,
                "original_message_id": originals[-1].message_id,
                "internal_message_id": internal_id,
            },
        )
    except BaseException as exc:
        store.release(completion_id, error=f"delivery: {exc}" or type(exc).__name__)
        raise
    store.finish(completion_id, turn_id=turn_id)
    return {"completion_id": completion_id, "turn_id": turn_id, "delivery": delivery}
