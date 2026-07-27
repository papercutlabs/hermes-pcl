"""PA universal turn-recording — the live-emit side of the PA-portal
observability system (Phase 1).

This module is the SINGLE SOURCE OF TRUTH for the universal turn-record shape.
After each turn completes, the hermes gateway writes an agent-keyed, universal
record of what the deployed PA agent did:

    Agent -> Chat -> Turn -> { all tool-calls, flat } + { Events }

It is universal (agent_id keys the rows), NOT TGG-specific.  A tool-call record
carries only universal fields (name / input / result / cost / timing) plus an
optional STRING ``client_entity_pointer`` (a client-defined entity reference) — it NEVER imports
client schema.

SHARED SHAPE CONTRACT
---------------------
The TS replay harness ``systems-papercut-labs/src/tenants/tgg/
christopher-processor.ts`` (``recordChristopherTurnResult``) records the same
turn outcome at replay time.  Its output rows MUST land in the IDENTICAL
universal shape defined here so the two engines (live hermes emit vs replay
backfill) cannot drift.  The field mapping between the christopher-processor TS
record and these dataclasses:

    TS recordChristopherTurnResult   ->   universal field
    --------------------------------     ------------------
    provider                              PaTurnRecord.provider
    model                                 PaTurnRecord.model
    status                                PaTurnRecord.turn_status
    error                                 PaTurnRecord.error
    modelInput / modelOutput (json)       PaTurnRecord.raw_turn_envelope
    caseEffects / actions                 -> PaEvent rows (agent's semantic acct)
    (per tool invocation)                 -> PaToolCall rows

When the replay path is ported (Phase 3), it must construct PaTurnRecord /
PaToolCall / PaEvent instances (or the identical dict shape) and write them
through the SAME SessionDB.record_pa_turn() method this module calls.  Do not
fork the row shape in the TS harness.

DELIVERY / SAFETY
-----------------
``record_turn`` is invoked from the gateway turn-boundary AFTER the response is
delivered (alongside the goal-judge).  Two hard safety properties, enforced by
the caller in run.py:

  1. The whole call is wrapped in try/except so a recording failure can NEVER
     break live processing or the agent's reply.
  2. The DB write is run off the event loop with a bounded timeout (fire-and-
     forget) so a slow / contended write can NEVER backpressure the agent loop.

The event buffer below lets the ``record_event`` agent-facing tool stage
semantic events mid-turn; the turn-boundary hook drains them with the correct
turn_id and writes them to ``pa_events``.

Events are PURELY agent-recorded: the agent decides what is meaningful and
calls ``record_event`` itself. There is NO mechanical floor synthesising
events — a turn where the agent records nothing simply has zero events (its
tool-calls are still captured). Coverage (events-per-turn) is monitored so a
drop in agent recording is visible.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ── Shared shape contract (single source of truth) ──────────────────────


@dataclass
class PaToolCall:
    """One tool invocation inside a turn.  UNIVERSAL — no client schema."""

    tool_name: Optional[str] = None
    input: Optional[Any] = None
    result: Optional[Any] = None
    cost_usd: Optional[float] = None
    duration_ms: Optional[int] = None
    # Optional STRING pointer at a client-defined entity reference.  A
    # pointer, NOT the client entity itself.  None for tools that touch no
    # client entity.
    client_entity_pointer: Optional[str] = None
    # Provider tool-call id (OpenAI-style ``tc.id``).  Lets the DB layer
    # dedup per-session so a payload extracted from the full conversation
    # history persists only the recording turn's delta.
    call_id: Optional[str] = None
    replay_run_id: Optional[str] = None
    replay_attempt_id: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PaEvent:
    """A semantic event — the agent's account of what happened in the turn.

    ``source`` is 'agent_recorded' for live events emitted by the agent via the
    record_event tool. The column is kept for future provenance (e.g. a Phase-3
    replay/backfill may mark records with a different source); live recording
    only ever writes 'agent_recorded'.
    """

    event_type: Optional[str] = None
    reason: Optional[str] = None
    evidence_message_refs: Optional[List[Any]] = None
    source: str = "agent_recorded"
    recorded_at: Optional[float] = None
    replay_run_id: Optional[str] = None
    replay_attempt_id: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PaTurnRecord:
    """The full universal turn envelope.  Captured at the turn-boundary."""

    turn_id: str
    agent_id: Optional[str] = None
    chat_id: Optional[str] = None
    session_id: Optional[str] = None
    message_refs: Optional[List[Any]] = None
    model: Optional[str] = None
    provider: Optional[str] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    # Largest single-call prompt this turn (the context size the model
    # actually saw) — max input/prompt tokens across the turn's model calls.
    context_window_peak: Optional[int] = None
    cost_usd: Optional[float] = None
    # turn_status / error are the HIGHEST-VALUE observability fields (seeing
    # FAILURES) and free at this boundary.
    turn_status: Optional[str] = None
    error: Optional[Any] = None
    latency_ms: Optional[int] = None
    raw_turn_envelope: Optional[Any] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    replay_run_id: Optional[str] = None
    replay_attempt_id: Optional[str] = None
    tool_calls: List[PaToolCall] = field(default_factory=list)
    events: List[PaEvent] = field(default_factory=list)

    def to_db_kwargs(self) -> Dict[str, Any]:
        """Flatten into the kwargs ``SessionDB.record_pa_turn`` accepts."""
        return {
            "turn_id": self.turn_id,
            "agent_id": self.agent_id,
            "chat_id": self.chat_id,
            "session_id": self.session_id,
            "message_refs": self.message_refs,
            "model": self.model,
            "provider": self.provider,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "context_window_peak": self.context_window_peak,
            "cost_usd": self.cost_usd,
            "turn_status": self.turn_status,
            "error": self.error,
            "latency_ms": self.latency_ms,
            "raw_turn_envelope": self.raw_turn_envelope,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "replay_run_id": self.replay_run_id,
            "replay_attempt_id": self.replay_attempt_id,
            "tool_calls": [tc.to_row() for tc in self.tool_calls],
            "events": [ev.to_row() for ev in self.events],
        }


def new_turn_id() -> str:
    """Generate a universal turn id."""
    return f"paturn_{uuid.uuid4().hex}"


# ── Agent event buffer (record_event staging) ───────────────────────────
#
# The ``record_event`` tool runs mid-turn inside the agent loop.  It does not
# know the turn_id (that is minted at the turn-boundary).  So it stages events
# into a per-session buffer keyed by session_id; the turn-boundary hook drains
# the buffer with the correct turn_id.  Thread-safe — tool calls run in a
# worker thread, the boundary hook on the event loop / its own thread.


class _PaEventBuffer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_session: Dict[str, List[PaEvent]] = {}

    def stage(self, session_id: str, event: PaEvent) -> None:
        if not session_id:
            return
        with self._lock:
            self._by_session.setdefault(session_id, []).append(event)

    def drain(self, session_id: str) -> List[PaEvent]:
        """Return and clear all staged events for a session."""
        if not session_id:
            return []
        with self._lock:
            return self._by_session.pop(session_id, [])

    def clear(self, session_id: str) -> None:
        with self._lock:
            self._by_session.pop(session_id, None)


# Process-global buffer — one per hermes process, shared by the tool and the
# turn-boundary hook.
_EVENT_BUFFER = _PaEventBuffer()


def stage_agent_event(
    session_id: str,
    *,
    event_type: Optional[str],
    reason: Optional[str],
    evidence_message_refs: Optional[List[Any]] = None,
) -> None:
    """Stage an agent_recorded event (called by the record_event tool)."""
    replay_run_id = None
    replay_attempt_id = None
    try:
        from gateway.replay import current_replay_context

        replay_ctx = current_replay_context()
        if replay_ctx is not None:
            replay_run_id = replay_ctx.run_id
            replay_attempt_id = replay_ctx.attempt_id
    except Exception:
        pass
    _EVENT_BUFFER.stage(
        session_id,
        PaEvent(
            event_type=event_type,
            reason=reason,
            evidence_message_refs=evidence_message_refs,
            source="agent_recorded",
            recorded_at=time.time(),
            replay_run_id=replay_run_id,
            replay_attempt_id=replay_attempt_id,
        ),
    )


def drain_agent_events(session_id: str) -> List[PaEvent]:
    """Drain staged agent events for a session (called by the boundary hook)."""
    return _EVENT_BUFFER.drain(session_id)


# ── Extraction helpers (agent_result -> universal record) ────────────────


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_tool_calls(
    messages: Optional[List[Dict[str, Any]]],
    *,
    replay_run_id: Optional[str] = None,
    replay_attempt_id: Optional[str] = None,
) -> List[PaToolCall]:
    """Flatten tool-calls out of the agent's conversation messages.

    The agent_result["messages"] list is the full conversation, where assistant
    messages carry ``tool_calls`` (OpenAI-style: each has function.name +
    function.arguments) and the paired ``role == 'tool'`` message carries the
    result keyed by tool_call_id.  We join them into universal PaToolCall rows.
    """
    if not messages:
        return []

    # Index tool results by tool_call_id for the join.
    results_by_id: Dict[str, Any] = {}
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("role") == "tool":
            tcid = msg.get("tool_call_id")
            if tcid is not None:
                results_by_id[str(tcid)] = msg.get("content")

    calls: List[PaToolCall] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        for tc in (msg.get("tool_calls") or []):
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") or {}
            name = fn.get("name") if isinstance(fn, dict) else None
            args = fn.get("arguments") if isinstance(fn, dict) else None
            tcid = tc.get("id")
            result = results_by_id.get(str(tcid)) if tcid is not None else None
            calls.append(
                PaToolCall(
                    tool_name=name or tc.get("name"),
                    input=args,
                    result=result,
                    cost_usd=None,
                    duration_ms=None,
                    client_entity_pointer=None,
                    call_id=str(tcid) if tcid is not None else None,
                    replay_run_id=replay_run_id,
                    replay_attempt_id=replay_attempt_id,
                )
            )
    return calls


def build_turn_record(
    *,
    session_id: Optional[str],
    agent_id: Optional[str],
    chat_id: Optional[str],
    agent_result: Optional[Dict[str, Any]],
    final_response: Optional[str],
    started_at: Optional[float],
    completed_at: Optional[float],
    replay_run_id: Optional[str] = None,
    replay_attempt_id: Optional[str] = None,
) -> PaTurnRecord:
    """Assemble a universal PaTurnRecord from a completed turn.

    Pure (no IO) so it is trivially testable: feed it an agent_result dict and
    assert the record shape.  The caller does the DB write + safety wrapping.
    """
    agent_result = agent_result or {}

    # turn_status: prefer explicit status, else derive from completed/error.
    completed_flag = agent_result.get("completed")
    error = agent_result.get("error")
    if error:
        turn_status = "failed"
    elif completed_flag is False:
        turn_status = "incomplete"
    else:
        turn_status = "completed"

    tool_calls = extract_tool_calls(
        agent_result.get("messages"),
        replay_run_id=replay_run_id,
        replay_attempt_id=replay_attempt_id,
    )

    # Tool-call counts/timing from the result if present (best-effort).
    started = started_at if started_at is not None else None
    completed = completed_at if completed_at is not None else time.time()
    latency_ms = None
    if started is not None:
        latency_ms = int(max(0.0, (completed - started)) * 1000)

    # Events: PURELY agent-recorded — drained from the buffer the record_event
    # tool stages into. No mechanical floor: a turn the agent records nothing
    # for has zero events (its tool-calls are still captured).
    events: List[PaEvent] = []
    if session_id:
        events.extend(drain_agent_events(session_id))
    if replay_run_id or replay_attempt_id:
        for event in events:
            if event.replay_run_id is None:
                event.replay_run_id = replay_run_id
            if event.replay_attempt_id is None:
                event.replay_attempt_id = replay_attempt_id

    # message_refs: the WA source message ids of the turn's INPUT (injected at
    # the gateway boundary from the MessageEvent — deterministic, recorded at
    # source per teren 2026-06-12). Fallback: ids of conversation messages the
    # turn produced, when they carry ids.
    message_refs: Optional[List[Any]] = None
    _src_ids = agent_result.get("turn_source_message_ids")
    if isinstance(_src_ids, list) and _src_ids:
        message_refs = [str(i) for i in _src_ids]
    msgs = agent_result.get("messages")
    if message_refs is None and isinstance(msgs, list):
        ids = [m.get("id") for m in msgs if isinstance(m, dict) and m.get("id") is not None]
        message_refs = ids or None

    # raw envelope: keep the full turn envelope so a missing column later is a
    # query, not a migration + replay.  Strip nothing here; the DB JSON-encodes.
    raw_envelope = {
        "final_response": final_response,
        "api_calls": agent_result.get("api_calls"),
        "completed": completed_flag,
        "messages": msgs,
        "model": agent_result.get("model"),
        "provider": agent_result.get("provider"),
    }
    if replay_run_id or replay_attempt_id:
        raw_envelope["execution_mode"] = "replay"
        raw_envelope["replay_run_id"] = replay_run_id
        raw_envelope["replay_attempt_id"] = replay_attempt_id

    return PaTurnRecord(
        turn_id=new_turn_id(),
        agent_id=agent_id,
        chat_id=chat_id,
        session_id=session_id,
        message_refs=message_refs,
        model=agent_result.get("model"),
        provider=agent_result.get("provider"),
        # Token counts: PER-TURN deltas. run_agent's "input_tokens"/
        # "output_tokens" are SESSION-CUMULATIVE counters (cached gateway
        # agents span many turns — recording them made pa_turns token columns
        # grow monotonically, e.g. sk-day26-v6 input 80k -> 2.28M over 14
        # turns). Prefer the turn-scoped fields run_conversation now emits;
        # fall back to the legacy fields for callers that don't provide them
        # (string-only paths, replay backfill).
        input_tokens=_as_int(
            agent_result.get("turn_input_tokens")
            if agent_result.get("turn_input_tokens") is not None
            else (agent_result.get("input_tokens") or agent_result.get("prompt_tokens"))
        ),
        output_tokens=_as_int(
            agent_result.get("turn_output_tokens")
            if agent_result.get("turn_output_tokens") is not None
            else (agent_result.get("output_tokens") or agent_result.get("completion_tokens"))
        ),
        # Context-window peak: max single-call prompt size this turn, emitted
        # by run_conversation as turn_context_window_peak. No legacy fallback
        # — callers that don't emit it record NULL, never a cumulative proxy.
        context_window_peak=(
            _as_int(agent_result.get("turn_context_window_peak"))
            if agent_result.get("turn_context_window_peak") is not None
            else None
        ),
        cost_usd=_as_float(
            agent_result.get("estimated_cost_usd") or agent_result.get("cost_usd")
        ),
        turn_status=turn_status,
        error=error,
        latency_ms=latency_ms,
        raw_turn_envelope=raw_envelope,
        started_at=started,
        completed_at=completed,
        replay_run_id=replay_run_id,
        replay_attempt_id=replay_attempt_id,
        tool_calls=tool_calls,
        events=events,
    )


def write_turn_record(session_db: Any, record: PaTurnRecord) -> Optional[str]:
    """Persist a PaTurnRecord through the SessionDB.  Returns turn_id or None.

    This is the single write path; the replay harness (Phase 3) must call the
    same ``session_db.record_pa_turn`` with the identical row shape.

    Thin and intentionally non-guarding — it propagates DB errors so callers
    can decide.  The gateway path uses ``safe_record_turn`` (below), which
    swallows.
    """
    if session_db is None:
        return None
    return session_db.record_pa_turn(**record.to_db_kwargs())


def safe_record_turn(session_db: Any, record: PaTurnRecord) -> Optional[str]:
    """Best-effort write that NEVER raises.

    This is the safety wrapper the gateway turn-boundary relies on (hard
    requirement (i)): a recording failure must never break live processing or
    the agent's reply.  Any exception is swallowed and None is returned.
    Centralised here so the swallow contract is unit-testable without importing
    the gateway.
    """
    try:
        return write_turn_record(session_db, record)
    except Exception:  # noqa: BLE001 - intentional swallow (observability only)
        return None
