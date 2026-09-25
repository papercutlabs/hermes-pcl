"""PA-203 round 2: background work returns through the durable consumer.

Teren, 25 Sep 2026: the durable consumer (Christopher's production entry)
never starts the gateway watcher, so background completions sat in memory
forever (scenario 3: 26 minutes, never delivered).  The consumer now drains
them: each completion is a durable record in its own inbox and becomes one
ordinary turn in the originating conversation, carrying the original request
verbatim and the result.  A restart loses nothing; failure and time limits
still complete.  The workflow runner emits one completion per run.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import durable_jsonl_consumer as durable
from gateway import pa_background_completions as background
from tests.gateway.test_durable_jsonl_consumer import _message, _write_jsonl
from tools import async_delegation
from tools.process_registry import process_registry

CHAT = "management@g.us"
NAMESPACE = durable.persistent_chat_namespace("fixture", "fixture-model")
SESSION_KEY = f"{NAMESPACE}:whatsapp:group:{CHAT}:krisleen"
REQUEST = "Check all 61 cases in the attached review against WhatsApp and send me the updated file."


@pytest.fixture(autouse=True)
def _clean_async_state():
    async_delegation._reset_for_tests()
    previous = async_delegation.set_durable_sink(None)
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield
    async_delegation.set_durable_sink(previous)
    async_delegation._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


def _origin():
    return {
        "session_key": SESSION_KEY, "platform": "whatsapp", "chat_id": CHAT,
        "source_message_ids": ["krisleen-request"], "pa_job_type": "tgg_management",
    }


def _setup_consumer(tmp_path: Path) -> argparse.Namespace:
    """A consumer whose inbox holds Krisleen's already-answered request."""
    config = tmp_path / "config.yaml"
    config.write_text(
        "model:\n  provider: fixture\n  default: fixture-model\npa:\n  enabled: true\n",
        encoding="utf-8",
    )
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({
        "version": 1, "enabled": True, "generation": 1, "changed_at": "2026-09-25T00:00:00Z",
    }), encoding="utf-8")
    source = tmp_path / "capture.jsonl"
    message = _message("krisleen-request", chat_id=CHAT)
    message.update({"senderId": "krisleen@s.whatsapp.net", "senderName": "Krisleen", "body": REQUEST})
    _write_jsonl(source, [message])
    cursor = tmp_path / "cursor.json"
    durable.initialize_cursor(source, cursor, position="start")
    inbox = durable.DurableInbox(tmp_path / "inbox.db")
    assert inbox.stage_from_source(source, cursor) == 1
    with sqlite3.connect(inbox.db_path) as conn:
        # Its own turn already ran (the "Started…" reply of scenario 2/3).
        conn.execute("UPDATE ingress_events SET status='completed'")
    return argparse.Namespace(
        config=str(config), source=str(source), cursor=str(cursor),
        inbox=str(tmp_path / "inbox.db"), status_file=str(tmp_path / "status.json"),
        lock_file=str(tmp_path / "consumer.lock"), state_db=str(tmp_path / "state.db"),
        case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "before"), processing_gate=str(gate),
        activity_lock_file=None, once=True, poll_seconds=0.01, max_records=10,
    )


def _fake_turns(monkeypatch):
    turns: list[dict] = []
    deliveries: list[dict] = []

    async def fake_process(records, **kwargs):
        event = kwargs["replay_messages"][0]
        turns.append({
            "records": [r.message_id for r in records], "event": event,
            "session_key_override": kwargs["replay_session_key_override"],
            "internal": kwargs["replay_internal_message_ids"],
        })
        return {
            "provider_errors": [],
            "handled": [{"message_ids": [event["messageId"]], "turn_id": f"turn-{len(turns)}"}],
            "captured_outbound": [{"kind": "send"}],
        }

    def fake_deliver(_inbox, **kwargs):
        deliveries.append(kwargs)
        return {"delivered": 1, "undelivered": 0, "suppressed": 0, "duplicate": 0}

    monkeypatch.setattr(durable, "process_live_records", fake_process)
    monkeypatch.setattr(durable, "deliver_management_replies", fake_deliver)
    monkeypatch.setattr(durable, "_new_gateway_runner", lambda *_a, **_k: object())
    return turns, deliveries


def _wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _dispatch(runner, *, time_limit=None, interrupt=None):
    return async_delegation.dispatch_async_delegation(
        goal="Build the 682-row workbook", context="sheet1 visible rows", toolsets=None,
        role="leaf", model="fixture-model", session_key=SESSION_KEY, runner=runner,
        interrupt_fn=interrupt, origin=_origin(), time_limit_seconds=time_limit,
    )


# ── 2. a completion becomes an inbox record, then a turn ─────────────────


def test_background_completion_becomes_an_inbox_record_then_one_turn(tmp_path, monkeypatch):
    args = _setup_consumer(tmp_path)
    turns, deliveries = _fake_turns(monkeypatch)
    store = background.CompletionStore(Path(args.inbox))
    background.install_sink(store)

    handle = _dispatch(lambda: {"status": "completed", "summary": "Workbook built: 682 rows."})
    assert handle["status"] == "dispatched"
    # Written durably before the helper ran.
    assert store.get(handle["delegation_id"])["chat_id"] == CHAT
    assert _wait_for(lambda: not process_registry.completion_queue.empty())

    # The consumer drains it into its own inbox, then processes it as a turn.
    assert asyncio.run(durable.run_consumer(args)) == 0

    row = store.get(handle["delegation_id"])
    assert row["status"] == "completed" and row["turn_id"] == "turn-1"
    assert len(turns) == 1
    turn = turns[0]
    assert turn["records"] == ["krisleen-request"]
    assert turn["session_key_override"] == SESSION_KEY
    assert turn["internal"] == (f"background-completion:{handle['delegation_id']}",)
    body = turn["event"]["body"]
    assert REQUEST in body  # Krisleen's request, verbatim
    assert "Workbook built: 682 rows." in body
    assert turn["event"]["quotedMessageId"] == "krisleen-request"
    assert deliveries[0]["completion"] == {
        "completion_id": handle["delegation_id"],
        "original_message_id": "krisleen-request",
        "internal_message_id": f"background-completion:{handle['delegation_id']}",
    }
    status = json.loads(Path(args.status_file).read_text())
    assert status["state"] == "running"


def test_a_dispatch_with_no_conversation_to_return_to_is_refused(tmp_path):
    store = background.CompletionStore(tmp_path / "inbox.db")
    background.install_sink(store)
    ran = []
    result = async_delegation.dispatch_async_delegation(
        goal="scheduled job helper", context=None, toolsets=None, role="leaf", model=None,
        session_key="", runner=lambda: ran.append(1) or {"status": "completed"},
        origin={"chat_id": "", "session_key": ""},
    )
    assert result["status"] == "rejected"
    assert "background=false" in result["error"]
    time.sleep(0.05)
    assert ran == []


def test_completion_delivery_has_its_own_key_and_may_send_files(tmp_path, monkeypatch):
    """The original turn's earlier reply to the same message cannot suppress it."""
    inbox = durable.DurableInbox(tmp_path / "inbox.db")
    record = durable.InboxRecord(
        seq=1, message_id="krisleen-request", chat_id=CHAT, start_offset=0, end_offset=1,
        raw={"messageId": "krisleen-request", "chatId": CHAT, "isGroup": True,
             "senderId": "krisleen@s.whatsapp.net", "body": REQUEST, "timestamp": 1},
    )
    monkeypatch.setattr(durable, "_management_selector_chats", lambda _p: frozenset({CHAT}))
    assert inbox.claim_reply_delivery(f"{CHAT}::krisleen-request", chat_id=CHAT, reply_to_message_id="krisleen-request")
    sent = []

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"success": true, "messageId": "sent-1"}'

    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=30: sent.append(json.loads(req.data)) or _Response())
    summary = durable.deliver_management_replies(
        inbox, config_path=tmp_path / "c.yaml",
        captured_outbound=[{"kind": "send", "args": [CHAT, "Here is the workbook."], "kwargs": {}}],
        batch_records=[record], gate_changed_at="2026-09-25T00:00:00Z",
        handled_groups=[{"message_ids": ["background-completion:deleg_1"], "turn_id": "t"}],
        completion={"completion_id": "deleg_1", "original_message_id": "krisleen-request",
                    "internal_message_id": "background-completion:deleg_1"},
    )
    assert summary["delivered"] == 1, summary
    assert sent[0]["replyTo"]["messageId"] == "krisleen-request"
    assert inbox.reply_delivery_status("completion::deleg_1::" + f"{CHAT}::krisleen-request") == "delivered"


# ── 3. a restart while a helper is running loses nothing ─────────────────


def test_restart_while_helper_running_loses_nothing(tmp_path, monkeypatch):
    args = _setup_consumer(tmp_path)
    turns, _ = _fake_turns(monkeypatch)
    store = background.CompletionStore(Path(args.inbox))
    background.install_sink(store)
    release = threading.Event()
    handle = _dispatch(lambda: release.wait(5) and {"status": "completed", "summary": "late"})
    assert store.get(handle["delegation_id"])["status"] == "running"

    # The process dies: the helper thread and the in-memory queue are gone.
    # A new consumer process starts on the same inbox.
    async_delegation.set_durable_sink(None)
    assert asyncio.run(durable.run_consumer(args)) == 0

    row = store.get(handle["delegation_id"])
    assert row["status"] == "completed" and row["outcome"] == "interrupted"
    assert len(turns) == 1
    body = turns[0]["event"]["body"]
    assert REQUEST in body and "restarted" in body
    status = json.loads(Path(args.status_file).read_text())
    assert status["background_completions"]["startup_recovery"]["interrupted"] == 1

    # The old helper finishing late cannot produce a second turn.
    release.set()
    assert not store.record_completion(handle["delegation_id"], outcome="completed", result={})


def test_restart_during_the_completion_turn_retries_it_unless_it_completed(tmp_path):
    store = background.CompletionStore(tmp_path / "inbox.db")
    for completion_id in ("done-turn", "cut-turn"):
        store.record_dispatch({"delegation_id": completion_id, "origin": _origin(), "goal": "g"})
        store.record_completion(completion_id, outcome="completed", result={"summary": "x"})
        assert store.claim(completion_id)
    summary = store.recover_after_restart({"background-completion:done-turn": "turn-9"})
    assert summary["processing_completed"] == 1 and summary["processing_retried"] == 1
    assert store.get("done-turn")["status"] == "completed"
    assert store.get("cut-turn")["status"] == "pending"


# ── 4. a failure or a time limit still produces a completion ─────────────


def test_failed_helper_produces_a_completion(tmp_path):
    store = background.CompletionStore(tmp_path / "inbox.db")
    background.install_sink(store)

    def boom():
        raise RuntimeError("sandbox unavailable")

    handle = _dispatch(boom)
    assert _wait_for(lambda: not process_registry.completion_queue.empty())
    assert background.maintain(store)["recorded"] == 1
    row = store.get(handle["delegation_id"])
    assert row["status"] == "pending" and row["outcome"] == "error"
    assert "sandbox unavailable" in row["result_json"]


def test_helper_past_its_time_limit_completes_as_timeout(tmp_path):
    store = background.CompletionStore(tmp_path / "inbox.db")
    background.install_sink(store)
    release = threading.Event()
    interrupted = []
    handle = _dispatch(
        lambda: release.wait(5) and {"status": "completed", "summary": "too late"},
        time_limit=0.05, interrupt=lambda: interrupted.append(1),
    )
    time.sleep(0.1)
    assert background.maintain(store)["expired"] == 1
    row = store.get(handle["delegation_id"])
    assert row["status"] == "pending" and row["outcome"] == "timeout"
    assert interrupted == [1]
    release.set()
    assert _wait_for(lambda: not process_registry.completion_queue.empty())
    assert background.maintain(store)["recorded"] == 0  # the late result makes no second turn
    assert store.get(handle["delegation_id"])["outcome"] == "timeout"


# ── 5-7. the workflow runner through its Hermes adapter ──────────────────


@pytest.fixture
def workflow_env(tmp_path, monkeypatch):
    from tools import pa_workflow_tool
    from tests.cron.test_pa203_readonly_delegation_children import (
        MANAGEMENT_ENABLED, _context, _parent,
    )
    from agent.pa_delegation import scope_delegation_children

    monkeypatch.setattr(pa_workflow_tool, "store_path", lambda: tmp_path / "pa_workflows.sqlite")
    calls: list[str] = []
    gate = {"crash_after": None}

    class Crash(BaseException):
        pass

    def fake_make_executor(_parent):
        def execute(kind, prompt, schema):
            if gate["crash_after"] is not None and len(calls) >= gate["crash_after"]:
                raise Crash()
            calls.append(prompt)
            jobs = [token for token in prompt.split() if "/JOB/" in token]
            return {"status": "ok", "text": json.dumps({
                "cases": [{"job_no": job, "finding": "no new update"} for job in jobs]
            })}
        return execute

    monkeypatch.setattr(pa_workflow_tool, "_make_executor", fake_make_executor)
    monkeypatch.setattr(pa_workflow_tool, "_runtime_credentials", lambda: {"api_key": "k"})
    monkeypatch.setattr(
        "gateway.session_context.get_session_env",
        lambda name, default="": {
            "HERMES_SESSION_CHAT_ID": CHAT, "HERMES_SESSION_PLATFORM": "whatsapp",
            "HERMES_SESSION_SOURCE_MESSAGE_REFS": '["krisleen-request"]',
            "HERMES_SESSION_USER_NAME": "Krisleen",
        }.get(name, default),
    )
    monkeypatch.setattr("tools.approval.get_current_session_key", lambda default="": SESSION_KEY)
    parent = _parent(list(MANAGEMENT_ENABLED))
    parent.valid_tool_names = {"tgg_case_query"}
    scope_delegation_children(parent, list(MANAGEMENT_ENABLED), _context())
    return SimpleNamespace(tool=pa_workflow_tool, parent=parent, calls=calls, gate=gate, Crash=Crash)


JOBS = [f"AM/JOB/2609/{n:04d}" for n in range(1, 62)]
SCRIPT = (
    f"jobs = {JOBS!r}\n"
    "schema = {'type': 'object', 'required': ['cases'], 'properties': {'cases': {'type': 'array'}}}\n"
    "found = map(chunks(jobs, 5), lambda group: task('readonly-investigation', "
    "'Check WhatsApp for ' + join(group), schema))\n"
    "output = [case for r in found if r['status'] == 'ok' for case in r['output']['cases']]\n"
)


def test_runner_fan_out_of_61_items_returns_one_completion(tmp_path, workflow_env, monkeypatch):
    store = background.CompletionStore(tmp_path / "inbox.db")
    background.install_sink(store)
    started = json.loads(workflow_env.tool.workflow_start(
        script=SCRIPT, title="61-case review", parent_agent=workflow_env.parent,
    ))
    assert started["status"] == "started", started
    run_id = started["run_id"]
    assert store.get(run_id)["status"] == "running"  # durable before any helper ran

    assert _wait_for(lambda: not process_registry.completion_queue.empty())
    time.sleep(0.1)
    events = []
    while not process_registry.completion_queue.empty():
        events.append(process_registry.completion_queue.get_nowait())
    assert [e["type"] for e in events] == ["workflow_run"]  # exactly one
    for event in events:
        process_registry.completion_queue.put(event)
    assert background.maintain(store)["recorded"] == 1
    assert len(workflow_env.calls) == 13
    row = store.get(run_id)
    assert row["status"] == "pending"
    result = json.loads(row["result_json"])
    assert sorted(case["job_no"] for case in result["output"]) == JOBS
    assert result["counts"]["ok"] == 13

    # The record backs a progress claim.
    status = json.loads(workflow_env.tool.workflow_status(run_id=run_id))
    assert status["started_by"]["user_name"] == "Krisleen"
    assert status["counts"] == {"pending": 0, "running": 0, "ok": 13, "failed": 0}

    # One completion, one turn.
    turns, _ = _fake_turns(monkeypatch)
    inbox = SimpleNamespace(message_id_selection=lambda ids: [durable.InboxRecord(
        seq=1, message_id="krisleen-request", chat_id=CHAT, start_offset=0, end_offset=1,
        raw={"messageId": "krisleen-request", "chatId": CHAT, "isGroup": True,
             "senderName": "Krisleen", "body": REQUEST, "timestamp": 1},
    )])
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: fixture\n  default: fixture-model\n", encoding="utf-8")
    assert store.claim(run_id)
    asyncio.run(background.process_claimed_completion(
        row=store.get(run_id), store=store, inbox=inbox, config_path=config,
        state_db=tmp_path / "state.db", gate_changed_at="2026-09-25T00:00:00Z", runner=object(),
    ))
    assert len(turns) == 1
    assert REQUEST in turns[0]["event"]["body"] and "AM/JOB/2609/0061" in turns[0]["event"]["body"]
    assert store.get(run_id)["status"] == "completed"


def test_runner_resumes_after_a_crash(tmp_path, workflow_env):
    tool = workflow_env.tool
    # Start the run in the calling turn and let the process "crash" after 4 helpers.
    workflow_env.gate["crash_after"] = 4
    store = background.CompletionStore(tmp_path / "inbox.db")
    background.install_sink(store)
    from agent.pa_workflow import WorkflowStore

    run_id = "wf-crashtest01"
    WorkflowStore(tool.store_path()).create_run(
        run_id, script=SCRIPT, title="61-case review",
        started_by={"chat_id": CHAT}, origin={**_origin(), "parent": {"enabled_toolsets": []}},
    )
    store.record_dispatch({"source": "workflow", "run_id": run_id, "origin": _origin(), "title": "t"})
    with pytest.raises(workflow_env.Crash):
        tool._run(run_id, object(), emit=True)
    assert len(workflow_env.calls) == 4
    assert process_registry.completion_queue.empty()

    # Restart: the consumer start-up keeps the run and resumes it.
    async_delegation.set_durable_sink(None)
    workflow_env.gate["crash_after"] = None
    new_store, summary, restore = background.start_for_consumer(tmp_path / "inbox.db", tmp_path / "state.db")
    try:
        assert summary["resumed"] == 1 and summary["interrupted"] == 0
        assert _wait_for(lambda: not process_registry.completion_queue.empty())
        time.sleep(0.1)
        assert background.maintain(new_store)["recorded"] == 1
    finally:
        restore()
    assert len(workflow_env.calls) == 13  # 4 before the crash + only the 9 unfinished
    result = json.loads(new_store.get("wf-crashtest01")["result_json"])
    assert sorted(case["job_no"] for case in result["output"]) == JOBS


def test_workflow_script_refuses_raw_tools(workflow_env):
    tool = workflow_env.tool
    for script in (
        'terminal("ls")',
        'delegate_task(goal="x", toolsets=["pa-business"])',
        'send_message("done")',
        'write_file("/work/x.xlsx", "x")',
        'tgg_whatsapp_case_submit(run_id="case-1")',
        "results().__class__",
        "import os",
    ):
        result = json.loads(tool.workflow_start(script=script, parent_agent=workflow_env.parent))
        assert "Script refused" in result["error"], (script, result)
    # A helper kind the runner does not offer ends the run without a helper.
    result = json.loads(tool.workflow_start(
        script="output = run(task('explicit-case-edit', 'update SK/JOB/1', {'type': 'object'}))",
        wait=True, parent_agent=workflow_env.parent,
    ))
    assert result["status"] == "failed" and workflow_env.calls == []


def test_workflow_is_refused_off_the_management_route(workflow_env):
    from tests.cron.test_pa203_readonly_delegation_children import _parent

    result = json.loads(workflow_env.tool.workflow_start(
        script="output = 1", parent_agent=_parent(["file", "delegation"]),
    ))
    assert "TGG Management route" in result["error"]


def test_gateway_post_turn_drain_leaves_workflow_completions_for_the_consumer():
    import queue

    from gateway.run import _drain_gateway_watch_events

    q = queue.Queue()
    q.put({"type": "workflow_run", "run_id": "wf-1"})
    q.put({"type": "async_delegation", "delegation_id": "deleg_1"})
    q.put({"type": "watch_match", "session_id": "p1"})
    assert [e["type"] for e in _drain_gateway_watch_events(q)] == ["watch_match"]
    left = []
    while not q.empty():
        left.append(q.get_nowait()["type"])
    assert sorted(left) == ["async_delegation", "workflow_run"]
