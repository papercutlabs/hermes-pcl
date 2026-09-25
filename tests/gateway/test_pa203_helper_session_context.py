"""PA-203 r2b replay defect: helpers must run with the originating turn's context.

In replay s6 (run wf-9e86a66f5c41) every workflow helper's business read failed
with "unknown PA business operation ...; known: none configured", while the
parent's calls in the same turn succeeded.  The run thread was a plain
``threading.Thread`` and did not carry the turn's context variables, so the
business bridge resolved no PA job brief.  These tests run a real registered
business tool, with the real TGG bridge configuration, from inside a helper
started through ``workflow_start`` (fresh and resumed) and through a
background ``delegate_task``.  Only the model and the HTTP transport are
replaced.
"""

from __future__ import annotations

import contextvars
import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import yaml

from agent.pa_constitution import load_constitution_data
from agent.pa_delegation import scope_delegation_children
from gateway import pa_background_completions as background
from gateway.session_context import clear_session_vars, set_session_vars
from tests.cron.test_pa203_readonly_delegation_children import (  # noqa: F401
    EXPLICIT_TOOLS,
    MANAGEMENT_ENABLED,
    READONLY,
    READONLY_TOOLS,
    _context,
    _parent,
    capability_toolsets,
)
from tests.test_pa_business_operation_scope import MGMT_CHAT_ID, TGG_CONFIG, TGG_CONSTITUTION
from tools import async_delegation, delegate_tool
from tools.process_registry import process_registry

SESSION_KEY = f"agent:live-drain:persistent-chat:p:m:whatsapp:group:{MGMT_CHAT_ID}:krisleen"


@pytest.fixture
def bridge_env(tmp_path, monkeypatch):
    """The real TGG config and constitution; HTTP answered locally."""
    config = yaml.safe_load(TGG_CONFIG.read_text(encoding="utf-8"))
    config["pa"] = dict(config["pa"])
    config["pa"]["enabled"] = True
    config["pa"].pop("constitution_path", None)
    config["pa"]["constitution"] = load_constitution_data(
        yaml.safe_load(TGG_CONSTITUTION.read_text(encoding="utf-8")), source=str(TGG_CONSTITUTION),
    )
    monkeypatch.setattr("hermes_cli.config.read_raw_config", lambda: config)
    requests: list[str] = []

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"ok": true, "rows": [[1]]}'

    def fake_urlopen(request, timeout=None):
        requests.append(request.full_url)
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    from tools import pa_workflow_tool

    monkeypatch.setattr(pa_workflow_tool, "store_path", lambda: tmp_path / "pa_workflows.sqlite")
    monkeypatch.setattr(pa_workflow_tool, "_runtime_credentials", lambda: {"api_key": "k"})
    monkeypatch.setattr(delegate_tool, "_load_config", lambda: {})

    # The "model" of every helper: call the real registered business tool on
    # whatever thread the helper runs, and report what it got back.
    helper_results: list[dict] = []

    def run_child(index, goal, child, parent_agent, **_):
        from tools.registry import registry

        entry = registry.get_entry("tgg_case_query")
        response = json.loads(entry.handler({"sql": "SELECT 1"}))
        helper_results.append(response)
        return {"task_index": index, "status": "completed",
                "summary": json.dumps({"business_ok": bool(response.get("ok"))}),
                "api_calls": 1, "duration_seconds": 0.1}

    monkeypatch.setattr(delegate_tool, "_run_single_child", run_child)

    def construct(**kwargs):
        child = MagicMock()
        child.valid_tool_names = set(READONLY_TOOLS)
        return child

    monkeypatch.setattr("run_agent.AIAgent", MagicMock(side_effect=construct))

    parent = _parent(list(MANAGEMENT_ENABLED))
    parent.valid_tool_names = set(EXPLICIT_TOOLS)
    scope_delegation_children(parent, list(MANAGEMENT_ENABLED), _context())

    async_delegation._reset_for_tests()
    previous = async_delegation.set_durable_sink(None)
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield {"tool": pa_workflow_tool, "parent": parent, "results": helper_results,
           "requests": requests, "tmp": tmp_path}
    async_delegation.set_durable_sink(previous)
    async_delegation._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    with pa_workflow_tool._active_lock:
        pa_workflow_tool._active_runs.clear()


def _turn_vars():
    return set_session_vars(
        platform="whatsapp", chat_id=MGMT_CHAT_ID, chat_name="TGG Management",
        user_id="krisleen@s.whatsapp.net", user_name="Krisleen", session_key=SESSION_KEY,
        session_id="session-1", source_message_refs='["krisleen-request"]', pa_job_type="tgg_management",
    )


def _wait(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


SCRIPT = (
    "output = map(['AM/JOB/2609/0001', 'AM/JOB/2609/0002'], lambda job: task("
    "'readonly-investigation', 'Check ' + job, {'type': 'object', 'required': ['business_ok']}))\n"
)


def test_the_bug_a_bare_thread_has_no_business_registry(bridge_env):
    """Control: outside the turn's context the bridge knows no operation."""
    from tools.registry import registry

    outcome = {}
    tokens = _turn_vars()
    try:
        inside = json.loads(registry.get_entry("tgg_case_query").handler({"sql": "SELECT 1"}))
        thread = threading.Thread(target=lambda: outcome.update(
            json.loads(registry.get_entry("tgg_case_query").handler({"sql": "SELECT 1"}))
        ))
        thread.start()
        thread.join()
    finally:
        clear_session_vars(tokens)
    assert inside.get("ok") is True
    assert outcome.get("ok") is not True
    assert "none configured" in json.dumps(outcome) or "error" in outcome


def test_workflow_helpers_run_real_business_operations(bridge_env):
    tool = bridge_env["tool"]
    store = background.CompletionStore(bridge_env["tmp"] / "inbox.db")
    background.install_sink(store)
    tokens = _turn_vars()
    try:
        started = json.loads(tool.workflow_start(script=SCRIPT, title="2 cases", parent_agent=bridge_env["parent"]))
    finally:
        clear_session_vars(tokens)
    assert started["status"] == "started", started
    assert _wait(lambda: started["run_id"] not in tool.active_workflow_ids())
    status = json.loads(tool.store_path().exists() and json.dumps(tool._store().run_status(started["run_id"])))
    assert status["status"] == "completed", status
    assert [r["output"] for r in status["output"]] == [{"business_ok": True}] * 2
    assert all(r.get("ok") is True for r in bridge_env["results"]), bridge_env["results"]
    assert len(bridge_env["requests"]) == 2


def test_resumed_workflow_helpers_run_real_business_operations(bridge_env):
    tool = bridge_env["tool"]
    store = background.CompletionStore(bridge_env["tmp"] / "inbox.db")
    background.install_sink(store)
    # Start in a turn but hold every helper, then "restart": the run is resumed
    # from a context that has no turn at all.
    tokens = _turn_vars()
    try:
        origin = tool._origin(bridge_env["parent"])
    finally:
        clear_session_vars(tokens)
    assert origin["session_env"]["HERMES_SESSION_PA_JOB_TYPE"] == "tgg_management"
    run_id = "wf-resumectx01"
    tool._store().create_run(run_id, script=SCRIPT, title="t", started_by={"chat_id": MGMT_CHAT_ID}, origin=origin)
    store.record_dispatch({"source": "workflow", "run_id": run_id, "origin": origin, "title": "t"})

    def resume_without_any_turn():
        assert tool.resume_workflow_runs([run_id]) == [run_id]

    contextvars.Context().run(resume_without_any_turn)
    assert _wait(lambda: run_id not in tool.active_workflow_ids())
    status = tool._store().run_status(run_id)
    assert status["status"] == "completed", status
    assert all(r.get("ok") is True for r in bridge_env["results"]), bridge_env["results"]
    assert len(bridge_env["results"]) == 2


def test_background_delegate_task_helper_runs_a_real_business_operation(bridge_env):
    tokens = _turn_vars()
    try:
        handle = json.loads(delegate_tool.delegate_task(
            goal="Check AM/JOB/2609/0001 in WhatsApp", background=True, parent_agent=bridge_env["parent"],
        ))
    finally:
        clear_session_vars(tokens)
    assert handle["status"] == "dispatched", handle
    assert _wait(lambda: bool(bridge_env["results"]))
    assert bridge_env["results"][0].get("ok") is True, bridge_env["results"]
