"""PA-203 r2f replay s3: large workflow results go to /work, not into the turn.

Workflow wf-0fbb35cb57fe ran 30 helpers whose results came to 235,819
characters.  The completion cut them to 60,457, workflow_status cut its
249k answer, a large tool result "could not be saved to sandbox", and about
74% of the findings were lost.  Here 60 helpers return large results.  The
completion turn and workflow_status carry a bounded summary and the exact
/work path; the file holds every result; a real python_sandbox call (the
jail replaced by the stand-in of the conversation-workspace tests) reads it.
"""

from __future__ import annotations

import json

import pytest

from gateway import pa_background_completions as background
from gateway.session_context import clear_session_vars, set_session_vars
from tests.cron.test_pa203_conversation_workspace import (  # noqa: F401
    LIVE_KEY,
    MANAGEMENT_CHAT,
    _live_turn,
    home,
)
from tools import async_delegation
from tools.process_registry import process_registry

HELPERS = 60
FINDING = "x" * 4500  # about 270,000 characters across 60 helpers
SCRIPT = (
    f"jobs = ['AM/JOB/2609/' + str(1000 + i) for i in range({HELPERS})]\n"
    "found = map(jobs, lambda job: task('readonly-investigation', 'Check ' + job, "
    "{'type': 'object', 'required': ['job', 'finding']}))\n"
    "output = [r['output'] for r in found]\n"
)
BOUND = 20_000


@pytest.fixture
def env(home, tmp_path, monkeypatch):
    from tools import pa_workflow_tool

    monkeypatch.setattr(pa_workflow_tool, "store_path", lambda: tmp_path / "pa_workflows.sqlite")

    def fake_make_executor(_parent):
        def execute(kind, prompt, schema):
            job = prompt.split()[1]
            return {"status": "ok", "text": json.dumps({"job": job, "finding": f"{job} {FINDING}"})}
        return execute

    monkeypatch.setattr(pa_workflow_tool, "_make_executor", fake_make_executor)
    async_delegation._reset_for_tests()
    previous = async_delegation.set_durable_sink(None)
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield pa_workflow_tool
    async_delegation.set_durable_sink(previous)
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


def _origin():
    return {"session_key": LIVE_KEY, "chat_id": MANAGEMENT_CHAT, "platform": "whatsapp",
            "source_message_ids": ["krisleen-request"]}


def _turn():
    return set_session_vars(platform="whatsapp", chat_id=MANAGEMENT_CHAT, session_key=LIVE_KEY,
                            session_id="live-session-1", pa_job_type="tgg_management")


def test_sixty_large_results_reach_the_turn_as_a_summary_and_a_readable_file(env, tmp_path):
    tool = env
    store = background.CompletionStore(tmp_path / "inbox.db")
    run_id = "wf-largeresult1"
    tool._store().create_run(run_id, script=SCRIPT, title="60 cases",
                             started_by={"chat_id": MANAGEMENT_CHAT}, origin=_origin())
    store.record_dispatch({"source": "workflow", "run_id": run_id, "origin": _origin(), "title": "60 cases"})
    tokens = _turn()
    try:
        with tool._active_lock:
            tool._active_runs[run_id] = None
        tool._run(run_id, object(), emit=True)
        status_text = tool.workflow_status(run_id=run_id)
    finally:
        clear_session_vars(tokens)
    assert background.maintain(store)["recorded"] == 1
    row = store.get(run_id)
    result = json.loads(row["result_json"])
    path = result["results_file"]
    assert path == f"/work/workflow-{run_id}-results.json"
    assert result["summary"]["results_csv"] == f"/work/workflow-{run_id}-results.csv"

    # The completion turn: bounded, with the counts, the path and the read line.
    _id, event = background.completion_event(row, [])
    body = event["body"]
    assert len(body) < BOUND, len(body)
    assert path in body and "python_sandbox" in body
    assert '"ok": 60' in body
    assert "more characters not shown" not in body

    # workflow_status: bounded too, and it points to the same file.
    status = json.loads(status_text)
    assert len(status_text) < BOUND and status["results_file"] == path
    assert status["counts"]["ok"] == HELPERS

    # A real python_sandbox call in the same conversation reads every result.
    tokens = _turn()
    try:
        read = _live_turn(
            "import json\n"
            f"data = json.loads((WORK / 'workflow-{run_id}-results.json').read_text())\n"
            "(WORK / 'result.json').write_text(json.dumps({\n"
            "    'helpers': len(data['results']),\n"
            "    'ok': sum(1 for r in data['results'] if r['status'] == 'ok'),\n"
            "    'finding_chars': sum(len(r['output']['finding']) for r in data['results']),\n"
            "    'output_items': len(data['output']),\n"
            "}))\n"
        )
    finally:
        clear_session_vars(tokens)
    value = read.get("result") or read
    assert value["helpers"] == HELPERS and value["ok"] == HELPERS, read
    assert value["output_items"] == HELPERS
    assert value["finding_chars"] > HELPERS * len(FINDING)


def test_a_large_tool_result_with_no_terminal_is_saved_to_work(env):
    from tools.tool_result_storage import maybe_persist_tool_result

    big = "row," * 60_000
    tokens = _turn()
    try:
        message = maybe_persist_tool_result(big, "tgg_case_query", "call-big-1", env=None, threshold=1000)
        read = _live_turn(
            "import json\n"
            "t = (WORK / 'tool-result-call-big-1.txt').read_text()\n"
            "(WORK / 'result.json').write_text(json.dumps({'chars': len(t)}))\n"
        )
    finally:
        clear_session_vars(tokens)
    assert "could not be saved to sandbox" not in message
    assert "/work/tool-result-call-big-1.txt" in message and "python_sandbox" in message
    value = read.get("result") or read
    assert value["chars"] == len(big), read


def test_a_large_background_helper_result_is_spilled_with_a_pointer(env, tmp_path):
    store = background.CompletionStore(tmp_path / "inbox.db")
    store.record_dispatch({"delegation_id": "deleg_big", "origin": _origin(), "goal": "review"})
    store.record_completion("deleg_big", outcome="completed", result={"summary": "finding " * 20_000})
    _id, event = background.completion_event(store.get("deleg_big"), [])
    assert len(event["body"]) < BOUND
    assert "/work/helper-deleg_big-result.txt" in event["body"]
    assert "more characters are in /work/helper-deleg_big-result.txt" in event["body"]
