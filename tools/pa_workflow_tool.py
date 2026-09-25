"""Hermes adapter for the PA workflow runner (PA-203 round 2).

The runner core, ``agent/pa_workflow.py``, knows nothing about Hermes.  This
adapter supplies what it needs:

1. **The child executor.**  Each child is a native delegation helper of the
   read-only investigation kind, built by ``delegate_tool._build_child_agent``
   with the parent's fixed PA scope, so its tools are fixed by the kind and
   never exceed the parent's.  The runner offers only that kind: a review of
   several cases is read-only, and case edits stay on the single-case route.
2. **The record.**  Every run is stored with who started it, from which chat
   and conversation, and every child's state, in ``$HERMES_HOME/pa_workflows.sqlite``.
   ``workflow_status`` reads that record, so a progress claim is backed by it.
3. **One completion per run.**  When a run finishes, exactly one
   ``workflow_run`` event goes onto the completion queue.  In Christopher's
   durable consumer that event becomes one durable inbox record and one turn
   (``gateway/pa_background_completions.py``).  Where no durable return path
   exists (for example a scheduled job), the run is carried out inside the
   calling turn instead and its result is the tool result.
4. **Resume.**  The consumer resumes unfinished runs at start-up from the
   stored child results; only unfinished children run again.

Tools: ``workflow_start`` and ``workflow_status`` (toolset ``pa-workflow``).
"""

from __future__ import annotations

import contextvars
import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from agent.pa_workflow import ScriptRefused, WorkflowRunner, WorkflowStore, validate_script

logger = logging.getLogger(__name__)

WORKFLOW_TOOLSET = "pa-workflow"
# The runner offers only the read-only investigation kind.
RUNNER_KINDS = ("readonly-investigation",)
RUN_TIME_LIMIT_SECONDS = 3 * 3600
_MAX_CONCURRENCY = 4
# At most this many runs execute at once in one process (review B6.3).
MAX_ACTIVE_RUNS = 3

# run_id -> the reason the host wants it stopped (None while it may run).
# "paused" leaves the run unfinished so it resumes; "timeout" ends it.
_active_runs: Dict[str, Optional[str]] = {}
_active_lock = threading.Lock()

_construction_lock = threading.Lock()
# run_id -> in-memory credentials of the live parent (never persisted)
_live_credentials: Dict[str, Dict[str, Any]] = {}


def store_path() -> Path:
    from hermes_constants import get_hermes_home

    return Path(get_hermes_home()) / "pa_workflows.sqlite"


def _store() -> WorkflowStore:
    return WorkflowStore(store_path())


# ── the parent a child is built from ────────────────────────────────────────

_PARENT_FIELDS = (
    "model", "provider", "base_url", "api_mode", "platform", "max_tokens",
)


class _ParentSpec:
    """A minimal parent for building helpers after the starting turn has ended.

    Carries the PA scope marker and the parent's fixed child toolsets and tool
    names, so ``_build_child_agent`` applies the same fixed-kind scope.
    """

    def __init__(self, spec: Dict[str, Any], credentials: Dict[str, Any]):
        from agent.pa_delegation import HELPER_KIND_TOOLSETS

        for name in _PARENT_FIELDS:
            setattr(self, name, spec.get(name))
        # Only the read-only toolset, whatever the starting parent had, so no
        # path through this parent can build an edit helper (review B1).
        readonly = HELPER_KIND_TOOLSETS[RUNNER_KINDS[0]]
        self.enabled_toolsets = [readonly] if readonly in (spec.get("enabled_toolsets") or []) else []
        self.valid_tool_names = set(spec.get("valid_tool_names") or [])
        self.api_key = credentials.get("api_key")
        if credentials.get("base_url"):
            self.base_url = credentials["base_url"]
        self._delegate_depth = int(spec.get("delegate_depth") or 0)
        self._active_children: list = []
        self._active_children_lock = threading.Lock()
        # Same marker agent.pa_delegation sets on a scoped Management parent.
        self._pa_delegation_child_scope = True

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        return None


def _parent_spec(parent_agent: Any) -> Dict[str, Any]:
    spec = {name: getattr(parent_agent, name, None) for name in _PARENT_FIELDS}
    spec["enabled_toolsets"] = list(getattr(parent_agent, "enabled_toolsets", None) or [])
    names = getattr(parent_agent, "valid_tool_names", None)
    spec["valid_tool_names"] = sorted(names) if isinstance(names, (set, frozenset, list, tuple)) else []
    spec["delegate_depth"] = int(getattr(parent_agent, "_delegate_depth", 0) or 0)
    return {k: v for k, v in spec.items() if isinstance(v, (str, int, float, list, type(None)))}


def _runtime_credentials() -> Dict[str, Any]:
    from gateway.run import _resolve_runtime_agent_kwargs

    return dict(_resolve_runtime_agent_kwargs())


def _make_executor(parent: Any):
    """Build and run one fixed-kind helper per child call."""
    from tools import delegate_tool
    import model_tools

    cfg = delegate_tool._load_config()
    creds = delegate_tool._resolve_delegation_credentials(cfg, parent)
    max_iterations = cfg.get("max_iterations", delegate_tool.DEFAULT_MAX_ITERATIONS)

    def execute(kind: str, prompt: str, schema: dict) -> dict:
        goal = (
            f"{prompt}\n\nReturn only JSON that matches this schema:\n"
            f"{json.dumps(schema, ensure_ascii=False, sort_keys=True)}"
        )
        # Child construction rewrites a module-global tool list; restore it.
        with _construction_lock:
            saved = list(model_tools._last_resolved_tool_names)
            try:
                child = delegate_tool._build_child_agent(
                    task_index=0,
                    goal=goal,
                    context=None,
                    toolsets=None,
                    model=creds["model"],
                    max_iterations=max_iterations,
                    task_count=1,
                    parent_agent=parent,
                    override_provider=creds["provider"],
                    override_base_url=creds["base_url"],
                    override_api_key=creds["api_key"],
                    override_api_mode=creds["api_mode"],
                    role="leaf",
                    profile=kind,
                )
                child._delegate_saved_tool_names = saved
            finally:
                model_tools._last_resolved_tool_names = saved
        result = delegate_tool._run_single_child(0, goal, child, parent)
        status = str(result.get("status") or "")
        return {
            "status": "ok" if status == "completed" else (status or "error"),
            "text": result.get("summary"),
            "error": result.get("error"),
        }

    return execute


# ── running and completing a run ────────────────────────────────────────────

# ── full results in /work, bounded text everywhere else (r2f replay s3) ─────

INLINE_OUTPUT_CHARS = 3000
SAMPLE_RESULTS = 3
LISTED_FAILURES = 10
LISTED_CHILDREN = 20


def _clip_text(value: Any, limit: int) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= limit else text[:limit] + " [...]"


def write_results_files(store: WorkflowStore, run_id: str, status: Dict[str, Any]) -> Dict[str, str]:
    """Write every helper result of a finished run into the sandbox ``/work``.

    ``workflow-<run>-results.json`` holds the run's output and every helper's
    full result; ``workflow-<run>-results.csv`` has one row per helper for
    spreadsheet work.  Returns the ``/work/...`` paths written.
    """
    import csv
    import io

    from tools.work_artifacts import resolve_owner, write_work_file

    # The sandbox folder this turn uses (a scheduled job's bound conversation,
    # else the session), falling back to the run's own conversation.
    run = store.get_run(run_id) or {}
    owner = resolve_owner(None) or str((run.get("origin") or {}).get("session_key") or "") or None
    children = sorted(store.children(run_id).values(), key=lambda c: c["seq"])
    full = {
        "run_id": run_id,
        "title": status.get("title"),
        "status": status.get("status"),
        "error": status.get("error"),
        "counts": status.get("counts"),
        "output": status.get("output"),
        "results": [
            {
                "seq": c["seq"], "kind": c["kind"], "task": c["task"], "status": c["status"],
                "attempts": c["attempts"], "error": c["error"], "output": c["output"],
            }
            for c in children
        ],
    }
    details = {"workflow_run": run_id}
    paths: Dict[str, str] = {}
    json_path = write_work_file(
        f"workflow-{run_id}-results.json",
        json.dumps(full, ensure_ascii=False, indent=1, default=str),
        owner=owner, details=details,
    )
    if json_path:
        paths["results_file"] = json_path
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["seq", "status", "task", "error", "attempts", "result"])
    for c in children:
        writer.writerow([
            c["seq"], c["status"], c["task"], c["error"] or "", c["attempts"],
            "" if c["output"] is None else json.dumps(c["output"], ensure_ascii=False, default=str),
        ])
    csv_path = write_work_file(
        f"workflow-{run_id}-results.csv", buffer.getvalue(), owner=owner, details=details,
    )
    if csv_path:
        paths["results_csv"] = csv_path
    return paths


def bounded_summary(store: WorkflowStore, run_id: str, status: Dict[str, Any],
                    paths: Dict[str, str]) -> Dict[str, Any]:
    """Counts, failures and a small sample, never the full results."""
    from tools.work_artifacts import read_hint

    children = sorted(store.children(run_id).values(), key=lambda c: c["seq"])
    failures = [c for c in children if c["status"] == "failed"]
    oks = [c for c in children if c["status"] == "ok"]
    summary: Dict[str, Any] = {
        "run_id": run_id,
        "title": status.get("title"),
        "status": status.get("status"),
        # Who started it: the run record Teren's ruling requires.
        "started_by": status.get("started_by") or (store.get_run(run_id) or {}).get("started_by"),
        "created_at": status.get("created_at"),
        "updated_at": status.get("updated_at"),
        "counts": status.get("counts"),
        "total_helpers": len(children),
        "error": status.get("error"),
        "failures": [
            {"seq": c["seq"], "task": _clip_text(c["task"], 160), "error": _clip_text(c["error"] or "", 300)}
            for c in failures[:LISTED_FAILURES]
        ],
        "sample": [
            {"seq": c["seq"], "task": _clip_text(c["task"], 160), "output": _clip_text(c["output"], 600)}
            for c in oks[:SAMPLE_RESULTS]
        ],
        **paths,
    }
    if len(failures) > LISTED_FAILURES:
        summary["failures_not_listed"] = len(failures) - LISTED_FAILURES
    output = status.get("output")
    if output is not None:
        encoded = json.dumps(output, ensure_ascii=False, default=str)
        if len(encoded) <= INLINE_OUTPUT_CHARS:
            summary["output"] = output
        else:
            summary["output_chars"] = len(encoded)
    if paths.get("results_file"):
        summary["read_full_results"] = read_hint(paths["results_file"])
    else:
        summary["read_full_results"] = (
            "The full results could not be written to /work; this summary is all that is available here."
        )
    return summary


def _finished_summary(store: WorkflowStore, run_id: str, status: Dict[str, Any]) -> Dict[str, Any]:
    return bounded_summary(store, run_id, status, write_results_files(store, run_id, status))


def _emit_completion(store: WorkflowStore, run_id: str, status: Dict[str, Any]) -> bool:
    """Put exactly one completion for this run on the completion queue."""
    if not store.claim_completion(run_id):
        return False
    from tools.process_registry import process_registry

    summary = _finished_summary(store, run_id, status)
    process_registry.completion_queue.put({
        "type": "workflow_run",
        "run_id": run_id,
        "status": "completed" if status.get("status") == "completed" else "failed",
        "counts": status.get("counts"),
        "summary": summary,
        "results_file": summary.get("results_file"),
        "error": status.get("error"),
    })
    return True


def stop_workflow(run_id: str, reason: str) -> bool:
    """Ask a run executing in this process to stop before its next helper."""
    with _active_lock:
        if run_id not in _active_runs:
            return False
        _active_runs[run_id] = reason
        return True


def pause_all_workflows() -> int:
    """Christopher is paused: stop every active run; each stays resumable."""
    with _active_lock:
        for run_id in _active_runs:
            _active_runs[run_id] = _active_runs[run_id] or "paused"
        return len(_active_runs)


def active_workflow_ids() -> set[str]:
    with _active_lock:
        return set(_active_runs)


def _register_active(run_id: str) -> bool:
    with _active_lock:
        if run_id in _active_runs:
            return False
        if len(_active_runs) >= MAX_ACTIVE_RUNS:
            return False
        _active_runs[run_id] = None
        return True


def _run(run_id: str, parent: Any, *, emit: bool) -> Dict[str, Any]:
    store = _store()
    record = store.get_run(run_id) or {}
    deadline = float(record.get("created_at") or time.time()) + RUN_TIME_LIMIT_SECONDS

    def stop_reason() -> Optional[str]:
        with _active_lock:
            reason = _active_runs.get(run_id)
        if reason:
            return reason
        return "timeout" if time.time() > deadline else None

    runner = WorkflowRunner(
        store, _make_executor(parent), kinds=RUNNER_KINDS, max_concurrency=_MAX_CONCURRENCY,
        stop_reason=stop_reason, max_script_seconds=RUN_TIME_LIMIT_SECONDS,
    )
    try:
        status = runner.run(run_id)
    except Exception as exc:  # the core never raises for script/child errors
        logger.exception("workflow run %s crashed", run_id)
        store.finish_run(run_id, status="failed", output=None, error=f"{type(exc).__name__}: {exc}")
        status = store.run_status(run_id)
    finally:
        _live_credentials.pop(run_id, None)
    try:
        # A paused run is still unfinished: no completion until it resumes and ends.
        if emit and status.get("status") != "running":
            _emit_completion(store, run_id, status)
    finally:
        # Free the slot only after the completion is queued, so nothing sees
        # a finished run as neither active nor completed.
        with _active_lock:
            _active_runs.pop(run_id, None)
    return status


def _session_env_snapshot() -> Dict[str, str]:
    """Every session context value the business bridge and tools read.

    A plain thread starts with no context variables, so a helper built there
    resolved no PA job brief and every business operation was "unknown"
    (r2b replay s6, wf-9e86a66f5c41).  The snapshot is persisted with the run
    so a resumed run re-establishes it too.
    """
    from gateway.session_context import _VAR_MAP, get_session_env
    from tools.approval import get_current_session_key

    env = {name: get_session_env(name, "") for name in _VAR_MAP}
    env = {name: value for name, value in env.items() if value}
    approval_key = get_current_session_key(default="")
    if approval_key:
        env["approval_session_key"] = approval_key
    return env


def _enter_session_env(env: Dict[str, str]) -> None:
    """Set the persisted session values in the current (run-owned) context."""
    from gateway.session_context import _VAR_MAP
    from tools.approval import set_current_session_key

    for name, value in (env or {}).items():
        var = _VAR_MAP.get(name)
        if var is not None:
            var.set(str(value))
    if env.get("approval_session_key"):
        set_current_session_key(env["approval_session_key"])


def _start_thread(
    run_id: str,
    parent: Any,
    *,
    context: Optional[contextvars.Context] = None,
    session_env: Optional[Dict[str, str]] = None,
) -> None:
    """Run a workflow on its own thread inside a context that carries the turn.

    A run started from a turn uses a copy of that turn's context.  A resumed
    run has no turn, so it gets a fresh context with the persisted session
    values set before any helper starts.
    """
    ctx = context if context is not None else contextvars.Context()

    def target() -> None:
        if session_env:
            _enter_session_env(session_env)
        _run(run_id, parent, emit=True)

    thread = threading.Thread(
        target=ctx.run, args=(target,), name=f"pa-workflow-{run_id}", daemon=True,
    )
    thread.start()


def _origin(parent_agent: Any) -> Dict[str, Any]:
    from gateway.session_context import get_session_env
    from tools.approval import get_current_session_key

    try:
        refs = json.loads(get_session_env("HERMES_SESSION_SOURCE_MESSAGE_REFS", "") or "[]")
    except (TypeError, ValueError):
        refs = []
    return {
        "session_key": get_current_session_key(default=""),
        "platform": get_session_env("HERMES_SESSION_PLATFORM", ""),
        "chat_id": get_session_env("HERMES_SESSION_CHAT_ID", ""),
        "source_message_ids": [str(ref) for ref in (refs or []) if ref],
        "pa_job_type": get_session_env("HERMES_SESSION_PA_JOB_TYPE", ""),
        "session_env": _session_env_snapshot(),
        "parent": _parent_spec(parent_agent),
    }


def workflow_start(
    script: str,
    title: Optional[str] = None,
    wait: Optional[bool] = None,
    parent_agent: Any = None,
) -> str:
    from agent.pa_delegation import HELPER_KIND_TOOLSETS, is_scoped
    from tools.async_delegation import get_durable_sink
    from tools.registry import tool_error

    if parent_agent is None or not is_scoped(parent_agent):
        return tool_error("workflow_start is available only on the TGG Management route.")
    readonly_toolset = HELPER_KIND_TOOLSETS[RUNNER_KINDS[0]]
    if readonly_toolset not in (getattr(parent_agent, "enabled_toolsets", None) or []):
        return tool_error(
            f"workflow_start needs the {readonly_toolset} toolset, which this conversation does not have."
        )
    try:
        validate_script(script or "")
    except ScriptRefused as exc:
        return tool_error(f"Script refused: {exc}")
    run_id = f"wf-{uuid.uuid4().hex[:12]}"
    if not _register_active(run_id):
        return tool_error(
            f"{MAX_ACTIVE_RUNS} workflow runs are already running; wait for one to finish "
            "(workflow_status shows their progress) before starting another."
        )
    title = (title or "").strip()[:200] or "workflow run"
    origin = _origin(parent_agent)
    started_by = {
        key: origin.get(key) for key in ("chat_id", "session_key", "source_message_ids", "platform")
    }
    from gateway.session_context import get_session_env

    # A scheduled Management job acts for the chat it delivers to (s7).
    started_by["deliver_chat_id"] = get_session_env("HERMES_CRON_AUTO_DELIVER_CHAT_ID", "")
    started_by["user_id"] = get_session_env("HERMES_SESSION_USER_ID", "")
    started_by["user_name"] = get_session_env("HERMES_SESSION_USER_NAME", "")
    store = _store()
    store.create_run(run_id, script=script, title=title, started_by=started_by, origin=origin)
    credentials = {"api_key": getattr(parent_agent, "api_key", None)}
    parent = _ParentSpec(origin["parent"], credentials)

    sink = get_durable_sink()
    background = sink is not None and not bool(wait)
    if background:
        try:
            sink.record_dispatch({
                "source": "workflow",
                "run_id": run_id,
                "origin": origin,
                "title": title,
                "goal": title,
                "deadline_at": time.time() + RUN_TIME_LIMIT_SECONDS,
            })
        except Exception as exc:
            # No conversation to return to (for example a scheduled job):
            # carry the run out inside this turn instead.
            logger.info("workflow %s runs in the calling turn: %s", run_id, exc)
            background = False
    if background:
        _live_credentials[run_id] = credentials
        _start_thread(
            run_id, parent, context=contextvars.copy_context(),
            session_env=origin.get("session_env"),
        )
        return json.dumps({
            "run_id": run_id,
            "status": "started",
            "note": (
                "The run's one result will come back to this conversation as a new "
                "message when every helper has finished. workflow_status shows its real progress."
            ),
        })
    status = _run(run_id, parent, emit=False)
    store.claim_completion(run_id)
    return json.dumps(_finished_summary(store, run_id, status), ensure_ascii=False, default=str)


def _release_on_error(fn):
    """Free the active-run slot if workflow_start fails before a run starts."""
    def wrapper(*args, **kwargs):
        before = active_workflow_ids()
        try:
            return fn(*args, **kwargs)
        except BaseException:
            with _active_lock:
                for run_id in active_workflow_ids() - before:
                    _active_runs.pop(run_id, None)
            raise
    return wrapper


workflow_start = _release_on_error(workflow_start)


def workflow_status(run_id: Optional[str] = None, parent_agent: Any = None) -> str:
    from gateway.session_context import get_session_env
    from tools.registry import tool_error

    # A scheduled job has no session chat; it acts for its delivery chat.
    chat_id = get_session_env("HERMES_SESSION_CHAT_ID", "") or get_session_env(
        "HERMES_CRON_AUTO_DELIVER_CHAT_ID", ""
    )
    from tools.approval import get_current_session_key

    session_key = get_current_session_key(default="") or get_session_env("HERMES_SESSION_KEY", "")
    store = _store()
    if not run_id:
        return tool_error("Provide the run_id that workflow_start returned.")
    run = store.get_run(run_id)
    if run is None:
        return tool_error(f"No workflow run {run_id}.")
    started_by = run.get("started_by") or {}
    run_chat = str(started_by.get("chat_id") or started_by.get("deliver_chat_id") or "")
    run_key = str(started_by.get("session_key") or "")
    same_chat = bool(chat_id and run_chat and chat_id == run_chat)
    same_session = bool(session_key and run_key and session_key == run_key)
    if not (same_chat or same_session):
        return tool_error(f"Workflow run {run_id} belongs to another conversation.")
    status = store.run_status(run_id)
    if status.get("status") != "running":
        # Finished: the bounded summary and the /work files with every result.
        return json.dumps(_finished_summary(store, run_id, status), ensure_ascii=False, default=str)
    children = status.get("children") or []
    status["children"] = children[:LISTED_CHILDREN]
    if len(children) > LISTED_CHILDREN:
        status["children_not_listed"] = len(children) - LISTED_CHILDREN
    status.pop("origin", None)
    return json.dumps(status, ensure_ascii=False, default=str)


# ── consumer start-up recovery ──────────────────────────────────────────────

def prepare_workflow_recovery(completion_store: Any) -> list[str]:
    """Decide, before interrupting anything, which workflow runs resume.

    A run already finished whose completion never reached the store is
    completed from its stored status now.  Unfinished runs are returned.
    """
    rows = completion_store.running(source="workflow")
    store = _store()
    tracked = {str(row["completion_id"]) for row in rows}
    # A run carried out inside a turn has no completion row; if the process
    # stopped during it, nothing can resume it, so it ends failed (review B6.4).
    for run in store.unfinished_runs():
        if run["run_id"] not in tracked and run["run_id"] not in active_workflow_ids():
            store.finish_run(
                run["run_id"], status="failed", output=None,
                error="the process stopped while this run was being carried out inside a turn",
            )
    keep: list[str] = []
    for row in rows:
        run_id = str(row["completion_id"])
        run = store.get_run(run_id)
        if run is None:
            continue
        status = store.run_status(run_id)
        if status.get("status") == "running":
            keep.append(run_id)
            continue
        store.claim_completion(run_id)
        summary = _finished_summary(store, run_id, status)
        completion_store.record_completion(
            run_id,
            outcome="completed" if status.get("status") == "completed" else "failed",
            result={
                "status": status.get("status"), "run_id": run_id,
                "counts": status.get("counts"), "summary": summary,
                "results_file": summary.get("results_file"), "error": status.get("error"),
            },
        )
    return keep


def resume_workflow_runs(run_ids: list[str]) -> list[str]:
    """Resume unfinished runs in this process from their stored results.

    Called by the consumer only while processing is enabled (review item 8).
    Returns the runs actually started.
    """
    store = _store()
    credentials: Dict[str, Any] = {}
    started: list[str] = []
    for run_id in run_ids:
        run = store.get_run(run_id) or {}
        if run.get("status") != "running" or not _register_active(run_id):
            continue
        origin = run.get("origin") or {}
        spec = dict(origin.get("parent") or {})
        if not credentials:
            try:
                credentials = _runtime_credentials()
            except Exception as exc:
                logger.error("workflow resume: no runtime credentials: %s", exc)
                credentials = {}
        _start_thread(
            run_id, _ParentSpec(spec, credentials), session_env=dict(origin.get("session_env") or {}),
        )
        started.append(run_id)
    return started


# ── registry ────────────────────────────────────────────────────────────────

WORKFLOW_START_SCHEMA = {
    "name": "workflow_start",
    "description": (
        "Run a large read-only review as one workflow: a short script fans the work out to "
        "read-only investigation helpers, checks each helper's answer against your JSON "
        "schema, retries failures and collects the results. The run is recorded; its one "
        "result returns to this conversation as a new message when it finishes. Script API: "
        "task(kind, prompt, schema) with kind 'readonly-investigation'; run(spec); "
        "parallel(specs); map(items, fn); chunks(items, size); join(items, sep); results(). "
        "Assign the final value to `output`. Plain Python expressions, for, if and lambda "
        "only; no imports, attributes, functions, files or other tools. map() starts the "
        "helpers itself and returns their results; there is no need to pass them to "
        "parallel(). Example: schema = {'type': 'object', 'required': ['cases']}; "
        "found = map(chunks(jobs, 5), lambda group: task('readonly-investigation', "
        "'Check WhatsApp for ' + join(group, ', '), schema)); "
        "output = [r['output'] for r in found if r['status'] == 'ok']. "
        "If the script stops early, every helper result that came back is still returned. "
        "Helpers only gather evidence: build any file yourself with python_sandbox."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "script": {"type": "string", "description": "The workflow script."},
            "title": {"type": "string", "description": "A short description of the run."},
            "wait": {
                "type": "boolean",
                "description": "Run inside this turn and return the result directly (default false).",
            },
        },
        "required": ["script"],
    },
}

WORKFLOW_STATUS_SCHEMA = {
    "name": "workflow_status",
    "description": (
        "Read a workflow run's recorded state: who started it, how many helpers are "
        "pending, running, finished or failed, and the output once it has finished."
    ),
    "parameters": {
        "type": "object",
        "properties": {"run_id": {"type": "string"}},
        "required": ["run_id"],
    },
}

from tools.registry import registry  # noqa: E402

registry.register(
    name="workflow_start",
    toolset=WORKFLOW_TOOLSET,
    schema=WORKFLOW_START_SCHEMA,
    handler=lambda args, **kw: workflow_start(
        script=args.get("script") or "",
        title=args.get("title"),
        wait=args.get("wait"),
        parent_agent=kw.get("parent_agent"),
    ),
)
registry.register(
    name="workflow_status",
    toolset=WORKFLOW_TOOLSET,
    schema=WORKFLOW_STATUS_SCHEMA,
    handler=lambda args, **kw: workflow_status(
        run_id=args.get("run_id"), parent_agent=kw.get("parent_agent"),
    ),
)
