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

def _emit_completion(store: WorkflowStore, run_id: str, status: Dict[str, Any]) -> bool:
    """Put exactly one completion for this run on the completion queue."""
    if not store.claim_completion(run_id):
        return False
    from tools.process_registry import process_registry

    process_registry.completion_queue.put({
        "type": "workflow_run",
        "run_id": run_id,
        "status": "completed" if status.get("status") == "completed" else "failed",
        "counts": status.get("counts"),
        "output": status.get("output"),
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
        with _active_lock:
            _active_runs.pop(run_id, None)
    # A paused run is still unfinished: no completion until it resumes and ends.
    if emit and status.get("status") != "running":
        _emit_completion(store, run_id, status)
    return status


def _start_thread(run_id: str, parent: Any) -> None:
    thread = threading.Thread(
        target=_run, args=(run_id, parent), kwargs={"emit": True},
        name=f"pa-workflow-{run_id}", daemon=True,
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
        _start_thread(run_id, parent)
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
    return json.dumps(status, ensure_ascii=False, default=str)


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

    chat_id = get_session_env("HERMES_SESSION_CHAT_ID", "")
    store = _store()
    if not run_id:
        return tool_error("Provide the run_id that workflow_start returned.")
    run = store.get_run(run_id)
    if run is None:
        return tool_error(f"No workflow run {run_id}.")
    run_chat = str(((run.get("started_by") or {}).get("chat_id")) or "")
    if not chat_id or not run_chat or chat_id != run_chat:
        return tool_error(f"Workflow run {run_id} belongs to another conversation.")
    return json.dumps(store.run_status(run_id), ensure_ascii=False, default=str)


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
        completion_store.record_completion(
            run_id,
            outcome="completed" if status.get("status") == "completed" else "failed",
            result={
                "status": status.get("status"), "run_id": run_id,
                "counts": status.get("counts"), "output": status.get("output"),
                "error": status.get("error"),
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
        spec = dict((run.get("origin") or {}).get("parent") or {})
        if not credentials:
            try:
                credentials = _runtime_credentials()
            except Exception as exc:
                logger.error("workflow resume: no runtime credentials: %s", exc)
                credentials = {}
        _start_thread(run_id, _ParentSpec(spec, credentials))
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
        "only; no imports, attributes, functions, files or other tools."
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
