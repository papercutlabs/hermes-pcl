"""Runtime-neutral workflow runner core for PA-203.

A parent agent writes a short script against a restricted API.  This module
checks the script, runs it, fans out helper children through an injected
executor, checks every child's output against a JSON Schema, retries failed
children, and persists each finished child the moment it finishes, so a
restarted process resumes a run instead of starting it again.

It deliberately imports nothing from the Hermes runtime: the Hermes adapter
(tools/pa_workflow_tool.py) supplies the child executor, the run origin and
the single completion that a finished run produces.

Script API (the only names a script can call, besides the safe builtins and
names the script binds itself):

    task(kind, prompt, schema)  -> a child spec; starts nothing
    run(spec)                   -> one child result
    parallel(specs)             -> child results in input order, run concurrently
    map(items, fn)              -> parallel([fn(x) for x in items])
    chunks(items, size)         -> items split into lists of at most ``size``
    join(items, sep=" ")        -> the items as text joined by ``sep`` (no methods exist)
    results()                   -> every finished child result of this run

The run's output is the script's global ``output`` if it assigns one, else
``results()``.  A child result is a dict with key, seq, kind, task, status
("ok" or "failed"), output, error and attempts.  A failed child never raises
into the script.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Iterable

import jsonschema

ChildExecutor = Callable[[str, str, dict], dict]

API_NAMES = frozenset({"task", "run", "parallel", "map", "chunks", "join", "results"})
_RANGE_CAP = 10_000


class ScriptRefused(ValueError):
    """The script uses something outside the workflow API."""


class _RunLimit(Exception):
    """A run exceeded a configured bound."""


def _safe_range(*args: int) -> range:
    r = range(*args)
    if len(r) > _RANGE_CAP:
        raise _RunLimit(f"range() is limited to {_RANGE_CAP} items")
    return r


SAFE_BUILTINS: dict[str, Any] = {
    "len": len, "range": _safe_range, "enumerate": enumerate, "zip": zip,
    "sorted": sorted, "min": min, "max": max, "sum": sum, "str": str,
    "int": int, "float": float, "bool": bool, "list": list, "dict": dict,
    "set": set, "tuple": tuple, "abs": abs, "any": any, "all": all,
    "round": round,
}

# ── Script check ──────────────────────────────────────────────────────────

_ALLOWED_NODES = (
    ast.Module, ast.Expr, ast.Assign, ast.AugAssign, ast.For, ast.If, ast.Pass,
    ast.Break, ast.Continue, ast.Compare, ast.BoolOp, ast.BinOp, ast.UnaryOp,
    ast.IfExp, ast.Call, ast.Name, ast.Constant, ast.List, ast.Tuple, ast.Dict,
    ast.Set, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp,
    ast.comprehension, ast.Lambda, ast.arguments, ast.arg, ast.keyword,
    ast.Subscript, ast.Slice, ast.JoinedStr, ast.FormattedValue,
    ast.operator, ast.unaryop, ast.cmpop, ast.boolop, ast.expr_context,
)

_ALLOWED_TEXT = (
    "A workflow script may use assignments, for loops, if statements, "
    "comprehensions, lambdas, literals, subscripts and calls to: "
    + ", ".join(sorted(API_NAMES)) + ", and the builtins "
    + ", ".join(sorted(SAFE_BUILTINS)) + "."
)


def validate_script(source: str, *, max_chars: int = 20000) -> ast.Module:
    """Parse ``source`` and refuse anything outside the workflow API."""
    if not isinstance(source, str) or not source.strip():
        raise ScriptRefused("The workflow script is empty. " + _ALLOWED_TEXT)
    if len(source) > max_chars:
        raise ScriptRefused(f"The workflow script is longer than {max_chars} characters.")
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise ScriptRefused(f"The workflow script does not parse: {exc.msg} (line {exc.lineno}).") from exc

    bound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)

    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ScriptRefused(
                f"The workflow script may not use {type(node).__name__} "
                f"(line {getattr(node, 'lineno', '?')}). " + _ALLOWED_TEXT
            )
        if isinstance(node, ast.Pow):
            raise ScriptRefused("The workflow script may not use the ** operator. " + _ALLOWED_TEXT)
        if isinstance(node, ast.Name) and node.id.startswith("_"):
            raise ScriptRefused(f"The workflow script may not use the name {node.id!r}. " + _ALLOWED_TEXT)
        if isinstance(node, ast.arg) and node.arg.startswith("_"):
            raise ScriptRefused(f"The workflow script may not use the name {node.arg!r}. " + _ALLOWED_TEXT)
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise ScriptRefused(
                    f"The workflow script may only call a plain name (line {node.lineno}). " + _ALLOWED_TEXT
                )
            name = node.func.id
            if name not in API_NAMES and name not in SAFE_BUILTINS and name not in bound:
                raise ScriptRefused(
                    f"The workflow script may not call {name!r}: tools, sending and file "
                    "writes are not available to a workflow. " + _ALLOWED_TEXT
                )
    return tree


# ── JSON helpers ──────────────────────────────────────────────────────────

_FENCE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.DOTALL | re.IGNORECASE)


def extract_json(text: str | None) -> Any:
    """Return the JSON value in a child's answer, or raise ValueError."""
    if not text or not str(text).strip():
        raise ValueError("the child returned no text")
    text = str(text).strip()
    try:
        return json.loads(text)
    except ValueError:
        pass
    for match in _FENCE.finditer(text):
        try:
            return json.loads(match.group(1).strip())
        except ValueError:
            continue
    decoder = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch in "{[":
            try:
                value, _ = decoder.raw_decode(text[i:])
                return value
            except ValueError:
                continue
    raise ValueError("the child's answer contains no JSON value")


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


# ── Store ─────────────────────────────────────────────────────────────────


def _now() -> float:
    return time.time()


class WorkflowStore:
    """Durable record of runs and their children (SQLite)."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    title TEXT,
                    script TEXT NOT NULL,
                    started_by_json TEXT NOT NULL,
                    origin_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('running','completed','failed')),
                    output_json TEXT,
                    error TEXT,
                    completion_claimed INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workflow_children (
                    run_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    task TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','running','ok','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    output_json TEXT,
                    error TEXT,
                    started_at REAL,
                    finished_at REAL,
                    PRIMARY KEY (run_id, key),
                    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.row_factory = sqlite3.Row
        return conn

    def _write(self, sql: str, params: Iterable[Any]) -> int:
        with self._lock:
            conn = self._connect()
            try:
                with conn:
                    return conn.execute(sql, tuple(params)).rowcount
            finally:
                conn.close()

    def _read(self, sql: str, params: Iterable[Any]) -> list[sqlite3.Row]:
        conn = self._connect()
        try:
            return conn.execute(sql, tuple(params)).fetchall()
        finally:
            conn.close()

    def create_run(self, run_id: str, *, script: str, title: str, started_by: dict, origin: dict) -> None:
        now = _now()
        self._write(
            "INSERT INTO workflow_runs(run_id,title,script,started_by_json,origin_json,status,"
            "created_at,updated_at) VALUES(?,?,?,?,?,'running',?,?)",
            (run_id, title, script, _canonical(started_by or {}), _canonical(origin or {}), now, now),
        )

    def get_run(self, run_id: str) -> dict | None:
        rows = self._read("SELECT * FROM workflow_runs WHERE run_id=?", (run_id,))
        return self._run_dict(rows[0]) if rows else None

    @staticmethod
    def _run_dict(row: sqlite3.Row) -> dict:
        return {
            "run_id": row["run_id"],
            "title": row["title"],
            "script": row["script"],
            "status": row["status"],
            "started_by": json.loads(row["started_by_json"]),
            "origin": json.loads(row["origin_json"]),
            "output": json.loads(row["output_json"]) if row["output_json"] is not None else None,
            "error": row["error"],
            "completion_claimed": bool(row["completion_claimed"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def children(self, run_id: str) -> dict[str, dict]:
        rows = self._read("SELECT * FROM workflow_children WHERE run_id=? ORDER BY seq", (run_id,))
        return {row["key"]: self._child_dict(row) for row in rows}

    @staticmethod
    def _child_dict(row: sqlite3.Row) -> dict:
        return {
            "key": row["key"],
            "seq": row["seq"],
            "kind": row["kind"],
            "task": row["task"],
            "status": row["status"],
            "output": json.loads(row["output_json"]) if row["output_json"] is not None else None,
            "error": row["error"],
            "attempts": row["attempts"],
        }

    def register_child(self, run_id: str, *, key: str, seq: int, kind: str, task: str, schema: dict) -> None:
        self._write(
            "INSERT OR IGNORE INTO workflow_children(run_id,key,seq,kind,task,schema_json,status)"
            " VALUES(?,?,?,?,?,?,'pending')",
            (run_id, key, seq, kind, task, _canonical(schema)),
        )

    def mark_child_running(self, run_id: str, key: str, attempts: int) -> None:
        self._write(
            "UPDATE workflow_children SET status='running',attempts=?,"
            "started_at=COALESCE(started_at,?) WHERE run_id=? AND key=?",
            (attempts, _now(), run_id, key),
        )
        self._touch(run_id)

    def finish_child(self, run_id: str, key: str, *, status: str, output: Any, error: str | None, attempts: int) -> None:
        self._write(
            "UPDATE workflow_children SET status=?,output_json=?,error=?,attempts=?,finished_at=?"
            " WHERE run_id=? AND key=?",
            (status, None if output is None else _canonical(output), error, attempts, _now(), run_id, key),
        )
        self._touch(run_id)

    def _touch(self, run_id: str) -> None:
        self._write("UPDATE workflow_runs SET updated_at=? WHERE run_id=?", (_now(), run_id))

    def finish_run(self, run_id: str, *, status: str, output: Any, error: str | None) -> None:
        try:
            output_json = None if output is None else _canonical(_jsonable(output))
        except (TypeError, ValueError):
            output_json = _canonical(repr(output))
            error = (error + "; " if error else "") + "output was not JSON-serialisable and is stored as text"
        self._write(
            "UPDATE workflow_runs SET status=?,output_json=?,error=?,updated_at=? WHERE run_id=?",
            (status, output_json, error, _now(), run_id),
        )

    def unfinished_runs(self) -> list[dict]:
        rows = self._read("SELECT * FROM workflow_runs WHERE status='running' ORDER BY created_at", ())
        return [self._run_dict(row) for row in rows]

    def claim_completion(self, run_id: str) -> bool:
        return self._write(
            "UPDATE workflow_runs SET completion_claimed=1 WHERE run_id=? AND completion_claimed=0",
            (run_id,),
        ) == 1

    def completion_claimed(self, run_id: str) -> bool:
        run = self.get_run(run_id)
        return bool(run and run["completion_claimed"])

    def run_status(self, run_id: str) -> dict:
        run = self.get_run(run_id)
        if run is None:
            raise KeyError(f"no workflow run {run_id!r}")
        children = list(self.children(run_id).values())
        counts = {"pending": 0, "running": 0, "ok": 0, "failed": 0}
        for child in children:
            counts[child["status"]] += 1
        status = {
            "run_id": run_id,
            "title": run["title"],
            "status": run["status"],
            "started_by": run["started_by"],
            "origin": run["origin"],
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
            "counts": counts,
            "total_children": len(children),
            "children": [
                {
                    "key": c["key"], "seq": c["seq"], "kind": c["kind"], "task": c["task"][:200],
                    "status": c["status"], "attempts": c["attempts"], "error": c["error"],
                }
                for c in children
            ],
            "error": run["error"],
        }
        if run["status"] != "running":
            status["output"] = run["output"]
        return status


# ── Runner ────────────────────────────────────────────────────────────────


class WorkflowRunner:
    """Execute or resume one stored run to completion."""

    def __init__(
        self,
        store: WorkflowStore,
        execute_child: ChildExecutor,
        *,
        kinds: Iterable[str],
        max_concurrency: int = 4,
        max_attempts: int = 3,
        max_children: int = 500,
    ):
        self.store = store
        self.execute_child = execute_child
        self.kinds = frozenset(kinds)
        self.max_concurrency = max(1, int(max_concurrency))
        self.max_attempts = max(1, int(max_attempts))
        self.max_children = max(1, int(max_children))

    def run(self, run_id: str) -> dict:
        record = self.store.get_run(run_id)
        if record is None:
            raise KeyError(f"no workflow run {run_id!r}")
        if record["status"] != "running":
            return self.store.run_status(run_id)
        try:
            tree = validate_script(record["script"])
            output = _Execution(self, run_id).execute(tree)
            self.store.finish_run(run_id, status="completed", output=output, error=None)
        except Exception as exc:  # script, limit or kind errors end the run failed
            self.store.finish_run(run_id, status="failed", output=None, error=f"{type(exc).__name__}: {exc}")
        return self.store.run_status(run_id)

    def _attempt_child(self, run_id: str, key: str, kind: str, prompt: str, schema: dict, prior_attempts: int) -> dict:
        attempts = prior_attempts
        error: str | None = None
        text_prompt = prompt
        while attempts < self.max_attempts:
            attempts += 1
            self.store.mark_child_running(run_id, key, attempts)
            try:
                reply = self.execute_child(kind, text_prompt, schema) or {}
                if reply.get("status") not in (None, "ok", "completed"):
                    raise ValueError(reply.get("error") or f"the child ended with status {reply.get('status')}")
                value = extract_json(reply.get("text"))
                jsonschema.validate(value, schema)
            except Exception as exc:
                error = exc.message if isinstance(exc, jsonschema.ValidationError) else str(exc)
                error = error[:500]
                text_prompt = (
                    f"{prompt}\n\nYour previous answer could not be used: {error}\n"
                    f"Return only JSON that matches this schema:\n{_canonical(schema)}"
                )
                continue
            self.store.finish_child(run_id, key, status="ok", output=value, error=None, attempts=attempts)
            return value
        self.store.finish_child(run_id, key, status="failed", output=None, error=error, attempts=attempts)
        return None


class _Execution:
    """One pass of a run's script, mapping each child call to a stable key."""

    def __init__(self, runner: WorkflowRunner, run_id: str):
        self.runner = runner
        self.run_id = run_id
        self.stored = runner.store.children(run_id)
        self.occurrences: dict[str, int] = {}
        self.seq = 0

    def execute(self, tree: ast.Module) -> Any:
        scope: dict[str, Any] = {"__builtins__": {}}
        scope.update(SAFE_BUILTINS)
        scope.update(
            task=self.task, run=self.run_one, parallel=self.parallel,
            map=self.map, chunks=self.chunks, join=self.join, results=self.results,
        )
        exec(compile(tree, "<workflow>", "exec"), scope)  # noqa: S102 - AST-whitelisted
        return scope["output"] if "output" in scope else self.results()

    # API --------------------------------------------------------------
    def task(self, kind: Any, prompt: Any, schema: Any) -> dict:
        if kind not in self.runner.kinds:
            raise ScriptRefused(
                f"Unknown helper kind {kind!r}; the kinds are {', '.join(sorted(self.runner.kinds))}."
            )
        if not isinstance(prompt, str) or not prompt.strip():
            raise ScriptRefused("A helper task needs a non-empty prompt string.")
        if not isinstance(schema, dict):
            raise ScriptRefused("A helper task needs a JSON Schema object as its schema.")
        try:
            jsonschema.Draft202012Validator.check_schema(schema)
        except jsonschema.SchemaError as exc:
            raise ScriptRefused(f"The helper output schema is not valid JSON Schema: {exc.message}") from exc
        return {"kind": kind, "prompt": prompt, "schema": schema}

    def run_one(self, spec: Any) -> dict:
        return self.parallel([spec])[0]

    def map(self, items: Any, fn: Any) -> list[dict]:
        return self.parallel([fn(item) for item in items])

    @staticmethod
    def chunks(items: Any, size: Any) -> list[list]:
        size = int(size)
        if size < 1:
            raise ScriptRefused("chunks() needs a size of at least 1.")
        items = list(items)
        return [items[i:i + size] for i in range(0, len(items), size)]

    @staticmethod
    def join(items: Any, sep: Any = " ") -> str:
        return str(sep).join(str(item) for item in items)

    def results(self) -> list[dict]:
        done = self.runner.store.children(self.run_id).values()
        return [self._result(c) for c in done if c["status"] in ("ok", "failed")]

    def parallel(self, specs: Any) -> list[dict]:
        specs = list(specs)
        for spec in specs:
            if not isinstance(spec, dict) or set(spec) != {"kind", "prompt", "schema"}:
                raise ScriptRefused("parallel() and run() take specs made by task(kind, prompt, schema).")
        keys = [self._assign(spec) for spec in specs]
        todo = [(key, spec) for key, spec in zip(keys, specs)
                if self.stored[key]["status"] not in ("ok", "failed")]
        if todo:
            pool = ThreadPoolExecutor(max_workers=min(self.runner.max_concurrency, len(todo)))
            try:
                futures = [
                    pool.submit(
                        self.runner._attempt_child, self.run_id, key, spec["kind"], spec["prompt"],
                        spec["schema"], 0,
                    )
                    for key, spec in todo
                ]
                for future in futures:
                    future.result()
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
        self.stored = self.runner.store.children(self.run_id)
        return [self._result(self.stored[key]) for key in keys]

    # internals --------------------------------------------------------
    def _assign(self, spec: dict) -> str:
        base = hashlib.sha256(
            _canonical([spec["kind"], spec["prompt"], spec["schema"]]).encode()
        ).hexdigest()
        n = self.occurrences.get(base, 0)
        self.occurrences[base] = n + 1
        key = hashlib.sha256(f"{base}:{n}".encode()).hexdigest()[:24]
        if key not in self.stored:
            if len(self.stored) >= self.runner.max_children:
                raise _RunLimit(f"a run may start at most {self.runner.max_children} helpers")
            seq = len(self.stored)
            self.runner.store.register_child(
                self.run_id, key=key, seq=seq, kind=spec["kind"], task=spec["prompt"], schema=spec["schema"],
            )
            self.stored[key] = {
                "key": key, "seq": seq, "kind": spec["kind"], "task": spec["prompt"],
                "status": "pending", "output": None, "error": None, "attempts": 0,
            }
        return key

    @staticmethod
    def _result(child: dict) -> dict:
        return {
            "key": child["key"], "seq": child["seq"], "kind": child["kind"], "task": child["task"],
            "status": child["status"] if child["status"] in ("ok", "failed") else "failed",
            "output": child["output"], "error": child["error"], "attempts": child["attempts"],
        }
