from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import tempfile
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Sequence

import yaml


ROOT = Path(__file__).resolve().parents[4]
DEPLOY_DIR = Path(__file__).resolve().parents[1]
REGISTRY_PATH = DEPLOY_DIR / "quality-checks.yaml"
NAIVE_JUDGE_SCHEMA_PATH = Path(__file__).with_name("naive-judge-schema.json")
REGISTRY_JUDGE_SCHEMA_PATH = Path(__file__).with_name("registry-judge-schema.json")
FIXTURE_DIR = ROOT / "tests/fixtures/clients/tgg/output-quality"
DEFAULT_STATE_DIR = Path.home() / ".marshal/state/tgg-output-quality-eval"
PORTAL_BASE = "https://systems.papercut-labs.com/tgg"
SSH_TARGET = "tgg-app-1"
INBOX_DB = "/home/pclaw/.hermes-christopher-tgg/runtime/capture-inbox.db"
TENANT_DB = "/home/pclaw/.systems-pcl/data/tenants/tgg.db"
RESULTS = {"pass", "fail", "unsure"}
MEDIA_ROOTS = (
    "/var/lib/tgg-capture/",
    "/home/pclaw/.hermes-christopher-tgg/",
    "/home/pclaw/.systems-pcl/data/media/tgg/hermes/",
)


class EvalError(RuntimeError):
    pass


class RunAlreadyActive(EvalError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dump(value) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


@dataclass(frozen=True)
class Check:
    id: str
    scope: str
    statement: str


@dataclass(frozen=True)
class Registry:
    version: int
    scope: str
    checks: tuple[Check, ...]
    digest: str

    @property
    def ids(self) -> tuple[str, ...]:
        return tuple(item.id for item in self.checks)


def load_registry(path: Path = REGISTRY_PATH) -> Registry:
    raw_bytes = path.read_bytes()
    raw = yaml.safe_load(raw_bytes)
    if not isinstance(raw, dict) or raw.get("version") != 1:
        raise EvalError("quality check registry must be a version-1 mapping")
    if raw.get("scope") != "tgg/christopher":
        raise EvalError("quality check registry has the wrong scope")
    entries = raw.get("checks")
    if not isinstance(entries, list) or not entries:
        raise EvalError("quality check registry has no checks")
    checks: list[Check] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise EvalError("quality check entries must be mappings")
        check_id = entry.get("id")
        scope = entry.get("scope")
        statement = entry.get("statement")
        if not isinstance(check_id, str) or not check_id or check_id in seen:
            raise EvalError(f"invalid or duplicate quality check id: {check_id!r}")
        if scope not in {"case", "portal"}:
            raise EvalError(f"invalid scope for {check_id}: {scope!r}")
        if not isinstance(statement, str) or not statement.strip():
            raise EvalError(f"empty statement for {check_id}")
        seen.add(check_id)
        checks.append(Check(check_id, scope, statement.strip()))
    return Registry(1, "tgg/christopher", tuple(checks), hashlib.sha256(raw_bytes).hexdigest())


class StateStore:
    def __init__(self, root: Path):
        self.root = root
        self.state_path = root / "state.json"
        self.runs_path = root / "runs.jsonl"
        self.defects_path = root / "defects.json"
        self.human_path = root / "human-catches.jsonl"

    def load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "cursor": 0,
                "last_success_at": None,
                "registry_hash": None,
                "batches_evaluated": 0,
                "batches_occurred": 0,
                "defect_trend": [],
                "loop_caught": 0,
                "human_caught": 0,
                "occurred_batch_keys": [],
                "last_completed_rows": 0,
                "last_unmapped_count": 0,
            }
        return json.loads(self.state_path.read_text())

    def save(self, state: dict[str, Any]) -> None:
        atomic_json(self.state_path, state)

    def defects(self) -> dict[str, str]:
        return json.loads(self.defects_path.read_text()) if self.defects_path.exists() else {}

    def save_defects(self, value: dict[str, str]) -> None:
        atomic_json(self.defects_path, value)

    @contextmanager
    def exclusive_run(self) -> Iterable[None]:
        """Hold one kernel-released writer lock for the full evaluator run."""
        self.root.mkdir(parents=True, exist_ok=True)
        lock_path = self.root / "run.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RunAlreadyActive("another evaluator run holds the state lock") from exc
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()} {iso(utc_now())}\n".encode())
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


REMOTE_SNAPSHOT_SCRIPT = r"""
import json, sqlite3, sys
cursor, inbox_path, tenant_path = int(sys.argv[1]), sys.argv[2], sys.argv[3]
def ro(path):
    c = sqlite3.connect("file:" + path + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA query_only=ON")
    return c
with ro(inbox_path) as db:
    rows = [dict(r) for r in db.execute(
        "SELECT * FROM ingress_events WHERE status='completed' AND seq>? ORDER BY seq", (cursor,)
    )]
message_ids = {str(r["message_id"]) for r in rows}
with ro(tenant_path) as db:
    observations = []
    case_ids = set()
    for row in db.execute("SELECT * FROM case_observations ORDER BY id"):
        item = dict(row)
        refs = []
        if item.get("source_ref"): refs.append(str(item["source_ref"]))
        try:
            fields = json.loads(item.get("fields") or "{}")
        except Exception:
            fields = {}
        stack = [fields]
        while stack:
            value = stack.pop()
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in ("source_ref","sourceRef") and child is not None: refs.append(str(child))
                    elif key in ("source_refs","sourceRefs") and isinstance(child, list):
                        refs.extend(str(x) for x in child)
                    else: stack.append(child)
            elif isinstance(value, list): stack.extend(value)
        if message_ids.intersection(refs):
            item["_matched_source_refs"] = sorted(message_ids.intersection(refs))
            observations.append(item)
            case_ids.add(int(item["case_id"]))
    if case_ids:
        marks = ",".join("?" for _ in case_ids)
        cases = [dict(r) for r in db.execute("SELECT * FROM cases WHERE id IN (" + marks + ")", tuple(sorted(case_ids)))]
    else:
        cases = []
print(json.dumps({"events": rows, "observations": observations, "cases": cases}, ensure_ascii=False))
"""


Command = Callable[..., subprocess.CompletedProcess[str]]


def run_command(argv: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    kwargs.setdefault("timeout", 900)
    return subprocess.run(list(argv), text=True, check=False, **kwargs)


def collect_snapshot(
    cursor: int,
    *,
    command: Command = run_command,
    target: str = SSH_TARGET,
    inbox_db: str = INBOX_DB,
    tenant_db: str = TENANT_DB,
) -> dict[str, Any]:
    argv = [
        "ssh",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        target,
        "python3",
        "-",
        str(cursor),
        inbox_db,
        tenant_db,
    ]
    result = command(argv, input=REMOTE_SNAPSHOT_SCRIPT, capture_output=True)
    if result.returncode:
        raise EvalError(f"read-only SSH snapshot failed: {result.stderr.strip()}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise EvalError("read-only SSH snapshot returned invalid JSON") from exc
    if not isinstance(value, dict) or not all(key in value for key in ("events", "observations", "cases")):
        raise EvalError("read-only SSH snapshot has an invalid envelope")
    return value


def _walk_media(value: Any) -> Iterable[str]:
    def candidate(path: str) -> bool:
        return path.startswith(MEDIA_ROOTS) or path.startswith("/media/tgg/hermes/")

    if isinstance(value, dict):
        for key, child in value.items():
            if key in {
                "media_path",
                "mediaPath",
                "retained_path",
                "retainedPath",
                "path",
                "filePath",
                "localPath",
            } and isinstance(child, str) and candidate(child):
                yield child
            elif key in {"media_paths", "mediaPaths", "media_urls", "mediaUrls"} and isinstance(child, list):
                for item in child:
                    if isinstance(item, str) and candidate(item):
                        yield item
                    elif isinstance(item, (dict, list)):
                        yield from _walk_media(item)
            elif key == "url" and isinstance(child, str) and candidate(child):
                yield child
            else:
                yield from _walk_media(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_media(child)


def event_raw(event: dict[str, Any]) -> dict[str, Any]:
    raw = event.get("raw_json")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"unparsed": raw}
    return raw if isinstance(raw, dict) else {}


def make_bundles(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    events = snapshot["events"]
    event_by_id = {str(item["message_id"]): item for item in events}
    event_ids = set(event_by_id)
    case_ids = {int(item["id"]) for item in snapshot["cases"]}
    observations_by_case: dict[int, list[dict[str, Any]]] = {}
    mapped: set[str] = set()
    for observation in snapshot["observations"]:
        if "_matched_source_refs" not in observation:
            fields = observation.get("fields")
            if isinstance(fields, str):
                try:
                    fields = json.loads(fields)
                except json.JSONDecodeError:
                    fields = {}
            refs: set[str] = set()
            stack = [fields]
            if observation.get("source_ref"):
                refs.add(str(observation["source_ref"]))
            while stack:
                value = stack.pop()
                if isinstance(value, dict):
                    for key, child in value.items():
                        if key in {"source_ref", "sourceRef"} and child is not None:
                            refs.add(str(child))
                        elif key in {"source_refs", "sourceRefs"} and isinstance(child, list):
                            refs.update(str(item) for item in child)
                        else:
                            stack.append(child)
                elif isinstance(value, list):
                    stack.extend(value)
            observation = {**observation, "_matched_source_refs": sorted(refs & event_ids)}
        case_id = int(observation["case_id"])
        if case_id not in case_ids:
            raise EvalError(f"observation references missing case row: {case_id}")
        observations_by_case.setdefault(case_id, []).append(observation)
        mapped.update(str(item) for item in observation.get("_matched_source_refs", []))
    bundles: list[dict[str, Any]] = []
    for case in snapshot["cases"]:
        observations = observations_by_case.get(int(case["id"]), [])
        refs = {
            str(ref)
            for observation in observations
            for ref in observation.get("_matched_source_refs", [])
        }
        messages = []
        for ref in sorted(refs, key=lambda item: int(event_by_id[item]["seq"])):
            event = event_by_id[ref]
            raw = event_raw(event)
            messages.append(
                {
                    "seq": event["seq"],
                    "message_id": ref,
                    "chat_id": event["chat_id"],
                    "raw_json": raw,
                    "retained_media_paths": sorted(set(_walk_media(raw))),
                }
            )
        bundles.append({"case": case, "observations": observations, "messages": messages})
    unmapped = sorted(set(event_by_id) - mapped, key=lambda item: int(event_by_id[item]["seq"]))
    return bundles, unmapped


def pull_retained_media(
    bundle: dict[str, Any],
    destination: Path,
    *,
    command: Command = run_command,
    target: str = SSH_TARGET,
) -> list[dict[str, str]]:
    """Copy only allow-listed retained source media from the VPS.

    ``scp`` is a source-only operation here. The remote paths come from the
    captured source record, never from portal output or shell interpolation.
    """
    destination.mkdir(parents=True, exist_ok=True)
    pulled: list[dict[str, str]] = []
    paths = {
        path
        for message in bundle["messages"]
        for path in message.get("retained_media_paths", [])
    }
    for remote_path in sorted(paths):
        if "\0" in remote_path or not remote_path.startswith("/"):
            raise EvalError(f"retained media path is not absolute: {remote_path!r}")
        if ".." in PurePosixPath(remote_path).parts:
            raise EvalError(f"retained media path contains traversal: {remote_path}")
        if remote_path.startswith("/media/tgg/hermes/"):
            remote_path = f"/home/pclaw/.systems-pcl/data{remote_path}"
        normalized = str(PurePosixPath(remote_path))
        if not any(normalized.startswith(root) for root in MEDIA_ROOTS):
            raise EvalError(f"retained media path is outside read-only roots: {remote_path}")
        remote_path = normalized
        suffix = Path(remote_path).suffix[:16]
        local = destination / f"{hashlib.sha256(remote_path.encode()).hexdigest()[:20]}{suffix}"
        result = command(
            [
                "scp",
                "-o",
                "ConnectTimeout=15",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
                "--",
                f"{target}:{remote_path}",
                str(local),
            ],
            capture_output=True,
        )
        if result.returncode:
            raise EvalError(f"read-only retained media pull failed: {result.stderr.strip()}")
        pulled.append({"remote_path": remote_path, "local_path": str(local)})
    bundle["retained_media"] = pulled
    return pulled


def case_url(job_no: str) -> str:
    query = urllib.parse.urlencode({"view": "cases", "mode": "maintenance", "case": job_no})
    return f"{PORTAL_BASE}?{query}"


class Browser:
    def __init__(self, command: Command = run_command):
        self.command = command

    def _run(self, session: str, args: Sequence[str]) -> str:
        result = self.command(
            ["agent-browser", "--session", session, "--json", *args],
            capture_output=True,
        )
        if result.returncode:
            raise EvalError(f"agent-browser {' '.join(args[:2])} failed: {result.stderr.strip()}")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise EvalError(f"agent-browser {' '.join(args[:2])} returned invalid JSON") from exc
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise EvalError(
                f"agent-browser {' '.join(args[:2])} did not succeed: "
                f"{json_dump(payload) if isinstance(payload, dict) else payload!r}"
            )
        data = payload.get("data")
        if args and args[0] == "snapshot":
            if not isinstance(data, dict) or not isinstance(data.get("snapshot"), str):
                raise EvalError("agent-browser snapshot response has no data.snapshot")
            return data["snapshot"]
        if len(args) >= 2 and args[:2] == ["get", "url"]:
            if not isinstance(data, dict) or not isinstance(data.get("url"), str):
                raise EvalError("agent-browser get url response has no data.url")
            return data["url"]
        return json_dump(data)

    def _wait_for_content(self, session: str, *, required: str, attempts: int = 30) -> str:
        last = ""
        for _ in range(attempts):
            last = self._run(session, ["snapshot"])
            lowered = last.lower()
            if required.lower() in lowered and not any(
                marker in lowered for marker in ("loading…", "loading...", "skeleton")
            ):
                return last
            self._run(session, ["wait", "1000"])
        raise EvalError(f"portal content did not settle for {required}; last snapshot={last[-500:]}")

    def capture(self, run_id: str, bundles: list[dict[str, Any]], run_dir: Path) -> dict[str, dict[str, str]]:
        session = f"tgg-output-eval-{run_id}"
        self._run(session, ["auth", "login", "tgg-pa-admin"])
        captures: dict[str, dict[str, str]] = {}
        portal_image = run_dir / "portal-cases.png"
        portal_snapshot = run_dir / "portal-cases.snapshot.txt"
        self._run(session, ["open", f"{PORTAL_BASE}?view=cases&mode=maintenance"])
        portal_content = self._wait_for_content(session, required="Cases")
        self._run(session, ["screenshot", "--full", str(portal_image)])
        portal_snapshot.write_text(portal_content, encoding="utf-8")
        for bundle in bundles:
            job_no = str(bundle["case"]["job_no"])
            token = hashlib.sha256(job_no.encode()).hexdigest()[:12]
            screenshot = run_dir / f"case-{token}.png"
            snapshot = run_dir / f"case-{token}.snapshot.txt"
            url = case_url(job_no)
            self._run(session, ["open", url])
            returned_url = self._run(session, ["get", "url"])
            if job_no not in urllib.parse.unquote(returned_url):
                raise EvalError(f"portal did not retain exact case URL for {job_no}")
            case_content = self._wait_for_content(session, required=job_no)
            self._run(session, ["screenshot", "--full", str(screenshot)])
            snapshot.write_text(case_content, encoding="utf-8")
            captures[job_no] = {
                "url": url,
                "screenshot": str(screenshot),
                "snapshot": str(snapshot),
                "portal_screenshot": str(portal_image),
                "portal_snapshot": str(portal_snapshot),
            }
        self._run(session, ["close"])
        return captures


def validate_judgment(value: Any, registry: Registry, maker_session_id: str) -> dict[str, Any]:
    if not isinstance(maker_session_id, str) or not maker_session_id.strip():
        raise EvalError("maker_session_id is required")
    if not isinstance(value, dict):
        raise EvalError("judge response must be a JSON object")
    required = {
        "checker_session_id",
        "naive_checker_session_id",
        "registry_checker_session_id",
        "naive_pass",
        "registry_pass",
        "source_to_page",
        "page_to_source",
        "manager_readability",
        "checks",
        "summary",
    }
    if set(value) != required:
        raise EvalError(f"judge response keys must be exactly {sorted(required)}")
    checker = value["checker_session_id"]
    if not isinstance(checker, str) or not checker:
        raise EvalError("judge checker_session_id is missing")
    if checker == maker_session_id:
        raise EvalError("checker session equals maker session")
    naive_checker = value["naive_checker_session_id"]
    registry_checker = value["registry_checker_session_id"]
    if not all(isinstance(item, str) and item for item in (naive_checker, registry_checker)):
        raise EvalError("two-pass checker session ids are missing")
    if maker_session_id in {naive_checker, registry_checker}:
        raise EvalError("checker session equals maker session")
    if naive_checker == registry_checker:
        raise EvalError("naive and registry passes reused one checker session")
    naive_pass = value["naive_pass"]
    registry_pass = value["registry_pass"]
    if not isinstance(naive_pass, dict) or set(naive_pass) != {
        "source_to_page", "page_to_source", "manager_readability", "summary"
    }:
        raise EvalError("naive pass has an invalid shape")
    if not isinstance(registry_pass, dict) or set(registry_pass) != {"checks"}:
        raise EvalError("registry pass has an invalid shape")
    for axis in ("source_to_page", "page_to_source", "manager_readability"):
        if value[axis] not in RESULTS:
            raise EvalError(f"invalid {axis} result")
    if not isinstance(value["summary"], str) or not value["summary"].strip():
        raise EvalError("judge summary is empty")
    checks = value["checks"]
    if any(value[axis] != naive_pass[axis] for axis in ("source_to_page", "page_to_source", "manager_readability")):
        raise EvalError("combined axes diverge from naive pass")
    if value["summary"] != naive_pass["summary"]:
        raise EvalError("combined summary diverges from naive pass")
    if checks != registry_pass["checks"]:
        raise EvalError("combined checks diverge from registry pass")
    if not isinstance(checks, list):
        raise EvalError("judge checks must be an array")
    found: dict[str, dict[str, Any]] = {}
    for result in checks:
        if not isinstance(result, dict) or set(result) != {"id", "result", "evidence"}:
            raise EvalError("judge check entries have an invalid shape")
        if result["id"] in found:
            raise EvalError(f"duplicate judge result: {result['id']}")
        if result["result"] not in RESULTS:
            raise EvalError(f"invalid judge result for {result['id']}")
        if not isinstance(result["evidence"], str) or not result["evidence"].strip():
            raise EvalError(f"empty evidence for {result['id']}")
        found[result["id"]] = result
    if set(found) != set(registry.ids):
        raise EvalError("judge result ids do not exactly match the registry")
    return value


class Judge:
    def __init__(self, command: Command = run_command):
        self.command = command

    def judge(
        self,
        bundle_path: Path,
        captures: dict[str, str],
        registry: Registry,
        maker_session_id: str,
    ) -> dict[str, Any]:
        naive_prompt = (
            "This is a bounded evaluation task, not repository development or durable-memory work. "
            "Return only the requested judgment JSON. You are an independent vision quality checker. "
            "Take the cold stance of a TGG manager seeing this page for the first time. Do not use, "
            "infer, or search for a checklist. Read the source bundle and accessibility snapshots "
            "named below and inspect both attached screenshots. Judge the general quality bar in both directions: every "
            "page claim must trace to source, every source fact must be represented, and the page must "
            "read sensibly to a TGG manager. Use unsure whenever evidence is insufficient; never "
            "silently pass uncertainty.\n"
            f"source_bundle={bundle_path}\ncase_snapshot={captures['snapshot']}\n"
            f"portal_snapshot={captures['portal_snapshot']}\n"
        )
        registry_prompt = (
            "This is the second, regression-floor pass. Return only the requested judgment JSON. "
            "Read the source bundle, accessibility snapshots, screenshots, and named registry. Run "
            "every registry check exactly once using its id verbatim. A check whose triggering source "
            "fact is absent is unsure, not pass. Never silently pass uncertainty.\n"
            f"source_bundle={bundle_path}\ncase_snapshot={captures['snapshot']}\n"
            f"portal_snapshot={captures['portal_snapshot']}\nregistry={REGISTRY_PATH}\n"
        )
        with tempfile.TemporaryDirectory(prefix="tgg-output-judge-") as tmp:
            try:
                source_bundle = json.loads(bundle_path.read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise EvalError("judge source bundle is unreadable JSON") from exc
            source_images = [
                str(item["local_path"])
                for item in source_bundle.get("retained_media", [])
                if isinstance(item, dict)
                and isinstance(item.get("local_path"), str)
                and Path(item["local_path"]).is_file()
            ][:20]
            if source_images:
                attachment_note = (
                    "\nAttachment order: image 1 is the rendered case page; image 2 is the portal "
                    "case list; subsequent images are retained source media in this exact order:\n"
                    + "\n".join(f"- {path}" for path in source_images)
                    + "\nUse the bundle's retained_media mapping to correlate each source image. "
                    "If more source media is referenced than attached, mark affected checks unsure.\n"
                )
                naive_prompt += attachment_note
                registry_prompt += attachment_note
            # `codex exec` inherits CODEX_THREAD_ID in managed sessions and
            # otherwise resumes the maker's thread. Remove only session
            # bindings so the checker is a genuinely fresh vision seat while
            # retaining the configured Codex home and credentials.
            checker_env = os.environ.copy()
            for key in (
                "CODEX_THREAD_ID",
                "MARSHAL_SESSION_ID",
                "MARSHAL_SPAWNED_BY",
                "CLAUDE_SESSION_ID",
                "CLAUDE_CODE_SESSION_ID",
                "CLAUDE_CODE_BRIDGE_SESSION_ID",
                "CLAUDE_CODE_CHILD_SESSION",
            ):
                checker_env.pop(key, None)

            def run_pass(
                prompt: str, schema: Path, output_name: str, *, isolated: bool = False
            ) -> tuple[dict[str, Any], str]:
                output = Path(tmp) / output_name
                argv = [
                    "codex", "exec", "--json", "--sandbox", "read-only",
                    "--output-schema", str(schema),
                    "--output-last-message", str(output),
                    "-i", captures["screenshot"],
                    "-i", captures["portal_screenshot"],
                ]
                if isolated:
                    naive_root = Path(tmp) / "naive-root"
                    naive_root.mkdir(exist_ok=True)
                    argv.extend(["-C", str(naive_root), "--skip-git-repo-check", "--ignore-rules"])
                for source_image in source_images:
                    argv.extend(["-i", source_image])
                argv.append("-")
                try:
                    result = self.command(
                        argv, input=prompt, capture_output=True, env=checker_env, timeout=120
                    )
                except subprocess.TimeoutExpired as exc:
                    raise EvalError(f"vision judge timed out: {output_name}") from exc
                if result.returncode:
                    raise EvalError(f"vision judge failed: {result.stderr.strip()}")
                checker_id = None
                for line in result.stdout.splitlines():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if event.get("type") == "thread.started":
                        checker_id = event.get("thread_id")
                if not checker_id:
                    raise EvalError("vision judge did not expose its checker session id")
                try:
                    value = json.loads(output.read_text())
                except (OSError, json.JSONDecodeError) as exc:
                    raise EvalError("vision judge did not write strict JSON") from exc
                if schema == NAIVE_JUDGE_SCHEMA_PATH:
                    required = {
                        "source_to_page", "page_to_source", "manager_readability", "summary"
                    }
                    if not isinstance(value, dict) or set(value) != required:
                        raise EvalError("naive judge pass has an invalid shape")
                    if any(value[axis] not in RESULTS for axis in required - {"summary"}):
                        raise EvalError("naive judge pass has an invalid result")
                    if not isinstance(value["summary"], str) or not value["summary"].strip():
                        raise EvalError("naive judge pass has an empty summary")
                elif (
                    not isinstance(value, dict)
                    or set(value) != {"checks"}
                    or not isinstance(value["checks"], list)
                ):
                    raise EvalError("registry judge pass has an invalid shape")
                return value, str(checker_id)

            naive, naive_checker = run_pass(
                naive_prompt, NAIVE_JUDGE_SCHEMA_PATH, "naive.json", isolated=True
            )
            registry_result, registry_checker = run_pass(
                registry_prompt, REGISTRY_JUDGE_SCHEMA_PATH, "registry.json"
            )
            if maker_session_id in {naive_checker, registry_checker}:
                raise EvalError("checker session equals maker session")
            value = {
                "checker_session_id": registry_checker,
                "naive_checker_session_id": naive_checker,
                "registry_checker_session_id": registry_checker,
                "naive_pass": naive,
                "registry_pass": registry_result,
                "source_to_page": naive["source_to_page"],
                "page_to_source": naive["page_to_source"],
                "manager_readability": naive["manager_readability"],
                "summary": naive["summary"],
                "checks": registry_result["checks"],
            }
            return validate_judgment(value, registry, maker_session_id)


def defect_key(job_no: str, check_id: str, message_ids: Sequence[str] = ()) -> str:
    # Identity is the case/check defect class, not the individual batch. An
    # open repair row absorbs later evidence; after it terminals, the same key
    # can create a new repair generation.
    raw = "\0".join([job_no, check_id])
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


class DefectFiler:
    def __init__(self, store: StateStore, command: Command = run_command):
        self.store = store
        self.command = command

    def file(
        self,
        *,
        job_no: str,
        check: dict[str, Any],
        screenshot: str,
        message_ids: list[str],
        judgment_path: str,
    ) -> tuple[str, bool]:
        key = defect_key(job_no, check["id"], message_ids)
        index = self.store.defects()
        if key in index:
            indexed_id = index[key]
            existing = self.command(
                ["pcl", "whiteboard", "get", "--id", indexed_id],
                capture_output=True,
            )
            if existing.returncode != 0:
                # Dedupe is safety-bearing. A broken read never licenses a
                # duplicate repair row every interval.
                return indexed_id, False
            try:
                row = json.loads(existing.stdout).get("data", {})
            except (json.JSONDecodeError, AttributeError):
                return indexed_id, False
            lane = row.get("lane")
            if not isinstance(lane, str):
                return indexed_id, False
            if lane.lower() not in {"done", "cancelled", "archived"}:
                return indexed_id, False
            index.pop(key, None)
            self.store.save_defects(index)
        search = self.command(
            ["pcl", "whiteboard", "search", "--query", f"tgg-output-defect:{key}", "--owner", "edna"],
            capture_output=True,
        )
        if search.returncode != 0:
            raise EvalError(f"defect dedupe search failed: {search.stderr.strip()}")
        try:
            rows = json.loads(search.stdout).get("data", [])
        except (json.JSONDecodeError, AttributeError) as exc:
            raise EvalError("defect dedupe search returned invalid JSON") from exc
        if not isinstance(rows, list):
            raise EvalError("defect dedupe search returned non-list data")
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("lane"), str):
                raise EvalError("defect dedupe search row has no lane")
        identity = f"tgg-output-defect:{key}"
        exact_rows = [
            row
            for row in rows
            if identity in str(row.get("title", ""))
            or identity in str(row.get("description", ""))
        ]
        if exact_rows:
            open_rows = [
                row
                for row in exact_rows
                if str(row.get("lane", "")).lower() not in {"done", "cancelled", "archived"}
            ]
        else:
            open_rows = []
        if open_rows:
            wb_id = str(open_rows[0].get("id") or open_rows[0].get("task_id"))
            index[key] = wb_id
            self.store.save_defects(index)
            return wb_id, False
        description = {
            "defect_key": f"tgg-output-defect:{key}",
            "job_no": job_no,
            "check_class": check["id"],
            "result": check["result"],
            "evidence": check["evidence"],
            "screenshot": screenshot,
            "message_ids": message_ids,
            "judgment": judgment_path,
        }
        with tempfile.TemporaryDirectory(prefix="tgg-output-defect-") as tmp:
            desc = Path(tmp) / "description.json"
            dod = Path(tmp) / "dod.json"
            desc.write_text(json.dumps(description, indent=2), encoding="utf-8")
            dod.write_text(
                json.dumps(
                    [
                        f"Fix and verify {check['id']} on TGG case {job_no}",
                        "Consumer-layer recheck passes against the cited source messages and screenshot",
                    ]
                ),
                encoding="utf-8",
            )
            result = self.command(
                [
                    "pcl",
                    "whiteboard",
                    "create",
                    "--title",
                    f"TGG output defect [{check['id']}] {job_no} tgg-output-defect:{key}",
                    "--owner",
                    "edna",
                    "--vertical",
                    "pa",
                    "--description-file",
                    str(desc),
                    "--dod-file",
                    str(dod),
                    "--dispatch-now",
                ],
                capture_output=True,
            )
        if result.returncode:
            raise EvalError(f"defect filing failed: {result.stderr.strip()}")
        wb_id = result.stdout.strip()
        if not wb_id:
            raise EvalError("defect filing returned no WB id")
        index[key] = wb_id
        self.store.save_defects(index)
        return wb_id, True


def trigger_due(
    state: dict[str, Any],
    completed_count: int,
    *,
    trigger: str,
    now: datetime,
) -> tuple[bool, str]:
    if completed_count <= 0:
        return False, "no-new-completions"
    if trigger in {"deploy", "daily", "force"}:
        return True, trigger
    if completed_count >= 25:
        return True, "completion-threshold"
    last = parse_time(state.get("last_success_at"))
    if last is None or now - last >= timedelta(hours=4):
        return True, "four-hour-nonzero"
    return False, "below-threshold-and-window"


class Evaluator:
    def __init__(
        self,
        store: StateStore,
        *,
        collect: Callable[[int], dict[str, Any]] = collect_snapshot,
        browser: Browser | None = None,
        judge: Judge | None = None,
        filer: DefectFiler | None = None,
        clock: Callable[[], datetime] = utc_now,
    ):
        self.store = store
        self.collect = collect
        self.browser = browser or Browser()
        self.judge = judge or Judge()
        self.filer = filer or DefectFiler(store)
        self.clock = clock

    def _golden(self, registry: Registry, maker_session_id: str) -> None:
        source = FIXTURE_DIR / "golden-source.json"
        screenshot = FIXTURE_DIR / "golden-page.png"
        snapshot = FIXTURE_DIR / "golden-page.snapshot.txt"
        actual = self.judge.judge(
            source,
            {
                "url": "fixture://tgg-output-quality/golden",
                "screenshot": str(screenshot),
                "snapshot": str(snapshot),
                "portal_screenshot": str(screenshot),
                "portal_snapshot": str(snapshot),
            },
            registry,
            maker_session_id,
        )
        validate_judgment(actual, registry, maker_session_id)
        expected = json.loads((FIXTURE_DIR / "golden-judge.json").read_text())
        validate_judgment(expected, registry, maker_session_id)
        expected_by_id = {item["id"]: item["result"] for item in expected["checks"]}
        actual_by_id = {item["id"]: item["result"] for item in actual["checks"]}
        if set(expected_by_id.values()) != RESULTS:
            raise EvalError("golden regression does not exercise pass, fail, and unsure")
        if actual_by_id != expected_by_id:
            mismatches = {
                check_id: {"expected": expected_by_id[check_id], "actual": actual_by_id.get(check_id)}
                for check_id in expected_by_id
                if actual_by_id.get(check_id) != expected_by_id[check_id]
            }
            raise EvalError(f"golden registry regression mismatch: {json_dump(mismatches)}")

    def run(self, *, trigger: str, maker_session_id: str, force: bool = False) -> dict[str, Any]:
        with self.store.exclusive_run():
            return self._run_locked(trigger=trigger, maker_session_id=maker_session_id, force=force)

    def _run_locked(self, *, trigger: str, maker_session_id: str, force: bool = False) -> dict[str, Any]:
        if not isinstance(maker_session_id, str) or not maker_session_id.strip():
            raise EvalError("maker_session_id is required")
        registry = load_registry()
        state = self.store.load()
        snapshot = self.collect(int(state["cursor"]))
        events = snapshot["events"]
        due, reason = trigger_due(
            state,
            len(events),
            trigger="force" if force else trigger,
            now=self.clock(),
        )
        if not due:
            return {"ok": True, "ran": False, "reason": reason, "new_completions": len(events)}
        occurrence_key = f"{events[0]['seq']}:{events[-1]['seq']}"
        occurrence_keys = list(state.get("occurred_batch_keys", []))
        if occurrence_key not in occurrence_keys:
            state["batches_occurred"] = int(state.get("batches_occurred", 0)) + 1
            occurrence_keys.append(occurrence_key)
            state["occurred_batch_keys"] = occurrence_keys[-100:]
        self.store.save(state)
        if state.get("registry_hash") != registry.digest:
            self._golden(registry, maker_session_id)
        bundles, unmapped = make_bundles(snapshot)
        start_cursor = int(state["cursor"])
        run_id = f"cursor-{start_cursor + 1}-{events[-1]['seq']}"
        run_dir = self.store.root / "runs" / run_id
        # A failed attempt deliberately keeps its evidence. Retrying the same
        # cursor window resumes that stable run directory rather than inventing
        # a second batch identity.
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "source-snapshot.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        captures = self.browser.capture(run_id, bundles, run_dir)
        new_defects = 0
        defects: list[dict[str, Any]] = []
        prepared: list[tuple[dict[str, Any], str, Path]] = []
        for bundle in bundles:
            job_no = str(bundle["case"]["job_no"])
            bundle_path = run_dir / f"bundle-{hashlib.sha256(job_no.encode()).hexdigest()[:12]}.json"
            pull_retained_media(
                bundle,
                run_dir / "media" / hashlib.sha256(job_no.encode()).hexdigest()[:12],
            )
            bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
            prepared.append((bundle, job_no, bundle_path))
        judgments: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    self.judge.judge,
                    bundle_path,
                    captures[job_no],
                    registry,
                    maker_session_id,
                ): job_no
                for _bundle, job_no, bundle_path in prepared
            }
            for future in as_completed(futures):
                judgments[futures[future]] = future.result()
        for bundle, job_no, bundle_path in prepared:
            judgment = judgments[job_no]
            judgment_path = bundle_path.with_name(bundle_path.stem + ".judgment.json")
            judgment_path.write_text(json.dumps(judgment, ensure_ascii=False, indent=2), encoding="utf-8")
            message_ids = [str(item["message_id"]) for item in bundle["messages"]]
            axis_checks = [
                {"id": axis, "result": judgment[axis], "evidence": judgment["summary"]}
                for axis in ("source_to_page", "page_to_source", "manager_readability")
            ]
            for check in [*judgment["checks"], *axis_checks]:
                if check["result"] == "pass":
                    continue
                wb_id, created = self.filer.file(
                    job_no=job_no,
                    check=check,
                    screenshot=captures[job_no]["screenshot"],
                    message_ids=message_ids,
                    judgment_path=str(judgment_path),
                )
                new_defects += int(created)
                defects.append({"wb_id": wb_id, "created": created, **check})
        completed_at = self.clock()
        state["cursor"] = max(int(item["seq"]) for item in events)
        state["last_success_at"] = iso(completed_at)
        state["registry_hash"] = registry.digest
        # The committed cursor covers every overlapping occurrence window
        # recorded since the previous cursor, including failed attempts whose
        # end sequence is now included.
        state["batches_evaluated"] = int(state.get("batches_occurred", 0))
        state["loop_caught"] = int(state.get("loop_caught", 0)) + new_defects
        state["last_completed_rows"] = len(events)
        state["last_unmapped_count"] = len(unmapped)
        trend = list(state.get("defect_trend", []))
        trend.append({"run_id": run_id, "at": iso(completed_at), "defects": len(defects), "new_defects": new_defects})
        state["defect_trend"] = trend[-30:]
        self.store.save(state)
        receipt = {
            "run_id": run_id,
            "trigger": reason,
            "started_cursor": start_cursor,
            "committed_cursor": state["cursor"],
            "completed_rows": len(events),
            "cases_evaluated": len(bundles),
            "unmapped_message_ids": unmapped,
            "defects": defects,
            "registry_hash": registry.digest,
            "completed_at": iso(completed_at),
        }
        atomic_json(run_dir / "receipt.json", receipt)
        append_jsonl(self.store.runs_path, receipt)
        return {"ok": True, "ran": True, **receipt}


def health(store: StateStore, now: datetime | None = None) -> dict[str, Any]:
    now = now or utc_now()
    state = store.load()
    last = parse_time(state.get("last_success_at"))
    silence_seconds = None if last is None else max(0, int((now - last).total_seconds()))
    occurred = int(state.get("batches_occurred", 0))
    evaluated = int(state.get("batches_evaluated", 0))
    coverage = 1.0 if occurred == 0 else evaluated / occurred
    ok = last is not None and silence_seconds is not None and silence_seconds <= 93600 and coverage >= 1.0
    return {
        "ok": ok,
        "logical_key": "loop.pa.tgg.output_eval",
        "red_owner": "edna",
        "max_silence": "PT26H",
        "cursor": int(state.get("cursor", 0)),
        "last_success_at": state.get("last_success_at"),
        "silence_seconds": silence_seconds,
        "batches_evaluated": evaluated,
        "batches_occurred": occurred,
        "coverage_ratio": coverage,
        "defect_trend": state.get("defect_trend", []),
        "human_caught": int(state.get("human_caught", 0)),
        "loop_caught": int(state.get("loop_caught", 0)),
        "last_completed_rows": int(state.get("last_completed_rows", 0)),
        "last_unmapped_count": int(state.get("last_unmapped_count", 0)),
    }


def initialize_cursor(store: StateStore, cursor: int, now: datetime | None = None) -> dict[str, Any]:
    if cursor < 0:
        raise EvalError("initial cursor must be non-negative")
    if store.root.exists() and any(store.root.iterdir()):
        raise EvalError("cursor initialization is allowed only on pristine evaluator state")
    state = store.load()
    state["cursor"] = cursor
    state["initialized_at"] = iso(now or utc_now())
    store.save(state)
    return {"ok": True, "initialized": True, "cursor": cursor}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate rendered TGG output against newly completed source messages.")
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    sub = parser.add_subparsers(dest="command_name", required=True)
    run = sub.add_parser("run", help="Collect and, when due, evaluate the next completed batch.")
    run.add_argument("--trigger", choices=("interval", "deploy", "daily"), default="interval")
    run.add_argument("--maker-session-id", default=os.environ.get("MARSHAL_SESSION_ID", ""))
    run.add_argument("--force", action="store_true")
    sub.add_parser("health", help="Emit fail-closed check JSON with coverage and defect metrics.")
    validate = sub.add_parser("validate", help="Validate the registry and golden result.")
    validate.add_argument("--maker-session-id", default=os.environ.get("MARSHAL_SESSION_ID", ""))
    human = sub.add_parser("record-human-catch", help="Record a human catch only after its named check exists.")
    human.add_argument("--check-id", required=True)
    human.add_argument("--evidence", required=True)
    initialize = sub.add_parser("initialize-cursor", help="Set the first cursor on pristine local evaluator state.")
    initialize.add_argument("--cursor", type=int, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = StateStore(args.state_dir)
    try:
        if args.command_name == "health":
            result = health(store)
        elif args.command_name == "validate":
            registry = load_registry()
            Evaluator(store)._golden(registry, args.maker_session_id)
            result = {"ok": True, "registry_hash": registry.digest, "checks": list(registry.ids)}
        elif args.command_name == "record-human-catch":
            registry = load_registry()
            if args.check_id not in registry.ids:
                raise EvalError("human catch must first be added as a named registry check")
            state = store.load()
            state["human_caught"] = int(state.get("human_caught", 0)) + 1
            store.save(state)
            append_jsonl(store.human_path, {"at": iso(utc_now()), "check_id": args.check_id, "evidence": args.evidence})
            result = {"ok": True, "check_id": args.check_id}
        elif args.command_name == "initialize-cursor":
            result = initialize_cursor(store, args.cursor)
        else:
            result = Evaluator(store).run(
                trigger=args.trigger,
                maker_session_id=args.maker_session_id,
                force=args.force,
            )
            current_health = health(store)
            result.update(
                {
                    key: current_health[key]
                    for key in (
                        "batches_evaluated",
                        "batches_occurred",
                        "coverage_ratio",
                        "defect_trend",
                        "human_caught",
                        "loop_caught",
                        "last_completed_rows",
                        "last_unmapped_count",
                    )
                }
            )
            result["ok"] = current_health["ok"]
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result.get("ok") else 1
    except RunAlreadyActive:
        current_health = health(store)
        result = {
            **current_health,
            "ran": False,
            "reason": "already-running",
        }
        print(json.dumps(result, sort_keys=True))
        return 0 if result["ok"] else 1
    except EvalError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
