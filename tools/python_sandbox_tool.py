"""Offline, kernel-jailed Python over configured read-only datasets."""

from __future__ import annotations

import contextvars
import hashlib
import importlib.util
import json
import logging
import os
import platform
import re
import shlex
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

try:
    import resource
except ImportError:  # pragma: no cover - Windows availability is fail-closed
    resource = None

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows availability is fail-closed
    fcntl = None

from hermes_constants import get_hermes_home
from tools.path_security import validate_within_dir
from tools.python_sandbox_access import (
    SandboxReaderPolicyError,
    configured_reader_uid,
    grant_reader_work_access,
)
from tools.python_sandbox_paths import (
    is_python_sandbox_dataset_name,
    python_sandbox_dataset_path,
)
from tools.registry import registry

logger = logging.getLogger(__name__)

DEFAULTS = {
    "wall_seconds": 120,
    "max_wall_seconds": 300,
    "cpu_seconds": 60,
    "memory_mb": 1024,
    "file_size_mb": 64,
    "scratch_mb": 64,
    "max_processes": 64,
    "max_open_files": 256,
    "max_snapshot_mb": 512,
}
STDOUT_CAP = 16 * 1024
STDERR_CAP = 4 * 1024
RESULT_CAP = 8 * 1024
INPUT_JSON_CAP = 32 * 1024
_PROBE: tuple[float, bool, str] | None = None
_PROBE_TTL = 30.0
_WORKSPACE_LOCKS: dict[str, threading.Lock] = {}
_WORKSPACE_LOCKS_GUARD = threading.Lock()
_WORKSPACE_LOCK_FILE = ".publish.lock"
# Owner of the one agent-wide workspace (``workspace: agent``).  Constant, so
# the folder never depends on a conversation, job, provider or model.
AGENT_WORKSPACE_OWNER = "hermes-agent-workspace"
# Beside ``work/``: which earlier per-conversation folders were merged in.
_MERGE_RECORD_FILE = ".merged-workspaces.json"
# Inside ``work/``: one JSON line per file received, sent or delivered.
FILES_LOG_NAME = "files-log.jsonl"
_LEGACY_WORKSPACE_RE = re.compile(r"^s_[0-9a-f]{64}$")
_EXPORT_COMPLETE = ".hermes-export-complete"
# A runtime-derived workspace owner bound by trusted scheduler code.  It is
# never a tool argument, so the model cannot select another conversation.
_BOUND_PUBLISHED: contextvars.ContextVar[set | None] = contextvars.ContextVar(
    "HERMES_SANDBOX_PUBLISHED_COLLECTOR", default=None
)
_BOUND_WORKSPACE_OWNER: contextvars.ContextVar[str] = contextvars.ContextVar(
    "HERMES_SANDBOX_WORKSPACE_OWNER", default=""
)

_SUPERVISOR_SOURCE = r"""\
import ctypes
import json
import os
import shutil
import stat
import subprocess
import sys

PR_SET_DUMPABLE = 4
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_DUMPABLE) failed")

cpu_seconds = int(sys.argv[1])
max_processes = int(sys.argv[2])
payload = (
    "import os,resource;"
    f"resource.setrlimit(resource.RLIMIT_CPU, ({cpu_seconds},{cpu_seconds + 1}));"
    f"resource.setrlimit(resource.RLIMIT_NPROC, ({max_processes},{max_processes}));"
    "os.execv('/venv/bin/python', ['/venv/bin/python','-I','/script.py'])"
)
completed = subprocess.run(
    [
        "/usr/bin/unshare",
        "--user",
        "--map-user=65534",
        "--map-group=65534",
        "/venv/bin/python",
        "-I",
        "-c",
        payload,
    ],
    check=False,
)

# Writable sqlite datasets are run inputs, not output artifacts. Remove the
# reserved copies after the payload exits so they are not exported or promoted.
for name in json.loads(sys.argv[3]):
    try:
        os.unlink(f"/work/{name}.db")
    except FileNotFoundError:
        pass

# Preserve regular artifacts only. A payload-controlled symlink must never
# become a live pointer into the host when /work is exported.
for root, dirs, files in os.walk("/work", topdown=True, followlinks=False):
    for name in list(dirs):
        path = os.path.join(root, name)
        if os.path.islink(path):
            os.unlink(path)
            dirs.remove(name)
    for name in files:
        path = os.path.join(root, name)
        if not stat.S_ISREG(os.lstat(path).st_mode):
            os.unlink(path)

subprocess.run(["mount", "-o", "remount,bind,rw", "/export"], check=True)
for name in os.listdir("/export"):
    path = os.path.join("/export", name)
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.unlink(path)
subprocess.run(["cp", "-a", "/work/.", "/export/"], check=True)
open("/export/.hermes-export-complete", "wb").close()
if completed.returncode < 0:
    os._exit(128 - completed.returncode)
raise SystemExit(completed.returncode)
"""


def _load_config() -> dict[str, Any]:
    """Read the raw top-level config section defensively."""
    try:
        from hermes_cli.config import read_raw_config

        raw = read_raw_config()
        section = raw.get("python_sandbox", {}) if isinstance(raw, dict) else {}
        return section if isinstance(section, dict) else {}
    except Exception:
        logger.debug("python_sandbox config read failed", exc_info=True)
        return {}


def _limits(config: Mapping[str, Any] | None = None) -> dict[str, int]:
    raw = (config or {}).get("limits", {})
    raw = raw if isinstance(raw, dict) else {}
    result: dict[str, int] = {}
    for key, default in DEFAULTS.items():
        try:
            value = int(raw.get(key, default))
        except (TypeError, ValueError):
            value = default
        result[key] = max(1, value)
    result["max_wall_seconds"] = max(5, result["max_wall_seconds"])
    result["wall_seconds"] = min(
        max(5, result["wall_seconds"]), result["max_wall_seconds"]
    )
    return result


def _wall_seconds(requested: Any, config: Mapping[str, Any] | None = None) -> int:
    limits = _limits(config)
    if requested is None:
        return limits["wall_seconds"]
    try:
        value = int(requested)
    except (TypeError, ValueError):
        value = limits["wall_seconds"]
    return min(max(5, value), limits["max_wall_seconds"])


def _probe(force: bool = False) -> tuple[bool, str]:
    global _PROBE
    config = _load_config()
    if config.get("enabled") is not True:
        return False, "python_sandbox.enabled is false or missing"
    required = ("unshare", "mount", "findmnt", "pivot_root")
    binaries = {name: shutil.which(name) for name in required}
    missing = [name for name, path in binaries.items() if not path]
    if missing:
        return False, f"required sandbox executable(s) missing: {', '.join(missing)}"
    binary = binaries["unshare"]
    now = time.monotonic()
    if not force and _PROBE is not None and now - _PROBE[0] < _PROBE_TTL:
        return _PROBE[1], _PROBE[2]
    if platform.system() != "Linux":
        answer = (False, "kernel namespace jail requires Linux")
    else:
        try:
            completed = subprocess.run(
                [
                    binary,
                    "--user",
                    "--map-root-user",
                    "--net",
                    "--mount",
                    "--pid",
                    "--fork",
                    "/bin/sh",
                    "-c",
                    "unshare --user --map-user=65534 --map-group=65534 true",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env={"PATH": "/usr/bin:/bin"},
            )
            answer = (
                completed.returncode == 0,
                (completed.stderr.strip() or "unshare namespace probe failed")
                if completed.returncode
                else "ok",
            )
        except Exception as exc:
            answer = (False, f"unshare namespace probe failed: {exc}")
    _PROBE = (now, answer[0], answer[1])
    return answer


def check_sandbox_available() -> bool:
    # Tool discovery must fail closed against current kernel state rather than
    # inheriting a prior successful probe from another test or runtime phase.
    return _probe(force=True)[0]


def _dataset_config(config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    datasets = config.get("datasets", {})
    if not isinstance(datasets, dict):
        return {}
    return {str(key): value for key, value in datasets.items() if isinstance(value, dict)}


def _snapshot_sqlite(source: Path, destination: Path, max_mb: int) -> None:
    """Create a WAL-safe point-in-time copy without exposing the live file."""
    if not source.is_file():
        raise ValueError(f"sqlite dataset does not exist: {source}")
    if source.stat().st_size > max_mb * 1024 * 1024:
        raise ValueError(f"sqlite snapshot exceeds max_snapshot_mb ({max_mb}MB)")
    destination.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, timeout=30)
    try:
        dst = sqlite3.connect(destination)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    if destination.stat().st_size > max_mb * 1024 * 1024:
        destination.unlink(missing_ok=True)
        raise ValueError(f"sqlite snapshot exceeds max_snapshot_mb ({max_mb}MB)")


def _resolve_datasets(
    names: list[str], config: Mapping[str, Any], inputs_dir: Path
) -> tuple[dict[str, Path], str | None]:
    available = _dataset_config(config)
    unknown = [name for name in names if name not in available]
    if unknown:
        valid = ", ".join(sorted(available)) or "(none)"
        return {}, f"unknown dataset(s): {', '.join(unknown)}; valid names: {valid}"
    mounts: dict[str, Path] = {}
    limits = _limits(config)
    max_mb = limits["max_snapshot_mb"]
    scratch_bytes = limits["scratch_mb"] * 1024 * 1024
    file_size_bytes = limits["file_size_mb"] * 1024 * 1024
    sqlite_bytes = 0
    for name in names:
        if not is_python_sandbox_dataset_name(name):
            return {}, f"invalid dataset name: {name!r}"
        spec = available[name]
        kind = spec.get("type")
        raw_path = spec.get("path")
        if kind not in {"sqlite", "path"} or not isinstance(raw_path, str):
            return {}, f"dataset {name!r} has invalid type/path configuration"
        configured = Path(raw_path).expanduser()
        if kind == "sqlite":
            destination = inputs_dir / f"{name}.db"
            try:
                _snapshot_sqlite(configured.resolve(), destination, max_mb)
            except Exception as exc:
                return {}, f"dataset {name!r}: {exc}"
            copied_bytes = destination.stat().st_size
            if copied_bytes > file_size_bytes:
                destination.unlink(missing_ok=True)
                return {}, (
                    f"dataset {name!r} writable sqlite copy is "
                    f"{copied_bytes / 1024**2:.1f}MB, exceeding file_size_mb "
                    f"({limits['file_size_mb']}MB)"
                )
            sqlite_bytes += copied_bytes
            if sqlite_bytes > scratch_bytes:
                destination.unlink(missing_ok=True)
                return {}, (
                    f"dataset {name!r} writable sqlite copy makes sqlite inputs "
                    f"{sqlite_bytes / 1024**2:.1f}MB, exceeding scratch_mb "
                    f"({limits['scratch_mb']}MB)"
                )
            mounts[name] = destination
        else:
            # A lexical declaration boundary stops a configured symlink from
            # silently widening the whitelist. An explicit root may widen it.
            root_value = spec.get("root")
            boundary = (
                Path(root_value).expanduser()
                if isinstance(root_value, str)
                else configured.parent
            )
            error = validate_within_dir(configured, boundary)
            if error:
                return {}, f"dataset {name!r}: {error}"
            resolved = configured.resolve()
            if not resolved.exists():
                return {}, f"dataset {name!r} does not exist: {configured}"
            mounts[name] = resolved
    return mounts, None


def _q(value: str | Path) -> str:
    return shlex.quote(str(value))


def _generate_init_script(
    run_dir: Path,
    mounts: Mapping[str, Path],
    venv: Path,
    tmpfs_mb: int = 64,
    base_prefix: Path | None = None,
    max_processes: int = 64,
    cpu_seconds: int = 60,
    scratch_mb: int = 64,
    sqlite_datasets: set[str] | None = None,
    seed_work: Path | None = None,
    strip_seed_metadata: bool = False,
) -> str:
    """Generate the mount plan executed as namespace-root."""
    jail = run_dir / "jail"
    lines = [
        "#!/bin/sh",
        "set -eu",
        f"JAIL={_q(jail)}",
        'mkdir -p "$JAIL"',
        f'mount -t tmpfs -o size={int(tmpfs_mb)}m,nosuid,nodev tmpfs "$JAIL"',
        'mkdir -p "$JAIL/usr" "$JAIL/bin" "$JAIL/lib" "$JAIL/lib64" '
        '"$JAIL/venv" "$JAIL/etc" "$JAIL/inputs" "$JAIL/work" "$JAIL/export" '
        '"$JAIL/proc" "$JAIL/seed" "$JAIL/.oldroot"',
        'ro_dir() { src=$1; dst=$2; [ -e "$src" ] || return 0; '
        'mount --rbind "$src" "$dst"; mount --make-rslave "$dst"; '
        'targets="$JAIL/.mount-targets"; : > "$targets"; '
        'findmnt -Rrn -o TARGET "$dst" > "$targets"; '
        '[ -s "$targets" ] || { echo "findmnt returned no targets for $dst" >&2; exit 1; }; '
        'sort -r -o "$targets" "$targets"; '
        'while IFS= read -r target; do mount -o remount,bind,ro "$target"; '
        'done < "$targets"; rm -f "$targets"; }',
        'ro_file() { src=$1; dst=$2; [ -e "$src" ] || return 0; '
        ': > "$dst"; mount --bind "$src" "$dst"; '
        'mount -o remount,bind,ro "$dst"; }',
        'ro_dir /usr "$JAIL/usr"',
        'ro_dir /bin "$JAIL/bin"',
        'ro_dir /lib "$JAIL/lib"',
        'ro_dir /lib64 "$JAIL/lib64"',
        f'ro_dir {_q(venv)} "$JAIL/venv"',
        'for f in /etc/ld.so.cache /etc/ld.so.conf /etc/localtime /etc/passwd; do '
        '[ -e "$f" ] && ro_file "$f" "$JAIL/etc/${f##*/}"; done',
        '[ -d /etc/ld.so.conf.d ] && { mkdir -p "$JAIL/etc/ld.so.conf.d"; '
        'ro_dir /etc/ld.so.conf.d "$JAIL/etc/ld.so.conf.d"; }',
        '[ -d /etc/alternatives ] && { mkdir -p "$JAIL/etc/alternatives"; '
        'ro_dir /etc/alternatives "$JAIL/etc/alternatives"; }',
        f'ro_file {_q(run_dir / "script.py")} "$JAIL/script.py"',
        f'ro_file {_q(run_dir / "supervisor.py")} "$JAIL/supervisor.py"',
        f'ro_file {_q(run_dir / "inputs" / "params.json")} '
        '"$JAIL/inputs/params.json"',
    ]
    base_prefix = (base_prefix or Path(sys.base_prefix)).resolve()
    system_roots = (Path("/usr"), Path("/bin"), Path("/lib"), Path("/lib64"))
    if not any(
        base_prefix == root or root in base_prefix.parents for root in system_roots
    ):
        base_target = f'"$JAIL{base_prefix.as_posix()}"'
        lines.append(f"mkdir -p {base_target}")
        lines.append(f"ro_dir {_q(base_prefix)} {base_target}")
    sqlite_datasets = set(sqlite_datasets or ())
    lines.append(
        f'mount -t tmpfs -o size={int(scratch_mb)}m,nosuid,nodev,noexec '
        'tmpfs "$JAIL/work"'
    )
    if seed_work is not None:
        lines.append(f'ro_dir {_q(seed_work)} "$JAIL/seed"')
        lines.append(
            'cp -R --no-preserve=all "$JAIL/seed/." "$JAIL/work/"'
            if strip_seed_metadata
            else 'cp -a "$JAIL/seed/." "$JAIL/work/"'
        )
        # result.json is the current invocation's return channel, not user
        # workspace state. A prior result must never be harvested as this run's.
        lines.append('rm -f "$JAIL/work/result.json"')
    for name, source in mounts.items():
        if name in sqlite_datasets:
            target = f'"$JAIL/work/{name}.db"'
            lines.append(f"cp {_q(source)} {target}")
            lines.append(f"chmod 600 {target}")
            continue
        target = f'"$JAIL/inputs/{name}"'
        lines.append(f"mkdir -p {target}" if source.is_dir() else f": > {target}")
        lines.append(
            ("ro_dir" if source.is_dir() else "ro_file")
            + f" {_q(source)} {target}"
        )
    lines.extend(
        [
            f'mount --bind {_q(run_dir / "work")} "$JAIL/export"',
            'mount -o remount,bind,ro "$JAIL/export"',
            'mount -t proc -o nosuid,nodev,noexec proc "$JAIL/proc"',
            # Assembly is complete. Freeze the tmpfs root before pivot;
            # nested /work stays a separate writable bind mount.
            'mount -o remount,bind,ro "$JAIL"',
            '/usr/sbin/pivot_root "$JAIL" "$JAIL/.oldroot"',
            "cd /",
            "umount -l /.oldroot",
            "cd /work",
            # Replace namespace-root shell with a non-dumpable supervisor.
            # The untrusted payload cannot ptrace PID 1 to borrow its mount
            # namespace capabilities.
            "exec /venv/bin/python -I /supervisor.py "
            f"{int(cpu_seconds)} {int(max_processes)} "
            f"{_q(json.dumps(sorted(sqlite_datasets)))}",
            "",
        ]
    )
    return "\n".join(lines)


def _build_env(
    mounts: Mapping[str, Path],
    source_env: Mapping[str, str] | None = None,
    *,
    sqlite_datasets: set[str] | None = None,
) -> dict[str, str]:
    source = dict(source_env or os.environ)
    # Do not inherit execute_code's configurable passthrough allowlist here:
    # this boundary is deliberately fixed and credential-blind.
    sqlite_datasets = set(sqlite_datasets or ())
    env = {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "SANDBOX_INPUTS": json.dumps(
            {
                name: (
                    f"/work/{name}.db"
                    if name in sqlite_datasets
                    else str(python_sandbox_dataset_path(name))
                )
                for name in mounts
            },
            sort_keys=True,
        ),
        "RESULT_PATH": "/work/result.json",
        "TMPDIR": "/work",
        "HOME": "/work",
        "TZ": source.get("HERMES_TIMEZONE", "UTC"),
        "LANG": "C.UTF-8",
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUTF8": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return env


def _preexec(limits: Mapping[str, int]):
    def apply() -> None:
        if resource is None:
            raise RuntimeError("resource limits unavailable")
        os.setsid()  # windows-footgun: ok — availability probe requires Linux
        resource.setrlimit(
            resource.RLIMIT_AS, (limits["memory_mb"] * 1024**2,) * 2
        )
        resource.setrlimit(
            resource.RLIMIT_FSIZE, (limits["file_size_mb"] * 1024**2,) * 2
        )
        resource.setrlimit(
            resource.RLIMIT_NOFILE, (limits["max_open_files"],) * 2
        )
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        os.nice(5)

    return apply


def _cap_head_tail(text: str, cap: int = STDOUT_CAP) -> tuple[str, bool]:
    data = text.encode("utf-8", errors="replace")
    if len(data) <= cap:
        return text, False
    head_n = int(cap * 0.4)
    tail_n = cap - head_n
    omitted = len(data) - cap
    head = data[:head_n].decode("utf-8", errors="replace")
    tail = data[-tail_n:].decode("utf-8", errors="replace")
    omitted_entries = max(0, text.count("\n") - head.count("\n") - tail.count("\n"))
    marker = f"[TRUNCATED: {omitted:,} bytes / {omitted_entries:,} entries omitted]"
    return f"{head}\n\n{marker}\n\n{tail}", True


def _clean(text: str) -> str:
    from agent.redact import redact_sensitive_text
    from tools.ansi_strip import strip_ansi

    return redact_sensitive_text(strip_ansi(text), force=True)


def _sanitize_value(value: Any) -> Any:
    """Sanitize every model-bound string while preserving JSON structure."""
    if isinstance(value, str):
        return _clean(value)
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, dict):
        return {
            _clean(key) if isinstance(key, str) else key: _sanitize_value(item)
            for key, item in value.items()
        }
    return value


def _drain_stream(
    stream,
    head_chunks: list[bytes],
    tail_chunks: list[bytes],
    total: list[int],
    head_cap: int,
    tail_cap: int,
) -> None:
    """Drain a pipe concurrently, retaining bounded head and rolling tail."""
    while True:
        chunk = stream.read(8192)
        if not chunk:
            return
        if isinstance(chunk, str):  # useful for simple mocked streams
            chunk = chunk.encode("utf-8", errors="replace")
        total[0] += len(chunk)
        have_head = sum(len(item) for item in head_chunks)
        if have_head < head_cap:
            take = min(head_cap - have_head, len(chunk))
            head_chunks.append(chunk[:take])
            chunk = chunk[take:]
        if chunk and tail_cap:
            tail_chunks.append(chunk)
            tail_size = sum(len(item) for item in tail_chunks)
            while tail_chunks and tail_size > tail_cap:
                overflow = tail_size - tail_cap
                if overflow >= len(tail_chunks[0]):
                    tail_size -= len(tail_chunks.pop(0))
                else:
                    tail_chunks[0] = tail_chunks[0][overflow:]
                    tail_size -= overflow


def _assemble_drain(
    head_chunks: list[bytes],
    tail_chunks: list[bytes],
    total: int,
    cap: int,
) -> str:
    head = b"".join(head_chunks)
    tail = b"".join(tail_chunks)
    if total <= cap:
        return (head + tail).decode("utf-8", errors="replace")
    omitted = max(0, total - len(head) - len(tail))
    return (
        head.decode("utf-8", errors="replace")
        + f"\n\n... [{omitted:,} chars omitted] ...\n\n"
        + tail.decode("utf-8", errors="replace")
    )


_CLIENT_ARTIFACT_NAME = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,126}\.xlsx$"
)
_CLIENT_ARTIFACT_RUN_ID = re.compile(r"^r_[a-f0-9]{8}$")
_ZIP_MAGIC = b"PK\x03\x04"


def _promote_workbook(
    source: Path,
    *,
    run_id: str,
    media_root: Path,
    media_ref_prefix: str,
) -> str | None:
    """Atomically promote one validated workbook into retained media."""
    try:
        with source.open("rb") as handle:
            if handle.read(len(_ZIP_MAGIC)) != _ZIP_MAGIC:
                return None
            handle.seek(0)
            digest = hashlib.sha256()
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None

    hexdigest = digest.hexdigest()
    # Deliverable filename: keep the model's chosen name (already constrained
    # by _CLIENT_ARTIFACT_NAME) so the client receives a readable document;
    # the run-id suffix keeps retained names collision-free across runs.
    stem = source.stem[:100]
    basename = f"{stem}__{run_id}.xlsx"
    media_root.mkdir(parents=True, exist_ok=True, mode=0o750)
    target = media_root / basename
    if target.exists():
        try:
            existing = hashlib.sha256(target.read_bytes()).hexdigest()
        except OSError:
            existing = ""
        if existing == hexdigest:
            os.chmod(target, 0o640)
            return f"{media_ref_prefix.rstrip('/')}/{basename}"

    tmp = media_root / f".{basename}.{uuid.uuid4().hex}.tmp"
    try:
        with source.open("rb") as src, tmp.open("xb") as dst:
            copied_digest = hashlib.sha256()
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                dst.write(chunk)
                copied_digest.update(chunk)
            dst.flush()
            os.fsync(dst.fileno())
        if copied_digest.hexdigest() != hexdigest:
            return None
        os.chmod(tmp, 0o640)
        os.replace(tmp, target)
        return f"{media_ref_prefix.rstrip('/')}/{basename}"
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _list_files(
    work: Path,
    *,
    run_id: str | None = None,
    artifact_url_base: Any = None,
    media_retention: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    files = []
    client_url_base = (
        artifact_url_base.rstrip("/")
        if isinstance(artifact_url_base, str)
        else ""
    )
    valid_run_id = isinstance(run_id, str) and bool(
        _CLIENT_ARTIFACT_RUN_ID.fullmatch(run_id)
    )
    retention = media_retention if isinstance(media_retention, Mapping) else {}
    media_root_value = retention.get("root") or retention.get("media_root")
    media_ref_prefix = retention.get("media_ref_prefix")
    promotion_enabled = (
        valid_run_id
        and isinstance(media_root_value, str)
        and bool(media_root_value.strip())
        and isinstance(media_ref_prefix, str)
        and media_ref_prefix.startswith("/media/")
    )
    for path in sorted(work.rglob("*")):
        try:
            metadata = path.stat(follow_symlinks=False)
        except OSError:
            continue
        if not stat.S_ISREG(metadata.st_mode) or path.name == "result.json":
            continue
        item: dict[str, Any] = {
            "path": f"work/{path.relative_to(work).as_posix()}",
            "bytes": metadata.st_size,
        }
        if (
            client_url_base
            and valid_run_id
            and _CLIENT_ARTIFACT_NAME.fullmatch(path.name)
        ):
            item["client_url"] = f"{client_url_base}/{run_id}/{path.name}"
        if promotion_enabled and _CLIENT_ARTIFACT_NAME.fullmatch(path.name):
            media_ref = _promote_workbook(
                path,
                run_id=run_id,
                media_root=Path(media_root_value).expanduser(),
                media_ref_prefix=media_ref_prefix,
            )
            if media_ref:
                item["media_ref"] = media_ref
        try:
            with path.open("rb") as handle:
                item["lines"] = sum(1 for _ in handle)
        except OSError:
            pass
        files.append(_sanitize_value(item))
    return files


def _harvest(
    work: Path,
    stdout: str,
    stderr: str,
    status: str,
    limits: Mapping[str, int],
    *,
    run_id: str | None = None,
    artifact_url_base: Any = None,
    media_retention: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    stdout, stdout_truncated = _cap_head_tail(_clean(stdout))
    stderr = _clean(stderr)[-STDERR_CAP:]
    result = None
    result_truncated = False
    result_path = work / "result.json"
    error = ""
    try:
        result_metadata = result_path.stat(follow_symlinks=False)
    except OSError:
        result_metadata = None
    if result_metadata is not None and stat.S_ISREG(result_metadata.st_mode):
        raw = result_path.read_bytes()
        if len(raw) > RESULT_CAP:
            status = "result_invalid"
            result_truncated = True
            error = (
                f"result.json is {len(raw) / 1024:.1f}KB (cap 8KB) — "
                "write detail to /work files and return counts + samples"
            )
        else:
            try:
                result = _sanitize_value(json.loads(raw.decode("utf-8")))
            except Exception as exc:
                status = "result_invalid"
                error = f"result.json is not valid JSON: {exc}"
    if status == "error" and (
        "MemoryError" in stderr or "Cannot allocate memory" in stderr
    ):
        status = "oom"
        error = (
            f"memory limit ({limits['memory_mb']}MB) exhausted — "
            "stream/chunk instead of loading everything"
        )
    return (
        {
            "status": status,
            "stdout": stdout,
            "stderr": stderr if status != "success" else "",
            "result": result,
            "files": _list_files(
                work,
                run_id=run_id,
                artifact_url_base=artifact_url_base,
                media_retention=media_retention,
            ),
            "truncated": {
                "stdout": stdout_truncated,
                "result": result_truncated,
            },
        },
        error,
    )


def _prune_runs(root: Path, config: Mapping[str, Any]) -> None:
    try:
        ttl = max(0, int(config.get("artifact_ttl_days", 7))) * 86400
        keep = max(1, int(config.get("max_runs_kept", 40)))
    except (TypeError, ValueError):
        ttl, keep = 7 * 86400, 40
    now = time.time()
    entries = (
        sorted(
            (path for path in root.iterdir() if path.is_dir()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if root.exists()
        else []
    )
    for index, path in enumerate(entries):
        if index >= keep or (ttl and now - path.stat().st_mtime > ttl):
            shutil.rmtree(path, ignore_errors=True)


def _workspace_mode(config: Mapping[str, Any]) -> str:
    """Return the configured scratch lifetime, preserving run scope by default.

    ``agent`` (PA-203 round 2) is one persistent folder for the whole agent:
    every conversation, scheduled job and delegated helper, independent of
    the provider, model and compaction.  ``session`` is one folder per
    stable session key; ``run`` is a fresh folder per call.
    """
    value = config.get("workspace")
    if value == "agent":
        return "agent"
    return "session" if value == "session" else "run"


def agent_workspace_enabled(config: Mapping[str, Any] | None = None) -> bool:
    """True when this deployment keeps one persistent agent-wide workspace."""
    return _workspace_mode(_load_config() if config is None else config) == "agent"


def _workspace_key(session_id: str, config: Mapping[str, Any] | None = None) -> str:
    """Name of the retained folder for ``session_id``.

    In ``agent`` mode every owner maps to the one agent folder, so each
    caller that derives a folder from an owner (sandbox runs, delivery,
    materialized query results, capability file return) lands in the same
    place whatever conversation, job, helper or model it runs under.
    """
    if agent_workspace_enabled(config):
        session_id = AGENT_WORKSPACE_OWNER
    return "s_" + hashlib.sha256(session_id.encode("utf-8")).hexdigest()


def resolve_sandbox_work_path(raw_path: Any, workspace_owner: str) -> Path:
    """Resolve one sandbox-local ``work/...`` reference for its owner.

    The model sees only the sandbox path.  Host paths stay private and are
    resolved at the final delivery boundary.  Symlinks are refused even when
    they point back into the workspace so a captured reference cannot change
    meaning between validation and use.
    """
    text = str(raw_path or "").strip()
    owner = str(workspace_owner or "").strip()
    config = _load_config()
    if agent_workspace_enabled(config):
        # One folder for everything: bring earlier folders in first so a
        # file made before the move still resolves at its old path.
        owner = AGENT_WORKSPACE_OWNER
        try:
            ensure_agent_workspace(config)
        except (OSError, RuntimeError):
            # Resolution still answers from the agent folder as it stands;
            # the merge is retried by the next caller.
            logger.exception("Could not merge earlier sandbox workspaces")
    if not owner:
        raise ValueError("sandbox workspace owner is unavailable")
    if text.startswith("/work/"):
        relative_text = text[len("/work/"):]
    elif text.startswith("work/"):
        relative_text = text[len("work/"):]
    else:
        raise ValueError("attachment is not a sandbox work path")
    relative = Path(relative_text)
    if (
        not relative_text
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ValueError("sandbox work path is invalid")

    work_root = (
        get_hermes_home()
        / "sandbox_workspaces"
        / _workspace_key(owner, config)
        / "work"
    )
    candidate = work_root / relative
    if not candidate.is_relative_to(work_root):
        raise ValueError("sandbox work path escapes its workspace")

    current = work_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("sandbox work path contains a symlink")
    try:
        resolved_root = work_root.resolve(strict=True)
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError("sandbox work file is unavailable") from exc
    if not resolved.is_relative_to(resolved_root) or not resolved.is_file():
        raise ValueError("sandbox work path is not a confined regular file")
    return resolved


def bind_workspace_owner(owner: str | None) -> contextvars.Token:
    """Bind a runtime-derived sandbox workspace owner for the current task.

    Only trusted runtime code calls this: the PA scheduler binds the
    originating Management conversation's stable session key so a scheduled
    job works in that conversation's persistent folder.  The value is never a
    model-facing argument.
    """
    return _BOUND_WORKSPACE_OWNER.set(str(owner or "").strip())


def reset_workspace_owner(token: contextvars.Token) -> None:
    _BOUND_WORKSPACE_OWNER.reset(token)


def bind_published_collector(collector: set) -> contextvars.Token:
    """Collect the ``work/`` paths this task's sandbox runs publish.

    The PA scheduler binds one set per scheduled run, so delivery to a chat
    other than the job's origin can attach only files the job itself made.
    """
    return _BOUND_PUBLISHED.set(collector)


def reset_published_collector(token: contextvars.Token) -> None:
    _BOUND_PUBLISHED.reset(token)


def current_workspace_owner(fallback: str | None = None) -> str:
    """Return the owner of the retained workspace for this task.

    Precedence: a scheduler-bound owner, then the gateway's stable
    ``HERMES_SESSION_KEY`` (survives compaction), then the caller's fallback.
    In ``agent`` mode the answer is always the one agent-wide owner.
    """
    if agent_workspace_enabled():
        return AGENT_WORKSPACE_OWNER
    bound = _BOUND_WORKSPACE_OWNER.get().strip()
    if bound:
        return bound
    from gateway.session_context import get_session_env

    return (
        get_session_env("HERMES_SESSION_KEY", "").strip()
        or str(fallback or "").strip()
    )


@contextmanager
def _workspace_lock(session_id: str, workspace: Path) -> Iterator[None]:
    """Hold one workspace's publication lock across threads and processes.

    The lock covers only the short seed snapshot and the per-file publication,
    never the sandbox run itself, so a scheduled job and a live turn sharing
    one conversation folder do not wait on each other's execution.  The
    thread lock orders callers in this process; ``flock`` on a file inside the
    workspace orders separate processes (scheduler, gateway, maintenance).
    """
    key = str(workspace)
    with _WORKSPACE_LOCKS_GUARD:
        thread_lock = _WORKSPACE_LOCKS.setdefault(key, threading.Lock())
    with thread_lock:
        workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
        if fcntl is None:  # pragma: no cover - sandbox is Linux-only
            yield
            return
        fd = os.open(
            workspace / _WORKSPACE_LOCK_FILE,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


def _work_manifest(
    work: Path, irregular: dict[str, str] | None = None
) -> dict[str, str]:
    """Map each regular retained file under ``work`` to its SHA-256.

    ``result.json`` is the per-invocation return channel, never workspace
    state, and symlinks or special files are never retained.  When
    ``irregular`` is given, every entry that is present but is not hashed is
    recorded there: ``"not_regular"`` for a symlink or special file and
    ``"unreadable"`` for a file that could not be read.  Publication uses it
    so that such an entry is never mistaken for a deletion.
    """
    manifest: dict[str, str] = {}
    if not work.is_dir() or work.is_symlink():
        return manifest
    for current, dirnames, filenames in os.walk(work, topdown=True, followlinks=False):
        current_path = Path(current)
        kept_dirs = []
        for name in dirnames:
            if (current_path / name).is_symlink():
                if irregular is not None:
                    irregular[(current_path / name).relative_to(work).as_posix()] = "not_regular"
            else:
                kept_dirs.append(name)
        dirnames[:] = kept_dirs
        for name in filenames:
            if name == "result.json":
                continue
            path = current_path / name
            relative = path.relative_to(work).as_posix()
            try:
                if not stat.S_ISREG(path.lstat().st_mode):
                    if irregular is not None:
                        irregular[relative] = "not_regular"
                    continue
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
            except OSError:
                if irregular is not None:
                    irregular[relative] = "unreadable"
                continue
            manifest[relative] = digest.hexdigest()
    return manifest


def _snapshot_workspace(work: Path, snapshot: Path) -> dict[str, str]:
    """Copy the retained work tree to a private per-run seed; return its manifest.

    The caller holds the workspace lock, so the manifest is exactly the state
    this run starts from.  It is the merge base for publication.
    """
    snapshot.mkdir(parents=True, exist_ok=True, mode=0o700)
    manifest = _work_manifest(work)
    for relative in manifest:
        source = work / relative
        target = snapshot / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Content and times only: host ownership and reader ACLs stay on the
        # retained folder and are never copied into the private run tree.
        shutil.copyfile(source, target, follow_symlinks=False)
        metadata = source.stat(follow_symlinks=False)
        os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
    # A copied file may have changed between hashing and copying only if a
    # writer bypassed the lock; hash the snapshot itself so the base is exact.
    return _work_manifest(snapshot)


def _alternate_name(relative: str, run_id: str, taken: set[str]) -> str:
    path = Path(relative)
    suffix = "".join(path.suffixes[-1:])
    stem = path.name[: len(path.name) - len(suffix)] if suffix else path.name
    for index in range(1000):
        tag = run_id if index == 0 else f"{run_id}-{index}"
        candidate = (path.parent / f"{stem}.{tag}{suffix}").as_posix()
        if candidate not in taken:
            return candidate
    raise RuntimeError("could not choose a distinct workspace name")


def _free_name(
    destination: Path, relative: str, run_id: str, taken: set[str]
) -> str:
    """A name beside ``relative`` that no file, directory or plan uses yet."""
    for index in range(1000):
        candidate = _alternate_name(relative, f"{run_id}-{index}" if index else run_id, set())
        if candidate not in taken and not os.path.lexists(destination / candidate):
            return candidate
    raise RuntimeError("could not choose a distinct workspace name")


def _blocked_prefix(root: Path, relative_parent: Path) -> str | None:
    """First ancestor of a target that exists in ``root`` but is not a real directory."""
    current = root
    for index, part in enumerate(relative_parent.parts):
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            return Path(*relative_parent.parts[: index + 1]).as_posix()
    return None


def _real_directory_chain(root: Path, relative_parent: Path) -> bool:
    current = root
    for part in relative_parent.parts:
        current = current / part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return True
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            return False
    return True


def _publish_workspace(
    source: Path,
    destination: Path,
    base: Mapping[str, str],
    *,
    run_id: str,
    hermes_home: Path,
    reader_uid: str | None = None,
) -> dict[str, Any]:
    """Merge one completed run into its retained workspace, file by file.

    The caller holds the workspace lock.  The retained folder is never removed
    or replaced as a whole, so a file another writer published while this run
    executed is kept.  Three-way rules against the run's seed ``base``:

    * a file this run did not change is left as the workspace has it now;
    * a file this run created or changed is installed atomically, unless
      another writer changed the same path since the seed: then the other
      writer's file keeps its name and this run's file is installed beside it
      under a distinct name, reported back as ``renamed``;
    * a seed file this run deleted is removed only if nobody changed it since,
      and only if the run's tree really lacks it: a retained file the run
      replaced with a directory, symlink or special file, or whose run copy
      could not be read, is kept and reported as ``kept``;
    * where the run replaced a file with a directory (or a directory with a
      file), both versions survive: the run's entry is installed under a
      distinct name and reported as ``renamed``;
    * a run file that could not be read is reported as ``unpublished``.

    Every installed file receives the configured reader ACL before it becomes
    visible under its final name.
    """
    irregular: dict[str, str] = {}
    out = _work_manifest(source, irregular)
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    current = _work_manifest(destination)
    taken = set(current) | set(out)
    installs: list[tuple[str, str]] = []  # (source relative, destination relative)
    produced: list[str] = []  # run-written files already present with equal bytes
    renamed: dict[str, str] = {}
    unpublished: list[str] = sorted(
        relative for relative, reason in irregular.items() if reason == "unreadable"
    )
    prefix_moves: dict[str, str] = {}  # blocked destination prefix -> its free name
    for relative, digest in sorted(out.items()):
        if base.get(relative) == digest:
            continue
        if current.get(relative) == digest:
            produced.append(relative)
            continue
        target = relative
        occupied_by_directory = (destination / relative).is_dir()
        if occupied_by_directory or (
            relative in current and current[relative] != base.get(relative)
        ):
            target = _free_name(destination, relative, run_id, taken)
            taken.add(target)
            renamed[relative] = target
        blocked = _blocked_prefix(destination, Path(target).parent)
        if blocked is not None:
            # A retained file (or symlink) sits where this run has a directory:
            # keep it, and move this run's directory beside it under one name.
            if blocked not in prefix_moves:
                prefix_moves[blocked] = _free_name(destination, blocked, run_id, taken)
                taken.add(prefix_moves[blocked])
            target = prefix_moves[blocked] + target[len(blocked):]
            taken.add(target)
            renamed[relative] = target
        if not _real_directory_chain(destination, Path(target).parent):
            unpublished.append(relative)
            renamed.pop(relative, None)
            continue
        installs.append((relative, target))

    removals: list[str] = []
    kept: list[str] = []
    for relative, digest in base.items():
        if relative in out or current.get(relative) != digest:
            continue
        if os.path.lexists(source / relative) or not _real_directory_chain(
            source, Path(relative).parent
        ):
            # The run still has something at this path (a directory, symlink,
            # special or unreadable file), or replaced a parent directory with
            # a file.  That is not a deletion: keep the retained copy.
            kept.append(relative)
            continue
        removals.append(relative)

    parent = destination.parent
    staged = parent / f".work-{uuid.uuid4().hex}.tmp"
    try:
        staged.mkdir(mode=0o700)
        for relative, target in installs:
            staged_file = staged / target
            staged_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copy2(source / relative, staged_file, follow_symlinks=False)
        for directory in sorted(
            path.relative_to(source)
            for path in source.rglob("*")
            if path.is_dir() and not path.is_symlink()
        ):
            if _real_directory_chain(destination, directory):
                (destination / directory).mkdir(
                    parents=True, exist_ok=True, mode=0o700
                )
        for _relative, target in installs:
            (destination / target).parent.mkdir(
                parents=True, exist_ok=True, mode=0o700
            )
        if reader_uid is not None:
            # Files are opened in staging; new directories are opened in place
            # before any file is renamed into them.
            grant_reader_work_access(hermes_home, staged, reader_uid)
            grant_reader_work_access(hermes_home, destination, reader_uid)
        for _relative, target in installs:
            os.replace(staged / target, destination / target)
        for relative in removals:
            try:
                (destination / relative).unlink()
            except FileNotFoundError:
                pass
        os.utime(parent, None)
    finally:
        shutil.rmtree(staged, ignore_errors=True)
    return {
        "renamed": renamed,
        "unpublished": unpublished,
        "kept": sorted(kept),
        "produced": sorted(produced + [target for _relative, target in installs]),
    }


def _newest_file_mtime(work: Path) -> float:
    newest = 0.0
    for current, _dirs, files in os.walk(work, followlinks=False):
        for name in files:
            try:
                newest = max(newest, (Path(current) / name).lstat().st_mtime)
            except OSError:
                continue
    return newest


def _merge_legacy_workspaces_locked(
    workspace_root: Path,
    workspace: Path,
    *,
    hermes_home: Path,
    reader_uid: str | None,
) -> list[dict[str, Any]]:
    """Merge every earlier per-conversation folder into the agent folder.

    The caller holds the agent workspace lock.  Nothing is deleted: each
    earlier folder is read under its own lock and left exactly as it was, and
    the merge uses the publication rules with an empty base, so a file the
    agent folder lacks is added, an identical file is left alone, and a
    same-named file with different bytes keeps both versions (the one already
    here keeps its name; the other is saved beside it with an ``m_<folder>``
    tag).  Folders are merged newest first, so the most recently used copy
    of a name keeps the plain name.  A record beside ``work/`` names each
    merged folder, so a merge never repeats.
    """
    record_path = workspace / _MERGE_RECORD_FILE
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        record = {}
    except (OSError, ValueError):
        logger.exception("Workspace merge record is unreadable; not merging")
        return []
    merged = record.get("merged") if isinstance(record, dict) else None
    if not isinstance(merged, dict):
        merged = {}
    if not workspace_root.is_dir():
        return []
    candidates = [
        path
        for path in workspace_root.iterdir()
        if path != workspace
        and _LEGACY_WORKSPACE_RE.fullmatch(path.name)
        and path.name not in merged
        and path.is_dir()
        and not path.is_symlink()
    ]
    if not candidates:
        return []
    candidates.sort(
        key=lambda path: (_newest_file_mtime(path / "work"), path.name),
        reverse=True,
    )
    results: list[dict[str, Any]] = []
    destination = workspace / "work"
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    for legacy in candidates:
        legacy_work = legacy / "work"
        entry: dict[str, Any] = {
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "files": 0,
            "same_name": [],
            "renamed": {},
            "unpublished": [],
        }
        if legacy_work.is_dir() and not legacy_work.is_symlink():
            with _workspace_lock(f"legacy:{legacy.name}", legacy):
                files = _work_manifest(legacy_work)
                outcome = _publish_workspace(
                    legacy_work,
                    destination,
                    {},
                    run_id=f"m_{legacy.name[2:10]}",
                    hermes_home=hermes_home,
                    reader_uid=reader_uid,
                )
            entry["files"] = len(files)
            entry["renamed"] = dict(outcome.get("renamed") or {})
            entry["unpublished"] = list(outcome.get("unpublished") or [])
            entry["same_name"] = sorted(
                set(outcome.get("produced") or []) - set(entry["renamed"].values())
            )
        merged[legacy.name] = entry
        results.append({"folder": legacy.name, **entry})
        record = {"format": "hermes-agent-workspace-merge/v1", "merged": merged}
        staged = record_path.with_name(f".{record_path.name}.{uuid.uuid4().hex}.tmp")
        staged.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(staged, record_path)
        logger.info(
            "Merged sandbox workspace %s into the agent workspace: %d files, %d renamed",
            legacy.name,
            entry["files"],
            len(entry["renamed"]),
        )
    return results


def ensure_agent_workspace(
    config: Mapping[str, Any] | None = None,
) -> Path | None:
    """Return the agent workspace ``work/`` folder, merging earlier folders.

    ``None`` when this deployment does not use ``workspace: agent``.
    """
    config = _load_config() if config is None else config
    if not agent_workspace_enabled(config):
        return None
    hermes_home = get_hermes_home()
    workspace_root = hermes_home / "sandbox_workspaces"
    workspace = workspace_root / _workspace_key(AGENT_WORKSPACE_OWNER, config)
    reader_uid = configured_reader_uid(config)
    with _workspace_lock(AGENT_WORKSPACE_OWNER, workspace):
        _merge_legacy_workspaces_locked(
            workspace_root, workspace, hermes_home=hermes_home, reader_uid=reader_uid
        )
        (workspace / "work").mkdir(parents=True, exist_ok=True, mode=0o700)
    return workspace / "work"


def _safe_import_name(name: str) -> str:
    cleaned = re.sub(r"[^\w.\- ]", "_", Path(str(name or "")).name).strip(" .")
    return cleaned or "file"


def record_workspace_file(
    source: str | Path,
    *,
    direction: str,
    folder: str,
    name: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> str | None:
    """Keep one file that came in or went out in the agent workspace.

    ``source`` is a host file.  When it already lives in the agent folder it
    is not copied; otherwise it is copied to ``work/<folder>/<name>`` with the
    publication rules, so an existing different file of that name keeps its
    name and this one is saved beside it.  Either way one line is appended to
    ``work/files-log.jsonl`` saying what the file is, which way it moved,
    when, and where it is now.  Returns the ``work/...`` path, or ``None``
    when the deployment has no agent workspace.
    """
    config = _load_config()
    if not agent_workspace_enabled(config):
        return None
    source_path = Path(source)
    if source_path.is_symlink() or not source_path.is_file():
        raise ValueError("workspace import source is not a regular file")
    hermes_home = get_hermes_home()
    workspace_root = hermes_home / "sandbox_workspaces"
    workspace = workspace_root / _workspace_key(AGENT_WORKSPACE_OWNER, config)
    work = workspace / "work"
    reader_uid = configured_reader_uid(config)
    digest = hashlib.sha256()
    with source_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    sha256 = digest.hexdigest()
    display_name = _safe_import_name(name or source_path.name)
    tag = f"i_{uuid.uuid4().hex[:8]}"
    staging_root = hermes_home / "sandbox_runs" / tag
    with _workspace_lock(AGENT_WORKSPACE_OWNER, workspace):
        _merge_legacy_workspaces_locked(
            workspace_root, workspace, hermes_home=hermes_home, reader_uid=reader_uid
        )
        work.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved = source_path.resolve()
        if resolved.is_relative_to(work.resolve()):
            relative = resolved.relative_to(work.resolve()).as_posix()
        else:
            relative = f"{folder.strip('/')}/{display_name}"
            staged_work = staging_root / "work"
            try:
                (staged_work / relative).parent.mkdir(
                    parents=True, exist_ok=True, mode=0o700
                )
                shutil.copyfile(source_path, staged_work / relative)
                if _work_manifest(staged_work).get(relative) != sha256:
                    raise OSError("workspace import changed while copying")
                outcome = _publish_workspace(
                    staged_work,
                    work,
                    {},
                    run_id=tag,
                    hermes_home=hermes_home,
                    reader_uid=reader_uid,
                )
            finally:
                shutil.rmtree(staging_root, ignore_errors=True)
            if relative in outcome.get("unpublished", []):
                raise OSError("workspace import could not be saved")
            relative = (outcome.get("renamed") or {}).get(relative, relative)
        entry = {
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "direction": direction,
            "path": f"work/{relative}",
            "name": display_name,
            "sha256": sha256,
            **{key: value for key, value in dict(details or {}).items() if value},
        }
        log_path = work / FILES_LOG_NAME
        if log_path.is_symlink() or (log_path.exists() and not log_path.is_file()):
            raise OSError("workspace files log is not a regular file")
        with open(
            log_path,
            "a",
            encoding="utf-8",
            opener=lambda path, flags: os.open(
                path, flags | getattr(os, "O_NOFOLLOW", 0), 0o600
            ),
        ) as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        if reader_uid is not None:
            grant_reader_work_access(hermes_home, work, reader_uid)
    return f"work/{relative}"


def _prune_workspaces(
    root: Path,
    config: Mapping[str, Any],
    *,
    exclude: Path | None = None,
) -> None:
    """Bound retained session workspaces by the artifact TTL and a count cap."""
    try:
        ttl = max(0, int(config.get("artifact_ttl_days", 7))) * 86400
        keep = max(1, int(config.get("max_session_workspaces", 40)))
    except (TypeError, ValueError):
        ttl, keep = 7 * 86400, 40
    now = time.time()
    entries = (
        sorted(
            (path for path in root.iterdir() if path.is_dir()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if root.exists()
        else []
    )
    kept = 0
    for path in entries:
        if exclude is not None and path == exclude:
            kept += 1
            continue
        expired = bool(ttl and now - path.stat().st_mtime > ttl)
        if expired or kept >= keep:
            shutil.rmtree(path, ignore_errors=True)
        else:
            kept += 1


def _kill_group(proc: subprocess.Popen, grace: float = 5.0) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)  # windows-footgun: ok — Linux-only jail
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(  # windows-footgun: ok — Linux-only jail
                proc.pid, getattr(signal, "SIGKILL", signal.SIGTERM)
            )
        except (ProcessLookupError, PermissionError):
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def _unavailable(reason: str) -> str:
    return json.dumps({"status": "unavailable", "error": reason, "result": None})


def _cpu_limit_exhausted(returncode: int, stderr: str) -> bool:
    """Recognize direct and util-linux-wrapped SIGXCPU termination."""
    if returncode in (-signal.SIGXCPU, 128 + signal.SIGXCPU):
        return True
    lowered = stderr.lower()
    return (
        "signal 24" in lowered
        or "cpu time limit" in lowered
        # util-linux unshare 2.39 emits this instead of propagating SIGXCPU
        # when its pid-namespace child dies at the CPU hard limit.
        or "sigprocmask unblock failed" in lowered
    )


def python_sandbox(
    code: str,
    datasets: list[str] | None = None,
    input_json: dict | None = None,
    timeout_seconds: Any = None,
    *,
    session_id: str | None = None,
) -> str:
    config = _load_config()
    available, reason = _probe(force=True)
    if not available:
        return _unavailable(reason)
    if _workspace_mode(config) == "agent":
        # One folder for every conversation, job and helper (PA-203 round 2).
        return _run_python_sandbox(
            code,
            datasets,
            input_json,
            timeout_seconds,
            config=config,
            session_id=AGENT_WORKSPACE_OWNER,
        )
    if _workspace_mode(config) == "session":
        if not isinstance(session_id, str) or not session_id.strip():
            return json.dumps(
                {
                    "status": "error",
                    "error": "session-scoped python_sandbox requires a session_id",
                    "result": None,
                }
            )
        # Runs sharing one workspace execute concurrently; _run_python_sandbox
        # locks only the seed snapshot and the per-file merge publication.
        return _run_python_sandbox(
            code,
            datasets,
            input_json,
            timeout_seconds,
            config=config,
            session_id=session_id,
        )
    return _run_python_sandbox(
        code,
        datasets,
        input_json,
        timeout_seconds,
        config=config,
        session_id=None,
    )


def _run_python_sandbox(
    code: str,
    datasets: list[str] | None,
    input_json: dict | None,
    timeout_seconds: Any,
    *,
    config: Mapping[str, Any],
    session_id: str | None,
) -> str:
    if not isinstance(code, str) or not code.strip():
        return json.dumps(
            {"status": "error", "error": "code must be a non-empty string", "result": None}
        )
    datasets = datasets or []
    if not isinstance(datasets, list) or not all(
        isinstance(name, str) for name in datasets
    ):
        return json.dumps(
            {"status": "error", "error": "datasets must be an array of names", "result": None}
        )
    try:
        params = json.dumps(input_json or {}, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        return json.dumps(
            {
                "status": "error",
                "error": f"input_json must be JSON-serializable: {exc}",
                "result": None,
            }
        )
    if len(params) > INPUT_JSON_CAP:
        return json.dumps(
            {"status": "error", "error": "input_json exceeds 32KB", "result": None}
        )

    try:
        reader_uid = configured_reader_uid(config)
    except SandboxReaderPolicyError as exc:
        return json.dumps({"status": "error", "error": str(exc), "result": None})

    run_id = f"r_{uuid.uuid4().hex[:8]}"
    hermes_home = get_hermes_home()
    root = hermes_home / "sandbox_runs"
    run = root / run_id
    inputs, work = run / "inputs", run / "work"
    workspace_root = hermes_home / "sandbox_workspaces"
    workspace = None
    seed_work = None
    agent_mode = _workspace_mode(config) == "agent"
    if session_id is not None:
        workspace = workspace_root / _workspace_key(session_id, config)
    for path in (root, run, inputs, work):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    (run / "script.py").write_text(code, encoding="utf-8")
    (run / "supervisor.py").write_text(_SUPERVISOR_SOURCE, encoding="utf-8")
    (inputs / "params.json").write_bytes(params)
    mounts, error = _resolve_datasets(datasets, config, inputs)
    if error:
        shutil.rmtree(inputs, ignore_errors=True)
        _prune_runs(root, config)
        status = "dataset_unknown" if error.startswith("unknown dataset") else "error"
        return json.dumps(
            {"status": status, "error": error, "run_id": run_id, "result": None}
        )
    seed_base: dict[str, str] = {}
    if workspace is not None:
        if not agent_mode:
            # The agent workspace keeps every earlier folder until an
            # operator removes it after the merge is verified.
            _prune_workspaces(workspace_root, config, exclude=workspace)
        retained_work = workspace / "work"
        seed_work = run / "seed"
        with _workspace_lock(session_id, workspace):
            if agent_mode:
                try:
                    _merge_legacy_workspaces_locked(
                        workspace_root,
                        workspace,
                        hermes_home=hermes_home,
                        reader_uid=reader_uid,
                    )
                except (OSError, SandboxReaderPolicyError) as exc:
                    logger.exception("Could not merge earlier sandbox workspaces")
                    shutil.rmtree(run, ignore_errors=True)
                    return json.dumps(
                        {
                            "status": "error",
                            "error": f"workspace merge failed: {exc}",
                            "result": None,
                        }
                    )
            retained_work.mkdir(parents=True, exist_ok=True, mode=0o700)
            if reader_uid is not None:
                try:
                    grant_reader_work_access(hermes_home, retained_work, reader_uid)
                except SandboxReaderPolicyError as exc:
                    logger.exception(
                        "Could not apply sandbox reader ACL before execution"
                    )
                    return json.dumps(
                        {"status": "error", "error": str(exc), "result": None}
                    )
            try:
                seed_base = _snapshot_workspace(retained_work, seed_work)
            except OSError as exc:
                logger.exception("Could not snapshot the sandbox workspace")
                shutil.rmtree(run, ignore_errors=True)
                return json.dumps(
                    {
                        "status": "error",
                        "error": f"workspace snapshot failed: {exc.strerror or exc}",
                        "result": None,
                    }
                )

    limits = _limits(config)
    sqlite_datasets = {
        name
        for name in datasets
        if _dataset_config(config).get(name, {}).get("type") == "sqlite"
    }
    wall = _wall_seconds(timeout_seconds, config)
    venv = Path(sys.prefix).resolve()
    init_path = run / "init.sh"
    init_path.write_text(
        _generate_init_script(
            run,
            mounts,
            venv,
            limits["file_size_mb"],
            Path(sys.base_prefix),
            limits["max_processes"],
            limits["cpu_seconds"],
            limits["scratch_mb"],
            sqlite_datasets,
            seed_work,
            reader_uid is not None,
        ),
        encoding="utf-8",
    )
    init_path.chmod(0o700)
    started = time.monotonic()
    status, stdout, stderr, returncode = "error", "", "", -1
    timed_out = False
    interrupted_requested = False
    try:
        proc = subprocess.Popen(
            [
                shutil.which("unshare") or "unshare",
                "--user",
                "--map-root-user",
                "--net",
                "--mount",
                "--pid",
                "--fork",
                "--kill-child",
                "/bin/sh",
                str(init_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            env=_build_env(mounts, sqlite_datasets=sqlite_datasets),
            preexec_fn=_preexec(limits),
        )
        stdout_head: list[bytes] = []
        stdout_tail: list[bytes] = []
        stdout_total = [0]
        stderr_tail: list[bytes] = []
        stderr_total = [0]
        stdout_reader = threading.Thread(
            target=_drain_stream,
            args=(
                proc.stdout,
                stdout_head,
                stdout_tail,
                stdout_total,
                int(STDOUT_CAP * 0.4),
                STDOUT_CAP - int(STDOUT_CAP * 0.4),
            ),
            daemon=True,
        )
        stderr_reader = threading.Thread(
            target=_drain_stream,
            args=(proc.stderr, [], stderr_tail, stderr_total, 0, STDERR_CAP),
            daemon=True,
        )
        stdout_reader.start()
        stderr_reader.start()
        deadline = time.monotonic() + wall
        activity_state = {"last_touch": started, "start": started}
        while proc.poll() is None:
            try:
                from tools.interrupt import is_interrupted

                interrupted = is_interrupted()
            except Exception:
                interrupted = False
            if interrupted or time.monotonic() >= deadline:
                timed_out = not interrupted
                interrupted_requested = interrupted
                _kill_group(proc)
                break
            try:
                from tools.environments.base import touch_activity_if_due

                touch_activity_if_due(activity_state, "python_sandbox running")
            except Exception:
                pass
            time.sleep(0.1)
        proc.wait(timeout=5)
        stdout_reader.join(timeout=3)
        stderr_reader.join(timeout=3)
        stdout = _assemble_drain(
            stdout_head, stdout_tail, stdout_total[0], STDOUT_CAP
        )
        stderr = _assemble_drain([], stderr_tail, stderr_total[0], STDERR_CAP)
        returncode = proc.returncode if proc.returncode is not None else -1
        if timed_out:
            status = "timeout"
        elif returncode == 0:
            status = "success"
        elif interrupted_requested:
            status = "error"
        elif _cpu_limit_exhausted(returncode, stderr):
            status = "error"
            stderr += (
                f"\nCPU limit ({limits['cpu_seconds']}s) exhausted — "
                "simplify the algorithm"
            )
        elif returncode in (-getattr(signal, "SIGKILL", signal.SIGTERM), 137):
            status = "oom"
            stderr += (
                f"\nmemory limit ({limits['memory_mb']}MB) exhausted — "
                "stream/chunk instead of loading everything"
            )
        else:
            status = "error"
    except Exception as exc:
        stderr = f"sandbox launch failed: {exc}"

    duration = round(time.monotonic() - started, 3)
    export_complete = work / _EXPORT_COMPLETE
    publication_error: SandboxReaderPolicyError | None = None
    publication: dict[str, Any] = {"renamed": {}, "unpublished": [], "kept": []}
    if export_complete.exists():
        export_complete.unlink()
        if workspace is not None:
            try:
                with _workspace_lock(session_id, workspace):
                    publication = _publish_workspace(
                        work,
                        workspace / "work",
                        seed_base,
                        run_id=run_id,
                        hermes_home=hermes_home,
                        reader_uid=reader_uid,
                    )
                collector = _BOUND_PUBLISHED.get()
                if collector is not None:
                    collector.update(publication.get("produced") or [])
            except SandboxReaderPolicyError as exc:
                logger.exception("Could not apply sandbox reader ACL before publication")
                publication_error = exc
    if publication_error is None:
        payload, harvest_error = _harvest(
            work,
            stdout,
            stderr,
            status,
            limits,
            run_id=run_id,
            artifact_url_base=config.get("artifact_url_base"),
            media_retention=config.get("media_retention"),
        )
    else:
        payload = {
            "status": "error",
            "error": str(publication_error),
            "result": None,
        }
        harvest_error = None
    payload.update(
        {
            "datasets_attached": datasets,
            "duration_seconds": duration,
            "run_id": run_id,
            "exit_code": returncode,
        }
    )
    if timed_out:
        payload["error"] = (
            f"killed at {wall}s — reduce work or raise timeout_seconds "
            f"(max {limits['max_wall_seconds']})"
        )
    elif interrupted_requested:
        payload["error"] = "sandbox execution interrupted"
    elif harvest_error:
        payload["error"] = harvest_error
    elif payload["status"] != "success":
        payload["error"] = (
            payload.get("stderr") or "sandbox process failed"
        ).strip().splitlines()[-1]
    renamed = publication.get("renamed") or {}
    if renamed:
        for item in payload.get("files") or []:
            relative = str(item.get("path") or "")[len("work/"):]
            if relative in renamed:
                item["path"] = f"work/{renamed[relative]}"
        payload["workspace_renamed"] = [
            {"written": f"work/{written}", "saved_as": f"work/{saved}"}
            for written, saved in sorted(renamed.items())
        ]
        payload["workspace_note"] = (
            "Another task using this folder changed a file with the same "
            "name while this run executed, or the folder already holds a "
            "different kind of entry at that name. The existing entry keeps "
            "its name; this run's file was saved as saved_as. Use "
            "files[].path exactly."
        )
    if publication.get("unpublished"):
        payload["workspace_unpublished"] = [
            f"work/{relative}" for relative in publication["unpublished"]
        ]
    if publication.get("kept"):
        payload["workspace_kept"] = [
            f"work/{relative}" for relative in publication["kept"]
        ]
        payload["workspace_kept_note"] = (
            "This run replaced these files with a directory, link or special "
            "file, or its copy could not be read. The retained folder "
            "keeps the previous file under the same name."
        )
    payload = _sanitize_value(payload)
    meta = {
        "run_id": run_id,
        "status": payload["status"],
        "duration_seconds": duration,
        "datasets": datasets,
        "limits": {**limits, "wall_seconds": wall},
        "exit_code": returncode,
    }
    (run / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    shutil.rmtree(inputs, ignore_errors=True)
    shutil.rmtree(run / "jail", ignore_errors=True)
    shutil.rmtree(run / "seed", ignore_errors=True)
    _prune_runs(root, config)
    if workspace is not None and not agent_mode:
        _prune_workspaces(workspace_root, config, exclude=workspace)
    return json.dumps(payload, ensure_ascii=False)


def _handle_python_sandbox(
    args: dict,
    *,
    session_id: str | None = None,
    **_: Any,
) -> str:
    available, reason = _probe(force=True)
    if not available:
        return _unavailable(reason)
    workspace_owner = current_workspace_owner(session_id)
    return python_sandbox(
        args.get("code", ""),
        args.get("datasets"),
        args.get("input_json"),
        args.get("timeout_seconds"),
        session_id=workspace_owner,
    )


_BASE_DESCRIPTION = (
    "Run Python offline in a locked sandbox on this machine — no network, "
    "no shell, read-only path datasets, one scratch directory (/work). Depending "
    "on deployment config, /work is run-scoped or persists across runs in this "
    "chat session. SQLite "
    "datasets are writable copies at their SANDBOX_INPUTS path under /work. Use it for "
    "batch computation the chat should not do item-by-item: comparing or "
    "reconciling lists across sources, counting or deduplicating more than "
    "~50 items, sums/statistics, and parsing spreadsheets or CSVs. Aggregate "
    "in code. Print only counts, totals, and up to ~20 examples. Write the "
    "structured answer to RESULT_PATH as JSON (8KB cap); keep large detail "
    "in /work files. files[].path is sandbox-local and may be attached only "
    "where the active sender supports it. When present, files[].media_ref is "
    "a retained attachment reference and client_url is its secondary shareable link."
)

PYTHON_SANDBOX_SCHEMA = {
    "name": "python_sandbox",
    "description": _BASE_DESCRIPTION,
    "parameters": {
        "type": "object",
        "properties": {
            "code": {
                "type": "string",
                "description": (
                    "Python 3 source. Dataset paths are in "
                    "os.environ['SANDBOX_INPUTS'] as JSON. Write final "
                    "structured JSON to os.environ['RESULT_PATH'] and print "
                    "a short summary."
                ),
            },
            "datasets": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Names of datasets to attach. Only attached datasets "
                    "exist in the sandbox."
                ),
            },
            "input_json": {
                "type": "object",
                "description": (
                    "Optional inline input (<=32KB), available at "
                    "/inputs/params.json."
                ),
            },
            "timeout_seconds": {
                "type": "integer",
                "minimum": 5,
                "maximum": 300,
                "description": "Wall-clock limit. Default 120.",
            },
        },
        "required": ["code"],
    },
}


def _schema_overrides() -> dict[str, str]:
    config = _load_config()
    datasets = _dataset_config(config)
    listing = (
        "; ".join(
            f"{name} — {spec.get('description', 'read-only local dataset')}"
            for name, spec in sorted(datasets.items())
        )
        or "none configured"
    )
    libraries = [
        name
        for name in ("numpy", "pandas", "openpyxl")
        if importlib.util.find_spec(name) is not None
    ]
    library_text = f" plus {', '.join(libraries)}." if libraries else "."
    workspace_text = (
        "\n\n/work in this deployment is one persistent folder shared by all "
        "your conversations, scheduled jobs and helpers, and it is kept when "
        "the model changes. Files people send you are saved under "
        "/work/received/, files you send or a scheduled job delivers that were "
        "not already in /work are saved under /work/sent/, and "
        f"/work/{FILES_LOG_NAME} has one line per file with its time, "
        "direction, chat and exact path. Check it for the newest version of "
        "a file before reworking an older one."
        if agent_workspace_enabled(config)
        else ""
    )
    return {
        "description": (
            f"{_BASE_DESCRIPTION}{workspace_text}\n\nDatasets (pass names in datasets): "
            f"{listing}. SQLite datasets are point-in-time read-only "
            f"snapshots. Libraries: Python stdlib{library_text}"
        )
    }


registry.register(
    name="python_sandbox",
    toolset="python-sandbox",
    schema=PYTHON_SANDBOX_SCHEMA,
    handler=_handle_python_sandbox,
    check_fn=check_sandbox_available,
    emoji="🧮",
    max_result_size_chars=40_000,
    dynamic_schema_overrides=_schema_overrides,
)
