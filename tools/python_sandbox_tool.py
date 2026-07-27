"""Offline, kernel-jailed Python over configured read-only datasets."""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import platform
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
from pathlib import Path
from typing import Any, Mapping

try:
    import resource
except ImportError:  # pragma: no cover - Windows availability is fail-closed
    resource = None

from hermes_constants import get_hermes_home
from tools.path_security import validate_within_dir
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

_SUPERVISOR_SOURCE = r"""\
import ctypes
import os
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
        if os.path.islink(path):
            os.unlink(path)

subprocess.run(["mount", "-o", "remount,bind,rw", "/export"], check=True)
subprocess.run(["cp", "-a", "/work/.", "/export/"], check=True)
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
    max_mb = _limits(config)["max_snapshot_mb"]
    for name in names:
        if not name or any(
            char
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for char in name
        ):
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
        '"$JAIL/proc" "$JAIL/.oldroot"',
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
    for name, source in mounts.items():
        target = f'"$JAIL/inputs/{name}"'
        lines.append(f"mkdir -p {target}" if source.is_dir() else f": > {target}")
        lines.append(
            ("ro_dir" if source.is_dir() else "ro_file")
            + f" {_q(source)} {target}"
        )
    lines.extend(
        [
            f'mount -t tmpfs -o size={int(scratch_mb)}m,nosuid,nodev,noexec '
            'tmpfs "$JAIL/work"',
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
            f"{int(cpu_seconds)} {int(max_processes)}",
            "",
        ]
    )
    return "\n".join(lines)


def _build_env(
    mounts: Mapping[str, Path], source_env: Mapping[str, str] | None = None
) -> dict[str, str]:
    source = dict(source_env or os.environ)
    # Do not inherit execute_code's configurable passthrough allowlist here:
    # this boundary is deliberately fixed and credential-blind.
    env = {
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "SANDBOX_INPUTS": json.dumps(
            {name: f"/inputs/{name}" for name in mounts}, sort_keys=True
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
    return f"{head}\n\n... [{omitted:,} chars omitted] ...\n\n{tail}", True


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


def _list_files(work: Path) -> list[dict[str, Any]]:
    files = []
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
            "files": _list_files(work),
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
) -> str:
    config = _load_config()
    available, reason = _probe(force=True)
    if not available:
        return _unavailable(reason)
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

    run_id = f"r_{uuid.uuid4().hex[:8]}"
    root = get_hermes_home() / "sandbox_runs"
    run = root / run_id
    inputs, work = run / "inputs", run / "work"
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

    limits = _limits(config)
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
            env=_build_env(mounts),
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
    payload, harvest_error = _harvest(work, stdout, stderr, status, limits)
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
    _prune_runs(root, config)
    return json.dumps(payload, ensure_ascii=False)


def _handle_python_sandbox(args: dict, **_: Any) -> str:
    available, reason = _probe(force=True)
    if not available:
        return _unavailable(reason)
    return python_sandbox(
        args.get("code", ""),
        args.get("datasets"),
        args.get("input_json"),
        args.get("timeout_seconds"),
    )


_BASE_DESCRIPTION = (
    "Run Python offline in a locked sandbox on this machine — no network, "
    "no shell, read-only data, one scratch directory (/work). Use it for "
    "batch computation the chat should not do item-by-item: comparing or "
    "reconciling lists across sources, counting or deduplicating more than "
    "~50 items, sums/statistics, and parsing spreadsheets or CSVs. Aggregate "
    "in code. Print only counts, totals, and up to ~20 examples. Write the "
    "structured answer to RESULT_PATH as JSON (8KB cap); keep large detail "
    "in /work files."
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
    return {
        "description": (
            f"{_BASE_DESCRIPTION}\n\nDatasets (pass names in datasets): "
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
