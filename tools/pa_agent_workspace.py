"""Host-side records of Christopher's one agent workspace (PA-203 round 2).

With ``python_sandbox.workspace: agent`` the sandbox writes the agent folder's
``work/`` directly.  Anything that must stay true about files lives beside it,
where sandbox code cannot write:

``records/``  (mounted read-only in the jail at ``/records``)
    ``received/``        canonical copies of files people sent
    ``sent/``            canonical copies of files that were sent, exactly as sent
    ``files-log.jsonl``  one line per file received, sent or delivered

``private/``  (never mounted)
    ``objects/<sha256>`` content store: every version a run produced or a
                         delivery captured
    ``index.json``       last known good state of ``work/`` (path, signature, sha)
    ``runs.jsonl``       which run context produced which path at which sha
    ``captures.jsonl``   which context asked to send which path at which sha

Every host read of a ``work/`` file opens it component by component with
``O_NOFOLLOW`` and checks it is a regular file, so a link swapped in by sandbox
code is never followed.  Walks of ``work/`` repair directory modes first and
fail closed: anything that cannot be inspected is an error, never a skip.
"""

from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import logging
import os
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

try:
    import fcntl
except ImportError:  # pragma: no cover - Linux/macOS only
    fcntl = None

logger = logging.getLogger(__name__)

RECORDS_DIR = "records"
PRIVATE_DIR = "private"
FILES_LOG_NAME = "files-log.jsonl"
QUARANTINE_FILE = ".quarantined"
_RECORD_LOCKS: dict[str, threading.Lock] = {}
_RECORD_LOCKS_GUARD = threading.Lock()


class WorkspaceInspectionError(RuntimeError):
    """Part of the workspace could not be inspected; fail closed."""


# Paths ------------------------------------------------------------------------


def agent_dir(config: Mapping[str, Any] | None = None) -> Path:
    from hermes_constants import get_hermes_home
    from tools.python_sandbox_tool import AGENT_WORKSPACE_OWNER, _workspace_key

    return (
        get_hermes_home()
        / "sandbox_workspaces"
        / _workspace_key(AGENT_WORKSPACE_OWNER, {"workspace": "agent"})
    )


def records_dir(base: Path | None = None) -> Path:
    return (base or agent_dir()) / RECORDS_DIR


def private_dir(base: Path | None = None) -> Path:
    return (base or agent_dir()) / PRIVATE_DIR


def ensure_layout(base: Path) -> None:
    for path in (
        base / "work",
        base / RECORDS_DIR / "received",
        base / RECORDS_DIR / "sent",
        base / PRIVATE_DIR / "objects",
    ):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)


def split_reference(raw_path: Any) -> tuple[str, str]:
    """``("work"|"records", relative)`` for a sandbox-visible reference."""
    text = str(raw_path or "").strip()
    for prefix, area in (
        ("/work/", "work"),
        ("work/", "work"),
        ("/records/", "records"),
        ("records/", "records"),
    ):
        if text.startswith(prefix):
            relative = text[len(prefix):]
            parts = relative.split("/")
            if not relative or any(part in {"", ".", ".."} for part in parts):
                raise ValueError("sandbox path is invalid")
            return area, relative
    raise ValueError("attachment is not a sandbox work or records path")


# Locks --------------------------------------------------------------------------


@contextmanager
def records_lock(base: Path) -> Iterator[None]:
    """Short lock for the host-only records; never held during a sandbox run."""
    private = private_dir(base)
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = str(private)
    with _RECORD_LOCKS_GUARD:
        thread_lock = _RECORD_LOCKS.setdefault(key, threading.Lock())
    with thread_lock:
        if fcntl is None:  # pragma: no cover
            yield
            return
        fd = os.open(private / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)


# Confined reads ------------------------------------------------------------------


def open_confined(root: Path, relative: str) -> int:
    """Open ``root/relative`` for reading without following any link.

    Every directory is opened with ``O_DIRECTORY|O_NOFOLLOW`` relative to its
    parent's descriptor and the leaf with ``O_NOFOLLOW|O_NONBLOCK``; the leaf
    must be a regular file.  Returns an open descriptor the caller closes.
    """
    parts = relative.split("/")
    if not relative or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("sandbox path is invalid")
    flags_dir = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        directory = os.open(root, flags_dir)
    except OSError as exc:
        raise ValueError("sandbox folder is unavailable") from exc
    try:
        for part in parts[:-1]:
            try:
                child = os.open(part, flags_dir, dir_fd=directory)
            except OSError as exc:
                raise ValueError("sandbox path is unavailable or contains a link") from exc
            os.close(directory)
            directory = child
        try:
            fd = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory,
            )
        except OSError as exc:
            raise ValueError("sandbox file is unavailable or is a link") from exc
    finally:
        os.close(directory)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise ValueError("sandbox path is not a regular file")
    return fd


def _signature(metadata: os.stat_result) -> list[int]:
    return [metadata.st_size, metadata.st_mtime_ns, metadata.st_ino]


def store_object(fd: int, base: Path) -> str:
    """Copy an open file into the content store; return its sha256."""
    objects = private_dir(base) / "objects"
    objects.mkdir(parents=True, exist_ok=True, mode=0o700)
    staged = objects / f".tmp-{uuid.uuid4().hex}"
    digest = hashlib.sha256()
    os.lseek(fd, 0, os.SEEK_SET)
    with open(staged, "xb") as out:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            out.write(chunk)
    sha = digest.hexdigest()
    target = objects / sha
    if target.exists():
        staged.unlink()
    else:
        os.replace(staged, target)
    return sha


def object_path(base: Path, sha: str, *, verify: bool = True) -> Path:
    path = private_dir(base) / "objects" / sha
    if not path.is_file() or path.is_symlink():
        raise ValueError("the recorded version of that file is unavailable")
    if verify and _file_sha(path) != sha:
        raise ValueError("the recorded version of that file failed its digest check")
    return path


def _file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    finally:
        os.close(fd)
    return digest.hexdigest()


def snapshot_work_file(base: Path, relative: str) -> str:
    fd = open_confined(base / "work", relative)
    try:
        return store_object(fd, base)
    finally:
        os.close(fd)


# Fail-closed walk of work/ --------------------------------------------------------


def inspect_work(work: Path, *, strip: bool) -> tuple[dict[str, list[int]], int, list[str]]:
    """Walk ``work`` so nothing can be hidden from it.

    Every directory is opened with ``O_DIRECTORY|O_NOFOLLOW`` and set to 0700
    through its descriptor before it is listed, which the owner can always
    do, so ``chmod 0300`` cannot hide its contents and a swapped-in link is
    never followed.  With ``strip``, symlinks and special files are removed
    and a regular file with more than one hard link is replaced by a private
    copy.  Returns the signature of each regular file, the total bytes, and
    the removed paths.  Anything that cannot be inspected raises
    :class:`WorkspaceInspectionError`.
    """
    files: dict[str, list[int]] = {}
    removed: list[str] = []
    total = 0
    dir_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

    def visit(fd: int, prefix: str) -> None:
        nonlocal total
        try:
            os.fchmod(fd, 0o700)
            names = os.listdir(fd)
        except OSError as exc:
            raise WorkspaceInspectionError(f"cannot list {prefix or '.'}: {exc}") from exc
        for name in names:
            relative = f"{prefix}{name}"
            try:
                metadata = os.stat(name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise WorkspaceInspectionError(f"cannot inspect {relative}: {exc}") from exc
            mode = metadata.st_mode
            if stat.S_ISDIR(mode):
                try:
                    _repair_directory_mode(fd, name)
                    child = os.open(name, dir_flags, dir_fd=fd)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise WorkspaceInspectionError(f"cannot open {relative}: {exc}") from exc
                try:
                    visit(child, relative + "/")
                finally:
                    os.close(child)
            elif stat.S_ISREG(mode):
                if strip:
                    try:
                        metadata = _normalise_file(fd, name, metadata)
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        raise WorkspaceInspectionError(f"cannot repair {relative}: {exc}") from exc
                files[relative] = _signature(metadata)
                total += metadata.st_size
            elif strip:
                try:
                    os.unlink(name, dir_fd=fd)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise WorkspaceInspectionError(f"cannot remove {relative}: {exc}") from exc
                removed.append(relative)

    try:
        parent = os.open(work.parent, dir_flags)
        try:
            _repair_directory_mode(parent, work.name)
        finally:
            os.close(parent)
        root = os.open(work, dir_flags)
    except FileNotFoundError:
        return files, 0, removed
    except OSError as exc:
        raise WorkspaceInspectionError(f"cannot open the work folder: {exc}") from exc
    try:
        visit(root, "")
    finally:
        os.close(root)
    return files, total, sorted(removed)


def _repair_directory_mode(parent_fd: int, name: str) -> None:
    """Set a directory to 0700 before opening it, never following a link.

    A directory set to 0300 or 0100 cannot be opened for listing, so its mode
    is repaired first: with ``lchmod`` semantics where the platform has them,
    otherwise through an ``O_PATH`` descriptor, which needs no read access.
    """
    if os.chmod in os.supports_follow_symlinks and os.chmod in os.supports_dir_fd:
        try:
            os.chmod(name, 0o700, dir_fd=parent_fd, follow_symlinks=False)
            return
        except NotImplementedError:
            pass
        except OSError as exc:
            if exc.errno not in (errno.ENOTSUP, errno.EOPNOTSUPP):
                raise
    if not hasattr(os, "O_PATH"):
        raise OSError(errno.ENOTSUP, "cannot repair a directory mode safely here")
    handle = os.open(
        name, os.O_PATH | os.O_NOFOLLOW | os.O_DIRECTORY, dir_fd=parent_fd
    )
    try:
        os.chmod(f"/proc/self/fd/{handle}", 0o700)
    finally:
        os.close(handle)


def _normalise_file(dir_fd: int, name: str, metadata: os.stat_result) -> os.stat_result:
    """Mode 0600, one link; returns the file's metadata afterwards."""
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0), dir_fd=dir_fd)
    try:
        current = os.fstat(fd)
        if not stat.S_ISREG(current.st_mode):
            raise OSError("entry changed type during inspection")
        if current.st_nlink > 1:
            staged = f".{name}.{uuid.uuid4().hex}.unlink"
            out = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=dir_fd)
            try:
                while True:
                    chunk = os.read(fd, 1024 * 1024)
                    if not chunk:
                        break
                    os.write(out, chunk)
            finally:
                os.close(out)
            os.replace(staged, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        if (current.st_mode & 0o7777) != 0o600:
            os.fchmod(fd, 0o600)
            current = os.fstat(fd)
        return current
    finally:
        os.close(fd)


def quarantine(base: Path, reason: str) -> None:
    private_dir(base).mkdir(parents=True, exist_ok=True, mode=0o700)
    (base / PRIVATE_DIR / QUARANTINE_FILE).write_text(
        json.dumps({"at": _now(), "reason": reason}), encoding="utf-8"
    )
    logger.error("Agent workspace quarantined: %s", reason)


def quarantined(base: Path) -> str | None:
    path = base / PRIVATE_DIR / QUARANTINE_FILE
    if not path.exists():
        return None
    try:
        return str(json.loads(path.read_text(encoding="utf-8")).get("reason") or "unknown")
    except (OSError, ValueError):
        return "unknown"


# Index, run ledger and captures --------------------------------------------------


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_index(base: Path) -> dict[str, dict[str, Any]]:
    try:
        data = json.loads((private_dir(base) / "index.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_index(base: Path, index: Mapping[str, Any]) -> None:
    private = private_dir(base)
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    staged = private / f".index.{uuid.uuid4().hex}.tmp"
    staged.write_text(json.dumps(index, sort_keys=True), encoding="utf-8")
    os.replace(staged, private / "index.json")


def _append(base: Path, name: str, entry: Mapping[str, Any]) -> None:
    private = private_dir(base)
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(
        private / name, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(entry), ensure_ascii=False, sort_keys=True) + "\n")


def _entries(base: Path, name: str) -> list[dict[str, Any]]:
    path = private_dir(base) / name
    if not path.is_file() or path.is_symlink():
        return []
    found = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                found.append(entry)
    return found


def sync_index(base: Path, files: Mapping[str, list[int]]) -> dict[str, dict[str, Any]]:
    """Bring the index up to the folder's current state before a run.

    A file whose signature changed since the index last saw it (a host write,
    a merge) is stored as an object, so a killed run can always restore it.
    """
    index = load_index(base)
    for relative in list(index):
        if relative not in files:
            del index[relative]
    for relative, signature in files.items():
        known = index.get(relative)
        if known and known.get("sig") == signature:
            continue
        try:
            sha = snapshot_work_file(base, relative)
        except ValueError:
            index.pop(relative, None)
            continue
        index[relative] = {"sig": signature, "sha": sha}
    save_index(base, index)
    return index


def record_run_outputs(
    base: Path,
    *,
    run_id: str,
    context: str,
    before: Mapping[str, Mapping[str, Any]],
    after: Mapping[str, list[int]],
) -> tuple[list[str], dict[str, str]]:
    """Store what a completed run created or changed; return paths and shas."""
    changed = sorted(
        relative
        for relative, signature in after.items()
        if (before.get(relative) or {}).get("sig") != signature
    )
    index = {relative: dict(value) for relative, value in before.items() if relative in after}
    shas: dict[str, str] = {}
    for relative in changed:
        try:
            sha = snapshot_work_file(base, relative)
        except ValueError:
            continue
        shas[relative] = sha
        index[relative] = {"sig": after[relative], "sha": sha}
        _append(
            base,
            "runs.jsonl",
            {"at": _now(), "run_id": run_id, "context": context, "path": relative, "sha256": sha},
        )
    save_index(base, index)
    return [relative for relative in changed if relative in shas], shas


def roll_back_run(
    base: Path,
    *,
    run_id: str,
    before: Mapping[str, Mapping[str, Any]],
    after: Mapping[str, list[int]],
) -> dict[str, list[str]]:
    """Undo a run that was stopped before it finished.

    Files it changed or deleted get their last known good version back from
    the content store; files it created are kept aside as
    ``<name>.incomplete-<run_id>`` so no half-written file sits at a real name.
    """
    work = base / "work"
    restored: list[str] = []
    set_aside: list[str] = []
    for relative, known in before.items():
        if after.get(relative) == known.get("sig"):
            continue
        try:
            source = object_path(base, str(known.get("sha")))
        except ValueError:
            continue
        target = work / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        staged = target.with_name(f".{target.name}.{uuid.uuid4().hex}.restore")
        with source.open("rb") as src, open(staged, "xb") as out:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        if target.is_dir() and not target.is_symlink():
            aside = target.with_name(f"{target.name}.incomplete-{run_id}")
            os.replace(target, aside)
            set_aside.append(aside.relative_to(work).as_posix())
        os.replace(staged, target)
        restored.append(relative)
    for relative in sorted(set(after) - set(before)):
        path = work / relative
        aside = path.with_name(f"{path.name}.incomplete-{run_id}")
        with contextlib.suppress(FileNotFoundError):
            os.replace(path, aside)
            set_aside.append(aside.relative_to(work).as_posix())
    files, _total, _removed = inspect_work(work, strip=True)
    sync_index(base, files)
    return {"restored": sorted(restored), "set_aside": sorted(set_aside)}


def _latest(base: Path, name: str, context: str, relative: str) -> str | None:
    found = None
    for entry in _entries(base, name):
        if entry.get("context") == context and entry.get("path") == relative:
            found = str(entry.get("sha256") or "") or found
    return found


def _latest_version(base: Path, context: str, relative: str) -> str | None:
    """The newest version this context produced or captured at ``relative``.

    Runs and captures are compared by time; on a tie the capture wins,
    because a capture follows the run that made the file.
    """
    best: tuple[str, int, str] | None = None
    for rank, name in ((0, "runs.jsonl"), (1, "captures.jsonl")):
        for entry in _entries(base, name):
            if entry.get("context") != context or entry.get("path") != relative:
                continue
            sha = str(entry.get("sha256") or "")
            if not sha:
                continue
            key = (str(entry.get("at") or ""), rank, sha)
            if best is None or key[:2] >= best[:2]:
                best = key
    return best[2] if best else None


def _captured_shas(base: Path, context: str) -> set[str]:
    return {
        str(entry.get("sha256"))
        for entry in _entries(base, "captures.jsonl")
        if entry.get("context") == context and entry.get("sha256")
    }


# Capture and delivery ------------------------------------------------------------


def capture_work_file(raw_path: Any, context: str) -> tuple[Path, str]:
    """Fix the version of a file at the moment a send is requested.

    The version is the one this context's own runs last produced at that
    path, when the path was since overwritten by another context; otherwise
    the file as it is now.  It is stored and recorded against the context.
    Returns a host path holding exactly that version, and its sha256.
    """
    base = agent_dir()
    ensure_layout(base)
    area, relative = split_reference(raw_path)
    if area == "records":
        fd = open_confined(records_dir(base), relative)
        try:
            sha = store_object(fd, base)
        finally:
            os.close(fd)
        return records_dir(base) / relative, sha
    try:
        current = snapshot_work_file(base, relative)
    except ValueError:
        current = None
    own = _latest(base, "runs.jsonl", context, relative) if context else None
    chosen = own if own and own != current and _object_exists(base, own) else current
    if chosen is None:
        raise ValueError("sandbox work file is unavailable")
    with records_lock(base):
        _append(
            base,
            "captures.jsonl",
            {"at": _now(), "context": context, "path": relative, "sha256": chosen},
        )
    if chosen == current:
        return base / "work" / relative, chosen
    # This context's own version was overwritten: hand back that version.
    materialised = Path(".captured") / uuid.uuid4().hex / Path(relative).name
    target = base / "work" / materialised
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with object_path(base, chosen).open("rb") as src, open(target, "xb") as out:
        out.write(src.read())
    with records_lock(base):
        _append(
            base,
            "captures.jsonl",
            {"at": _now(), "context": context, "path": materialised.as_posix(), "sha256": chosen},
        )
    return target, chosen


def _object_exists(base: Path, sha: str) -> bool:
    path = private_dir(base) / "objects" / sha
    return path.is_file() and not path.is_symlink()


def delivery_snapshot(
    raw_path: Any, context: str, *, any_file_allowed: bool
) -> tuple[Path, str]:
    """Return an immutable copy under ``records/sent/`` to send, and its sha256.

    A ``work/`` path is sent at the version this context captured or last
    produced there, never at whatever another context left at that path.  A
    file this context neither made nor captured is sent only when
    ``any_file_allowed``.  The copy's digest is checked before it is returned.
    """
    base = agent_dir()
    ensure_layout(base)
    area, relative = split_reference(raw_path)
    if area == "records":
        fd = open_confined(records_dir(base), relative)
        try:
            sha = store_object(fd, base)
        finally:
            os.close(fd)
        if relative.startswith("sent/"):
            return records_dir(base) / relative, sha
        source_name = Path(relative).name
    else:
        expected = _latest_version(base, context, relative) if context else None
        if expected:
            sha = expected
        else:
            try:
                sha = snapshot_work_file(base, relative)
            except ValueError as exc:
                raise ValueError(str(exc)) from exc
            if not any_file_allowed and sha not in _captured_shas(base, context):
                raise ValueError(
                    "this conversation or job did not make or capture that file; "
                    "it may attach only files it made"
                )
        source_name = Path(relative).name
    source = object_path(base, sha)
    with records_lock(base):
        target = _keep_named(records_dir(base) / "sent", source_name, source, sha)
    if _file_sha(target) != sha:
        raise ValueError("the file to send failed its digest check")
    _grant_reader(base)
    return target, sha


def _keep_named(folder: Path, name: str, source: Path, sha: str) -> Path:
    """Place ``source`` in ``folder`` as ``name``, keeping both on a clash."""
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    stem, dot, suffix = name.rpartition(".")
    if not dot:
        stem, suffix = name, ""
    for index in range(1000):
        candidate_name = name if index == 0 else (
            f"{stem}.{sha[:8]}-{index}.{suffix}" if suffix else f"{stem}.{sha[:8]}-{index}"
        )
        candidate = folder / candidate_name
        if os.path.lexists(candidate):
            if candidate.is_file() and not candidate.is_symlink() and _file_sha(candidate) == sha:
                return candidate
            continue
        staged = folder / f".{candidate_name}.{uuid.uuid4().hex}.tmp"
        with source.open("rb") as src, open(staged, "xb") as out:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        os.chmod(staged, 0o600)
        if os.path.lexists(candidate):
            staged.unlink()
            continue
        os.replace(staged, candidate)
        return candidate
    raise RuntimeError("could not choose a distinct records name")


def _grant_reader(base: Path) -> None:
    from hermes_constants import get_hermes_home
    from tools.python_sandbox_access import configured_reader_uid, grant_reader_work_access
    from tools.python_sandbox_tool import _load_config

    reader_uid = configured_reader_uid(_load_config())
    if reader_uid is not None:
        grant_reader_work_access(get_hermes_home(), records_dir(base), reader_uid)


# Received and sent files, and the files log --------------------------------------


def keep_file(
    source: str | Path,
    *,
    direction: str,
    folder: str,
    name: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> str:
    """Keep one file that came in or went out, and log it.

    The canonical copy goes to ``records/<folder>/`` (read-only to sandbox
    code); a file already under ``records/`` is not copied again.  Returns the
    ``records/...`` path.  Takes only the short records lock.
    """
    import re

    base = agent_dir()
    ensure_layout(base)
    records = records_dir(base)
    source_path = Path(source)
    if source_path.is_symlink() or not source_path.is_file():
        raise ValueError("workspace import source is not a regular file")
    fd = os.open(source_path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("workspace import source is not a regular file")
        sha = store_object(fd, base)
    finally:
        os.close(fd)
    display = re.sub(r"[^\w.\- ]", "_", Path(str(name or source_path.name)).name).strip(" .") or "file"
    with records_lock(base):
        resolved = source_path.resolve()
        if resolved.is_relative_to(records.resolve()):
            relative = resolved.relative_to(records.resolve()).as_posix()
        else:
            target = _keep_named(records / folder.strip("/"), display, object_path(base, sha), sha)
            relative = target.relative_to(records).as_posix()
        entry = {
            "at": _now(),
            "direction": direction,
            "path": f"records/{relative}",
            "name": display,
            "sha256": sha,
            **{key: value for key, value in dict(details or {}).items() if value},
        }
        log = records / FILES_LOG_NAME
        fd = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    _grant_reader(base)
    return f"records/{relative}"


def files_log_entries() -> list[dict[str, Any]]:
    log = records_dir() / FILES_LOG_NAME
    try:
        fd = os.open(log, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0))
    except OSError:
        return []
    with os.fdopen(fd, encoding="utf-8") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            return []
        found = []
        for line in handle:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if isinstance(entry, dict):
                found.append(entry)
        return found
