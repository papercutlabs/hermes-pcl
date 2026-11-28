"""Optional named-reader ACLs for retained Python sandbox work outputs.

The policy is deliberately narrower than ordinary file sharing.  A configured
reader may traverse ``HERMES_HOME`` and the retained workspace ancestors, then
read the contents of ``sandbox_workspaces/<owner>/work``.  No sandbox run,
configuration, session, credential, or other HERMES_HOME path is granted.
"""

from __future__ import annotations

import os
import platform
import re
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any, Mapping

try:
    import pwd
except ImportError:  # pragma: no cover - unavailable on Windows
    pwd = None


class SandboxReaderPolicyError(RuntimeError):
    """The configured sandbox reader policy could not be applied safely."""


_ACCOUNT_NAME_RE = re.compile(r"^[a-z_][a-z0-9_.-]*[$]?$", re.IGNORECASE)
_WORKSPACE_NAME_RE = re.compile(r"^s_[0-9a-f]{64}$")


def configured_reader_uid(config: Mapping[str, Any] | None) -> str | None:
    """Return the existing account UID selected by ``reader_identity``.

    Unset is the compatibility path and performs no account or ACL lookup.
    Names and decimal UIDs are accepted so deployments can bind the policy to
    their real service identity without adding a shared group.
    """
    raw = (config or {}).get("reader_identity")
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int)):
        raise SandboxReaderPolicyError(
            "python_sandbox.reader_identity must be an account name or decimal UID"
        )
    text = str(raw)
    if not text or text != text.strip():
        raise SandboxReaderPolicyError(
            "python_sandbox.reader_identity must not be empty or contain surrounding whitespace"
        )
    if pwd is None or platform.system() != "Linux":
        raise SandboxReaderPolicyError(
            "python_sandbox.reader_identity requires Linux account ACL support"
        )
    try:
        if text.isdecimal():
            if text != "0" and text.startswith("0"):
                raise ValueError("UID must use canonical decimal form")
            account = pwd.getpwuid(int(text, 10))
        else:
            if not _ACCOUNT_NAME_RE.fullmatch(text):
                raise ValueError("account name has invalid characters")
            account = pwd.getpwnam(text)
    except (KeyError, OverflowError, ValueError) as exc:
        raise SandboxReaderPolicyError(
            "python_sandbox.reader_identity does not name an existing Linux account"
        ) from exc
    return str(account.pw_uid)


def _require_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise SandboxReaderPolicyError(f"{label} is unavailable") from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SandboxReaderPolicyError(f"{label} must be a real directory")


def _setfacl(*args: str) -> None:
    if platform.system() != "Linux":
        raise SandboxReaderPolicyError("sandbox reader ACLs require Linux")
    executable = shutil.which("setfacl")
    if not executable:
        raise SandboxReaderPolicyError("setfacl is required for sandbox reader ACLs")
    try:
        completed = subprocess.run(
            [executable, *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SandboxReaderPolicyError("sandbox reader ACL update failed") from exc
    if completed.returncode:
        raise SandboxReaderPolicyError("sandbox reader ACL update failed")


def preserve_reader_home_access(home: Path, reader_uid: str) -> None:
    """Keep home private while preserving configured reader traversal.

    This is one ACL mutation rather than ``chmod(0700)`` followed by a grant,
    so startup never temporarily masks an already-configured reader.  The
    default named-user denial makes newly created home siblings private even
    when their requested creation mode includes group or world read bits.
    """
    home = Path(home)
    _require_directory(home, "HERMES_HOME")
    _setfacl(
        "-m",
        (
            f"u::rwx,u:{reader_uid}:--x,g::---,m::--x,o::---,"
            f"d:u::rwx,d:u:{reader_uid}:---,d:g::r-x,d:m::r-x,d:o::---"
        ),
        "--",
        str(home),
    )


def _walk_confined_work_tree(work: Path) -> tuple[list[Path], list[Path]]:
    """Return real directories/files in one work tree and reject unsafe entries."""
    _require_directory(work, "sandbox work directory")
    directories = [work]
    files: list[Path] = []
    for current, dirnames, filenames in os.walk(work, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in dirnames:
            path = current_path / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                raise SandboxReaderPolicyError(
                    "sandbox work tree contains a non-directory or symlinked directory"
                )
            directories.append(path)
        for name in filenames:
            path = current_path / name
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                raise SandboxReaderPolicyError(
                    "sandbox work tree contains a symlink or non-regular file"
                )
            files.append(path)
    return directories, files


def inspect_reader_work_scope(
    home: Path, work: Path
) -> tuple[Path, Path, list[Path], list[Path]]:
    """Validate one bounded work scope without changing an ACL."""
    home = Path(home)
    work = Path(work)
    workspace = work.parent
    workspace_root = workspace.parent
    expected_root = home / "sandbox_workspaces"
    # ``records`` is the agent workspace's host-written records folder
    # (PA-203); the reader reads sent files from it.
    valid_work_name = work.name in {"work", "records"} or (
        work.name.startswith(".work-") and work.name.endswith(".tmp")
    )
    if (
        workspace_root != expected_root
        or not _WORKSPACE_NAME_RE.fullmatch(workspace.name)
        or not valid_work_name
    ):
        raise SandboxReaderPolicyError("sandbox work path is outside its workspace")
    _require_directory(home, "HERMES_HOME")
    _require_directory(workspace_root, "sandbox workspace root")
    _require_directory(workspace, "sandbox workspace")
    directories, files = _walk_confined_work_tree(work)

    # The root-owned cutover denies existing non-work siblings before it
    # enables home traversal.  Runtime only verifies that no symlink appeared;
    # it must not try to mutate root-owned private siblings on every run.
    for child in workspace_root.iterdir():
        if child.is_symlink():
            raise SandboxReaderPolicyError("sandbox workspace root contains a symlink")
    for child in workspace.iterdir():
        if child.is_symlink():
            raise SandboxReaderPolicyError("sandbox workspace contains a symlink")

    return workspace_root, workspace, directories, files


def preflight_reader_work_access(home: Path, work: Path) -> None:
    """Refuse an unsafe work tree before a cutover performs any mutation."""
    inspect_reader_work_scope(home, work)


def grant_reader_work_access(home: Path, work: Path, reader_uid: str) -> None:
    """Grant one reader traversal plus read-only access to a retained work tree."""
    workspace_root, workspace, directories, files = inspect_reader_work_scope(
        Path(home), Path(work)
    )

    # Workspace ancestors remain unlistable.  Their defaults deny newly
    # created siblings; each real work tree is explicitly opened below.
    for path in (workspace_root, workspace):
        _setfacl(
            "-m",
            f"u:{reader_uid}:--x,d:u:{reader_uid}:---",
            "--",
            str(path),
        )

    for offset in range(0, len(directories), 128):
        _setfacl(
            "-m",
            f"u:{reader_uid}:r-x,d:u:{reader_uid}:r-x",
            "--",
            *(str(path) for path in directories[offset : offset + 128]),
        )
    for offset in range(0, len(files), 128):
        _setfacl(
            "-m",
            f"u:{reader_uid}:r--",
            "--",
            *(str(path) for path in files[offset : offset + 128]),
        )
