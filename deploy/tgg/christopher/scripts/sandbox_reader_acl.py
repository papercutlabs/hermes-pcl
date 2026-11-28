#!/usr/bin/env python3
"""Prepare, apply, or roll back Christopher's bounded sandbox reader ACLs.

This helper is intentionally local-only.  It does not connect to a host,
change config, restart a service, or call a provider.  Run ``prepare`` before
installing ``python_sandbox.reader_identity``; run ``apply`` from the same
unchanged filesystem state; retain the receipt for ``rollback``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.python_sandbox_access import (  # noqa: E402
    SandboxReaderPolicyError,
    configured_reader_uid,
    grant_reader_work_access,
    inspect_reader_work_scope,
    preflight_reader_work_access,
    preserve_reader_home_access,
)


class CutoverError(RuntimeError):
    """The ACL cutover precondition or mutation failed."""


_WORKSPACE_NAME_RE = re.compile(r"^s_[0-9a-f]{64}$")


def _executable(name: str) -> str:
    value = shutil.which(name)
    if not value:
        raise CutoverError(f"{name} is required")
    return value


def _run(command: list[str], *, input_text: str | None = None) -> str:
    try:
        completed = subprocess.run(
            command,
            input=input_text,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CutoverError(f"command failed to execute: {Path(command[0]).name}") from exc
    if completed.returncode:
        raise CutoverError(f"command failed: {Path(command[0]).name}")
    return completed.stdout


def _scope(home: Path) -> tuple[list[Path], Path | None]:
    if not home.is_absolute():
        raise CutoverError("--home must be absolute")
    try:
        mode = home.lstat().st_mode
    except OSError as exc:
        raise CutoverError("HERMES_HOME is unavailable") from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise CutoverError("HERMES_HOME must be a real directory")
    children = sorted(home.iterdir(), key=lambda path: path.name)
    if any(path.is_symlink() for path in children):
        raise CutoverError("top-level HERMES_HOME symlinks are not accepted")
    workspace_root = home / "sandbox_workspaces"
    return children, workspace_root if workspace_root.is_dir() else None


def _snapshot(home: Path) -> tuple[str, list[str]]:
    getfacl = _executable("getfacl")
    children, workspace_root = _scope(home)
    parts = [_run([getfacl, "--absolute-names", "-P", "--", str(home)])]
    for path in children:
        if workspace_root is not None and path == workspace_root:
            continue
        parts.append(_run([getfacl, "--absolute-names", "-P", "--", str(path)]))
    if workspace_root is not None:
        parts.append(
            _run(
                [
                    getfacl,
                    "--absolute-names",
                    "-R",
                    "-P",
                    "--",
                    str(workspace_root),
                ]
            )
        )
    return "".join(parts), [path.name for path in children]


def _reader_uid(value: str) -> str:
    return configured_reader_uid({"reader_identity": value}) or ""


def _write_receipt(path: Path, payload: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise CutoverError("--receipt must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except Exception:
        path.unlink(missing_ok=True)
        raise


def prepare(args: argparse.Namespace) -> int:
    home = Path(args.home)
    uid = _reader_uid(args.reader)
    acl_text, children = _snapshot(home)
    payload = {
        "schema": 1,
        "home": str(home),
        "reader_uid": uid,
        "top_level_children": children,
        "acl_sha256": hashlib.sha256(acl_text.encode()).hexdigest(),
        "acl_restore": acl_text,
    }
    _write_receipt(Path(args.receipt), payload)
    print(json.dumps({"status": "prepared", "receipt": args.receipt}, sort_keys=True))
    return 0


def _load_receipt(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CutoverError("receipt is unreadable or invalid") from exc
    required = {
        "schema",
        "home",
        "reader_uid",
        "top_level_children",
        "acl_sha256",
        "acl_restore",
    }
    if not isinstance(payload, dict) or set(payload) != required or payload["schema"] != 1:
        raise CutoverError("receipt shape is invalid")
    acl_text = payload["acl_restore"]
    if not isinstance(acl_text, str) or hashlib.sha256(acl_text.encode()).hexdigest() != payload["acl_sha256"]:
        raise CutoverError("receipt ACL digest does not match")
    return payload


def _validate_receipt_scope(
    payload: dict[str, Any], *, require_unchanged_children: bool
) -> tuple[Path, str, list[Path], Path | None]:
    home = Path(payload["home"])
    children, workspace_root = _scope(home)
    if (
        require_unchanged_children
        and [path.name for path in children] != payload["top_level_children"]
    ):
        raise CutoverError("HERMES_HOME top-level scope changed after prepare")
    uid = str(payload["reader_uid"])
    if uid != _reader_uid(uid):
        raise CutoverError("receipt reader account is no longer present")
    return home, uid, children, workspace_root


def _existing_restore_text(acl_text: str) -> tuple[str, list[str]]:
    """Drop snapshot blocks whose paths disappeared after workspace replacement."""
    retained = []
    skipped = []
    for block in acl_text.strip().split("\n\n"):
        file_line = next(
            (line for line in block.splitlines() if line.startswith("# file: ")),
            None,
        )
        if file_line is None:
            raise CutoverError("receipt ACL block has no absolute file path")
        saved_path = file_line.removeprefix("# file: ")
        if Path(saved_path).exists():
            retained.append(block)
        else:
            skipped.append(saved_path)
    return "\n\n".join(retained) + ("\n" if retained else ""), skipped


def _restore_preimage(payload: dict[str, Any]) -> list[str]:
    restore_text, skipped = _existing_restore_text(payload["acl_restore"])
    if restore_text:
        _run([_executable("setfacl"), "--restore=-"], input_text=restore_text)
    return skipped


def _current_policy_paths(home: Path) -> tuple[list[Path], list[Path]]:
    """Enumerate only paths that this policy can have tagged."""
    children, workspace_root = _scope(home)
    paths = [home, *children]
    directories = [home, *(path for path in children if path.is_dir())]
    if workspace_root is not None:
        for child in sorted(workspace_root.iterdir(), key=lambda path: path.name):
            if child.is_symlink():
                raise CutoverError("sandbox workspace root contains a symlink")
            if child not in paths:
                paths.append(child)
            if child.is_dir() and child not in directories:
                directories.append(child)
            if not (child.is_dir() and _WORKSPACE_NAME_RE.fullmatch(child.name)):
                continue
            for sibling in sorted(child.iterdir(), key=lambda path: path.name):
                if sibling.is_symlink():
                    raise CutoverError("sandbox workspace contains a symlink")
                if sibling not in paths:
                    paths.append(sibling)
                if sibling.is_dir() and sibling not in directories:
                    directories.append(sibling)
                if sibling.name == "work" and sibling.is_dir():
                    _root, _workspace, work_dirs, work_files = inspect_reader_work_scope(
                        home, sibling
                    )
                    for work_path in [*work_dirs, *work_files]:
                        if work_path not in paths:
                            paths.append(work_path)
                    for work_dir in work_dirs:
                        if work_dir not in directories:
                            directories.append(work_dir)
    return paths, directories


def _remove_current_reader_entries(home: Path, uid: str) -> None:
    setfacl = _executable("setfacl")
    getfacl = _executable("getfacl")
    paths, directories = _current_policy_paths(home)
    directory_set = set(directories)
    access_paths = []
    default_paths = []
    access_marker = f"user:{uid}:"
    default_marker = f"default:user:{uid}:"
    for path in paths:
        acl = _run([getfacl, "-cpn", "--", str(path)])
        lines = acl.splitlines()
        if any(line.startswith(access_marker) for line in lines):
            access_paths.append(path)
        if path in directory_set and any(
            line.startswith(default_marker) for line in lines
        ):
            default_paths.append(path)
    for offset in range(0, len(access_paths), 128):
        _run(
            [
                setfacl,
                "-x",
                f"u:{uid}",
                "--",
                *(str(path) for path in access_paths[offset : offset + 128]),
            ]
        )
    for offset in range(0, len(default_paths), 128):
        _run(
            [
                setfacl,
                "-x",
                f"d:u:{uid}",
                "--",
                *(str(path) for path in default_paths[offset : offset + 128]),
            ]
        )


def apply(args: argparse.Namespace) -> int:
    payload = _load_receipt(Path(args.receipt))
    home, uid, children, workspace_root = _validate_receipt_scope(
        payload, require_unchanged_children=True
    )
    current_acl, _ = _snapshot(home)
    if hashlib.sha256(current_acl.encode()).hexdigest() != payload["acl_sha256"]:
        raise CutoverError("ACL state changed after prepare")

    setfacl = _executable("setfacl")
    private_siblings: list[Path] = [
        path for path in children if workspace_root is None or path != workspace_root
    ]
    workspaces: list[Path] = []
    if workspace_root is not None:
        for child in sorted(workspace_root.iterdir(), key=lambda path: path.name):
            if child.is_symlink():
                raise CutoverError("sandbox workspace root contains a symlink")
            if child.is_dir() and _WORKSPACE_NAME_RE.fullmatch(child.name):
                workspaces.append(child)
                for sibling in sorted(child.iterdir(), key=lambda path: path.name):
                    if sibling.is_symlink():
                        raise CutoverError("sandbox workspace contains a symlink")
                    if sibling.name != "work":
                        private_siblings.append(sibling)
            else:
                private_siblings.append(child)
    for workspace in workspaces:
        work = workspace / "work"
        if work.exists():
            preflight_reader_work_access(home, work)
    mutation_started = False
    try:
        if private_siblings:
            mutation_started = True
            _run(
                [
                    setfacl,
                    "-m",
                    f"u:{uid}:---",
                    "--",
                    *(str(path) for path in private_siblings),
                ]
            )
        # Existing private siblings are denied before home traversal is enabled.
        mutation_started = True
        preserve_reader_home_access(home, uid)
        for workspace in workspaces:
            work = workspace / "work"
            if work.exists():
                grant_reader_work_access(home, work, uid)
    except Exception as exc:
        if mutation_started:
            try:
                _restore_preimage(payload)
            except Exception as restore_exc:
                raise CutoverError(
                    "ACL cutover failed and automatic preimage restore also failed"
                ) from restore_exc
        raise CutoverError("ACL cutover failed; preimage restored") from exc
    print(json.dumps({"status": "applied", "home": str(home), "reader_uid": uid}, sort_keys=True))
    return 0


def rollback(args: argparse.Namespace) -> int:
    payload = _load_receipt(Path(args.receipt))
    home, uid, _children, _workspace_root = _validate_receipt_scope(
        payload, require_unchanged_children=False
    )
    _remove_current_reader_entries(home, uid)
    skipped = _restore_preimage(payload)
    print(
        json.dumps(
            {
                "status": "rolled_back",
                "home": str(home),
                "reader_uid": uid,
                "missing_preimage_paths_skipped": skipped,
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    if platform.system() != "Linux":
        raise CutoverError("sandbox reader ACL cutover requires Linux")
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--home", required=True)
    prepare_parser.add_argument("--reader", required=True)
    prepare_parser.add_argument("--receipt", required=True)
    for name in ("apply", "rollback"):
        command = commands.add_parser(name)
        command.add_argument("--receipt", required=True)
    args = parser.parse_args()
    return {"prepare": prepare, "apply": apply, "rollback": rollback}[args.command](args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (CutoverError, SandboxReaderPolicyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
