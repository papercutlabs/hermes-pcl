"""Carry live conversations from the engine-keyed namespace to the stable one.

PA-203 round 2.  Earlier releases keyed every live-drain conversation on the
engine, ``agent:live-drain:persistent-chat:{provider}:{model}:<chat key>``, so
a model or provider change started every conversation from empty.  The live
namespace is now ``agent:live-drain:conversation`` with no engine in it.

This module carries each existing conversation to the new key once:

1. ``sessions/sessions.json``: for each chat, the most recently updated
   engine-keyed entry is copied under the new key, with the same session id,
   so the transcript, compaction lineage and token counters continue.
2. In ``python_sandbox.workspace: session`` mode only, the conversation's
   workspace folder ``s_<sha256(key)>`` is copied to the new key's folder.  In
   ``agent`` mode the folder does not depend on the key, so nothing is copied.

It never changes or removes an old entry or folder, so a rollback to a runtime
that still uses the engine-keyed namespace finds its conversations as they
were.  It never overwrites a new-key entry or folder that already exists, so
running it again is a no-op.  It must run before any ``SessionStore`` in the
same process loads ``sessions.json``.  The durable consumer calls it at start,
and the release runs it with the service stopped:
``python -m gateway.conversation_key_carryover check|apply --hermes-home <home>``.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

NEW_NAMESPACE = "agent:live-drain:conversation"
LEGACY_PREFIX = "agent:live-drain:persistent-chat"


def _platform_values() -> frozenset[str]:
    try:
        from gateway.config import Platform

        return frozenset(str(item.value) for item in Platform)
    except Exception:  # pragma: no cover - import guard for a bare release check
        return frozenset({"whatsapp", "telegram", "slack", "discord", "signal"})


def parse_legacy_key(key: str, platforms: frozenset[str] | None = None) -> tuple[str, str, str] | None:
    """Split an engine-keyed live session key into (provider, model, chat key).

    Returns ``None`` for anything else, including the bare pre-engine
    ``agent:live-drain:persistent-chat:<platform>:...`` keys, which are never
    carried.
    """
    platforms = _platform_values() if platforms is None else platforms
    prefix = LEGACY_PREFIX + ":"
    if not key.startswith(prefix):
        return None
    parts = key[len(prefix):].split(":", 2)
    if len(parts) != 3 or not all(parts):
        return None
    provider, model, chat_key = parts
    if provider in platforms or chat_key.split(":", 1)[0] not in platforms:
        return None
    return provider, model, chat_key


def _updated_at(entry: Mapping[str, Any]) -> float:
    for field in ("updated_at", "created_at"):
        raw = entry.get(field)
        if isinstance(raw, str) and raw:
            try:
                return datetime.fromisoformat(raw).timestamp()
            except ValueError:
                continue
    return float("-inf")


def plan_carryover(entries: Mapping[str, Any]) -> dict[str, Any]:
    """Decide which engine-keyed entry each chat carries.  Pure; no IO."""
    platforms = _platform_values()
    candidates: dict[str, list[str]] = {}
    for key, entry in entries.items():
        if not isinstance(entry, Mapping) or parse_legacy_key(key, platforms) is None:
            continue
        chat_key = parse_legacy_key(key, platforms)[2]
        candidates.setdefault(chat_key, []).append(key)
    carry: dict[str, str] = {}
    already_present: dict[str, str] = {}
    for chat_key, keys in sorted(candidates.items()):
        # Newest conversation wins; the key breaks a tie deterministically.
        source = max(keys, key=lambda item: (_updated_at(entries[item]), item))
        target = f"{NEW_NAMESPACE}:{chat_key}"
        if target in entries:
            already_present[target] = source
        else:
            carry[target] = source
    return {
        "carry": carry,
        "already_present": already_present,
        "legacy_entries": sum(len(keys) for keys in candidates.values()),
    }


def _workspace_folder(key: str) -> str:
    # The session-mode folder name (tools.python_sandbox_tool._workspace_key).
    return "s_" + hashlib.sha256(key.encode("utf-8")).hexdigest()


def _workspace_mode(hermes_home: Path) -> str:
    config_path = hermes_home / "config.yaml"
    try:
        import yaml

        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except (OSError, ValueError, ImportError):
        return "unknown"
    sandbox = data.get("python_sandbox") if isinstance(data, dict) else None
    value = sandbox.get("workspace") if isinstance(sandbox, dict) else None
    return str(value or "run")


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    previous = path.stat() if path.exists() else None
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".sessions_", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        if previous is not None:
            os.chmod(tmp, previous.st_mode & 0o7777)
            try:
                os.chown(tmp, previous.st_uid, previous.st_gid)
            except PermissionError:
                pass
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def carry_over(
    hermes_home: Path, *, apply: bool, sessions_dir: Path | None = None
) -> dict[str, Any]:
    """Carry engine-keyed conversations to the stable namespace.

    ``apply=False`` only reports.  The result lists every carried key, every
    key left alone because the new key already exists, and every workspace
    folder copied.
    """
    hermes_home = Path(hermes_home)
    sessions_file = Path(sessions_dir or hermes_home / "sessions") / "sessions.json"
    result: dict[str, Any] = {
        "contract": "pa203-conversation-key-carryover/v1",
        "applied": bool(apply),
        "sessions_file": str(sessions_file),
        "carried": {},
        "already_present": {},
        "legacy_entries": 0,
        "workspace_mode": _workspace_mode(hermes_home),
        "workspaces_copied": {},
        "workspaces_skipped": {},
    }
    if not sessions_file.exists():
        return result
    entries = json.loads(sessions_file.read_text(encoding="utf-8"))
    if not isinstance(entries, dict):
        raise ValueError("sessions.json is not a mapping")
    plan = plan_carryover(entries)
    result["carried"] = dict(plan["carry"])
    result["already_present"] = dict(plan["already_present"])
    result["legacy_entries"] = plan["legacy_entries"]

    if result["workspace_mode"] == "session":
        root = hermes_home / "sandbox_workspaces"
        for target, source in plan["carry"].items():
            old = root / _workspace_folder(source)
            new = root / _workspace_folder(target)
            if not old.is_dir():
                continue
            if new.exists():
                result["workspaces_skipped"][new.name] = "target exists"
                continue
            result["workspaces_copied"][new.name] = old.name
            if apply:
                staging = root / f".{new.name}.carryover-{os.getpid()}"
                shutil.rmtree(staging, ignore_errors=True)
                shutil.copytree(old, staging, symlinks=True)
                os.replace(staging, new)

    if apply and plan["carry"]:
        updated = dict(entries)
        for target, source in plan["carry"].items():
            entry = copy.deepcopy(entries[source])
            entry["session_key"] = target
            updated[target] = entry
        _atomic_write_json(sessions_file, updated)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=("check", "apply"))
    parser.add_argument("--hermes-home", required=True)
    args = parser.parse_args(argv)
    result = carry_over(Path(args.hermes_home), apply=args.action == "apply")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
