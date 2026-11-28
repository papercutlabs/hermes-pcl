#!/usr/bin/env python3
"""Check, merge and verify the move to Christopher's one agent workspace.

PA-203 round 2.  ``python_sandbox.workspace: agent`` gives every
conversation, scheduled job and helper one folder.  The runtime merges the
earlier per-conversation, per-model folders into it on first use; this helper
lets a release see that merge before and prove it after.  Local only: it
changes no config and restarts nothing.  Run it as the Hermes user with
``HERMES_HOME`` set.

  check   read-only: earlier folders, their files and bytes, the names that
          will be kept twice, and the merged size against
          ``limits.workspace_mb`` (runs are refused over it).
  apply   needs ``workspace: agent``: runs the merge, then proves that every
          file of every earlier folder is in the agent folder with the same
          bytes.  Earlier folders are never changed or removed.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hermes_constants import get_hermes_home  # noqa: E402
from tools import python_sandbox_tool as sandbox  # noqa: E402


def _agent_folder() -> Path:
    return (
        get_hermes_home()
        / "sandbox_workspaces"
        / sandbox._workspace_key(sandbox.AGENT_WORKSPACE_OWNER, {"workspace": "agent"})
    )


def _earlier_folders() -> list[Path]:
    root = get_hermes_home() / "sandbox_workspaces"
    agent = _agent_folder()
    if not root.is_dir():
        return []
    return sorted(
        path
        for path in root.iterdir()
        if path != agent
        and sandbox._LEGACY_WORKSPACE_RE.fullmatch(path.name)
        and path.is_dir()
        and not path.is_symlink()
    )


def _sizes(work: Path, manifest: dict[str, str]) -> int:
    return sum((work / relative).stat().st_size for relative in manifest)


def check() -> dict:
    config = sandbox._load_config()
    agent_work = _agent_folder() / "work"
    folders = []
    by_name: dict[str, set[str]] = defaultdict(set)
    unique: dict[tuple[str, str], int] = {}
    for folder in [agent_work.parent, *_earlier_folders()]:
        work = folder / "work"
        manifest = sandbox._work_manifest(work)
        for relative, digest in manifest.items():
            by_name[relative].add(digest)
            unique.setdefault((relative, digest), (work / relative).stat().st_size)
        if folder != agent_work.parent:
            folders.append(
                {"folder": folder.name, "files": len(manifest), "bytes": _sizes(work, manifest)}
            )
    workspace_mb = sandbox._limits(config)["workspace_mb"]
    merged_bytes = sum(unique.values())
    return {
        "workspace_mode": sandbox._workspace_mode(config),
        "agent_folder": agent_work.parent.name,
        "agent_folder_exists": agent_work.is_dir(),
        "earlier_folders": folders,
        "names_kept_twice": sorted(name for name, digests in by_name.items() if len(digests) > 1),
        "merged_bytes": merged_bytes,
        "workspace_mb": workspace_mb,
        "under_half_limit": merged_bytes < workspace_mb * 1024 * 1024 // 2,
    }


def apply() -> dict:
    config = sandbox._load_config()
    if not sandbox.agent_workspace_enabled(config):
        raise SystemExit("python_sandbox.workspace is not 'agent'; nothing merged")
    work = sandbox.ensure_agent_workspace(config)
    present = set(sandbox._work_manifest(work).values())
    missing = []
    for folder in _earlier_folders():
        for relative, digest in sandbox._work_manifest(folder / "work").items():
            if digest not in present:
                missing.append(f"{folder.name}/work/{relative}")
    record_path = work.parent / sandbox._MERGE_RECORD_FILE
    record = json.loads(record_path.read_text()) if record_path.exists() else {}
    return {
        "agent_folder": work.parent.name,
        "merged_folders": sorted((record.get("merged") or {}).keys()),
        "files_in_agent_folder": len(sandbox._work_manifest(work)),
        "missing": missing,
        "ok": not missing,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", choices=("check", "apply"))
    args = parser.parse_args(argv)
    result = check() if args.action == "check" else apply()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
