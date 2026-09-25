"""Write a large result where the python sandbox can read it (PA-203 r2f).

r2f replay s3: a workflow's 30 helper results came to 235,819 characters.
The completion message and ``workflow_status`` cut them inline, a large tool
result could not be saved anywhere the sandbox reads ("Full output could not
be saved to sandbox"), and about 74% of the findings were lost.  Anything too
large to carry inline is now written here, into the conversation's sandbox
``work/`` folder, which python_sandbox sees as ``/work``.  In the agent-wide
workspace the write is also recorded in the files log.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _work_dir(owner: str) -> Path:
    from hermes_constants import get_hermes_home
    from tools.python_sandbox_tool import _workspace_key

    return Path(get_hermes_home()) / "sandbox_workspaces" / _workspace_key(owner) / "work"


def resolve_owner(owner: str | None = None) -> str:
    """The sandbox workspace owner: the one given, else this turn's owner."""
    if owner:
        return str(owner)
    from gateway.session_context import get_session_env
    from tools.python_sandbox_tool import current_workspace_owner

    return str(current_workspace_owner(get_session_env("HERMES_SESSION_ID", "")) or "")


def write_work_file(
    name: str,
    content: str | bytes,
    *,
    owner: str | None = None,
    details: dict[str, Any] | None = None,
) -> str | None:
    """Write ``content`` as ``work/<name>`` and return its ``/work/...`` path.

    Returns ``None`` when no workspace can be resolved.  The write is
    exclusive and no-follow, then atomically renamed, because sandbox code
    may be running in the same folder.
    """
    try:
        resolved_owner = resolve_owner(owner)
        if not resolved_owner:
            return None
        from tools.python_sandbox_tool import _load_config, agent_workspace_enabled

        safe = _SAFE.sub("_", str(name)).strip("._") or f"result-{uuid.uuid4().hex[:8]}"
        data = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        work = _work_dir(resolved_owner)
        work.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = work / safe
        tmp = work / f".{safe}.{uuid.uuid4().hex}.tmp"
        fd = os.open(
            tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600,
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp, path)
        sha = hashlib.sha256(data).hexdigest()
        if agent_workspace_enabled():
            _log(safe, sha, details)
        else:
            try:
                from hermes_constants import get_hermes_home
                from tools.python_sandbox_access import configured_reader_uid, grant_reader_work_access

                reader_uid = configured_reader_uid(_load_config())
                if reader_uid is not None:
                    grant_reader_work_access(Path(get_hermes_home()), work, reader_uid)
            except Exception:
                logger.debug("could not grant the sandbox reader access to %s", path, exc_info=True)
        return f"/work/{safe}"
    except Exception:
        logger.warning("could not write %s into the sandbox workspace", name, exc_info=True)
        return None


def _log(name: str, sha: str, details: dict[str, Any] | None) -> None:
    """Record a host-written ``work/`` file in the agent workspace files log."""
    try:
        from tools.pa_agent_workspace import (
            FILES_LOG_NAME, _now, agent_dir, ensure_layout, records_dir, records_lock,
        )

        base = agent_dir()
        ensure_layout(base)
        entry = {
            "at": _now(), "direction": "produced", "path": f"work/{name}", "name": name,
            "sha256": sha, **{k: v for k, v in dict(details or {}).items() if v},
        }
        with records_lock(base):
            log = records_dir(base) / FILES_LOG_NAME
            fd = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    except Exception:
        logger.debug("could not record %s in the files log", name, exc_info=True)


def read_hint(path: str) -> str:
    """The plain line that tells Christopher where the full result is."""
    return (
        f"The full results are in {path}. Read that file with python_sandbox "
        "(for example json.load(open(path))) before using or summarising them."
    )
