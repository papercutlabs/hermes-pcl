"""Canonical, non-persistent Hermes one-turn session carrier."""
from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Any, Mapping


def _remove_ephemeral_session_files(sessions_dir: Path, session_id: str) -> None:
    """Remove both canonical SessionDB and AIAgent transcript name forms."""
    paths = [
        sessions_dir / f"{session_id}.json",
        sessions_dir / f"{session_id}.jsonl",
        sessions_dir / f"session_{session_id}.json",
        sessions_dir / f"session_{session_id}.jsonl",
    ]
    paths.extend(sessions_dir.glob(f"request_dump_{session_id}_*.json"))
    for path in paths:
        path.unlink(missing_ok=True)
    if any(path.exists() for path in paths):
        raise RuntimeError("ephemeral transcript remains")


def run_ephemeral_session(*, prompt: str, system_prompt: str, model: str,
                          max_iterations: int, allowed_tool_names: list[str],
                          provider: str | None = None,
                          explicit_api_key: str | None = None,
                          session_prefix: str = "ephemeral") -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one canonical-provider turn and erase its session/transcript.

    This is deliberately not a gateway or delivery carrier.  The audit holds
    only non-sensitive identity/hash/lifecycle facts, never the prompt or
    tool arguments.

    ``explicit_api_key`` is optional and only forwarded to
    ``resolve_oneshot_runtime``.  Omitting it resolves exactly as before.  The
    value is never placed in the audit, the returned lifecycle or a log: the
    audit below carries identity, hashes and lifecycle facts only, and that is
    unchanged by this argument.
    """
    from hermes_constants import get_hermes_home
    from hermes_state import SessionDB
    from run_agent import AIAgent
    from hermes_cli.oneshot import _oneshot_clarify_callback, resolve_oneshot_runtime

    _cfg, effective_model, runtime = resolve_oneshot_runtime(model, provider, explicit_api_key)
    session_id = f"{session_prefix}-{uuid.uuid4().hex}"
    db = SessionDB()
    agent = None
    outcome: dict[str, Any] = {}
    terminal_reason = "failed"
    cleanup = {"ended": False, "deleted": False, "agent_closed": False, "db_closed": False}
    cleanup_errors: list[str] = []
    primary_error: Exception | None = None
    try:
        db.create_session(session_id, "ephemeral", model=effective_model, model_config={"provider": runtime.get("provider")})
        agent = AIAgent(api_key=runtime.get("api_key"), base_url=runtime.get("base_url"), provider=runtime.get("provider"),
                        api_mode=runtime.get("api_mode"), model=effective_model, max_iterations=max_iterations,
                        quiet_mode=True, platform="cli", session_id=session_id, session_db=db,
                        credential_pool=runtime.get("credential_pool"), allowed_tool_names=list(allowed_tool_names),
                        skip_memory=True, skip_context_files=True, clarify_callback=_oneshot_clarify_callback,
                        ephemeral_system_prompt=system_prompt)
        agent.suppress_status_output = True
        agent.stream_delta_callback = None
        agent.tool_gen_callback = None
        outcome = agent.run_conversation(prompt)
        terminal_reason = "completed"
    except Exception as exc:
        primary_error = exc
    finally:
        try:
            db.end_session(session_id, terminal_reason)
            cleanup["ended"] = True
        except Exception as exc: cleanup_errors.append(f"end:{type(exc).__name__}")
        try:
            if agent is not None:
                agent.close()
                cleanup["agent_closed"] = True
        except Exception as exc: cleanup_errors.append(f"agent_close:{type(exc).__name__}")
        finally:
            try:
                sessions_dir = get_hermes_home() / "sessions"
                deleted = db.delete_session(session_id, sessions_dir=sessions_dir)
                _remove_ephemeral_session_files(sessions_dir, session_id)
                cleanup["deleted"] = bool(deleted)
            except Exception as exc:
                cleanup_errors.append(f"delete:{type(exc).__name__}")
            try: db.close(); cleanup["db_closed"] = True
            except Exception as exc: cleanup_errors.append(f"db_close:{type(exc).__name__}")
    if cleanup_errors or not all(cleanup.values()):
        cleanup_error = RuntimeError("EPHEMERAL_SESSION_CLEANUP_FAILED:" + ",".join(cleanup_errors or ["unproven"]))
        if primary_error is not None:
            raise cleanup_error from primary_error
        raise cleanup_error
    if primary_error is not None:
        raise primary_error
    return outcome, {"session_id": session_id, "provider": runtime.get("provider"), "model": effective_model,
                     "loaded_tools": sorted(agent.valid_tool_names) if agent is not None else [],
                     "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(), "terminal_reason": terminal_reason,
                     "cleanup": cleanup}
