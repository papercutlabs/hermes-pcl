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


def _fallback_chain(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The config's fallback_providers (or legacy fallback_model), valid entries only."""
    raw = cfg.get("fallback_providers") or cfg.get("fallback_model") or []
    entries = raw if isinstance(raw, list) else [raw]
    return [entry for entry in entries if isinstance(entry, dict) and entry.get("provider") and entry.get("model")]


def resolve_session_runtime(model: str, provider: str | None) -> tuple[str, dict[str, Any], list[dict[str, Any]], str | None]:
    """Resolve the engine for one ephemeral session: (model, runtime, fallback chain, fallback provider used).

    The requested provider is the primary.  When the host's configured provider is a
    direct API profile, the session follows the host.  When the primary cannot be
    resolved (login exhausted or unavailable), the first resolvable configured
    fallback is used, with the credentials its own provider entry names.
    """
    from hermes_cli.auth import AuthError
    from hermes_cli.config import load_config
    from hermes_cli.oneshot import resolve_oneshot_runtime
    from hermes_cli.runtime_provider import resolve_runtime_provider

    cfg = load_config()
    chain = _fallback_chain(cfg)
    model_cfg = cfg.get("model") or {}
    host_provider = str(model_cfg.get("provider") or "").strip().lower() if isinstance(model_cfg, dict) else ""
    if host_provider and host_provider != "openai-codex":
        host_model = (model or "").strip() or str(model_cfg.get("default") or model_cfg.get("model") or "")
        return host_model, resolve_runtime_provider(requested=host_provider, target_model=host_model or None), chain, None
    try:
        _cfg, effective_model, runtime = resolve_oneshot_runtime(model, provider)
        return effective_model, runtime, chain, None
    except AuthError as primary_error:
        # Only an unusable login (exhausted, revoked, label unavailable) reaches the fallback, as in the gateway;
        # any other error is raised so a fault is never answered by spending the API key.
        for entry in chain:
            try:
                runtime = resolve_runtime_provider(
                    requested=str(entry["provider"]).strip(),
                    explicit_base_url=entry.get("base_url"),
                    explicit_api_key=entry.get("api_key"),
                )
            except Exception:
                continue
            runtime = dict(runtime, primary_unavailable=f"{type(primary_error).__name__}: {str(primary_error)[:160]}")
            return str(entry["model"]).strip(), runtime, chain, str(entry["provider"]).strip()
        raise primary_error


def run_ephemeral_session(*, prompt: str, system_prompt: str, model: str,
                          max_iterations: int, allowed_tool_names: list[str],
                          provider: str | None = None,
                          session_prefix: str = "ephemeral") -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one canonical-provider turn and erase its session/transcript.

    This is deliberately not a gateway or delivery carrier.  The audit holds
    only non-sensitive identity/hash/lifecycle facts, never the prompt or
    tool arguments.
    """
    from hermes_constants import get_hermes_home
    from hermes_state import SessionDB
    from run_agent import AIAgent
    from hermes_cli.oneshot import _oneshot_clarify_callback

    effective_model, runtime, fallback_chain, fallback_used = resolve_session_runtime(model, provider)
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
                        fallback_model=fallback_chain or None, skip_memory=True, skip_context_files=True, clarify_callback=_oneshot_clarify_callback,
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
    served_provider = getattr(agent, "provider", None) if agent is not None else None
    return outcome, {"session_id": session_id, "provider": runtime.get("provider"), "model": effective_model,
                     "fallback_configured_provider": fallback_used, "served_provider": served_provider,
                     "primary_unavailable": runtime.get("primary_unavailable"),
                     "loaded_tools": sorted(agent.valid_tool_names) if agent is not None else [],
                     "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(), "terminal_reason": terminal_reason,
                     "cleanup": cleanup}
