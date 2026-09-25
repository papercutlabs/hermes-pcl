"""
Cron job scheduler - executes due jobs.

Provides tick() which checks for due jobs and runs them. The gateway
calls this every 60 seconds from a background thread.

Uses a file-based lock (~/.hermes/cron/.tick.lock) so only one tick
runs at a time if multiple processes overlap.
"""

import asyncio
import concurrent.futures
import contextvars
import json
import logging
import os
import shutil
import subprocess
import sys

# fcntl is Unix-only; on Windows use msvcrt for file locking
try:
    import fcntl
except ImportError:
    fcntl = None
    try:
        import msvcrt
    except ImportError:
        msvcrt = None
from pathlib import Path
from typing import Any, List, Mapping, Optional

# Add parent directory to path for imports BEFORE repo-level imports.
# Without this, standalone invocations (e.g. after `hermes update` reloads
# the module) fail with ModuleNotFoundError for hermes_time et al.
sys.path.insert(0, str(Path(__file__).parent.parent))

from hermes_constants import get_hermes_home
from hermes_cli.config import load_config, _expand_env_vars
from hermes_time import now as _hermes_now
from utils import is_truthy_value

logger = logging.getLogger(__name__)


class CronPromptInjectionBlocked(Exception):
    """Raised by _build_job_prompt when the fully-assembled prompt trips the
    injection scanner. Caught in run_job so the operator sees a clean
    "job blocked" delivery instead of the scheduler crashing.

    Assembled-prompt scanning (including loaded skill content) plugs the
    gap from #3968: create-time scanning only covers the user-supplied
    prompt field; skill content loaded at runtime was never scanned, so a
    malicious skill could carry an injection payload that reached the
    non-interactive (auto-approve) cron agent.
    """


def _resolve_cron_enabled_toolsets(job: dict, cfg: dict) -> list[str] | None:
    """Resolve the toolset list for a cron job.

    Precedence:
    1. Per-job ``enabled_toolsets`` (set via ``cronjob`` tool on create/update).
       Keeps the agent's job-scoped toolset override intact — #6130.
    2. Per-platform ``hermes tools`` config for the ``cron`` platform.
       Mirrors gateway behavior (``_get_platform_tools(cfg, platform_key)``)
       so users can gate cron toolsets globally without recreating every job.
    3. ``None`` on any lookup failure — AIAgent loads the full default set
       (legacy behavior before this change, preserved as the safety net).

    _DEFAULT_OFF_TOOLSETS ({moa, homeassistant, rl}) are removed by
    ``_get_platform_tools`` for unconfigured platforms, so fresh installs
    get cron WITHOUT ``moa`` by default (issue reported by Norbert —
    surprise $4.63 run).
    """
    per_job = job.get("enabled_toolsets")
    if per_job:
        return per_job
    try:
        from hermes_cli.tools_config import _get_platform_tools  # lazy: avoid heavy import at cron module load
        return sorted(_get_platform_tools(cfg or {}, "cron"))
    except Exception as exc:
        logger.warning(
            "Cron toolset resolution failed, falling back to full default toolset: %s",
            exc,
        )
        return None


def _resolve_cron_pa_context(job: dict, cfg: Mapping[str, Any] | None):
    """Resolve the PA job brief for a scheduled run, when configured."""
    config = cfg if isinstance(cfg, Mapping) else {}
    pa_config = dict(config.get("pa") or {}) if isinstance(config.get("pa"), Mapping) else {}
    if not is_truthy_value(pa_config.get("enabled"), default=False):
        return None
    if job.get("pa_job_type"):
        pa_config["job_type"] = str(job.get("pa_job_type"))
    metadata = {
        "source": {"platform": "cron"},
        "job": {
            "id": job.get("id"),
            "name": job.get("name"),
        },
        "job_type": job.get("pa_job_type"),
    }
    from agent.pa_constitution import resolve_context

    return resolve_context(pa_config, metadata)


def _merge_cron_pa_toolsets(
    enabled_toolsets: list[str] | None,
    disabled_toolsets: list[str] | None,
    pa_context: Any,
) -> tuple[list[str] | None, list[str] | None]:
    """Apply PA job-brief toolset scope to cron agent construction."""
    if pa_context is None:
        return enabled_toolsets, disabled_toolsets
    job_brief = getattr(pa_context, "job_brief", None)
    if job_brief is None:
        return enabled_toolsets, disabled_toolsets

    job_enabled = list(getattr(job_brief, "enabled_toolsets", ()) or ())
    job_disabled = list(getattr(job_brief, "disabled_toolsets", ()) or ())
    next_enabled = job_enabled if job_enabled else enabled_toolsets
    next_disabled = list(disabled_toolsets or [])
    # The TGG Management capability supplies a dedicated read-only child
    # surface. Honor its explicit delegation selection at this PA boundary;
    # the generic cron default must not silently contradict that brief.
    if (
        getattr(pa_context, "job_type", None) == "tgg_management"
        and "tgg-management-readonly-investigation" in job_enabled
        and "delegation" in job_enabled
    ):
        next_disabled = [name for name in next_disabled if name != "delegation"]
    next_disabled.extend(job_disabled)

    def _dedupe(values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        seen: set[str] = set()
        out: list[str] = []
        for value in values:
            text = str(value).strip()
            if text and text not in seen:
                out.append(text)
                seen.add(text)
        return out or None

    return _dedupe(next_enabled), _dedupe(next_disabled)


def _resolve_cron_pa_workspace_owner(job: dict) -> str | None:
    """Return the originating Management conversation's workspace owner.

    A scheduled job created from a TGG Management conversation works in that
    conversation's persistent sandbox folder, so a later turn in the same
    conversation simply has the files the job produced.  The owner is the
    runtime-derived stable session key of the job's recorded origin chat,
    never a model-supplied value.  Any other job keeps its per-run folder.
    """
    if str(job.get("pa_job_type") or "").strip() != "tgg_management":
        return None
    origin = _resolve_origin(job)
    if not origin:
        return None
    config_path = _get_hermes_home() / "config.yaml"
    try:
        import yaml

        raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        pa_config = raw.get("pa") if isinstance(raw, Mapping) else None
        if not isinstance(pa_config, Mapping) or not is_truthy_value(
            pa_config.get("enabled"), default=False
        ):
            return None
        from gateway.durable_jsonl_consumer import (
            management_conversation_session_key,
        )

        return management_conversation_session_key(
            config_path,
            platform=str(origin.get("platform") or ""),
            chat_id=str(origin.get("chat_id") or ""),
        )
    except Exception as exc:
        logger.warning(
            "Job '%s': could not resolve its Management conversation workspace; "
            "using a per-run workspace: %s",
            job.get("id"),
            exc,
        )
        return None


def _render_cron_pa_ephemeral_prompt(pa_context: Any) -> str:
    if pa_context is None:
        return ""
    from agent.pa_constitution import render_identity_prompt, render_job_prompt

    return (
        render_identity_prompt(pa_context.constitution)
        + "\n\n"
        + render_job_prompt(pa_context)
    ).strip()


def _record_cron_pa_behavior_event(session_db: Any, pa_context: Any, job: dict, session_id: str) -> None:
    if session_db is None or pa_context is None:
        return
    recorder = getattr(session_db, "record_pa_behavior_event", None)
    if recorder is None:
        return
    try:
        recorder(
            constitution_id=pa_context.constitution.id,
            constitution_hash=pa_context.identity_hash,
            job_type=pa_context.job_type,
            job_hash=pa_context.job_hash,
            session_id=session_id,
            session_source="cron",
            source_metadata={"job_id": job.get("id"), "job_name": job.get("name")},
            actor="cron",
            source="cron",
            metadata={"behavior_hash": pa_context.behavior_hash},
        )
    except Exception as exc:
        logger.debug("Job '%s': failed to record PA behavior event: %s", job.get("id"), exc)

# Valid delivery platforms — used to validate user-supplied platform names
# in cron delivery targets, preventing env var enumeration via crafted names.
_KNOWN_DELIVERY_PLATFORMS = frozenset({
    "telegram", "discord", "slack", "whatsapp", "signal",
    "matrix", "mattermost", "homeassistant", "dingtalk", "feishu",
    "wecom", "wecom_callback", "weixin", "sms", "email", "webhook", "bluebubbles",
    "qqbot", "yuanbao",
})

# Platforms that support a configured cron/notification home target, mapped to
# the environment variable used by gateway setup/runtime config.
_HOME_TARGET_ENV_VARS = {
    "matrix": "MATRIX_HOME_ROOM",
    "telegram": "TELEGRAM_HOME_CHANNEL",
    "discord": "DISCORD_HOME_CHANNEL",
    "slack": "SLACK_HOME_CHANNEL",
    "signal": "SIGNAL_HOME_CHANNEL",
    "mattermost": "MATTERMOST_HOME_CHANNEL",
    "sms": "SMS_HOME_CHANNEL",
    "email": "EMAIL_HOME_ADDRESS",
    "dingtalk": "DINGTALK_HOME_CHANNEL",
    "feishu": "FEISHU_HOME_CHANNEL",
    "wecom": "WECOM_HOME_CHANNEL",
    "weixin": "WEIXIN_HOME_CHANNEL",
    "bluebubbles": "BLUEBUBBLES_HOME_CHANNEL",
    "qqbot": "QQBOT_HOME_CHANNEL",
    "whatsapp": "WHATSAPP_HOME_CHANNEL",
}

# Legacy env var names kept for back-compat.  Each entry is the current
# primary env var → the previous name.  _get_home_target_chat_id falls
# back to the legacy name if the primary is unset, so users who set the
# old name before the rename keep working until they migrate.
_LEGACY_HOME_TARGET_ENV_VARS = {
    "QQBOT_HOME_CHANNEL": "QQ_HOME_CHANNEL",
}

from cron.jobs import get_due_jobs, mark_job_run, save_job_output, advance_next_run

# Sentinel: when a cron agent has nothing new to report, it can start its
# response with this marker to suppress delivery.  Output is still saved
# locally for audit.
SILENT_MARKER = "[SILENT]"

# Backward-compatible module override used by tests and emergency monkeypatches.
_hermes_home: Path | None = None


def _get_hermes_home() -> Path:
    """Resolve Hermes home dynamically while preserving test monkeypatch hooks."""
    return _hermes_home or get_hermes_home()


def _get_lock_paths() -> tuple[Path, Path]:
    """Resolve cron lock paths at call time so profile/env changes are honored."""
    hermes_home = _get_hermes_home()
    lock_dir = hermes_home / "cron"
    return lock_dir, lock_dir / ".tick.lock"


def _resolve_origin(job: dict) -> Optional[dict]:
    """Extract origin info from a job, preserving any extra routing metadata.

    Treats non-dict origins (free-form provenance strings, ints, lists from
    migration scripts or hand-edited jobs.json) as missing instead of
    crashing with ``AttributeError`` on ``origin.get(...)``. Without this
    guard, a job tagged with e.g. ``"combined-digest-replaces-x-and-y"``
    crashed every fire attempt with
    ``'str' object has no attribute 'get'`` — ``mark_job_run`` recorded the
    failure, but the next tick re-loaded the same poisoned origin and
    crashed identically until the field was patched manually (#18722).
    """
    origin = job.get("origin")
    if not isinstance(origin, dict):
        return None
    platform = origin.get("platform")
    chat_id = origin.get("chat_id")
    if platform and chat_id:
        return origin
    return None


def _plugin_cron_env_var(platform_name: str) -> str:
    """Return the cron home-channel env var registered by a plugin platform.

    Falls through the platform registry so plugins that set
    ``cron_deliver_env_var`` on their ``PlatformEntry`` get cron delivery
    support without editing this module.
    """
    try:
        from hermes_cli.plugins import discover_plugins
        discover_plugins()  # idempotent
        from gateway.platform_registry import platform_registry
        entry = platform_registry.get(platform_name.lower())
        if entry and entry.cron_deliver_env_var:
            return entry.cron_deliver_env_var
    except Exception:
        pass
    return ""


def _is_known_delivery_platform(platform_name: str) -> bool:
    """Whether ``platform_name`` is a valid cron delivery target.

    Hardcoded built-ins in ``_KNOWN_DELIVERY_PLATFORMS`` are checked first;
    plugin platforms registered via ``PlatformEntry`` are accepted if they
    provide a ``cron_deliver_env_var``.
    """
    name = platform_name.lower()
    if name in _KNOWN_DELIVERY_PLATFORMS:
        return True
    return bool(_plugin_cron_env_var(name))


def _resolve_home_env_var(platform_name: str) -> str:
    """Return the env var name for a platform's cron home channel.

    Built-in platforms are in ``_HOME_TARGET_ENV_VARS``; plugin platforms are
    resolved from the platform registry.
    """
    name = platform_name.lower()
    env_var = _HOME_TARGET_ENV_VARS.get(name)
    if env_var:
        return env_var
    return _plugin_cron_env_var(name)


def _get_home_target_chat_id(platform_name: str) -> str:
    """Return the configured home target chat/room ID for a delivery platform."""
    env_var = _resolve_home_env_var(platform_name)
    if not env_var:
        return ""
    value = os.getenv(env_var, "")
    if not value:
        legacy = _LEGACY_HOME_TARGET_ENV_VARS.get(env_var)
        if legacy:
            value = os.getenv(legacy, "")
    return value


def _get_home_target_thread_id(platform_name: str) -> Optional[str]:
    """Return the optional thread/topic ID for a platform home target."""
    env_var = _resolve_home_env_var(platform_name)
    if not env_var:
        return None
    value = os.getenv(f"{env_var}_THREAD_ID", "").strip()
    if not value:
        legacy = _LEGACY_HOME_TARGET_ENV_VARS.get(env_var)
        if legacy:
            value = os.getenv(f"{legacy}_THREAD_ID", "").strip()
    return value or None


def _iter_home_target_platforms():
    """Iterate built-in + plugin platform names that expose a home channel.

    Used by the ``deliver=origin`` fallback when the job has no origin.
    """
    for name in _HOME_TARGET_ENV_VARS:
        yield name
    try:
        from hermes_cli.plugins import discover_plugins
        discover_plugins()  # idempotent
        from gateway.platform_registry import platform_registry
        for entry in platform_registry.plugin_entries():
            if entry.cron_deliver_env_var and entry.name not in _HOME_TARGET_ENV_VARS:
                yield entry.name
    except Exception:
        pass


def _resolve_single_delivery_target(job: dict, deliver_value: str) -> Optional[dict]:
    """Resolve one concrete auto-delivery target for a cron job."""

    origin = _resolve_origin(job)

    if deliver_value == "local":
        return None

    if deliver_value == "origin":
        if origin:
            return {
                "platform": origin["platform"],
                "chat_id": str(origin["chat_id"]),
                "thread_id": origin.get("thread_id"),
            }
        # Origin missing (e.g. job created via API/script) — try each
        # platform's home channel as a fallback instead of silently dropping.
        for platform_name in _iter_home_target_platforms():
            chat_id = _get_home_target_chat_id(platform_name)
            if chat_id:
                logger.info(
                    "Job '%s' has deliver=origin but no origin; falling back to %s home channel",
                    job.get("name", job.get("id", "?")),
                    platform_name,
                )
                return {
                    "platform": platform_name,
                    "chat_id": chat_id,
                    "thread_id": _get_home_target_thread_id(platform_name),
                }
        return None

    if ":" in deliver_value:
        platform_name, rest = deliver_value.split(":", 1)
        platform_key = platform_name.lower()

        from tools.send_message_tool import _parse_target_ref

        parsed_chat_id, parsed_thread_id, is_explicit = _parse_target_ref(platform_key, rest)
        if is_explicit:
            chat_id, thread_id = parsed_chat_id, parsed_thread_id
        else:
            chat_id, thread_id = rest, None

        # Resolve human-friendly labels like "Alice (dm)" to real IDs.
        try:
            from gateway.channel_directory import resolve_channel_name
            resolved = resolve_channel_name(platform_key, chat_id)
            if resolved:
                parsed_chat_id, parsed_thread_id, resolved_is_explicit = _parse_target_ref(platform_key, resolved)
                if resolved_is_explicit:
                    chat_id = parsed_chat_id
                    if parsed_thread_id is not None:
                        thread_id = parsed_thread_id
                else:
                    chat_id = resolved
        except Exception:
            pass

        return {
            "platform": platform_name,
            "chat_id": chat_id,
            "thread_id": thread_id,
        }

    platform_name = deliver_value
    if origin and origin.get("platform") == platform_name:
        return {
            "platform": platform_name,
            "chat_id": str(origin["chat_id"]),
            "thread_id": origin.get("thread_id"),
        }

    if not _is_known_delivery_platform(platform_name):
        return None
    chat_id = _get_home_target_chat_id(platform_name)
    if not chat_id:
        return None

    return {
        "platform": platform_name,
        "chat_id": chat_id,
        "thread_id": _get_home_target_thread_id(platform_name),
    }


def _normalize_deliver_value(deliver) -> str:
    """Normalize a stored/submitted ``deliver`` value to its canonical string form.

    The contract is that ``deliver`` is a string (``"local"``, ``"origin"``,
    ``"telegram"``, ``"telegram:-1001:17"``, or comma-separated combinations).
    Historically some callers — MCP clients passing an array, direct edits of
    ``jobs.json``, or stale code paths — have stored a list/tuple like
    ``["telegram"]``.  ``str(["telegram"])`` would serialize to the literal
    string ``"['telegram']"``, which is not a known platform and fails
    resolution silently.  Flatten lists/tuples into a comma-separated string
    so both forms work.  Returns ``"local"`` for anything falsy.
    """
    if deliver is None or deliver == "":
        return "local"
    if isinstance(deliver, (list, tuple)):
        parts = [str(p).strip() for p in deliver if str(p).strip()]
        return ",".join(parts) if parts else "local"
    return str(deliver)


# Routing intent tokens — resolved at fire time, not create time, so a
# job created before Telegram was wired up will pick up Telegram once it
# comes online.  ``all`` expands into the set of connected platforms
# (those with a configured home chat_id) in _expand_routing_tokens.
_ROUTING_TOKENS: frozenset[str] = frozenset({"all"})


def _expand_routing_tokens(part: str) -> List[str]:
    """Expand a routing-intent token to concrete platform names.

    ``all`` expands to every platform in ``_iter_home_target_platforms()``
    that has a configured home chat_id right now.  Unknown / non-token
    values pass through unchanged as a single-element list, so the caller
    can treat every token uniformly.
    """
    token = part.lower()
    if token not in _ROUTING_TOKENS:
        return [part]
    expanded: List[str] = []
    for platform_name in _iter_home_target_platforms():
        if _get_home_target_chat_id(platform_name):
            expanded.append(platform_name)
    return expanded


def _resolve_delivery_targets(job: dict) -> List[dict]:
    """Resolve all concrete auto-delivery targets for a cron job.

    Accepts the legacy comma-separated ``deliver`` string plus the
    ``all`` routing-intent token, which expands to every platform with
    a configured home channel.  Tokens may be combined with explicit
    targets: ``origin,all`` and ``all,telegram:-100:17`` both work.
    Duplicate (platform, chat_id, thread_id) tuples are collapsed by the
    existing dedup pass.
    """
    deliver = _normalize_deliver_value(job.get("deliver", "local"))
    if deliver == "local":
        return []

    raw_parts = [p.strip() for p in deliver.split(",") if p.strip()]

    # Expand routing intents.
    parts: List[str] = []
    for raw in raw_parts:
        parts.extend(_expand_routing_tokens(raw))

    seen = set()
    targets = []
    for part in parts:
        target = _resolve_single_delivery_target(job, part)
        if target:
            key = (target["platform"].lower(), str(target["chat_id"]), target.get("thread_id"))
            if key not in seen:
                seen.add(key)
                targets.append(target)
    return targets


def _resolve_delivery_target(job: dict) -> Optional[dict]:
    """Resolve the concrete auto-delivery target for a cron job, if any."""
    targets = _resolve_delivery_targets(job)
    return targets[0] if targets else None


# Media extension sets — audio routing is centralized in gateway.platforms.base
# via should_send_media_as_audio() so Telegram-specific rules stay in one place.
_VIDEO_EXTS = frozenset({'.mp4', '.mov', '.avi', '.mkv', '.webm', '.3gp'})
_IMAGE_EXTS = frozenset({'.jpg', '.jpeg', '.png', '.webp', '.gif'})


def _send_media_via_adapter(
    adapter,
    chat_id: str,
    media_files: list,
    metadata: dict | None,
    loop,
    job: dict,
    platform=None,
) -> None:
    """Send extracted MEDIA files as native platform attachments via a live adapter.

    Routes each file to the appropriate adapter method (send_voice, send_image_file,
    send_video, send_document) based on file extension — mirroring the routing logic
    in ``BasePlatformAdapter._process_message_background``.
    """
    from pathlib import Path

    from gateway.platforms.base import should_send_media_as_audio

    for media_path, _is_voice in media_files:
        try:
            ext = Path(media_path).suffix.lower()
            route_platform = platform if platform is not None else getattr(adapter, "platform", None)
            if should_send_media_as_audio(route_platform, ext, is_voice=_is_voice):
                coro = adapter.send_voice(chat_id=chat_id, audio_path=media_path, metadata=metadata)
            elif ext in _VIDEO_EXTS:
                coro = adapter.send_video(chat_id=chat_id, video_path=media_path, metadata=metadata)
            elif ext in _IMAGE_EXTS:
                coro = adapter.send_image_file(chat_id=chat_id, image_path=media_path, metadata=metadata)
            else:
                coro = adapter.send_document(chat_id=chat_id, file_path=media_path, metadata=metadata)

            from agent.async_utils import safe_schedule_threadsafe
            future = safe_schedule_threadsafe(coro, loop)
            if future is None:
                logger.warning(
                    "Job '%s': cannot send media %s, gateway loop unavailable",
                    job.get("id", "?"), media_path,
                )
                return
            try:
                result = future.result(timeout=30)
            except TimeoutError:
                future.cancel()
                raise
            if result and not getattr(result, "success", True):
                logger.warning(
                    "Job '%s': media send failed for %s: %s",
                    job.get("id", "?"), media_path, getattr(result, "error", "unknown"),
                )
        except Exception as e:
            logger.warning("Job '%s': failed to send media %s: %s", job.get("id", "?"), media_path, e)


def _deliver_whatsapp_via_tgg_reply_bridge(
    job: dict,
    chat_id: str,
    text: str,
    media_files: list,
    *,
    workspace_owner: str | None = None,
    allowed_work_paths: set[str] | None = None,
    receipt: dict | None = None,
) -> Optional[str]:
    """Deliver Christopher cron output through the durable consumer bridge.

    ``receipt``, when given, is filled with what was actually delivered:
    ``text_message_id`` and one ``files`` item per attachment with its
    WhatsApp message id and its agent-workspace path (PA-203).

    Christopher deliberately has Hermes's ordinary WhatsApp adapter disabled:
    inbound capture and outbound policy live in the separate guarded bridge.
    Cron jobs created in that management lane must use the same bridge instead
    of treating the disabled generic adapter as an invitation to enable a
    second WhatsApp gateway.
    """
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen
    from pathlib import Path

    bridge_url = os.environ.get("TGG_REPLY_BRIDGE_URL", "").strip().rstrip("/")
    if not bridge_url:
        return "Christopher WhatsApp bridge is not configured"

    def resolve_media_path(value: str) -> Path:
        text = str(value or "")
        from tools.python_sandbox_tool import agent_workspace_enabled

        if agent_workspace_enabled() and text.startswith(
            ("work/", "/work/", "records/", "/records/")
        ):
            # PA-203: send an immutable, digest-checked copy of the version
            # this job made or captured, never whatever sits at the path now.
            from tools.pa_agent_workspace import delivery_snapshot

            path, _sha = delivery_snapshot(
                text,
                str(workspace_owner or ""),
                any_file_allowed=allowed_work_paths is None,
            )
            return path
        if text.startswith(("work/", "/work/")):
            from tools.python_sandbox_tool import resolve_sandbox_work_path

            if allowed_work_paths is not None:
                relative = text[len("/work/"):] if text.startswith("/work/") else text[len("work/"):]
                if relative not in allowed_work_paths:
                    raise ValueError(
                        "this scheduled job did not produce that file; outside its "
                        "origin chat it may attach only files it made"
                    )
            return resolve_sandbox_work_path(text, str(workspace_owner or ""))
        candidate = Path(text)
        # Promoted refs are absolute-looking `/media/...` handles; every
        # other absolute local path retains the established cron behaviour.
        if candidate.is_absolute() and not text.startswith("/media/"):
            return candidate.resolve(strict=True)
        config = load_config() or {}
        pa = config.get("pa") if isinstance(config, dict) else None
        retention = pa.get("media_retention") if isinstance(pa, dict) else None
        root = Path(str((retention or {}).get("media_root") or "")).expanduser()
        prefix = str((retention or {}).get("media_ref_prefix") or "").rstrip("/")
        expected = f"{prefix}/"
        if not root.is_absolute() or not prefix or not text.startswith(expected):
            raise ValueError("untrusted promoted media reference")
        name = text[len(expected):]
        if not name or "/" in name or "\\" in name or name in {".", ".."}:
            raise ValueError("promoted media reference escapes configured root")
        resolved = (root / name).resolve(strict=True)
        if not resolved.is_file() or not resolved.is_relative_to(root.resolve()):
            raise ValueError("promoted media reference is unavailable")
        return resolved

    last_message_id = {"value": ""}

    def post(endpoint: str, payload: dict) -> Optional[str]:
        last_message_id["value"] = ""
        try:
            request = Request(
                f"{bridge_url}/{endpoint}",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=30) as response:
                status = int(getattr(response, "status", 0) or 0)
                body = json.loads(response.read() or b"{}")
            if status == 200 and body.get("success") is True:
                last_message_id["value"] = str(body.get("messageId") or "")
                return None
            return f"bridge {endpoint} outcome unconfirmed (http {status})"
        except HTTPError as exc:
            return f"bridge {endpoint} refused (http {exc.code})"
        except (URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            return f"bridge {endpoint} failed: {exc}"

    resolved_media_files: list[tuple[Path, bool]] = []
    for media_path, is_voice in media_files:
        try:
            resolved_media_files.append((resolve_media_path(str(media_path)), is_voice))
        except (OSError, ValueError) as exc:
            return (
                "attachment was not delivered; use the exact files[].path returned "
                f"by the sandbox or an existing /media/... reference ({exc})"
            )

    delivered = receipt if receipt is not None else {}
    delivered.setdefault("files", [])
    if text.strip():
        error = post("send", {"chatId": chat_id, "message": text})
        if error:
            return error
        delivered["text"] = text
        delivered["text_message_id"] = last_message_id["value"]
    for resolved_media, _is_voice in resolved_media_files:
        suffix = resolved_media.suffix.lower()
        media_type = (
            "image" if suffix in _IMAGE_EXTS else "video" if suffix in _VIDEO_EXTS
            else "audio" if suffix in {".aac", ".m4a", ".mp3", ".ogg", ".opus", ".wav"}
            else "document"
        )
        error = post("send-media", {
            "chatId": chat_id,
            "filePath": str(resolved_media),
            "mediaType": media_type,
            "fileName": resolved_media.name,
        })
        if error:
            return error
        delivered["files"].append(
            {
                "name": resolved_media.name,
                "message_id": last_message_id["value"],
                "workspace_path": _keep_delivered_file_in_workspace(
                    job,
                    chat_id,
                    resolved_media,
                    message_id=last_message_id["value"],
                    text_message_id=str(delivered.get("text_message_id") or ""),
                ),
            }
        )
    return None


def _keep_delivered_file_in_workspace(
    job: dict,
    chat_id: str,
    path,
    *,
    message_id: str = "",
    text_message_id: str = "",
) -> Optional[str]:
    """Keep a file a scheduled job delivered in the agent workspace (PA-203).

    A job may attach a file it never built, such as an existing workbook at a
    retained media path.  A later chat turn must be able to find exactly the
    file that was delivered, so the file is copied into the agent workspace
    (unless it is already there) and logged.  A failure here never turns a
    completed delivery into a failed one.
    """
    try:
        from tools.python_sandbox_tool import record_workspace_file

        return record_workspace_file(
            path,
            direction="scheduled_delivery",
            folder="sent",
            details={
                "chat_id": str(chat_id),
                "message_id": message_id,
                "text_message_id": text_message_id,
                "scheduled_job_id": str(job.get("id") or ""),
                "scheduled_job_name": str(job.get("name") or ""),
            },
        )
    except Exception as exc:
        logger.warning(
            "Job '%s': delivered file was not kept in the agent workspace: %s",
            job.get("id", "?"),
            exc,
        )
        return None


def _record_delivery_in_conversation(
    job: dict, conversation_session_key: str, receipt: dict
) -> bool:
    """Append a scheduled delivery to its Management conversation (PA-203).

    The delivery is stored as an assistant message in the originating
    conversation's history, the same way a normal reply is stored, so a
    later turn sees what was sent, when, and which workspace file it was.
    """
    text = str(receipt.get("text") or "").strip()
    files = list(receipt.get("files") or [])
    if not text and not files:
        return False
    when = _hermes_now()
    name = str(job.get("name") or job.get("id") or "scheduled job")
    lines = [
        f"[Scheduled job '{name}' delivered this to the chat at "
        f"{when:%H:%M} on {when.day} {when:%b}.]"
    ]
    if text:
        lines.append(text)
    for item in files:
        where = item.get("workspace_path")
        lines.append(
            f"[Attached file: /{where}]" if where else f"[Attached file: {item.get('name')}]"
        )
    try:
        from gateway.mirror import mirror_to_session_key

        return mirror_to_session_key(
            conversation_session_key, "\n".join(lines), source_label="cron"
        )
    except Exception as exc:
        logger.warning(
            "Job '%s': delivery was not recorded in its conversation: %s",
            job.get("id", "?"),
            exc,
        )
        return False


def _deliver_result(
    job: dict,
    content: str,
    adapters=None,
    loop=None,
    *,
    workspace_owner: str | None = None,
    published_paths: set[str] | None = None,
    origin_may_attach_any: bool = True,
    conversation_session_key: str | None = None,
) -> Optional[str]:
    """
    Deliver job output to the configured target(s) (origin chat, specific platform, etc.).

    ``published_paths`` (PA-203) is set when the job works in a folder it
    shares: its origin conversation's persistent folder, or the agent-wide
    workspace.  A delivery may attach any file of the folder only to the
    job's origin chat and only when ``origin_may_attach_any`` (a Management
    job); every other delivery may attach only the files this run's sandbox
    calls published.

    ``conversation_session_key`` (PA-203) is the originating Management
    conversation; a delivery to that origin chat is appended to its history.

    When ``adapters`` and ``loop`` are provided (gateway is running), tries to
    use the live adapter first — this supports E2EE rooms (e.g. Matrix) where
    the standalone HTTP path cannot encrypt.  Falls back to standalone send if
    the adapter path fails or is unavailable.

    Returns None on success, or an error string on failure.
    """
    targets = _resolve_delivery_targets(job)
    if not targets:
        if job.get("deliver", "local") != "local":
            msg = f"no delivery target resolved for deliver={job.get('deliver', 'local')}"
            logger.warning("Job '%s': %s", job["id"], msg)
            return msg
        return None  # local-only jobs don't deliver — not a failure

    from tools.send_message_tool import _send_to_platform
    from gateway.config import load_gateway_config, Platform

    # Optionally wrap the content with a header/footer so the user knows this
    # is a cron delivery.  Wrapping is on by default; set cron.wrap_response: false
    # in config.yaml for clean output.
    wrap_response = True
    try:
        user_cfg = load_config()
        wrap_response = user_cfg.get("cron", {}).get("wrap_response", True)
    except Exception:
        pass

    if wrap_response:
        task_name = job.get("name", job["id"])
        job_id = job.get("id", "")
        delivery_content = (
            f"Cronjob Response: {task_name}\n"
            f"(job_id: {job_id})\n"
            f"-------------\n\n"
            f"{content}\n\n"
            f"To stop or manage this job, send me a new message (e.g. \"stop reminder {task_name}\")."
        )
    else:
        delivery_content = content

    # Extract MEDIA: tags so attachments are forwarded as files, not raw text
    from gateway.platforms.base import BasePlatformAdapter
    media_files, cleaned_delivery_content = BasePlatformAdapter.extract_media(delivery_content)

    try:
        config = load_gateway_config()
    except Exception as e:
        msg = f"failed to load gateway config: {e}"
        logger.error("Job '%s': %s", job["id"], msg)
        return msg

    delivery_errors = []

    for target in targets:
        platform_name = target["platform"]
        chat_id = target["chat_id"]
        thread_id = target.get("thread_id")

        # Diagnostic: log thread_id for topic-aware delivery debugging
        origin = _resolve_origin(job) or {}
        origin_thread = origin.get("thread_id")
        if origin_thread and not thread_id:
            logger.warning(
                "Job '%s': origin has thread_id=%s but delivery target lost it "
                "(deliver=%s, target=%s)",
                job["id"], origin_thread, job.get("deliver", "local"), target,
            )
        elif thread_id:
            logger.debug(
                "Job '%s': delivering to %s:%s thread_id=%s",
                job["id"], platform_name, chat_id, thread_id,
            )

        # Built-in names resolve to their enum member; plugin platform names
        # create dynamic members via Platform._missing_().
        try:
            platform = Platform(platform_name.lower())
        except (ValueError, KeyError):
            msg = f"unknown platform '{platform_name}'"
            logger.warning("Job '%s': %s", job["id"], msg)
            delivery_errors.append(msg)
            continue

        pconfig = config.platforms.get(platform)
        if not pconfig or not pconfig.enabled:
            if platform == Platform.WHATSAPP and os.environ.get("TGG_REPLY_BRIDGE_URL", "").strip():
                origin_target = _resolve_origin(job) or {}
                to_origin = (
                    str(origin_target.get("platform") or "").lower() == platform_name.lower()
                    and str(origin_target.get("chat_id") or "") == str(chat_id)
                )
                bridge_receipt: dict = {}
                error = _deliver_whatsapp_via_tgg_reply_bridge(
                    job,
                    chat_id,
                    cleaned_delivery_content,
                    media_files,
                    receipt=bridge_receipt,
                    workspace_owner=workspace_owner,
                    allowed_work_paths=(
                        None
                        if published_paths is None
                        or (to_origin and origin_may_attach_any)
                        else set(published_paths)
                    ),
                )
                if to_origin and conversation_session_key:
                    # Whatever reached the chat is recorded, even when a
                    # later attachment in the same delivery failed.
                    _record_delivery_in_conversation(
                        job, conversation_session_key, bridge_receipt
                    )
                if error:
                    logger.error("Job '%s': %s", job["id"], error)
                    delivery_errors.append(error)
                else:
                    logger.info(
                        "Job '%s': delivered to whatsapp:%s via Christopher reply bridge",
                        job["id"], chat_id,
                    )
                continue
            msg = f"platform '{platform_name}' not configured/enabled"
            logger.warning("Job '%s': %s", job["id"], msg)
            delivery_errors.append(msg)
            continue

        # Prefer the live adapter when the gateway is running — this supports E2EE
        # rooms (e.g. Matrix) where the standalone HTTP path cannot encrypt.
        runtime_adapter = (adapters or {}).get(platform)
        delivered = False
        if runtime_adapter is not None and loop is not None and getattr(loop, "is_running", lambda: False)():
            send_metadata = {"thread_id": thread_id} if thread_id else None
            try:
                # Send cleaned text (MEDIA tags stripped) — not the raw content
                text_to_send = cleaned_delivery_content.strip()
                adapter_ok = True
                if text_to_send:
                    from agent.async_utils import safe_schedule_threadsafe
                    future = safe_schedule_threadsafe(
                        runtime_adapter.send(chat_id, text_to_send, metadata=send_metadata),
                        loop,
                    )
                    if future is None:
                        adapter_ok = False
                    else:
                        try:
                            send_result = future.result(timeout=60)
                        except TimeoutError:
                            future.cancel()
                            raise
                        if send_result and not getattr(send_result, "success", True):
                            err = getattr(send_result, "error", "unknown")
                            logger.warning(
                                "Job '%s': live adapter send to %s:%s failed (%s), falling back to standalone",
                                job["id"], platform_name, chat_id, err,
                            )
                            adapter_ok = False  # fall through to standalone path

                # Send extracted media files as native attachments via the live adapter
                if adapter_ok and media_files:
                    _send_media_via_adapter(
                        runtime_adapter,
                        chat_id,
                        media_files,
                        send_metadata,
                        loop,
                        job,
                        platform=platform,
                    )

                if adapter_ok:
                    logger.info("Job '%s': delivered to %s:%s via live adapter", job["id"], platform_name, chat_id)
                    delivered = True
            except Exception as e:
                logger.warning(
                    "Job '%s': live adapter delivery to %s:%s failed (%s), falling back to standalone",
                    job["id"], platform_name, chat_id, e,
                )

        if not delivered:
            # Standalone path: run the async send in a fresh event loop (safe from any thread)
            coro = _send_to_platform(platform, pconfig, chat_id, cleaned_delivery_content, thread_id=thread_id, media_files=media_files)
            try:
                result = asyncio.run(coro)
            except RuntimeError:
                # asyncio.run() checks for a running loop before awaiting the coroutine;
                # when it raises, the original coro was never started — close it to
                # prevent "coroutine was never awaited" RuntimeWarning, then retry in a
                # fresh thread that has no running loop.
                coro.close()
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, _send_to_platform(platform, pconfig, chat_id, cleaned_delivery_content, thread_id=thread_id, media_files=media_files))
                    result = future.result(timeout=30)
            except Exception as e:
                msg = f"delivery to {platform_name}:{chat_id} failed: {e}"
                logger.error("Job '%s': %s", job["id"], msg)
                delivery_errors.append(msg)
                continue

            if result and result.get("error"):
                msg = f"delivery error: {result['error']}"
                logger.error("Job '%s': %s", job["id"], msg)
                delivery_errors.append(msg)
                continue

            logger.info("Job '%s': delivered to %s:%s", job["id"], platform_name, chat_id)

    if delivery_errors:
        return "; ".join(delivery_errors)
    return None


_DEFAULT_SCRIPT_TIMEOUT = 120  # seconds
# Backward-compatible module override used by tests and emergency monkeypatches.
_SCRIPT_TIMEOUT = _DEFAULT_SCRIPT_TIMEOUT


def _get_script_timeout() -> int:
    """Resolve cron pre-run script timeout from module/env/config with a safe default."""
    if _SCRIPT_TIMEOUT != _DEFAULT_SCRIPT_TIMEOUT:
        try:
            timeout = int(float(_SCRIPT_TIMEOUT))
            if timeout > 0:
                return timeout
        except Exception:
            logger.warning("Invalid patched _SCRIPT_TIMEOUT=%r; using env/config/default", _SCRIPT_TIMEOUT)

    env_value = os.getenv("HERMES_CRON_SCRIPT_TIMEOUT", "").strip()
    if env_value:
        try:
            timeout = int(float(env_value))
            if timeout > 0:
                return timeout
        except Exception:
            logger.warning("Invalid HERMES_CRON_SCRIPT_TIMEOUT=%r; using config/default", env_value)

    try:
        cfg = load_config() or {}
        cron_cfg = cfg.get("cron", {}) if isinstance(cfg, dict) else {}
        configured = cron_cfg.get("script_timeout_seconds")
        if configured is not None:
            timeout = int(float(configured))
            if timeout > 0:
                return timeout
    except Exception as exc:
        logger.debug("Failed to load cron script timeout from config: %s", exc)

    return _DEFAULT_SCRIPT_TIMEOUT


def _run_job_script(script_path: str) -> tuple[bool, str]:
    """Execute a cron job's data-collection script and capture its output.

    Scripts must reside within HERMES_HOME/scripts/.  Both relative and
    absolute paths are resolved and validated against this directory to
    prevent arbitrary script execution via path traversal or absolute
    path injection.

    Supported interpreters (chosen by file extension):

    * ``.sh`` / ``.bash`` — run with ``/bin/bash``
    * anything else — run with the current Python interpreter
      (``sys.executable``), preserving the original behaviour for
      Python-based pre-check and data-collection scripts.

    Shell support lets ``no_agent=True`` jobs ship classic bash watchdogs
    (the `memory-watchdog.sh` pattern) without wrapping them in Python.

    Args:
        script_path: Path to the script.  Relative paths are resolved
            against HERMES_HOME/scripts/.  Absolute and ~-prefixed paths
            are also validated to ensure they stay within the scripts dir.

    Returns:
        (success, output) — on failure *output* contains the error message so the
        LLM can report the problem to the user.
    """
    from hermes_constants import get_hermes_home

    scripts_dir = _get_hermes_home() / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir_resolved = scripts_dir.resolve()

    raw = Path(script_path).expanduser()
    if raw.is_absolute():
        path = raw.resolve()
    else:
        path = (scripts_dir / raw).resolve()

    # Guard against path traversal, absolute path injection, and symlink
    # escape — scripts MUST reside within HERMES_HOME/scripts/.
    try:
        path.relative_to(scripts_dir_resolved)
    except ValueError:
        return False, (
            f"Blocked: script path resolves outside the scripts directory "
            f"({scripts_dir_resolved}): {script_path!r}"
        )

    if not path.exists():
        return False, f"Script not found: {path}"
    if not path.is_file():
        return False, f"Script path is not a file: {path}"

    script_timeout = _get_script_timeout()

    # Pick an interpreter by extension.  Bash for .sh/.bash, Python for
    # everything else.  We deliberately do NOT honour the file's own
    # shebang: the scripts dir is trusted, but keeping the interpreter
    # choice explicit here keeps the allowed surface small and auditable.
    suffix = path.suffix.lower()
    if suffix in {".sh", ".bash"}:
        # Resolve bash dynamically so Windows (Git Bash) and Linux/macOS
        # all work.  On native Windows without Git for Windows installed
        # shutil.which returns None — fall back to a clear error rather
        # than a FileNotFoundError with a confusing "[WinError 2]"
        # traceback.
        _bash = shutil.which("bash") or (
            "/bin/bash" if os.path.isfile("/bin/bash") else None
        )
        if _bash is None:
            return False, (
                f"Cannot run .sh/.bash script {path.name!r}: bash not found on PATH. "
                "On Windows, install Git for Windows (which ships Git Bash) "
                "or rewrite the script as Python (.py)."
            )
        argv = [_bash, str(path)]
    else:
        argv = [sys.executable, str(path)]

    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=script_timeout,
            cwd=str(path.parent),
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        # Redact secrets from both stdout and stderr before any return path.
        try:
            from agent.redact import redact_sensitive_text
            stdout = redact_sensitive_text(stdout)
            stderr = redact_sensitive_text(stderr)
        except Exception:
            pass

        if result.returncode != 0:
            parts = [f"Script exited with code {result.returncode}"]
            if stderr:
                parts.append(f"stderr:\n{stderr}")
            if stdout:
                parts.append(f"stdout:\n{stdout}")
            return False, "\n".join(parts)

        return True, stdout

    except subprocess.TimeoutExpired:
        return False, f"Script timed out after {script_timeout}s: {path}"
    except Exception as exc:
        return False, f"Script execution failed: {exc}"


def _parse_wake_gate(script_output: str) -> bool:
    """Parse the last non-empty stdout line of a cron job's pre-check script
    as a wake gate.

    The convention (ported from nanoclaw #1232): if the last stdout line is
    JSON like ``{"wakeAgent": false}``, the agent is skipped entirely — no
    LLM run, no delivery. Any other output (non-JSON, missing flag, gate
    absent, or ``wakeAgent: true``) means wake the agent normally.

    Returns True if the agent should wake, False to skip.
    """
    if not script_output:
        return True
    stripped_lines = [line for line in script_output.splitlines() if line.strip()]
    if not stripped_lines:
        return True
    last_line = stripped_lines[-1].strip()
    try:
        gate = json.loads(last_line)
    except (json.JSONDecodeError, ValueError):
        return True
    if not isinstance(gate, dict):
        return True
    return gate.get("wakeAgent", True) is not False


def _build_job_prompt(job: dict, prerun_script: Optional[tuple] = None) -> str:
    """Build the effective prompt for a cron job, optionally loading one or more skills first.

    Args:
        job: The cron job dict.
        prerun_script: Optional ``(success, stdout)`` from a script that has
            already been executed by the caller (e.g. for a wake-gate check).
            When provided, the script is not re-executed and the cached
            result is used for prompt injection. When omitted, the script
            (if any) runs inline as before.
    """
    prompt = str(job.get("prompt") or "")
    skills = job.get("skills")

    # Run data-collection script if configured, inject output as context.
    script_path = job.get("script")
    if script_path:
        if prerun_script is not None:
            success, script_output = prerun_script
        else:
            success, script_output = _run_job_script(script_path)
        if success:
            if script_output:
                prompt = (
                    "## Script Output\n"
                    "The following data was collected by a pre-run script. "
                    "Use it as context for your analysis.\n\n"
                    f"```\n{script_output}\n```\n\n"
                    f"{prompt}"
                )
            else:
                # Script produced no output — nothing to report, skip AI call.
                return None
        else:
            prompt = (
                "## Script Error\n"
                "The data-collection script failed. Report this to the user.\n\n"
                f"```\n{script_output}\n```\n\n"
                f"{prompt}"
            )

    # Inject output from referenced cron jobs as context.
    context_from = job.get("context_from")
    if context_from:
        from cron.jobs import OUTPUT_DIR
        if isinstance(context_from, str):
            context_from = [context_from]
        for source_job_id in context_from:
            # Guard against path traversal — valid job IDs are 12-char hex strings
            if not source_job_id or not all(c in "0123456789abcdef" for c in source_job_id):
                logger.warning("context_from: skipping invalid job_id %r", source_job_id)
                continue
            try:
                job_output_dir = OUTPUT_DIR / source_job_id
                if not job_output_dir.exists():
                    continue  # silent skip — no output yet
                output_files = sorted(
                    job_output_dir.glob("*.md"),
                    key=lambda f: f.stat().st_mtime,
                    reverse=True,
                )
                if not output_files:
                    continue  # silent skip — no output yet
                latest_output = output_files[0].read_text(encoding="utf-8").strip()
                # Truncate to 8K characters to avoid prompt bloat
                _MAX_CONTEXT_CHARS = 8000
                if len(latest_output) > _MAX_CONTEXT_CHARS:
                    latest_output = latest_output[:_MAX_CONTEXT_CHARS] + "\n\n[... output truncated ...]"
                if latest_output:
                    prompt = (
                        f"## Output from job '{source_job_id}'\n"
                        "The following is the most recent output from a preceding "
                        "cron job. Use it as context for your analysis.\n\n"
                        f"```\n{latest_output}\n```\n\n"
                        f"{prompt}"
                    )
                else:
                    continue  # silent skip — empty output
            except (OSError, PermissionError) as e:
                logger.warning("context_from: failed to read output for job %r: %s", source_job_id, e)
                # silent skip — do not pollute the prompt with error messages

    # Always prepend cron execution guidance so the agent knows how
    # delivery works and can suppress delivery when appropriate.
    cron_hint = (
        "[IMPORTANT: You are running as a scheduled cron job. "
        "DELIVERY: Your final response will be automatically delivered "
        "to the user — do NOT use send_message or try to deliver "
        "the output yourself. Just produce your report/output as your "
        "final response and the system handles the rest. "
        "SILENT: If there is genuinely nothing new to report, respond "
        "with exactly \"[SILENT]\" (nothing else) to suppress delivery. "
        "Never combine [SILENT] with content — either report your "
        "findings normally, or say [SILENT] and nothing more.]\n\n"
    )
    prompt = cron_hint + prompt
    if skills is None:
        legacy = job.get("skill")
        skills = [legacy] if legacy else []
    elif isinstance(skills, str):
        skills = [skills]

    skill_names = [str(name).strip() for name in skills if str(name).strip()]
    if not skill_names:
        return _scan_assembled_cron_prompt(prompt, job)

    from tools.skills_tool import skill_view
    from tools.skill_usage import bump_use

    parts = []
    skipped: list[str] = []
    for skill_name in skill_names:
        loaded = json.loads(skill_view(skill_name))
        if not loaded.get("success"):
            error = loaded.get("error") or f"Failed to load skill '{skill_name}'"
            logger.warning("Cron job '%s': skill not found, skipping — %s", job.get("name", job.get("id")), error)
            skipped.append(skill_name)
            continue

        # Bump usage so the curator sees this skill as actively used.
        try:
            bump_use(skill_name)
        except Exception:
            logger.debug("Cron job: failed to bump skill usage for '%s'", skill_name, exc_info=True)

        content = str(loaded.get("content") or "").strip()
        if parts:
            parts.append("")
        parts.extend(
            [
                f'[IMPORTANT: The user has invoked the "{skill_name}" skill, indicating they want you to follow its instructions. The full skill content is loaded below.]',
                "",
                content,
            ]
        )

    if skipped:
        notice = (
            f"[IMPORTANT: The following skill(s) were listed for this job but could not be found "
            f"and were skipped: {', '.join(skipped)}. "
            f"Start your response with a brief notice so the user is aware, e.g.: "
            f"'⚠️ Skill(s) not found and skipped: {', '.join(skipped)}']"
        )
        parts.insert(0, notice)

    if prompt:
        parts.extend(["", f"The user has provided the following instruction alongside the skill invocation: {prompt}"])
    return _scan_assembled_cron_prompt("\n".join(parts), job)


def _scan_assembled_cron_prompt(assembled: str, job: dict) -> str:
    """Scan the fully-assembled cron prompt (including skill content) for
    injection patterns. Raises ``CronPromptInjectionBlocked`` when a match
    fires so ``run_job`` can surface a clear refusal to the operator.

    Plugs the #3968 gap: ``_scan_cron_prompt`` runs on the user-supplied
    prompt at create/update, but skill content is loaded from disk at
    runtime and was never scanned. Since cron runs non-interactively
    (auto-approves tool calls), a malicious skill carrying an injection
    payload bypassed every gate.
    """
    from tools.cronjob_tools import _scan_cron_prompt

    scan_error = _scan_cron_prompt(assembled)
    if scan_error:
        job_label = job.get("name") or job.get("id") or "<unknown>"
        logger.warning(
            "Cron job '%s': assembled prompt blocked by injection scanner — %s",
            job_label,
            scan_error,
        )
        raise CronPromptInjectionBlocked(scan_error)
    return assembled


def run_job(job: dict) -> tuple[bool, str, str, Optional[str]]:
    """
    Execute a single cron job.
    
    Returns:
        Tuple of (success, full_output_doc, final_response, error_message)
    """
    job_id = job["id"]
    job_name = str(job.get("name") or job.get("prompt") or job_id or "cron job")

    # ---------------------------------------------------------------
    # no_agent short-circuit — the script IS the job, no LLM involvement.
    # ---------------------------------------------------------------
    # This mirrors the classic "run a bash script on a timer, send its
    # stdout to telegram" watchdog pattern. The agent path is skipped
    # entirely: no AIAgent, no prompt, no tool loop, no token spend.
    #
    # We check this BEFORE importing run_agent / constructing SessionDB so
    # a pure-script tick never pays for the agent machinery it isn't going
    # to use. Keep this block self-contained.
    #
    # Semantics:
    #   - script stdout (trimmed) → delivered verbatim as the final message
    #   - empty stdout            → silent run (no delivery, success=True)
    #   - non-zero exit / timeout → delivered as an error alert, success=False
    #   - wakeAgent=false gate    → treated like empty stdout (silent), since
    #                               the whole point of no_agent is that there
    #                               is no agent to wake
    if job.get("no_agent"):
        script_path = job.get("script")
        if not script_path:
            err = "no_agent=True but no script is set for this job"
            logger.error("Job '%s': %s", job_id, err)
            return False, "", "", err

        # Apply workdir if configured — lets scripts use predictable relative
        # paths. For no_agent jobs this is just the subprocess cwd (not an
        # agent TERMINAL_CWD bridge).
        _job_workdir = (job.get("workdir") or "").strip() or None
        _prior_cwd = None
        if _job_workdir and Path(_job_workdir).is_dir():
            _prior_cwd = os.getcwd()
            try:
                os.chdir(_job_workdir)
            except OSError:
                _prior_cwd = None

        try:
            ok, output = _run_job_script(script_path)
        finally:
            if _prior_cwd is not None:
                try:
                    os.chdir(_prior_cwd)
                except OSError:
                    pass

        now_iso = _hermes_now().strftime("%Y-%m-%d %H:%M:%S")

        if not ok:
            # Script crashed / timed out / exited non-zero.  Deliver the
            # error so the user knows the watchdog itself broke — silent
            # failure for an alerting job is the worst-case outcome.
            alert = (
                f"⚠ Cron watchdog '{job_name}' script failed\n\n"
                f"{output}\n\n"
                f"Time: {now_iso}"
            )
            doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** script failed\n\n"
                f"{output}\n"
            )
            return False, doc, alert, output

        # Honour the wakeAgent gate as a silent signal — `wakeAgent: false`
        # means "nothing to report this tick", same as empty stdout.
        if not _parse_wake_gate(output):
            logger.info(
                "Job '%s' (no_agent): wakeAgent=false gate — silent run", job_id
            )
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** silent (wakeAgent=false)\n"
            )
            return True, silent_doc, SILENT_MARKER, None

        if not output.strip():
            logger.info("Job '%s' (no_agent): empty stdout — silent run", job_id)
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {now_iso}\n"
                f"**Mode:** no_agent (script)\n"
                f"**Status:** silent (empty output)\n"
            )
            return True, silent_doc, SILENT_MARKER, None

        doc = (
            f"# Cron Job: {job_name}\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {now_iso}\n"
            f"**Mode:** no_agent (script)\n\n"
            f"---\n\n"
            f"{output}\n"
        )
        return True, doc, output, None

    # ---------------------------------------------------------------
    # Default (LLM) path — import and construct the agent machinery now
    # that we know we actually need it. Doing these imports here instead of
    # at module top keeps no_agent ticks from paying for AIAgent / SessionDB
    # construction costs.
    # ---------------------------------------------------------------
    from run_agent import AIAgent

    # Initialize SQLite session store so cron job messages are persisted
    # and discoverable via session_search (same pattern as gateway/run.py).
    _session_db = None
    try:
        from hermes_state import SessionDB
        _session_db = SessionDB()
    except Exception as e:
        logger.debug("Job '%s': SQLite session store not available: %s", job.get("id", "?"), e)

    # Wake-gate: if this job has a pre-check script, run it BEFORE building
    # the prompt so a ``{"wakeAgent": false}`` response can short-circuit
    # the whole agent run. We pass the result into _build_job_prompt so
    # the script is only executed once.
    prerun_script = None
    script_path = job.get("script")
    if script_path:
        prerun_script = _run_job_script(script_path)
        _ran_ok, _script_output = prerun_script
        if _ran_ok and not _parse_wake_gate(_script_output):
            logger.info(
                "Job '%s' (ID: %s): wakeAgent=false, skipping agent run",
                job_name, job_id,
            )
            silent_doc = (
                f"# Cron Job: {job_name}\n\n"
                f"**Job ID:** {job_id}\n"
                f"**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                "Script gate returned `wakeAgent=false` — agent skipped.\n"
            )
            return True, silent_doc, SILENT_MARKER, None

    try:
        prompt = _build_job_prompt(job, prerun_script=prerun_script)
    except CronPromptInjectionBlocked as block_exc:
        # Assembled prompt (user prompt + loaded skill content) tripped the
        # injection scanner. Refuse to run the agent this tick and surface
        # a clear failure to the operator so they see WHY the scheduled job
        # didn't run and can audit the offending skill.
        logger.warning(
            "Job '%s' (ID: %s): blocked by prompt-injection scanner — %s",
            job_name, job_id, block_exc,
        )
        blocked_doc = (
            f"# Cron Job: {job_name}\n\n"
            f"**Job ID:** {job_id}\n"
            f"**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"**Status:** BLOCKED\n\n"
            "The assembled prompt (user prompt + loaded skill content) tripped "
            "the cron injection scanner and the agent was NOT run.\n\n"
            f"**Scanner result:** {block_exc}\n\n"
            "Audit the skill(s) attached to this job for prompt-injection "
            "payloads or invisible-unicode markers. If the skill is legitimate "
            "and the match is a false positive, rephrase the content to avoid "
            "the threat pattern (`tools/cronjob_tools.py::_CRON_THREAT_PATTERNS`)."
        )
        return False, blocked_doc, "", str(block_exc)
    if prompt is None:
        logger.info("Job '%s': script produced no output, skipping AI call.", job_name)
        return True, "", SILENT_MARKER, None
    origin = _resolve_origin(job)
    _cron_session_id = str(job.get("_runtime_session_id") or "") or (
        f"cron_{job_id}_{_hermes_now().strftime('%Y%m%d_%H%M%S')}"
    )

    logger.info("Running job '%s' (ID: %s)", job_name, job_id)
    logger.info("Prompt: %s", prompt[:100])

    agent = None

    # Mark this as a cron session so the approval system can apply cron_mode.
    # This env var is process-wide and persists for the lifetime of the
    # scheduler process — every job this process runs is a cron job.
    os.environ["HERMES_CRON_SESSION"] = "1"

    # Use ContextVars for per-job session/delivery state so parallel jobs
    # don't clobber each other's targets (os.environ is process-global).
    from gateway.session_context import set_session_vars, clear_session_vars, _VAR_MAP

    # Cron execution is an internal scheduler context, not a live inbound
    # gateway message. Do not seed HERMES_SESSION_* contextvars from the
    # stored ``origin`` (which is delivery routing metadata, not a sender
    # identity). Several tool consumers branch on these vars during job
    # execution and would otherwise behave as if a real user from the
    # origin chat was driving the agent:
    #   - tools/terminal_tool.py: background-process notification routing
    #     (notify_on_complete / watch_patterns) reads HERMES_SESSION_PLATFORM
    #     and HERMES_SESSION_CHAT_ID to populate watcher_platform / chat_id,
    #     which would route completion notifications to the origin chat
    #     instead of via HERMES_CRON_AUTO_DELIVER_* below.
    #   - tools/tts_tool.py: picks Opus vs MP3 based on
    #     HERMES_SESSION_PLATFORM == "telegram".
    #   - tools/skills_tool.py + agent/prompt_builder.py: per-platform
    #     skill-disable lists and the system-prompt cache key both consume
    #     HERMES_SESSION_PLATFORM.
    #   - tools/send_message_tool.py: mirror source labelling and the
    #     send_message gate read HERMES_SESSION_PLATFORM.
    # Cron output delivery itself reads job["origin"] directly via
    # _resolve_origin(job) and the HERMES_CRON_AUTO_DELIVER_* vars set
    # below, so clearing HERMES_SESSION_* here does not affect delivery.
    _ctx_tokens = set_session_vars(
        platform="",
        chat_id="",
        chat_name="",
        session_key=_cron_session_id,
        session_id=_cron_session_id,
    )
    # Sandbox workspace owner (PA-203): a job created from a Management
    # conversation works in that conversation's persistent folder.  This binds
    # only the python_sandbox owner; HERMES_SESSION_KEY stays the cron run's,
    # so approvals, business-operation provenance and session tools still see
    # an internal scheduled run.  tick() resolves it once and passes it in so
    # delivery resolves attachments from the same folder.
    from tools.python_sandbox_tool import (
        bind_published_collector,
        bind_workspace_owner,
        reset_published_collector,
        reset_workspace_owner,
    )
    _published = job.get("_runtime_published")
    _published_token = (
        bind_published_collector(_published) if isinstance(_published, set) else None
    )
    _workspace_owner = (
        str(job.get("_runtime_workspace_owner") or "")
        or _resolve_cron_pa_workspace_owner(job)
        or _cron_session_id
    )
    _workspace_owner_token = bind_workspace_owner(_workspace_owner)
    _cron_delivery_vars = (
        "HERMES_CRON_AUTO_DELIVER_PLATFORM",
        "HERMES_CRON_AUTO_DELIVER_CHAT_ID",
        "HERMES_CRON_AUTO_DELIVER_THREAD_ID",
    )
    for _var_name in _cron_delivery_vars:
        _VAR_MAP[_var_name].set("")

    # Per-job working directory.  When set (and validated at create/update
    # time), we point TERMINAL_CWD at it so:
    #   - build_context_files_prompt() picks up AGENTS.md / CLAUDE.md /
    #     .cursorrules from the job's project dir, AND
    #   - the terminal, file, and code-exec tools run commands from there.
    #
    # tick() serializes workdir-jobs outside the parallel pool, so mutating
    # os.environ["TERMINAL_CWD"] here is safe for those jobs.  For workdir-less
    # jobs we leave TERMINAL_CWD untouched — preserves the original behaviour
    # (skip_context_files=True, tools use whatever cwd the scheduler has).
    _job_workdir = (job.get("workdir") or "").strip() or None
    if _job_workdir and not Path(_job_workdir).is_dir():
        # Directory was removed between create-time validation and now.  Log
        # and drop back to old behaviour rather than crashing the job.
        logger.warning(
            "Job '%s': configured workdir %r no longer exists — running without it",
            job_id, _job_workdir,
        )
        _job_workdir = None
    _prior_terminal_cwd = os.environ.get("TERMINAL_CWD", "_UNSET_")
    if _job_workdir:
        os.environ["TERMINAL_CWD"] = _job_workdir
        logger.info("Job '%s': using workdir %s", job_id, _job_workdir)

    try:
        # Re-read .env and config.yaml fresh every run so provider/key
        # changes take effect without a gateway restart.
        from dotenv import load_dotenv
        try:
            load_dotenv(str(_get_hermes_home() / ".env"), override=True, encoding="utf-8")
        except UnicodeDecodeError:
            load_dotenv(str(_get_hermes_home() / ".env"), override=True, encoding="latin-1")

        delivery_target = _resolve_delivery_target(job)
        if delivery_target:
            _VAR_MAP["HERMES_CRON_AUTO_DELIVER_PLATFORM"].set(delivery_target["platform"])
            _VAR_MAP["HERMES_CRON_AUTO_DELIVER_CHAT_ID"].set(str(delivery_target["chat_id"]))
            _VAR_MAP["HERMES_CRON_AUTO_DELIVER_THREAD_ID"].set(
                ""
                if delivery_target.get("thread_id") is None
                else str(delivery_target["thread_id"])
            )

        model = job.get("model") or os.getenv("HERMES_MODEL") or ""

        # Load config.yaml for model, reasoning, prefill, toolsets, provider routing
        _cfg = {}
        try:
            import yaml
            _cfg_path = str(_get_hermes_home() / "config.yaml")
            if os.path.exists(_cfg_path):
                with open(_cfg_path, encoding="utf-8") as _f:
                    _cfg = yaml.safe_load(_f) or {}
                _cfg = _expand_env_vars(_cfg)
                _model_cfg = _cfg.get("model", {})
                if not job.get("model"):
                    if isinstance(_model_cfg, str):
                        model = _model_cfg
                    elif isinstance(_model_cfg, dict):
                        model = _model_cfg.get("default", model)
        except Exception as e:
            logger.warning("Job '%s': failed to load config.yaml, using defaults: %s", job_id, e)

        # Apply IPv4 preference if configured.
        try:
            from hermes_constants import apply_ipv4_preference
            _net_cfg = _cfg.get("network", {})
            if isinstance(_net_cfg, dict) and _net_cfg.get("force_ipv4"):
                apply_ipv4_preference(force=True)
        except Exception:
            pass

        # Reasoning config from config.yaml
        from hermes_constants import parse_reasoning_effort
        effort = str(_cfg.get("agent", {}).get("reasoning_effort", "")).strip()
        reasoning_config = parse_reasoning_effort(effort)

        # Prefill messages from env or config.yaml
        prefill_messages = None
        prefill_file = os.getenv("HERMES_PREFILL_MESSAGES_FILE", "") or _cfg.get("prefill_messages_file", "")
        if prefill_file:
            pfpath = Path(prefill_file).expanduser()
            if not pfpath.is_absolute():
                pfpath = _get_hermes_home() / pfpath
            if pfpath.exists():
                try:
                    with open(pfpath, "r", encoding="utf-8") as _pf:
                        prefill_messages = json.load(_pf)
                    if not isinstance(prefill_messages, list):
                        prefill_messages = None
                except Exception as e:
                    logger.warning("Job '%s': failed to parse prefill messages file '%s': %s", job_id, pfpath, e)
                    prefill_messages = None

        # Max iterations
        max_iterations = _cfg.get("agent", {}).get("max_turns") or _cfg.get("max_turns") or 90

        # Provider routing
        pr = _cfg.get("provider_routing", {})

        from hermes_cli.runtime_provider import (
            resolve_runtime_provider,
            format_runtime_provider_error,
        )
        from hermes_cli.auth import AuthError
        try:
            # Do not inject HERMES_INFERENCE_PROVIDER here. resolve_runtime_provider()
            # already prefers persisted config over stale shell/env overrides when
            # no explicit provider is requested. Passing the env var here short-
            # circuits that precedence and can resurrect old providers (for
            # example DeepSeek) for cron jobs that do not pin provider/model.
            runtime_kwargs = {
                "requested": job.get("provider"),
            }
            if job.get("base_url"):
                runtime_kwargs["explicit_base_url"] = job.get("base_url")
            runtime = resolve_runtime_provider(**runtime_kwargs)
        except AuthError as auth_exc:
            # Primary provider auth failed — try fallback chain before giving up.
            logger.warning("Job '%s': primary auth failed (%s), trying fallback", job_id, auth_exc)
            fb = _cfg.get("fallback_providers") or _cfg.get("fallback_model")
            fb_list = (fb if isinstance(fb, list) else [fb]) if fb else []
            runtime = None
            for entry in fb_list:
                if not isinstance(entry, dict):
                    continue
                try:
                    fb_kwargs = {"requested": entry.get("provider")}
                    if entry.get("base_url"):
                        fb_kwargs["explicit_base_url"] = entry["base_url"]
                    if entry.get("api_key"):
                        fb_kwargs["explicit_api_key"] = entry["api_key"]
                    runtime = resolve_runtime_provider(**fb_kwargs)
                    logger.info("Job '%s': fallback resolved to %s", job_id, runtime.get("provider"))
                    break
                except Exception as fb_exc:
                    logger.debug("Job '%s': fallback %s failed: %s", job_id, entry.get("provider"), fb_exc)
            if runtime is None:
                raise RuntimeError(format_runtime_provider_error(auth_exc)) from auth_exc
        except Exception as exc:
            message = format_runtime_provider_error(exc)
            raise RuntimeError(message) from exc

        fallback_model = _cfg.get("fallback_providers") or _cfg.get("fallback_model") or None
        credential_pool = None
        runtime_provider = str(runtime.get("provider") or "").strip().lower()
        if runtime_provider:
            try:
                from agent.credential_pool import load_pool
                pool = load_pool(runtime_provider)
                if pool.has_credentials():
                    credential_pool = pool
                    logger.info(
                        "Job '%s': loaded credential pool for provider %s with %d entries",
                        job_id,
                        runtime_provider,
                        len(pool.entries()),
                    )
            except Exception as e:
                logger.debug("Job '%s': failed to load credential pool for %s: %s", job_id, runtime_provider, e)

        # Initialize MCP servers so configured mcp_servers are available to
        # the agent's tool registry before AIAgent is constructed. Without
        # this, cron jobs never saw any MCP tools — only the gateway / CLI
        # paths called discover_mcp_tools() at startup. Idempotent: subsequent
        # ticks short-circuit on already-connected servers inside
        # register_mcp_servers(). Non-fatal on failure: a broken MCP server
        # shouldn't kill an otherwise-working cron job. See #4219.
        try:
            from tools.mcp_tool import discover_mcp_tools
            _mcp_tools = discover_mcp_tools()
            if _mcp_tools:
                logger.info(
                    "Job '%s': %d MCP tool(s) available",
                    job_id, len(_mcp_tools),
                )
        except Exception as _mcp_exc:
            logger.warning(
                "Job '%s': MCP initialization failed (non-fatal): %s",
                job_id, _mcp_exc,
            )

        pa_resolved_context = _resolve_cron_pa_context(job, _cfg)
        # Dedicated PA business tools resolve their bridge from task-local
        # runtime context.  Bind the already-authorized cron brief here; this
        # does not make cron look like an inbound WhatsApp message.
        from gateway.session_context import set_session_pa_job_type
        set_session_pa_job_type(getattr(pa_resolved_context, "job_type", None))
        cron_enabled_toolsets, cron_disabled_toolsets = _merge_cron_pa_toolsets(
            _resolve_cron_enabled_toolsets(job, _cfg),
            # Generic cron exclusions remain the default. The PA toolset
            # adapter can honor an explicitly configured read-only TGG
            # delegation route without reopening messaging or scheduling.
            ["cronjob", "messaging", "clarify", "delegation"],
            pa_resolved_context,
        )
        _record_cron_pa_behavior_event(
            _session_db,
            pa_resolved_context,
            job,
            _cron_session_id,
        )

        agent = AIAgent(
            model=model,
            api_key=runtime.get("api_key"),
            base_url=runtime.get("base_url"),
            provider=runtime.get("provider"),
            api_mode=runtime.get("api_mode"),
            acp_command=runtime.get("command"),
            acp_args=runtime.get("args"),
            max_iterations=max_iterations,
            reasoning_config=reasoning_config,
            prefill_messages=prefill_messages,
            fallback_model=fallback_model,
            credential_pool=credential_pool,
            providers_allowed=pr.get("only"),
            providers_ignored=pr.get("ignore"),
            providers_order=pr.get("order"),
            provider_sort=pr.get("sort"),
            openrouter_min_coding_score=(_cfg.get("openrouter") or {}).get("min_coding_score"),
            enabled_toolsets=cron_enabled_toolsets,
            disabled_toolsets=cron_disabled_toolsets,
            ephemeral_system_prompt=_render_cron_pa_ephemeral_prompt(pa_resolved_context) or None,
            quiet_mode=True,
            # Cron jobs should always inherit the user's SOUL.md identity from
            # HERMES_HOME. When a workdir is configured, also inject project
            # context files (AGENTS.md / CLAUDE.md / .cursorrules) from there.
            # Without a workdir, keep cwd context discovery disabled.
            skip_context_files=not bool(_job_workdir),
            load_soul_identity=True,
            skip_memory=True,  # Cron system prompts would corrupt user representations
            platform="cron",
            session_id=_cron_session_id,
            gateway_session_key=_cron_session_id,
            session_db=_session_db,
        )
        # A delegated child of a scheduled Management job gets only the
        # read-only investigation toolset, whatever the parent asks for.
        from agent.pa_delegation import scope_delegation_children
        scope_delegation_children(agent, cron_enabled_toolsets, pa_resolved_context)
        
        # Run the agent with an *inactivity*-based timeout: the job can run
        # for hours if it's actively calling tools / receiving stream tokens,
        # but a hung API call or stuck tool with no activity for the configured
        # duration is caught and killed.  Default 600s (10 min inactivity);
        # override via HERMES_CRON_TIMEOUT env var.  0 = unlimited.
        #
        # Uses the agent's built-in activity tracker (updated by
        # _touch_activity() on every tool call, API call, and stream delta).
        _raw_cron_timeout = os.getenv("HERMES_CRON_TIMEOUT", "").strip()
        if _raw_cron_timeout:
            try:
                _cron_timeout = float(_raw_cron_timeout)
            except (ValueError, TypeError):
                logger.warning(
                    "Invalid HERMES_CRON_TIMEOUT=%r; using default 600s",
                    _raw_cron_timeout,
                )
                _cron_timeout = 600.0
        else:
            _cron_timeout = 600.0
        _cron_inactivity_limit = _cron_timeout if _cron_timeout > 0 else None
        _POLL_INTERVAL = 5.0
        _cron_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # Preserve scheduler-scoped ContextVar state (for example skill-declared
        # env passthrough registrations) when the cron run hops into the worker
        # thread used for inactivity timeout monitoring.
        _cron_context = contextvars.copy_context()
        _cron_future = _cron_pool.submit(_cron_context.run, agent.run_conversation, prompt)
        _inactivity_timeout = False
        try:
            if _cron_inactivity_limit is None:
                # Unlimited — just wait for the result.
                result = _cron_future.result()
            else:
                result = None
                while True:
                    done, _ = concurrent.futures.wait(
                        {_cron_future}, timeout=_POLL_INTERVAL,
                    )
                    if done:
                        result = _cron_future.result()
                        break
                    # Agent still running — check inactivity.
                    _idle_secs = 0.0
                    if hasattr(agent, "get_activity_summary"):
                        try:
                            _act = agent.get_activity_summary()
                            _idle_secs = _act.get("seconds_since_activity", 0.0)
                        except Exception:
                            pass
                    if _idle_secs >= _cron_inactivity_limit:
                        _inactivity_timeout = True
                        break
        except Exception:
            _cron_pool.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            _cron_pool.shutdown(wait=False, cancel_futures=True)

        if _inactivity_timeout:
            # Build diagnostic summary from the agent's activity tracker.
            _activity = {}
            if hasattr(agent, "get_activity_summary"):
                try:
                    _activity = agent.get_activity_summary()
                except Exception:
                    pass
            _last_desc = _activity.get("last_activity_desc", "unknown")
            _secs_ago = _activity.get("seconds_since_activity", 0)
            _cur_tool = _activity.get("current_tool")
            _iter_n = _activity.get("api_call_count", 0)
            _iter_max = _activity.get("max_iterations", 0)

            logger.error(
                "Job '%s' idle for %.0fs (inactivity limit %.0fs) "
                "| last_activity=%s | iteration=%s/%s | tool=%s",
                job_name, _secs_ago, _cron_inactivity_limit,
                _last_desc, _iter_n, _iter_max,
                _cur_tool or "none",
            )
            if hasattr(agent, "interrupt"):
                agent.interrupt("Cron job timed out (inactivity)")
            raise TimeoutError(
                f"Cron job '{job_name}' idle for "
                f"{int(_secs_ago)}s (limit {int(_cron_inactivity_limit)}s) "
                f"— last activity: {_last_desc}"
            )

        # Guard against non-dict returns from run_conversation under error conditions
        if not isinstance(result, dict):
            raise RuntimeError(
                f"agent.run_conversation returned {type(result).__name__} instead of dict: {result!r}"
            )

        # If the agent itself reported failure (e.g. all retries exhausted on
        # API errors, model abort, mid-run interrupt), do not silently mark the
        # job as successful. run_agent populates `failed=True`/`completed=False`
        # on these paths and may put the error into `final_response`, which
        # would otherwise be delivered as if it were the agent's reply and the
        # job's `last_status` set to "ok". Raise so the except handler below
        # builds the proper failure tuple. (issue #17855)
        if result.get("failed") is True or result.get("completed") is False:
            _err_text = (
                result.get("error")
                or (result.get("final_response") or "").strip()
                or "agent reported failure"
            )
            raise RuntimeError(_err_text)

        final_response = result.get("final_response", "") or ""
        # Strip leaked placeholder text that upstream may inject on empty completions.
        if final_response.strip() == "(No response generated)":
            final_response = ""
        # Use a separate variable for log display; keep final_response clean
        # for delivery logic (empty response = no delivery).
        logged_response = final_response if final_response else "(No response generated)"
        
        output = f"""# Cron Job: {job_name}

**Job ID:** {job_id}
**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}
**Schedule:** {job.get('schedule_display', 'N/A')}

## Prompt

{prompt}

## Response

{logged_response}
"""
        
        logger.info("Job '%s' completed successfully", job_name)
        return True, output, final_response, None
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        logger.exception("Job '%s' failed: %s", job_name, error_msg)
        
        output = f"""# Cron Job: {job_name} (FAILED)

**Job ID:** {job_id}
**Run Time:** {_hermes_now().strftime('%Y-%m-%d %H:%M:%S')}
**Schedule:** {job.get('schedule_display', 'N/A')}

## Prompt

{prompt}

## Error

```
{error_msg}
```
"""
        return False, output, "", error_msg

    finally:
        # Restore TERMINAL_CWD to whatever it was before this job ran.  We
        # only ever mutate it when the job has a workdir; see the setup block
        # at the top of run_job for the serialization guarantee.
        if _job_workdir:
            if _prior_terminal_cwd == "_UNSET_":
                os.environ.pop("TERMINAL_CWD", None)
            else:
                os.environ["TERMINAL_CWD"] = _prior_terminal_cwd
        # Clean up ContextVar session/delivery state for this job.
        clear_session_vars(_ctx_tokens)
        try:
            reset_workspace_owner(_workspace_owner_token)
            if _published_token is not None:
                reset_published_collector(_published_token)
        except ValueError:
            # Reset from a different context; the tick's per-job copied
            # context is discarded after the job anyway.
            pass
        for _var_name in _cron_delivery_vars:
            _VAR_MAP[_var_name].set("")
        if _session_db:
            try:
                _session_db.end_session(_cron_session_id, "cron_complete")
            except (Exception, KeyboardInterrupt) as e:
                logger.debug("Job '%s': failed to end session: %s", job_id, e)
            try:
                _session_db.close()
            except (Exception, KeyboardInterrupt) as e:
                logger.debug("Job '%s': failed to close SQLite session store: %s", job_id, e)
        # Release subprocesses, terminal sandboxes, browser daemons, and the
        # main OpenAI/httpx client held by this ephemeral cron agent. Without
        # this, a gateway that ticks cron every N minutes leaks fds per job
        # until it hits EMFILE (#10200 / "too many open files").
        try:
            if agent is not None:
                agent.close()
        except (Exception, KeyboardInterrupt) as e:
            logger.debug("Job '%s': failed to close agent resources: %s", job_id, e)
        # Each cron run spins up a short-lived worker thread whose event loop
        # dies as soon as the ``ThreadPoolExecutor`` shuts down. Any async
        # httpx clients cached under that loop are now unusable — reap them
        # so their transports don't accumulate in the process-global cache.
        try:
            from agent.auxiliary_client import cleanup_stale_async_clients
            cleanup_stale_async_clients()
        except Exception as e:
            logger.debug("Job '%s': failed to reap stale auxiliary clients: %s", job_id, e)


def tick(verbose: bool = True, adapters=None, loop=None) -> int:
    """
    Check and run all due jobs.
    
    Uses a file lock so only one tick runs at a time, even if the gateway's
    in-process ticker and a standalone daemon or manual tick overlap.
    
    Args:
        verbose: Whether to print status messages
        adapters: Optional dict mapping Platform → live adapter (from gateway)
        loop: Optional asyncio event loop (from gateway) for live adapter sends
    
    Returns:
        Number of jobs executed (0 if another tick is already running)
    """
    lock_dir, lock_file = _get_lock_paths()
    lock_dir.mkdir(parents=True, exist_ok=True)

    # Cross-platform file locking: fcntl on Unix, msvcrt on Windows
    lock_fd = None
    try:
        lock_fd = open(lock_file, "w", encoding="utf-8")
        if fcntl:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt:
            msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
    except (OSError, IOError):
        logger.debug("Tick skipped — another instance holds the lock")
        if lock_fd is not None:
            lock_fd.close()
        return 0

    try:
        due_jobs = get_due_jobs()

        if verbose and not due_jobs:
            logger.info("%s - No jobs due", _hermes_now().strftime('%H:%M:%S'))
            return 0

        if verbose:
            logger.info("%s - %s job(s) due", _hermes_now().strftime('%H:%M:%S'), len(due_jobs))

        # Advance next_run_at for all recurring jobs FIRST, under the file lock,
        # before any execution begins.  This preserves at-most-once semantics.
        for job in due_jobs:
            advance_next_run(job["id"])

        # Resolve max parallel workers: env var > config.yaml > unbounded.
        # Set HERMES_CRON_MAX_PARALLEL=1 to restore old serial behaviour.
        _max_workers: Optional[int] = None
        try:
            _env_par = os.getenv("HERMES_CRON_MAX_PARALLEL", "").strip()
            if _env_par:
                _max_workers = int(_env_par) or None
        except (ValueError, TypeError):
            logger.warning("Invalid HERMES_CRON_MAX_PARALLEL value; defaulting to unbounded")
        if _max_workers is None:
            try:
                _ucfg = load_config() or {}
                _cfg_par = (
                    _ucfg.get("cron", {}) if isinstance(_ucfg, dict) else {}
                ).get("max_parallel_jobs")
                if _cfg_par is not None:
                    _max_workers = int(_cfg_par) or None
            except Exception:
                pass

        if verbose:
            logger.info(
                "Running %d job(s) in parallel (max_workers=%s)",
                len(due_jobs),
                _max_workers if _max_workers else "unbounded",
            )

        def _process_job(job: dict) -> bool:
            """Run one due job end-to-end: execute, save, deliver, mark."""
            try:
                cron_session_id = (
                    f"cron_{job['id']}_{_hermes_now().strftime('%Y%m%d_%H%M%S')}"
                )
                run_input = dict(job)
                run_input["_runtime_session_id"] = cron_session_id
                # One owner for the whole scheduled run: the job's sandbox
                # work and its attachment delivery use the same folder.
                management_owner = _resolve_cron_pa_workspace_owner(job)
                workspace_owner = management_owner or cron_session_id
                run_input["_runtime_workspace_owner"] = workspace_owner
                # With one agent-wide workspace (PA-203 round 2) every job
                # shares the folder, so every job's delivery is bounded:
                # only a Management job may attach any folder file, and only
                # to its origin chat.
                from tools.python_sandbox_tool import agent_workspace_enabled

                shared_workspace = agent_workspace_enabled()
                # Files this run's sandbox calls publish.  A delivery to any
                # chat other than the job's origin may attach only these.
                published: set[str] = set()
                run_input["_runtime_published"] = published
                success, output, final_response, error = run_job(run_input)

                output_file = save_job_output(job["id"], output)
                if verbose:
                    logger.info("Output saved to: %s", output_file)

                # Deliver the final response to the origin/target chat.
                # If the agent responded with [SILENT], skip delivery (but
                # output is already saved above).  Failed jobs always deliver.
                deliver_content = final_response if success else f"⚠️ Cron job '{job.get('name', job['id'])}' failed:\n{error}"
                should_deliver = bool(deliver_content)
                if should_deliver and success and SILENT_MARKER in deliver_content.strip().upper():
                    logger.info("Job '%s': agent returned %s — skipping delivery", job["id"], SILENT_MARKER)
                    should_deliver = False

                delivery_error = None
                if should_deliver:
                    try:
                        delivery_error = _deliver_result(
                            job,
                            deliver_content,
                            adapters=adapters,
                            loop=loop,
                            workspace_owner=workspace_owner,
                            published_paths=(
                                published
                                if management_owner or shared_workspace
                                else None
                            ),
                            origin_may_attach_any=management_owner is not None,
                            conversation_session_key=management_owner,
                        )
                    except Exception as de:
                        delivery_error = str(de)
                        logger.error("Delivery failed for job %s: %s", job["id"], de)

                # Treat empty final_response as a soft failure so last_status
                # is not "ok" — the agent ran but produced nothing useful.
                # (issue #8585)
                if success and not final_response:
                    success = False
                    error = "Agent completed but produced empty response (model error, timeout, or misconfiguration)"

                mark_job_run(
                    job["id"],
                    success and not delivery_error,
                    error,
                    delivery_error=delivery_error,
                )
                return True

            except Exception as e:
                logger.error("Error processing job %s: %s", job['id'], e)
                mark_job_run(job["id"], False, str(e))
                return False

        # Partition due jobs: those with a per-job workdir mutate
        # os.environ["TERMINAL_CWD"] inside run_job, which is process-global —
        # so they MUST run sequentially to avoid corrupting each other.  Jobs
        # without a workdir leave env untouched and stay parallel-safe.
        workdir_jobs = [j for j in due_jobs if (j.get("workdir") or "").strip()]
        parallel_jobs = [j for j in due_jobs if not (j.get("workdir") or "").strip()]

        _results: list = []

        # Sequential pass for workdir jobs.
        for job in workdir_jobs:
            _ctx = contextvars.copy_context()
            _results.append(_ctx.run(_process_job, job))

        # Parallel pass for the rest — same behaviour as before.
        if parallel_jobs:
            with concurrent.futures.ThreadPoolExecutor(max_workers=_max_workers) as _tick_pool:
                _futures = []
                for job in parallel_jobs:
                    _ctx = contextvars.copy_context()
                    _futures.append(_tick_pool.submit(_ctx.run, _process_job, job))
                _results.extend(f.result() for f in _futures)

        # Best-effort sweep of MCP stdio subprocesses that survived their
        # session teardown during this tick.  Runs AFTER every job has
        # finished so active sessions (including live user chats) are
        # never touched — only PIDs explicitly detected as orphans in
        # tools.mcp_tool._run_stdio's finally block are reaped.
        try:
            from tools.mcp_tool import _kill_orphaned_mcp_children
            _kill_orphaned_mcp_children()
        except Exception as _e:
            logger.debug("Post-tick MCP orphan cleanup failed: %s", _e)

        return sum(_results)
    finally:
        if fcntl:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        elif msvcrt:
            try:
                msvcrt.locking(lock_fd.fileno(), msvcrt.LK_UNLCK, 1)
            except (OSError, IOError):
                pass
        lock_fd.close()


if __name__ == "__main__":
    tick(verbose=True)
