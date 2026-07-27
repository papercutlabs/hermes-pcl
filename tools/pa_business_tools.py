"""Opt-in PA business-fact bridge tools.

The bridge deliberately keeps business facts outside Hermes-owned state. It
only calls configured HTTP endpoints or local commands and returns their JSON
results to the caller.
"""
from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

from tools.registry import registry, tool_error, tool_result


DEFAULT_TIMEOUT_SECONDS = 30.0

ILINKED_READ_OPERATIONS = frozenset(
    {"ilinked_lookup", "ilinked_status", "ilinked_wc_lookup"}
)
ILINKED_EXPLICIT_CUES = ("ilinked", "hdb", "source-system", "source system")
WC_EXPLICIT_CUES = ("wc", "work costing", "costing")
ILINKED_ALLOW_PAYLOAD_KEYS = (
    "explicit_source_system",
    "source_system_requested",
    "ilinked_requested",
)
JOB_NO_RE = r"^[A-Z]{2}/JOB/\d{4}/\d{4}$"


@dataclass(frozen=True)
class PABusinessOperation:
    name: str
    kind: str
    method: str = "POST"
    url: str | None = None
    command: tuple[str, ...] | None = None
    headers: Mapping[str, str] | None = None
    tenant: str | None = None
    auth: Mapping[str, Any] | None = None
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    path_params: tuple[str, ...] = ()


@dataclass(frozen=True)
class PABusinessBridgeConfig:
    operations: Mapping[str, PABusinessOperation]
    tenant: str | None = None
    auth: Mapping[str, Any] | None = None
    # Operations that ARE configured for this client but are scoped out of the
    # resolved job brief (per-chat toolset scoping). They are absent from
    # ``operations`` so they can never execute; kept here only so the refusal
    # can say "not permitted in this chat" instead of "unknown operation".
    denied_operations: frozenset[str] = frozenset()
    media_root: Path | None = None
    media_ref_prefix: str = "/media"


class OperationNotPermitted(ValueError):
    """Raised when a configured operation is scoped out of the active job brief.

    This is the per-chat tool-scoping guarantee: the mgmt chat and the ingest
    chats share one process and one client operation registry, so the brief's
    ``business_operations`` block is what mechanically separates them.
    """

    code = "OPERATION_NOT_PERMITTED"

    def __init__(self, *, operation: str, job_type: str | None, permitted: Iterable[str]):
        self.operation = operation
        self.job_type = job_type
        self.permitted = tuple(sorted(permitted))
        scope = f"job brief {job_type!r}" if job_type else "the active job brief"
        super().__init__(
            "OPERATION_NOT_PERMITTED: "
            f"operation {operation!r} is not available in this chat ({scope}). "
            f"Permitted operations: {', '.join(self.permitted) or 'none'}"
        )


class TenantScopeMismatch(ValueError):
    """Raised when a PA business operation crosses the resolved client tenant."""

    code = "TENANT_SCOPE_MISMATCH"

    def __init__(self, *, current_tenant: str | None, target_tenant: str | None, operation: str):
        self.current_tenant = current_tenant
        self.target_tenant = target_tenant
        self.operation = operation
        super().__init__(
            "TENANT_SCOPE_MISMATCH: "
            f"operation {operation!r} targets tenant {target_tenant!r} "
            f"from resolved tenant {current_tenant!r}"
        )


def _bridge_section(config: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(config, Mapping):
        return {}
    pa_section = config.get("pa")
    if isinstance(pa_section, Mapping):
        nested = pa_section.get("business")
        if isinstance(nested, Mapping):
            return nested
        for key in ("pa_business", "pa-business", "pa_business_bridge"):
            value = pa_section.get(key)
            if isinstance(value, Mapping):
                return value
    for key in ("pa_business", "pa-business", "pa_business_bridge"):
        value = config.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def load_business_bridge_config(
    config: Mapping[str, Any] | None,
    *,
    pa_context: Any | None = None,
) -> PABusinessBridgeConfig:
    """Parse bridge config from a Hermes-style config mapping.

    Expected shape:

        pa_business:
          operations:
            lookup_case:
              type: http
              url: http://127.0.0.1:8080/cases/lookup
              method: POST
              headers: {X-Bridge: pa}
            local_check:
              type: command
              command: [python, -c, "..."]

    Unknown or absent configuration produces an empty, inactive bridge.
    """
    section = _bridge_section(config)
    client_bridge = _client_business_bridge(pa_context)
    tenant = _client_tenant(pa_context)
    auth = _client_auth(pa_context)
    if isinstance(client_bridge.get("auth"), Mapping):
        auth = client_bridge["auth"]
    if client_bridge.get("tenant"):
        tenant = str(client_bridge.get("tenant"))
    media_root = _configured_media_root(config, client_bridge=client_bridge)
    media_ref_prefix = _configured_media_ref_prefix(
        config, client_bridge=client_bridge
    )
    raw_operations = section.get("operations", {})
    client_operations = client_bridge.get("operations", {})
    if client_operations:
        if not isinstance(client_operations, Mapping):
            raise ValueError("constitution.client.business_bridge.operations must be a mapping")
        raw_operations = {**dict(raw_operations), **dict(client_operations)}
    if not isinstance(raw_operations, Mapping):
        raise ValueError("pa_business.operations must be a mapping")

    operations: dict[str, PABusinessOperation] = {}
    for name, raw in raw_operations.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"operation {name!r} must be a mapping")

        op_name = str(name)
        kind = str(raw.get("type") or raw.get("kind") or "").strip().lower()
        if kind not in {"http", "command"}:
            raise ValueError(f"operation {op_name!r} type must be 'http' or 'command'")

        timeout = float(raw.get("timeout", DEFAULT_TIMEOUT_SECONDS))
        if timeout <= 0:
            raise ValueError(f"operation {op_name!r} timeout must be positive")

        if kind == "http":
            url = str(raw.get("url") or "").strip()
            if not url:
                raise ValueError(f"operation {op_name!r} requires url")
            method = str(raw.get("method") or "POST").strip().upper()
            headers = raw.get("headers") or {}
            if not isinstance(headers, Mapping):
                raise ValueError(f"operation {op_name!r} headers must be a mapping")
            op_auth = raw.get("auth")
            if op_auth is not None and not isinstance(op_auth, Mapping):
                raise ValueError(f"operation {op_name!r} auth must be a mapping")
            raw_path_params = raw.get("path_params") or ()
            if isinstance(raw_path_params, (str, bytes)) or not isinstance(
                raw_path_params, (list, tuple)
            ):
                raise ValueError(
                    f"operation {op_name!r} path_params must be a list of strings"
                )
            path_params = tuple(str(p) for p in raw_path_params)
            operations[op_name] = PABusinessOperation(
                name=op_name,
                kind=kind,
                method=method,
                url=url,
                headers={str(k): str(v) for k, v in headers.items()},
                tenant=str(raw.get("tenant")) if raw.get("tenant") is not None else None,
                auth=dict(op_auth) if isinstance(op_auth, Mapping) else None,
                timeout=timeout,
                path_params=path_params,
            )
            continue

        command = raw.get("command")
        if isinstance(command, str):
            command_tuple = (command,)
        elif isinstance(command, list) and all(isinstance(part, str) for part in command):
            command_tuple = tuple(command)
        else:
            raise ValueError(
                f"operation {op_name!r} command must be a string or list of strings"
            )
        if not command_tuple:
            raise ValueError(f"operation {op_name!r} requires command")
        operations[op_name] = PABusinessOperation(
            name=op_name,
            kind=kind,
            command=command_tuple,
            tenant=str(raw.get("tenant")) if raw.get("tenant") is not None else None,
            timeout=timeout,
        )

    permitted, denied = _scope_operations_to_job_brief(operations, pa_context)

    return PABusinessBridgeConfig(
        operations=permitted,
        tenant=tenant,
        auth=dict(auth) if isinstance(auth, Mapping) else None,
        denied_operations=denied,
        media_root=media_root,
        media_ref_prefix=media_ref_prefix,
    )


def _configured_media_root(
    config: Mapping[str, Any] | None,
    *,
    client_bridge: Mapping[str, Any] | None = None,
) -> Path | None:
    """Read the client-configured Systems media root without inventing paths."""
    if isinstance(client_bridge, Mapping) and client_bridge.get("media_root"):
        return Path(str(client_bridge["media_root"])).expanduser()
    if not isinstance(config, Mapping):
        return None
    pa = config.get("pa")
    retention = pa.get("media_retention") if isinstance(pa, Mapping) else None
    if not isinstance(retention, Mapping):
        return None
    raw = retention.get("media_root") or retention.get("root")
    return Path(str(raw)).expanduser() if raw else None


def _configured_media_ref_prefix(
    config: Mapping[str, Any] | None,
    *,
    client_bridge: Mapping[str, Any] | None = None,
) -> str:
    """Read the opaque URL prefix that maps directly to ``media_root``."""
    raw: Any = None
    if isinstance(client_bridge, Mapping):
        raw = client_bridge.get("media_ref_prefix")
    if raw is None and isinstance(config, Mapping):
        pa = config.get("pa")
        retention = pa.get("media_retention") if isinstance(pa, Mapping) else None
        if isinstance(retention, Mapping):
            raw = retention.get("media_ref_prefix")
    prefix = str(raw or "/media").strip().rstrip("/")
    segments = prefix.split("/")
    if (
        not (prefix == "/media" or prefix.startswith("/media/"))
        or prefix == ""
        or any(segment in {"", ".", ".."} for segment in segments[1:])
    ):
        raise ValueError(
            "INVALID_MEDIA_REF_PREFIX: media_ref_prefix must be /media or /media/<segments>"
        )
    return prefix


def _job_brief_business_operations(pa_context: Any | None) -> Mapping[str, Any]:
    """Return the resolved job brief's ``business_operations`` scoping block."""
    job_brief = getattr(pa_context, "job_brief", None)
    if job_brief is None:
        return {}
    scoping = getattr(job_brief, "business_operations", None)
    return scoping if isinstance(scoping, Mapping) else {}


def _scope_operations_to_job_brief(
    operations: Mapping[str, PABusinessOperation],
    pa_context: Any | None,
) -> tuple[dict[str, PABusinessOperation], frozenset[str]]:
    """Restrict the configured operation registry to the active job brief.

    This is the enforcement half of per-chat tool scoping. Christopher runs one
    process across the ingest chats and the management chats; the selector picks
    the job brief, and this drops every operation that brief does not permit
    from the registry entirely. A dropped operation cannot execute by any path —
    the generic ``pa_business_read``/``pa_business_write`` tools and every
    dedicated tool (``tgg_case_create`` and friends) all resolve through this
    same registry.

    Briefs that declare no ``business_operations`` block are left unscoped, so
    existing deployments are unaffected.
    """
    scoping = _job_brief_business_operations(pa_context)
    if not scoping:
        return dict(operations), frozenset()

    allowed = tuple(scoping.get("allowed", ()) or ())
    denied = tuple(scoping.get("denied", ()) or ())

    permitted: dict[str, PABusinessOperation] = {}
    for name, op in operations.items():
        if allowed and name not in allowed:
            continue
        if name in denied:
            continue
        permitted[name] = op

    return permitted, frozenset(set(operations) - set(permitted))


def _client_business_bridge(pa_context: Any | None) -> Mapping[str, Any]:
    constitution = getattr(pa_context, "constitution", None)
    client = getattr(constitution, "client", None)
    if not isinstance(client, Mapping):
        return {}
    for key in ("business_bridge", "business", "pa_business_bridge"):
        value = client.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _client_tenant(pa_context: Any | None) -> str | None:
    constitution = getattr(pa_context, "constitution", None)
    client = getattr(constitution, "client", None)
    if not isinstance(client, Mapping):
        return None
    for key in ("tenant", "tenant_id", "id", "slug"):
        value = client.get(key)
        if value:
            return str(value)
    name = client.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip().lower().replace(" ", "-")
    return None


def _client_auth(pa_context: Any | None) -> Mapping[str, Any]:
    constitution = getattr(pa_context, "constitution", None)
    client = getattr(constitution, "client", None)
    if not isinstance(client, Mapping):
        return {}
    auth = client.get("auth")
    return auth if isinstance(auth, Mapping) else {}


def _runtime_pa_context(config: Mapping[str, Any] | None) -> Any | None:
    if not isinstance(config, Mapping):
        return None
    pa_config = config.get("pa")
    if not isinstance(pa_config, Mapping) or not pa_config.get("enabled"):
        return None
    try:
        from agent.pa_constitution import resolve_context
        from gateway.session_context import get_session_env

        metadata = {
            "source": {
                "platform": get_session_env("HERMES_SESSION_PLATFORM", ""),
                "chat_id": get_session_env("HERMES_SESSION_CHAT_ID", ""),
                "chat_name": get_session_env("HERMES_SESSION_CHAT_NAME", ""),
                "thread_id": get_session_env("HERMES_SESSION_THREAD_ID", ""),
                "user_id": get_session_env("HERMES_SESSION_USER_ID", ""),
                "user_name": get_session_env("HERMES_SESSION_USER_NAME", ""),
            },
            "session_key": get_session_env("HERMES_SESSION_KEY", ""),
        }
        return resolve_context(pa_config, metadata)
    except Exception:
        return None


def _load_runtime_bridge_config() -> PABusinessBridgeConfig:
    try:
        from hermes_cli.config import read_raw_config
    except Exception:
        return PABusinessBridgeConfig(operations={})
    config = read_raw_config()
    return load_business_bridge_config(config, pa_context=_runtime_pa_context(config))


def _bridge_available() -> bool:
    try:
        return bool(_load_runtime_bridge_config().operations)
    except Exception:
        return False


def _json_payload(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValueError("payload must be a mapping")
    return dict(payload)


def _parse_jsonish(text: str) -> dict[str, Any]:
    if not text.strip():
        return {}
    parsed = json.loads(text)
    if isinstance(parsed, dict):
        return parsed
    return {"result": parsed}


CASE_SEARCH_ADDRESS_KEYS = (
    "address",
    "block",
    "street",
    "unit",
)
CASE_SEARCH_WORK_KEYS = (
    "workType",
    "work_type",
    "problem",
    "issue",
)
CASE_SEARCH_TEXT_KEYS = CASE_SEARCH_ADDRESS_KEYS + CASE_SEARCH_WORK_KEYS


def _dedupe_text_parts(raw_parts: list[str]) -> str:
    seen: set[str] = set()
    parts: list[str] = []
    for part in raw_parts:
        normalized = " ".join(part.split())
        if not normalized:
            continue
        marker = normalized.lower()
        if marker in seen:
            continue
        seen.add(marker)
        parts.append(normalized)
    return " ".join(parts)


def _case_search_address_text(payload: Mapping[str, Any]) -> str:
    raw_parts: list[str] = []
    address = str(payload.get("address") or "").strip()
    if address:
        raw_parts.append(address)
    else:
        block = str(payload.get("block") or "").strip()
        street = str(payload.get("street") or "").strip()
        unit = str(payload.get("unit") or "").strip()
        if block:
            raw_parts.append(f"Blk {block}" if not block.lower().startswith("blk") else block)
        if street:
            raw_parts.append(street)
        if unit:
            raw_parts.append(unit if unit.startswith("#") else f"#{unit}")
    return _dedupe_text_parts(raw_parts)


def _case_search_work_text(payload: Mapping[str, Any]) -> str:
    raw_parts: list[str] = []
    for key in ("workType", "work_type", "problem", "issue"):
        value = str(payload.get(key) or "").strip()
        if value:
            raw_parts.append(value)
    return _dedupe_text_parts(raw_parts)


def _normalize_case_search_payload(clean: dict[str, Any]) -> dict[str, Any]:
    """Shape a case-search payload for the operator candidate-search API.

    Structured anchors (block / unit / job number) are PRESERVED as payload
    keys — the API runs a tiered candidate search on them (unit_exact >
    job_no > block_street_fuzzy > text_like) and each returned row carries a
    match_basis naming why it surfaced. The free `search` text is still
    composed from address/street parts and feeds the text + street-fuzzy
    tiers. (The old behavior squashed everything into one whole-string LIKE,
    which made "Rivervale Cres" vs "Rivervale Crescent" return zero results.)
    """
    if "search" not in clean and clean.get("query") is not None:
        clean["search"] = clean["query"]
    address_text = _case_search_address_text(clean)
    work_text = _case_search_work_text(clean)
    has_address_terms = any(str(clean.get(key) or "").strip() for key in CASE_SEARCH_ADDRESS_KEYS)
    has_work_terms = any(str(clean.get(key) or "").strip() for key in CASE_SEARCH_WORK_KEYS)
    if address_text and has_address_terms:
        clean["search"] = address_text
    elif work_text and (has_work_terms or not str(clean.get("search") or "").strip()):
        clean["search"] = work_text
    # Partial/typo'd job fragments are allowed on the candidate search — use
    # the non-path 'job_no' key (contains-match), never strict-validated jobNo.
    job_no = str(clean.get("job_no") or clean.get("jobNo") or "").strip()
    clean.pop("jobNo", None)
    clean.pop("job_no", None)
    block = str(clean.get("block") or "").strip()
    unit = str(clean.get("unit") or "").strip()
    clean.pop("query", None)
    clean.pop("zone", None)
    for key in CASE_SEARCH_TEXT_KEYS:
        clean.pop(key, None)
    if job_no:
        clean["job_no"] = job_no
    if block:
        clean["block"] = block
    if unit:
        clean["unit"] = unit
    return clean


def _normalize_operation_payload(operation: str, payload: Mapping[str, Any] | None) -> dict[str, Any]:
    clean = _json_payload(payload)
    op = operation.strip().lower()
    if op.endswith("case_search"):
        clean = _normalize_case_search_payload(clean)
    return clean


def _validate_operation_payload(op: PABusinessOperation, payload: Mapping[str, Any] | None) -> None:
    request_payload = _json_payload(payload)
    if "jobNo" in request_payload:
        value = str(request_payload.get("jobNo") or "").strip().upper()
        if value and not re.match(JOB_NO_RE, value):
            raise ValueError(
                "INVALID_JOB_NO: jobNo must look like SK/JOB/2604/2376; "
                f"{request_payload.get('jobNo')!r} looks like a unit or free-text reference. "
                "Use case_search/tgg_case_search with search=<address, unit, or work text> first."
            )
    for param_name in op.path_params:
        if param_name not in request_payload:
            raise ValueError(f"operation {op.name!r} requires path_param {param_name!r} in payload")
        if param_name == "jobNo":
            value = str(request_payload.get(param_name) or "").strip().upper()
            if not value or not re.match(JOB_NO_RE, value):
                raise ValueError(
                    "INVALID_JOB_NO: jobNo must look like SK/JOB/2604/2376; "
                    f"{request_payload.get(param_name)!r} looks like a unit or free-text reference. "
                    "Use case_search/tgg_case_search with search=<address, unit, or work text> first."
                )


def _execute_http_operation(
    op: PABusinessOperation,
    payload: Mapping[str, Any] | None,
    bridge_config: PABusinessBridgeConfig,
) -> dict[str, Any]:
    request_payload = _json_payload(payload)
    url = op.url or ""
    data: bytes | None = None
    headers = {"Accept": "application/json", **dict(op.headers or {})}
    headers.update(_auth_headers(op.auth or bridge_config.auth or {}))
    try:
        from gateway.replay import current_replay_context
        _replay_ctx = current_replay_context()
        if _replay_ctx is not None:
            if hasattr(_replay_ctx, "bridge_headers"):
                headers.update({
                    key: value
                    for key, value in _replay_ctx.bridge_headers().items()
                    if key not in headers
                })
            else:
                headers.setdefault("X-Replay-Run-Id", _replay_ctx.run_id)
                headers.setdefault("X-Replay-Attempt-Id", _replay_ctx.attempt_id)
    except Exception:
        pass

    # Path-param substitution: extract listed keys from payload, URL-encode,
    # and replace {name} placeholders in op.url. Remaining payload becomes
    # the query string (GET) or JSON body (non-GET).
    for param_name in op.path_params:
        placeholder = "{" + param_name + "}"
        if placeholder not in url:
            raise ValueError(
                f"operation {op.name!r} declares path_param {param_name!r} "
                f"but URL has no {placeholder} placeholder"
            )
        if param_name not in request_payload:
            raise ValueError(
                f"operation {op.name!r} requires path_param {param_name!r} in payload"
            )
        value = request_payload.pop(param_name)
        encoded = urllib.parse.quote(str(value), safe="")
        url = url.replace(placeholder, encoded)

    if op.method == "GET":
        if request_payload:
            query = urllib.parse.urlencode(request_payload, doseq=True)
            separator = "&" if urllib.parse.urlparse(url).query else "?"
            url = f"{url}{separator}{query}"
    else:
        data = json.dumps(request_payload).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(url, data=data, headers=headers, method=op.method)
    try:
        with urllib.request.urlopen(request, timeout=op.timeout) as response:
            body = response.read().decode("utf-8")
            result = _parse_jsonish(body)
            result.setdefault("status_code", response.status)
            return result
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = _parse_jsonish(body)
        except json.JSONDecodeError:
            parsed = {"body": body}
        parsed["status_code"] = exc.code
        parsed.setdefault("error", f"HTTP {exc.code}")
        return parsed


def _execute_command_operation(
    op: PABusinessOperation,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not op.command:
        raise ValueError(f"operation {op.name!r} has no command configured")
    completed = subprocess.run(
        op.command,
        input=json.dumps(_json_payload(payload)),
        text=True,
        capture_output=True,
        timeout=op.timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"operation {op.name!r} exited {completed.returncode}: "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    return _parse_jsonish(completed.stdout)


def execute_business_operation(
    config: Mapping[str, Any] | PABusinessBridgeConfig | None,
    operation: str,
    payload: Mapping[str, Any] | None = None,
    *,
    pa_context: Any | None = None,
) -> dict[str, Any]:
    """Execute a configured business operation and return its JSON-ish result."""
    bridge_config = (
        config
        if isinstance(config, PABusinessBridgeConfig)
        else load_business_bridge_config(config, pa_context=pa_context)
    )
    op = bridge_config.operations.get(operation)
    # Older convenience tools can still emit pre-registry names. If the
    # requested legacy name is not explicitly configured, resolve it to the
    # canonical operation before validation/execution. The registry remains
    # canonical-only; legacy configs and fixtures still win when explicit.
    if op is None:
        canonical_operation = {
            "tgg_clarification_request": "tgg_clarification_raise",
            "tgg_message_history_search": "message_search",
            "tgg_case_update_state": "tgg_case_update",
        }.get(operation)
        if canonical_operation:
            operation = canonical_operation
            op = bridge_config.operations.get(operation)
    if op is None and operation in bridge_config.denied_operations:
        # Configured for this client, but scoped out of the active job brief.
        # Distinguish this from a genuine typo so the model can self-correct
        # towards what it IS allowed to do here instead of retrying blindly.
        raise OperationNotPermitted(
            operation=operation,
            job_type=getattr(pa_context, "job_type", None),
            permitted=bridge_config.operations,
        )
    if op is None:
        known = ", ".join(sorted(bridge_config.operations)) or "none configured"
        raise ValueError(f"unknown PA business operation {operation!r}; known: {known}")
    if op.tenant and bridge_config.tenant and op.tenant != bridge_config.tenant:
        raise TenantScopeMismatch(
            current_tenant=bridge_config.tenant,
            target_tenant=op.tenant,
            operation=operation,
        )

    normalized_payload = _normalize_operation_payload(operation, payload)
    _validate_operation_payload(op, normalized_payload)

    if op.kind == "http":
        return _execute_http_operation(op, normalized_payload, bridge_config)
    if op.kind == "command":
        return _execute_command_operation(op, normalized_payload)
    raise ValueError(f"unsupported PA business operation type {op.kind!r}")


def _auth_headers(auth: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(auth, Mapping) or not auth:
        return {}
    token = auth.get("token")
    token_env = auth.get("token_env")
    if not token and token_env:
        token = os.getenv(str(token_env), "")
    if not token:
        return {}
    auth_type = str(auth.get("type") or auth.get("kind") or "header").strip().lower()
    if auth_type in {"oauth", "bearer"}:
        return {"Authorization": f"Bearer {token}"}
    header = str(auth.get("header") or "Authorization")
    scheme = auth.get("scheme")
    value = f"{scheme} {token}" if scheme else str(token)
    return {header: value}


# ── Live agent behavior config ─────────────────────────────────────────────
#
# The PS spine exposes an operator-tuned agent_config the deployed agent reads
# on every decision turn. The bridge reaches it through a configured GET
# operation (default name ``agent_config_read``); the gateway injects the
# rendered behavior block into the decision-turn prompt. Every accessor below
# fails soft — an inactive bridge, an unconfigured operation (a client
# without this operation configured), or a failed call yields an empty result
# so the agent simply runs on its constitution defaults.

AGENT_CONFIG_OPERATION = "agent_config_read"
AGENT_ACTION_OPERATIONS = (
    "agent_action_record",
    "agent_actions_write",
    "record_agent_action",
)
AGENT_ACTION_TYPES = {
    "observation",
    "photo-pair-classified",
    "dry-run-reply",
    "executed-reply",
    "config-mutation",
}
AGENT_ACTION_STATUSES = {
    "pending",
    "dry-run",
    "executed",
    "suppressed",
}


def _env_truthy(*names: str) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _resolve_bridge(
    config: Mapping[str, Any] | PABusinessBridgeConfig | None,
) -> PABusinessBridgeConfig | None:
    """Resolve a bridge config from an explicit arg, or the runtime config."""
    try:
        if isinstance(config, PABusinessBridgeConfig):
            return config
        if config is not None:
            return load_business_bridge_config(config)
        return _load_runtime_bridge_config()
    except Exception:
        return None


def record_agent_action(
    *,
    agent_id: str,
    engagement_id: str,
    action_type: str,
    payload: dict,
    source: str | None = None,
    cost_usd: float = 0.0,
    tokens_input: int = 0,
    tokens_output: int = 0,
    status: str = "pending",
    turn_id: str | None = None,
    config: Mapping[str, Any] | PABusinessBridgeConfig | None = None,
) -> bool:
    """Record an agent action to dev.agent_actions. Fails soft."""
    try:
        if _env_truthy("HERMES_PA_AGENT_ACTION_DRY_RUN", "HERMES_PA_BUSINESS_DRY_RUN"):
            return True
        if not agent_id or not engagement_id:
            return False
        if action_type not in AGENT_ACTION_TYPES or status not in AGENT_ACTION_STATUSES:
            return False
        if not isinstance(payload, dict):
            return False

        bridge = _resolve_bridge(config)
        if bridge is None:
            return False
        operation = next(
            (name for name in AGENT_ACTION_OPERATIONS if name in bridge.operations),
            None,
        )
        if operation is None:
            return False

        result = execute_business_operation(
            bridge,
            operation=operation,
            payload={
                "agent_id": agent_id,
                "engagement_id": engagement_id,
                "action_type": action_type,
                "payload": payload,
                "source": source,
                "cost_usd": float(cost_usd or 0.0),
                "tokens_input": int(tokens_input or 0),
                "tokens_output": int(tokens_output or 0),
                "status": status,
                "turn_id": turn_id,
            },
        )
        status_code = int(result.get("status_code") or 200)
        if status_code >= 400:
            return False
        return result.get("ok") is not False
    except Exception:
        return False


def fetch_agent_config_view(
    config: Mapping[str, Any] | PABusinessBridgeConfig | None = None,
    *,
    operation: str = AGENT_CONFIG_OPERATION,
) -> dict[str, Any]:
    """Fetch the live agent-config view ``{config, directives, keys}``.

    Returns ``{}`` when the bridge is inactive, the operation is not configured
    (e.g. a client without this operation configured), or the call fails.
    """
    bridge = _resolve_bridge(config)
    if bridge is None or operation not in bridge.operations:
        return {}
    try:
        result = execute_business_operation(bridge, operation=operation)
    except Exception:
        return {}
    data = result.get("data")
    if isinstance(data, Mapping):
        return dict(data)
    # Tolerate a flat response shape that omits the {ok, data} envelope.
    if isinstance(result.get("config"), Mapping):
        return {k: v for k, v in result.items() if k != "status_code"}
    return {}


def fetch_agent_config_map(
    config: Mapping[str, Any] | PABusinessBridgeConfig | None = None,
    *,
    operation: str = AGENT_CONFIG_OPERATION,
) -> dict[str, Any]:
    """The resolved agent-config key→value map. Empty when unavailable."""
    view = fetch_agent_config_view(config, operation=operation)
    cfg = view.get("config")
    return dict(cfg) if isinstance(cfg, Mapping) else {}


def read_agent_config(
    key: str,
    config: Mapping[str, Any] | PABusinessBridgeConfig | None = None,
    *,
    operation: str = AGENT_CONFIG_OPERATION,
) -> Any:
    """Read one live agent-config value from the PS spine.

    Returns ``None`` when the key is absent or the config cannot be reached.
    """
    return fetch_agent_config_map(config, operation=operation).get(key)


def render_agent_config_prompt(
    config: Mapping[str, Any] | PABusinessBridgeConfig | None = None,
    *,
    operation: str = AGENT_CONFIG_OPERATION,
) -> str:
    """Render the live behavior-config block for decision-turn prompt injection.

    Empty string when no config is available — the agent then runs on its
    constitution defaults with no extra block.
    """
    view = fetch_agent_config_view(config, operation=operation)
    directives = view.get("directives")
    if not isinstance(directives, list) or not directives:
        return ""
    lines = [
        "## Live Behavior Configuration",
        "Operations-tuned settings for this client. They may change between "
        "messages — apply them to your reply right now:",
    ]
    lines.extend(f"- {directive}" for directive in directives)
    return "\n".join(lines)


def _truthy_marker(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _payload_allows_ilinked(payload: Mapping[str, Any] | None) -> bool:
    if not isinstance(payload, Mapping):
        return False
    return any(_truthy_marker(payload.get(key)) for key in ILINKED_ALLOW_PAYLOAD_KEYS)


def _strip_control_payload_keys(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    clean = _json_payload(payload)
    for key in ILINKED_ALLOW_PAYLOAD_KEYS:
        clean.pop(key, None)
    return clean


def _user_task_allows_ilinked(
    operation: str,
    user_task: Any,
    payload: Mapping[str, Any] | None = None,
) -> bool:
    """Fail-closed guard for TGG source-system reads.

    Christopher's operator DB may contain facts imported from iLinked, but
    live iLinked read operations are opt-in. The model sometimes infers
    iLinked from vague words like "latest" or "chase"; this guard keeps the
    source-system bridge closed unless the current user message explicitly
    asks for it.
    """
    if operation not in ILINKED_READ_OPERATIONS:
        return True
    text = str(user_task or "").lower()
    if text:
        if any(cue in text for cue in ILINKED_EXPLICIT_CUES):
            return True
        if operation == "ilinked_wc_lookup" and any(cue in text for cue in WC_EXPLICIT_CUES):
            return True
        return False
    return _payload_allows_ilinked(payload)


def _handle_business_read(args: Mapping[str, Any], **kwargs: Any) -> str:
    return _handle_business_call(args, user_task=kwargs.get("user_task"))


def _handle_business_write(args: Mapping[str, Any], **kwargs: Any) -> str:
    if _env_truthy("HERMES_PA_BUSINESS_DRY_RUN"):
        operation = str(args.get("operation") or "").strip()
        if not operation:
            return tool_error("operation is required")
        payload = _normalize_operation_payload(operation, _strip_control_payload_keys(args.get("payload") or {}))
        try:
            bridge = _load_runtime_bridge_config()
            op = bridge.operations.get(operation)
            if op is None:
                known = ", ".join(sorted(bridge.operations)) or "none configured"
                return tool_error(f"unknown PA business operation {operation!r}; known: {known}")
            _validate_operation_payload(op, payload)
        except Exception as exc:
            return tool_error(exc)
        return tool_result({
            "ok": True,
            "dry_run": True,
            "operation": operation,
            "payload": payload,
        })
    return _handle_business_call(args, user_task=kwargs.get("user_task"))


def _dry_run_business_result(operation: str, payload: Mapping[str, Any]) -> str | None:
    if not _env_truthy("HERMES_PA_BUSINESS_DRY_RUN"):
        return None
    try:
        bridge = _load_runtime_bridge_config()
        op = bridge.operations.get(operation)
        if op is None:
            known = ", ".join(sorted(bridge.operations)) or "none configured"
            return tool_error(f"unknown PA business operation {operation!r}; known: {known}")
        _validate_operation_payload(op, payload)
    except Exception as exc:
        return tool_error(exc)
    return tool_result({
        "ok": True,
        "dry_run": True,
        "operation": operation,
        "payload": dict(payload),
    })


# ── last-seen case state (v6.3 item 4b, WB f6845320 — 1018/1092 receipt) ──
#
# The tgg_case_observation backend response carries only {observationId}; the
# agent attaching evidence therefore never sees the case's CURRENT state in
# the tool result, and a stale "completed" carried in conversation memory can
# survive right past a same-turn lookup that said "open". A fresh fetch per
# observation is not worth an extra round-trip, so instead we remember the
# most recent backend-returned state per jobNo from lookup/search/write
# results and echo it prominently into the observation success result.
# Deterministic, code-layer surface (match-the-layer); the constitution
# carries the judgment-side rule ("this-turn tool result wins").

_LAST_SEEN_CASE_STATE: dict[str, str] = {}
_LAST_SEEN_CASE_STATE_MAX = 512


def _case_state_key(job_no: Any) -> str | None:
    text = " ".join(str(job_no or "").split()).upper()
    return text or None


def _remember_case_state(job_no: Any, state: Any) -> None:
    key = _case_state_key(job_no)
    state_text = str(state or "").strip()
    if not key or not state_text:
        return
    # Bounded cache: drop the oldest entry past the cap (long-lived gateway).
    if key not in _LAST_SEEN_CASE_STATE and len(_LAST_SEEN_CASE_STATE) >= _LAST_SEEN_CASE_STATE_MAX:
        _LAST_SEEN_CASE_STATE.pop(next(iter(_LAST_SEEN_CASE_STATE)), None)
    _LAST_SEEN_CASE_STATE[key] = state_text


def _harvest_case_states(result: Any, _depth: int = 0) -> None:
    """Record state for every case-shaped dict (jobNo/job_no + state) in a
    backend result — covers lookup ({data: {case: ...}}), search candidates,
    and any write response that echoes the case."""
    if _depth > 6:
        return
    try:
        if isinstance(result, Mapping):
            job_no = result.get("jobNo") or result.get("job_no")
            if job_no is not None and "state" in result:
                _remember_case_state(job_no, result.get("state"))
            for value in result.values():
                if isinstance(value, (Mapping, list, tuple)):
                    _harvest_case_states(value, _depth + 1)
        elif isinstance(result, (list, tuple)):
            for item in result:
                _harvest_case_states(item, _depth + 1)
    except Exception:
        pass


def _annotate_observation_result(raw: str, job_no: Any) -> str:
    """Echo the last-known backend state for the observed case into the
    observation tool result so fresh state is in-face at attach time."""
    key = _case_state_key(job_no)
    state = _LAST_SEEN_CASE_STATE.get(key) if key else None
    if not state:
        return raw
    try:
        data = json.loads(raw)
    except Exception:
        return raw
    if not isinstance(data, dict) or data.get("error") or "caseState" in data:
        return raw
    if data.get("ok") is False:
        return raw
    data["caseState"] = state
    data["caseStateNote"] = (
        "current backend state for this case (from the most recent "
        "lookup/search result) — any state claim must restate THIS value, "
        "not conversation memory"
    )
    return json.dumps(data, ensure_ascii=False)


def _handle_tgg_read(operation: str, payload: Mapping[str, Any]) -> str:
    try:
        result = execute_business_operation(
            _load_runtime_bridge_config(),
            operation=operation,
            payload=payload,
        )
    except Exception as exc:
        return tool_error(exc)
    _harvest_case_states(result)
    return tool_result(result)


def _handle_tgg_write(operation: str, payload: Mapping[str, Any]) -> str:
    dry = _dry_run_business_result(operation, payload)
    if dry is not None:
        return dry
    try:
        result = execute_business_operation(
            _load_runtime_bridge_config(),
            operation=operation,
            payload=payload,
        )
    except Exception as exc:
        return tool_error(exc)
    _harvest_case_states(result)
    return tool_result(_shape_attach_unjustified_result(result))


def _handle_tgg_case_lookup(args: Mapping[str, Any], **_kwargs: Any) -> str:
    return _handle_tgg_read("tgg_case_lookup", {"jobNo": args.get("jobNo")})


_TGG_IMAGE_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp"})
_TGG_IMAGE_MIMES = frozenset(
    {"image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp"}
)
_TGG_IMAGE_SIGNATURES = (
    ("image/jpeg", b"\xff\xd8\xff", 0),
    ("image/png", b"\x89PNG\r\n\x1a\n", 0),
    ("image/gif", b"GIF8", 0),
    ("image/webp", b"WEBP", 8),
)

_TGG_SPREADSHEET_EXTENSIONS = frozenset({".xlsx", ".csv"})
_TGG_SPREADSHEET_MIMES = {
    ".xlsx": frozenset(
        {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    ),
    ".csv": frozenset({"text/csv", "application/csv"}),
}
_TGG_SPREADSHEET_CANONICAL_MIME = {
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
}
_TGG_XLSX_MAIN_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
)
_TGG_SPREADSHEET_MACRO_MARKERS = ("macroenabled", "vbaproject", "macrosheet")
_TGG_SPREADSHEET_MAX_ZIP_ENTRIES = 20_000
_TGG_SPREADSHEET_MAX_UNCOMPRESSED_BYTES = 512 * 1024 * 1024
_TGG_SPREADSHEET_MAX_FILE_BYTES = 64 * 1024 * 1024
_TGG_SPREADSHEET_MAX_JOB_NUMBERS = 10_000
_TGG_JOB_HEADER_NAMES = frozenset(
    {"job no", "job number", "job no.", "job number."}
)


def _sniff_tgg_csv(path: Path) -> bool:
    """Return whether a bounded prefix is plausibly comma-delimited text."""
    with path.open("rb") as handle:
        sample = handle.read(128 * 1024)
    if not sample or b"\x00" in sample:
        return False
    try:
        text = sample.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False
    if any(ord(char) < 32 and char not in "\r\n\t" for char in text):
        return False
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return False
    widths = [len(next(csv.reader([line]))) for line in lines[:20]]
    return bool(widths and max(widths) >= 2 and sum(width >= 2 for width in widths) >= 1)


def _sniff_tgg_xlsx(path: Path) -> bool:
    """Validate an OOXML workbook without extracting or executing macros."""
    with path.open("rb") as handle:
        if handle.read(4) not in {b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"}:
            return False
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if (
                len(infos) > _TGG_SPREADSHEET_MAX_ZIP_ENTRIES
                or sum(info.file_size for info in infos)
                > _TGG_SPREADSHEET_MAX_UNCOMPRESSED_BYTES
            ):
                return False
            names = {info.filename.lower() for info in infos}
            if "[content_types].xml" not in names or "xl/workbook.xml" not in names:
                return False
            if any(
                marker in name
                for name in names
                for marker in ("vbaproject", "macrosheet")
            ):
                return False
            content_types = archive.read("[Content_Types].xml")
    except (
        KeyError,
        OSError,
        RuntimeError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ):
        return False
    try:
        root = ElementTree.fromstring(content_types)
    except ElementTree.ParseError:
        return False
    declared_types = {
        str(element.attrib.get("ContentType") or "").lower()
        for element in root.iter()
    }
    if any(
        marker in content_type
        for content_type in declared_types
        for marker in _TGG_SPREADSHEET_MACRO_MARKERS
    ):
        return False
    return _TGG_XLSX_MAIN_CONTENT_TYPE in declared_types


def validate_tgg_spreadsheet(
    path: str | Path,
    *,
    declared_mime: str,
) -> str:
    """Validate an explicitly allowlisted spreadsheet by MIME, suffix and bytes.

    Returns the canonical detected MIME. Unsupported/macro-bearing files are
    refused. A declared MIME that disagrees with the content uses the same
    PROVENANCE_DIVERGENCE contract as retained case images.
    """
    candidate = Path(path).expanduser().resolve(strict=True)
    if not candidate.is_file():
        raise ValueError("INVALID_MEDIA_REF: spreadsheet is not a regular file")
    if candidate.stat().st_size > _TGG_SPREADSHEET_MAX_FILE_BYTES:
        raise ValueError("SPREADSHEET_TOO_LARGE: file exceeds the media gate limit")
    suffix = candidate.suffix.lower()
    mime = str(declared_mime or "").split(";", 1)[0].strip().lower()
    if suffix not in _TGG_SPREADSHEET_EXTENSIONS:
        raise ValueError("UNSUPPORTED_MEDIA_TYPE: spreadsheet format is not allowlisted")
    if mime not in _TGG_SPREADSHEET_MIMES[suffix]:
        raise ValueError(
            "PROVENANCE_DIVERGENCE: spreadsheet MIME does not match extension"
        )
    sniff = _sniff_tgg_xlsx if suffix == ".xlsx" else _sniff_tgg_csv
    if not sniff(candidate):
        raise ValueError(
            "PROVENANCE_DIVERGENCE: spreadsheet MIME does not match bytes"
        )
    return _TGG_SPREADSHEET_CANONICAL_MIME[suffix]


def _xlsx_column_index(cell_ref: str) -> int:
    letters = "".join(char for char in str(cell_ref or "") if char.isalpha())
    value = 0
    for char in letters.upper():
        value = value * 26 + ord(char) - ord("A") + 1
    return value


def _xlsx_cell_text(cell: ElementTree.Element, shared: list[str]) -> str:
    cell_type = str(cell.attrib.get("t") or "")
    if cell_type == "inlineStr":
        return "".join(
            str(node.text or "")
            for node in cell.iter()
            if node.tag.rsplit("}", 1)[-1] == "t"
        ).strip()
    raw = next(
        (
            str(node.text or "")
            for node in cell
            if node.tag.rsplit("}", 1)[-1] == "v"
        ),
        "",
    )
    if cell_type == "s" and raw:
        try:
            return shared[int(raw)].strip()
        except (IndexError, ValueError):
            return ""
    return raw.strip()


def _xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    except ElementTree.ParseError as exc:
        raise ValueError("INVALID_SPREADSHEET: malformed shared strings") from exc
    return [
        "".join(
            str(node.text or "")
            for node in item.iter()
            if node.tag.rsplit("}", 1)[-1] == "t"
        )
        for item in root
        if item.tag.rsplit("}", 1)[-1] == "si"
    ]


def _xlsx_sheet_paths(archive: zipfile.ZipFile) -> list[str]:
    try:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        relationships = ElementTree.fromstring(
            archive.read("xl/_rels/workbook.xml.rels")
        )
    except (KeyError, ElementTree.ParseError) as exc:
        raise ValueError("INVALID_SPREADSHEET: malformed workbook relationships") from exc
    targets = {
        str(rel.attrib.get("Id") or ""): str(rel.attrib.get("Target") or "")
        for rel in relationships
    }
    paths: list[str] = []
    for sheet in workbook.iter():
        if sheet.tag.rsplit("}", 1)[-1] != "sheet":
            continue
        relationship_id = next(
            (
                str(value)
                for key, value in sheet.attrib.items()
                if key.rsplit("}", 1)[-1] == "id"
            ),
            "",
        )
        target = targets.get(relationship_id, "")
        if not target:
            continue
        normalized = target.lstrip("/")
        if not normalized.startswith("xl/"):
            normalized = f"xl/{normalized}"
        if ".." in Path(normalized).parts:
            raise ValueError("INVALID_SPREADSHEET: worksheet escapes workbook root")
        paths.append(normalized)
    return paths


def _extract_xlsx_job_numbers(path: Path) -> list[str]:
    jobs: list[str] = []
    seen: set[str] = set()
    with zipfile.ZipFile(path) as archive:
        shared = _xlsx_shared_strings(archive)
        for sheet_path in _xlsx_sheet_paths(archive):
            try:
                sheet_stream = archive.open(sheet_path)
            except KeyError as exc:
                raise ValueError("INVALID_SPREADSHEET: malformed worksheet") from exc
            job_column = 0
            try:
                for _event, row in ElementTree.iterparse(
                    sheet_stream, events=("end",)
                ):
                    if row.tag.rsplit("}", 1)[-1] != "row":
                        continue
                    values: dict[int, str] = {}
                    for cell in row:
                        if cell.tag.rsplit("}", 1)[-1] != "c":
                            continue
                        column = _xlsx_column_index(
                            str(cell.attrib.get("r") or "")
                        )
                        if column:
                            values[column] = _xlsx_cell_text(cell, shared)
                    if not job_column:
                        for column, value in values.items():
                            normalized = " ".join(value.lower().split())
                            if normalized in _TGG_JOB_HEADER_NAMES:
                                job_column = column
                                break
                        row.clear()
                        continue
                    job_no = " ".join(
                        values.get(job_column, "").upper().split()
                    )
                    if (
                        job_no
                        and re.fullmatch(JOB_NO_RE, job_no)
                        and job_no not in seen
                    ):
                        seen.add(job_no)
                        jobs.append(job_no)
                        if len(jobs) > _TGG_SPREADSHEET_MAX_JOB_NUMBERS:
                            raise ValueError(
                                "SPREADSHEET_TOO_LARGE: too many unique job numbers"
                            )
                    row.clear()
            except ElementTree.ParseError as exc:
                raise ValueError("INVALID_SPREADSHEET: malformed worksheet") from exc
            finally:
                sheet_stream.close()
    return jobs


def _extract_csv_job_numbers(path: Path) -> list[str]:
    jobs: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        job_column: int | None = None
        for row in reader:
            if job_column is None:
                for index, value in enumerate(row):
                    normalized = " ".join(str(value).lower().split())
                    if normalized in _TGG_JOB_HEADER_NAMES:
                        job_column = index
                        break
                continue
            if job_column >= len(row):
                continue
            job_no = " ".join(str(row[job_column]).upper().split())
            if job_no and re.fullmatch(JOB_NO_RE, job_no) and job_no not in seen:
                seen.add(job_no)
                jobs.append(job_no)
                if len(jobs) > _TGG_SPREADSHEET_MAX_JOB_NUMBERS:
                    raise ValueError(
                        "SPREADSHEET_TOO_LARGE: too many unique job numbers"
                    )
    return jobs


def extract_tgg_spreadsheet_job_numbers(path: str | Path) -> list[str]:
    """Open a media-gate-approved spreadsheet and return unique job numbers."""
    candidate = Path(path).expanduser().resolve(strict=True)
    suffix = candidate.suffix.lower()
    canonical_mime = _TGG_SPREADSHEET_CANONICAL_MIME.get(suffix)
    if canonical_mime is None:
        raise ValueError("UNSUPPORTED_MEDIA_TYPE: spreadsheet format is not allowlisted")
    validate_tgg_spreadsheet(candidate, declared_mime=canonical_mime)
    extractor = _extract_xlsx_job_numbers if suffix == ".xlsx" else _extract_csv_job_numbers
    jobs = extractor(candidate)
    if not jobs:
        raise ValueError(
            "JOB_NUMBER_COLUMN_NOT_FOUND: no recognized job-number column with valid jobs"
        )
    return jobs


def _handle_tgg_spreadsheet_job_numbers(
    args: Mapping[str, Any], **_kwargs: Any
) -> str:
    try:
        path = str(args.get("path") or "").strip()
        if not path:
            raise ValueError("path is required")
        try:
            from hermes_cli.config import read_raw_config

            config = read_raw_config()
            pa = config.get("pa") if isinstance(config, Mapping) else None
            retention = (
                pa.get("media_retention") if isinstance(pa, Mapping) else None
            )
            roots = (
                retention.get("source_roots")
                if isinstance(retention, Mapping)
                else None
            )
            if isinstance(roots, (str, bytes)):
                roots = [roots]
            candidate = Path(path).expanduser().resolve(strict=True)
            allowed_roots = [
                Path(str(root)).expanduser().resolve(strict=True)
                for root in (roots or [])
            ]
            if not allowed_roots or not any(
                candidate.is_relative_to(root) for root in allowed_roots
            ):
                raise ValueError
        except (OSError, RuntimeError, TypeError, ValueError):
            raise ValueError(
                "INVALID_MEDIA_REF: spreadsheet is unavailable or outside configured roots"
            ) from None
        jobs = extract_tgg_spreadsheet_job_numbers(candidate)
        return tool_result(
            {
                "ok": True,
                "jobNumbers": jobs,
                "count": len(jobs),
                "next": (
                    "Feed these exact job numbers to the existing tgg_case_query "
                    "cross-check; do not infer case state from the workbook."
                ),
            }
        )
    except Exception as exc:
        return tool_error(exc)


def _case_media_items(result: Mapping[str, Any]) -> list[Any]:
    """Extract only the compact case-media endpoint's documented file list."""
    candidates: list[Any] = [
        result.get("files"),
        result.get("media"),
        result.get("items"),
    ]
    data = result.get("data")
    if isinstance(data, Mapping):
        candidates.extend((data.get("files"), data.get("media"), data.get("items")))
        case = data.get("case")
        if isinstance(case, Mapping):
            candidates.extend((case.get("files"), case.get("media"), case.get("items")))
    for candidate in candidates:
        if isinstance(candidate, list):
            return candidate
    return []


def _resolve_case_photo(
    item: Any, media_root: Path, media_ref_prefix: str = "/media"
) -> tuple[str, str] | None:
    """Resolve one opaque /media ref to an existing contained image.

    The Systems API is allowed to name only opaque refs.  Local paths are
    derived here so the model never supplies a filesystem path to the lookup.
    """
    mime = ""
    if isinstance(item, Mapping):
        raw_ref = item.get("ref") or item.get("url") or item.get("mediaRef")
        mime = str(
            item.get("mime") or item.get("mimeType") or item.get("contentType") or ""
        ).split(";", 1)[0].strip().lower()
    else:
        raw_ref = item
    ref = str(raw_ref or "").strip()
    parsed = urllib.parse.urlsplit(ref)
    # Absolute URLs, query strings and fragments are not opaque media refs.
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("INVALID_MEDIA_REF: case media must be an opaque /media/... ref")
    path = urllib.parse.unquote(parsed.path)
    prefix = media_ref_prefix.rstrip("/")
    if not path.startswith(f"{prefix}/") or path == f"{prefix}/":
        raise ValueError(
            "INVALID_MEDIA_REF: case media must match the configured opaque media prefix"
        )
    relative = path.removeprefix(f"{prefix}/")
    root = media_root.expanduser().resolve(strict=True)
    candidate = (root / relative).resolve(strict=True)
    if not candidate.is_relative_to(root) or not candidate.is_file():
        raise ValueError("INVALID_MEDIA_REF: case media is missing or escapes configured root")
    suffix = candidate.suffix.lower()
    if mime and mime not in _TGG_IMAGE_MIMES:
        return None
    if suffix not in _TGG_IMAGE_EXTENSIONS:
        return None
    prefix = candidate.read_bytes()[:16]
    detected = next(
        (
            detected_mime
            for detected_mime, signature, offset in _TGG_IMAGE_SIGNATURES
            if prefix[offset : offset + len(signature)] == signature
        ),
        None,
    )
    if detected is None:
        return None
    if mime and not (
        mime == detected or (mime == "image/jpg" and detected == "image/jpeg")
    ):
        raise ValueError("PROVENANCE_DIVERGENCE: case media MIME does not match bytes")
    return path, str(candidate)


def _handle_tgg_case_photos(args: Mapping[str, Any], **_kwargs: Any) -> str:
    job_no = str(args.get("job_no") or "").strip().upper()
    if not job_no or not re.fullmatch(JOB_NO_RE, job_no):
        return tool_error(
            "INVALID_JOB_NO: tgg_case_photos requires a real job number such as "
            "SK/JOB/2604/2376; numeric case ids and free text are not accepted"
        )
    try:
        bridge = _load_runtime_bridge_config()
        if bridge.media_root is None:
            raise ValueError("MEDIA_ROOT_NOT_CONFIGURED: case photo root is unavailable")
        result = execute_business_operation(
            bridge,
            operation="tgg_case_media",
            payload={"jobNo": job_no},
        )
        if result.get("ok") is False or int(result.get("status_code") or 200) >= 400:
            return tool_result(result)
        photos: list[dict[str, str]] = []
        seen: set[str] = set()
        for item in _case_media_items(result):
            resolved = _resolve_case_photo(
                item, bridge.media_root, bridge.media_ref_prefix
            )
            if resolved is None:
                continue
            ref, local_path = resolved
            if local_path in seen:
                continue
            seen.add(local_path)
            photos.append({"media_ref": ref, "image_path": local_path})
        response: dict[str, Any] = {
            "ok": True,
            "jobNo": job_no,
            "photos": photos,
            "count": len(photos),
        }
        if not photos:
            response["message"] = "no retained case photos"
        return tool_result(response)
    except Exception as exc:
        return tool_error(exc)


def _handle_tgg_case_query(args: Mapping[str, Any], **_kwargs: Any) -> str:
    return _handle_tgg_read("tgg_case_query", {"sql": args.get("sql")})


def _handle_tgg_case_search(args: Mapping[str, Any], **_kwargs: Any) -> str:
    payload = _normalize_case_search_payload(dict(args))
    payload["limit"] = payload.get("limit", 10)
    for key in ("serviceLine", "sourceStatus", "progressStatus", "state"):
        if args.get(key) is not None:
            payload[key] = args.get(key)
    return _handle_tgg_read("tgg_case_search", payload)


def _history_before_ts_cap() -> int | None:
    """Replay-only future cap for message-history search.

    The replay harness sets HERMES_PA_HISTORY_BEFORE_TS per turn (epoch seconds
    of the turn's latest message + 1) so a replayed agent can never see archive
    messages from after the moment being replayed. Live runtime never sets the
    variable, so live searches are uncapped.
    """
    try:
        from gateway.replay import current_history_before_ts
        replay_cap = current_history_before_ts()
        if replay_cap is not None:
            return int(replay_cap)
    except Exception:
        pass
    raw = os.getenv("HERMES_PA_HISTORY_BEFORE_TS")
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _apply_before_ts(payload: dict[str, Any], args: Mapping[str, Any]) -> None:
    """Apply caller-supplied before_ts, clamped by the replay future-cap.

    The replay cap always wins when present (a replayed agent must never see
    archive messages from after the moment being replayed)."""
    before_ts: int | None = None
    raw = args.get("before_ts")
    if raw is not None:
        try:
            before_ts = int(float(raw))
        except (TypeError, ValueError):
            before_ts = None
    cap = _history_before_ts_cap()
    if before_ts is not None and cap is not None:
        payload["before_ts"] = min(before_ts, cap)
    elif before_ts is not None:
        payload["before_ts"] = before_ts
    elif cap is not None:
        payload["before_ts"] = cap


def _handle_tgg_message_history_search(args: Mapping[str, Any], **_kwargs: Any) -> str:
    payload: dict[str, Any] = {}
    for key in ("q", "block", "unit"):
        value = str(args.get(key) or "").strip()
        if value:
            payload[key] = value
    job_no = str(args.get("jobNo") or args.get("job_no") or "").strip()
    if job_no:
        # Deliberately the non-path 'job_no' key: partial/typo'd fragments are
        # allowed here (contains-match), unlike the strict jobNo validation on
        # lookup/observation operations.
        payload["job_no"] = job_no
    chat_jid = str(args.get("chat_jid") or args.get("chatJid") or "").strip()
    if chat_jid:
        payload["chat_jid"] = chat_jid
    limit = args.get("limit")
    if isinstance(limit, int) and limit > 0:
        payload["limit"] = min(limit, 50)
    _apply_before_ts(payload, args)
    return _handle_tgg_read("tgg_message_history_search", payload)


def _handle_generic_message_history_search(args: Mapping[str, Any], **_kwargs: Any) -> str:
    """Client-agnostic message-history search: q/chat_jid/before_ts/limit ONLY.

    The generic alias deliberately carries no client-shaped params (no
    block/unit/job_no) — those live on the tgg_-prefixed variant.  Any extra
    keys a model passes are ignored rather than forwarded."""
    payload: dict[str, Any] = {}
    q = str(args.get("q") or "").strip()
    if q:
        payload["q"] = q
    chat_jid = str(args.get("chat_jid") or args.get("chatJid") or "").strip()
    if chat_jid:
        payload["chat_jid"] = chat_jid
    limit = args.get("limit")
    if isinstance(limit, int) and limit > 0:
        payload["limit"] = min(limit, 50)
    _apply_before_ts(payload, args)
    return _handle_tgg_read("tgg_message_history_search", payload)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _handle_tgg_clarification_request(args: Mapping[str, Any], **_kwargs: Any) -> str:
    question = str(args.get("question") or "").strip()
    if not question:
        return tool_error("clarification_request requires a non-empty question")
    payload: dict[str, Any] = {"question": question}
    candidates = _string_list(args.get("candidate_job_nos") or args.get("candidateJobNos"))
    if candidates:
        payload["candidate_job_nos"] = candidates
    evidence = _string_list(args.get("evidence_message_refs") or args.get("evidenceMessageRefs"))
    if evidence:
        payload["evidence_message_refs"] = evidence
    context = str(args.get("context") or "").strip()
    if context:
        payload["context"] = context
    return _handle_tgg_write("tgg_clarification_request", payload)


def _handle_generic_clarification_request(args: Mapping[str, Any], **_kwargs: Any) -> str:
    """Client-agnostic clarification: question/candidate_refs/evidence/context.

    ``candidate_refs`` is the agnostic name for candidate entity identifiers
    (the tgg_ variant uses candidate_job_nos); the wire payload keeps the
    backend's key so the endpoint is unchanged."""
    question = str(args.get("question") or "").strip()
    if not question:
        return tool_error("clarification_request requires a non-empty question")
    payload: dict[str, Any] = {"question": question}
    candidates = _string_list(args.get("candidate_refs") or args.get("candidateRefs"))
    if candidates:
        payload["candidate_job_nos"] = candidates
    evidence = _string_list(args.get("evidence_message_refs") or args.get("evidenceMessageRefs"))
    if evidence:
        payload["evidence_message_refs"] = evidence
    context = str(args.get("context") or "").strip()
    if context:
        payload["context"] = context
    return _handle_tgg_write("tgg_clarification_request", payload)


def _coerce_observed_at_epoch(value: Any) -> Any:
    """Coerce an observed_at value to epoch SECONDS (int) when possible.

    The tgg_case_update_state backend expects epoch seconds; agents naturally
    produce ISO-8601 strings (SK day-26 v6: Christopher sent
    '2026-05-26T11:02:58+08:00', got rejected, and burned a retry call with
    epoch). Accept both shapes at the tool boundary:

      * int/float (or numeric string)  -> int(value)
      * ISO-8601 string                -> int(datetime.timestamp());
        naive timestamps are treated as SGT (UTC+8) — TGG operates in
        Asia/Singapore and all message timestamps in scope are SGT.

    Anything unparseable is returned UNCHANGED so the backend stays the
    authority on rejection (no new tool-side validation surface).
    """
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return text
    try:
        return int(float(text))
    except ValueError:
        pass
    from datetime import datetime, timedelta, timezone

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
    return int(parsed.timestamp())


_SGT = ZoneInfo("Asia/Singapore")
_MONTH_NUMBERS = {
    name: month
    for month, names in enumerate(
        (
            (),
            ("jan", "january"),
            ("feb", "february"),
            ("mar", "march"),
            ("apr", "april"),
            ("may",),
            ("jun", "june"),
            ("jul", "july"),
            ("aug", "august"),
            ("sep", "sept", "september"),
            ("oct", "october"),
            ("nov", "november"),
            ("dec", "december"),
        )
    )
    for name in names
}
_ENGLISH_DATE_ONLY_RE = re.compile(
    r"^(?:receipt\s*date\s*:?\s*)?"
    r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})$",
    re.IGNORECASE,
)
_ISO_DATE_ONLY_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})$"
)
_SEPARATED_DATE_ONLY_RE = re.compile(
    r"^(?:receipt\s*date\s*:?\s*)?"
    r"(?P<day>\d{1,2})[-/](?P<month>[A-Za-z]+|\d{1,2})[-/](?P<year>\d{4})$",
    re.IGNORECASE,
)
_MONTH_FIRST_DATE_ONLY_RE = re.compile(
    r"^(?:receipt\s*date\s*:?\s*)?"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})$",
    re.IGNORECASE,
)


def _date_only_sgt_epoch(value: Any) -> int | None:
    """Return midnight-SGT epoch for a supported date-only string."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    match = _ISO_DATE_ONLY_RE.fullmatch(text)
    if match:
        raw_month = match.group("month")
    else:
        match = (
            _ENGLISH_DATE_ONLY_RE.fullmatch(text)
            or _SEPARATED_DATE_ONLY_RE.fullmatch(text)
            or _MONTH_FIRST_DATE_ONLY_RE.fullmatch(text)
        )
        if not match:
            return None
        raw_month = match.group("month").lower()
    month = int(raw_month) if raw_month.isdigit() else _MONTH_NUMBERS.get(raw_month)
    if month is None:
        return None
    try:
        parsed = datetime(
            int(match.group("year")),
            month,
            int(match.group("day")),
            tzinfo=_SGT,
        )
    except ValueError:
        return None
    return int(parsed.timestamp())


def _case_receipt_source_values(payload: Mapping[str, Any]) -> Iterable[Any]:
    """Yield explicit receipt sources before considering receivedAt."""
    for key in ("receiptDate", "receipt_date"):
        if payload.get(key) not in (None, ""):
            yield payload[key]
    evidence = payload.get("evidence")
    if isinstance(evidence, Mapping):
        for key in ("receiptDate", "receipt_date", "job_receipt_date"):
            if evidence.get(key) not in (None, ""):
                yield evidence[key]


def _normalise_case_receipt_epoch(payload: Mapping[str, Any]) -> Any:
    """Resolve the receipt epoch without model-side timezone arithmetic."""
    supplied = payload.get("receivedAt")
    if supplied in (None, ""):
        supplied = payload.get("received_at")
    supplied_epoch = _coerce_observed_at_epoch(supplied)

    source_epochs: list[int] = []
    for source_value in _case_receipt_source_values(payload):
        normalised = _date_only_sgt_epoch(source_value)
        if normalised is None:
            raise ValueError(
                "receiptDate must be a date only, for example "
                "'20 July 2026', '20-Jul-2026', '20/07/2026', "
                "'Jul 20, 2026', or '2026-07-20'"
            )
        source_epochs.append(normalised)
    if len(set(source_epochs)) > 1:
        raise ValueError(
            "conflicting receiptDate values were supplied; retry with the "
            "single exact date from the cited job sheet"
        )
    if source_epochs:
        return source_epochs[0]
    if isinstance(supplied, str):
        date_only_epoch = _date_only_sgt_epoch(supplied)
        if date_only_epoch is not None:
            return date_only_epoch
    return supplied_epoch


def _prepare_case_create_receipt(payload: Mapping[str, Any]) -> dict[str, Any] | str:
    """Bind receivedAt and dueAt mechanically on every case-create path."""
    prepared = dict(payload)
    try:
        receipt_timestamp = _normalise_case_receipt_epoch(prepared)
    except ValueError as exc:
        return tool_error(str(exc))
    if not isinstance(receipt_timestamp, int):
        return tool_error(
            "tgg_case_create requires receivedAt as the job-sheet receipt epoch "
            "seconds or receiptDate as a date-only source string."
        )
    prepared["receivedAt"] = receipt_timestamp
    prepared["dueAt"] = receipt_timestamp + 30 * 86400
    for key in ("received_at", "receiptDate", "receipt_date"):
        prepared.pop(key, None)
    return prepared


def _handle_tgg_case_update_state(args: Mapping[str, Any], **_kwargs: Any) -> str:
    job_no = str(args.get("job_no") or args.get("jobNo") or "").strip()
    if not job_no:
        return tool_error("tgg_case_update_state requires job_no")
    state = str(args.get("state") or "").strip().lower()
    if state != "completed":
        return tool_error(
            "tgg_case_update_state only accepts state='completed' (v1); "
            f"got {state!r}"
        )
    payload: dict[str, Any] = {"jobNo": job_no, "state": "completed"}
    evidence = _string_list(
        args.get("evidence_message_refs") or args.get("evidenceMessageRefs")
    )
    if evidence:
        payload["evidenceMessageRefs"] = evidence
    observed_at_raw = args.get("observed_at")
    if observed_at_raw is None:
        observed_at_raw = args.get("observedAt")
    observed_at = _coerce_observed_at_epoch(observed_at_raw)
    if observed_at is not None and observed_at != "":
        payload["observedAt"] = observed_at
    return _handle_tgg_write("tgg_case_update_state", payload)


def _handle_tgg_case_observation(args: Mapping[str, Any], **_kwargs: Any) -> str:
    raw = dict(args)
    fields = raw.get("fields") if isinstance(raw.get("fields"), Mapping) else {}
    fields = dict(fields)
    for source_key, field_key in (
        ("observedAt", "observed_at"),
        ("sourceRefs", "source_refs"),
        ("messageText", "message_text"),
        ("senderName", "sender_name"),
        ("chatName", "chat_name"),
    ):
        if raw.get(source_key) is not None and field_key not in fields:
            fields[field_key] = raw.get(source_key)
    source_refs = _filter_placeholder_source_refs(
        _string_list(fields.get("source_refs") or raw.get("sourceRefs"))
    )
    if not source_refs:
        source_refs = _current_turn_source_refs()
        if not source_refs:
            return tool_error(
                "tgg_case_observation requires non-empty sourceRefs. Cite the "
                "WhatsApp message id(s) that support this observation; photos are "
                "attached server-side from those source refs."
            )
    fields["source_refs"] = source_refs
    # Media attachment is mechanical. Christopher cites source messages; the
    # systems API derives media_refs/photo_count from message_ledger. Strip any
    # model-supplied raw media/photo fields so the LLM cannot silently create
    # broken gallery refs.
    for key in ("mediaRefs", "media_refs", "photoCount", "photo_count"):
        raw.pop(key, None)
        fields.pop(key, None)
    payload = {
        "jobNo": raw.get("jobNo"),
        "source": raw.get("source") or "whatsapp",
        "fields": fields,
        "notes": raw.get("notes"),
        "confidence": raw.get("confidence"),
    }
    # v6.3 item 4b: the backend observation response carries only
    # {observationId} — echo the case's last-known backend state into the
    # success result so fresh state is in-face at evidence-attach time.
    return _annotate_observation_result(
        _handle_tgg_write("tgg_case_observation", payload),
        payload.get("jobNo"),
    )


_JOB_NO_RE = re.compile(r"\b[A-Z]{2}/JOB/\d{4}/\d{1,4}\b")

_CREATE_JOB_NO_ALIASES = ("reportedJobNo", "reported_job_no", "job_no", "jobno", "jobNumber")


def _handle_tgg_case_create(args: Mapping[str, Any], **_kwargs: Any) -> str:
    payload = dict(args)

    # New source documents are their own citation. Bind the current turn's
    # exact message ids mechanically so the case and its opening observation
    # retain the same evidence the model read.
    if not payload.get("evidenceMessageRefs") and not payload.get("evidence_message_refs"):
        current_refs = _current_turn_source_refs()
        if current_refs:
            payload["evidenceMessageRefs"] = current_refs
            # A source-native job sheet is direct observed evidence, not an
            # inferred candidate. Keep the backend decision gate explicit even
            # when the model omits this mechanical classification.
            payload.setdefault("confidence", "high")

    # Alias coercion: the model sometimes invents sibling names for the job
    # number param (PG day-26 run passed reportedJobNo); the backend only
    # honors jobNo and silently mints a WA/JOB placeholder otherwise.
    if not str(payload.get("jobNo") or "").strip():
        for alias in _CREATE_JOB_NO_ALIASES:
            value = str(payload.get(alias) or "").strip()
            if value and _JOB_NO_RE.search(value.upper()):
                payload["jobNo"] = value.upper()
                break
    for alias in _CREATE_JOB_NO_ALIASES:
        payload.pop(alias, None)

    # Corrective gate: cases enter the ledger only from HDB job sheets, so a
    # create without jobNo is bounced. When the evidence text plainly carries
    # a job number the param almost always got lost — bounce with the found
    # token(s) so the model self-corrects in-turn. confirmNoJobNo=true is the
    # explicit-operator-instruction escape hatch.
    if not str(payload.get("jobNo") or "").strip() and not payload.get("confirmNoJobNo"):
        evidence_blob = json.dumps(payload.get("evidence") or {}, ensure_ascii=False)
        found = sorted(set(_JOB_NO_RE.findall(evidence_blob.upper())))
        if found:
            return tool_error(
                "JOB_NO_OMITTED: the evidence text contains job number(s) "
                f"{', '.join(found)} but no jobNo was passed — cases are "
                "created only under an HDB job number. If the number belongs "
                "to THIS case, re-call with jobNo set to it exactly. If it "
                "only references a different/previous case, this report is "
                "not a new case: record a tgg_case_observation against the "
                "matched case or hold it via tgg_clarification_request."
            )
        return tool_error(
            "JOB_NO_REQUIRED: cases enter the ledger only from HDB job "
            "sheets — tgg_case_create requires an explicit HDB jobNo. A "
            "worker report with no job sheet is not a new case: record it "
            "with tgg_case_observation against a matched case, or hold it "
            "via tgg_clarification_request (\"no HDB job sheet found for "
            "this report — holding it as pending; send the job number when "
            "issued\"). confirmNoJobNo: true is allowed only on explicit "
            "operator instruction."
        )
    payload.pop("confirmNoJobNo", None)
    prepared = _prepare_case_create_receipt(payload)
    if isinstance(prepared, str):
        return prepared
    payload = prepared
    return _handle_tgg_write("tgg_case_create", payload)


def _handle_business_call(args: Mapping[str, Any], *, user_task: Any = None) -> str:
    operation = str(args.get("operation") or "").strip()
    if not operation:
        return tool_error("operation is required")
    payload = args.get("payload") or {}
    if not _user_task_allows_ilinked(operation, user_task, payload):
        return tool_error(
            "iLinked/source-system reads are opt-in. Use operator DB operations "
            "for normal case questions. Call iLinked operations only when the "
            "current user message explicitly says iLinked, HDB, source-system, "
            "or asks for WC/work-costing details. If the current user did make "
            "that explicit request, include source_system_requested=true in the "
            "payload."
        )
    payload = _strip_control_payload_keys(payload)
    try:
        bridge = _load_runtime_bridge_config()
        configured = bridge.operations.get(operation)
        canonical = None
        if configured is None:
            canonical = {
                "tgg_clarification_request": "tgg_clarification_raise",
                "tgg_message_history_search": "message_search",
                "tgg_case_update_state": "tgg_case_update",
            }.get(operation)
            configured = bridge.operations.get(canonical) if canonical else None
        effective_operation = canonical or operation
        if effective_operation == "tgg_case_observation":
            payload = _bind_observation_source_refs(payload)
        elif effective_operation == "tgg_case_create":
            payload = dict(payload)
            if not payload.get("evidenceMessageRefs") and not payload.get("evidence_message_refs"):
                current_refs = _current_turn_source_refs()
                if current_refs:
                    payload["evidenceMessageRefs"] = current_refs
                    payload.setdefault("confidence", "high")
            prepared = _prepare_case_create_receipt(payload)
            if isinstance(prepared, str):
                return prepared
            payload = prepared
        # Recover only declared path params accidentally placed beside the
        # generic payload object; arbitrary top-level args never cross over.
        if configured is not None:
            for key in configured.path_params:
                if payload.get(key) is None and args.get(key) is not None:
                    payload[key] = args.get(key)
        result = execute_business_operation(
            bridge,
            operation=operation,
            payload=payload,
        )
    except Exception as exc:
        return tool_error(exc)
    return tool_result(_shape_attach_unjustified_result(result))


def _current_turn_source_refs() -> list[str]:
    """Exact inbound refs bound by the gateway for this concurrent turn."""
    try:
        from gateway.session_context import get_session_env

        encoded_refs = get_session_env("HERMES_SESSION_SOURCE_MESSAGE_REFS", "")
        decoded_refs = json.loads(encoded_refs) if encoded_refs else []
        return _string_list(decoded_refs)
    except Exception:
        return []


# The constitution instructs the model to OMIT sourceRefs so the runtime binds
# the current turn's real message ids. Some models instead pass the literal
# placeholder string "current_turn"; a stored placeholder resolves no WhatsApp
# excerpt and can never derive media (stage-1 backprocess finding, 2026-07-20).
# A placeholder is not a citable id — treat it exactly like an omitted field.
_SOURCE_REF_PLACEHOLDERS = frozenset({"current_turn"})


def _filter_placeholder_source_refs(refs: list[str]) -> list[str]:
    """Drop placeholder tokens the model emits instead of real message ids."""
    return [
        ref for ref in refs if ref.strip().lower() not in _SOURCE_REF_PLACEHOLDERS
    ]


def _bind_observation_source_refs(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize observation sourceRefs: strip placeholders, bind turn refs.

    Collects refs from every location the backend honors (top-level
    sourceRefs/source_refs and the same keys inside fields), drops placeholder
    and empty entries, and when nothing real remains binds the gateway's
    current-turn message ids — exactly as when the field was omitted.
    Payloads that already carry only real ids pass through untouched.
    """
    fields = payload.get("fields")
    fields = fields if isinstance(fields, Mapping) else None
    collected: list[str] = []
    for container in (payload, fields or {}):
        for key in ("sourceRefs", "source_refs"):
            collected.extend(_string_list(container.get(key)))
    cleaned: list[str] = []
    for ref in _filter_placeholder_source_refs(collected):
        if ref not in cleaned:
            cleaned.append(ref)
    if cleaned and len(cleaned) == len(collected):
        return payload
    payload = dict(payload)
    payload.pop("source_refs", None)
    if fields is not None and ("sourceRefs" in fields or "source_refs" in fields):
        fields = dict(fields)
        fields.pop("sourceRefs", None)
        fields.pop("source_refs", None)
        payload["fields"] = fields
    bound = cleaned or _current_turn_source_refs()
    if bound:
        payload["sourceRefs"] = bound
    else:
        payload.pop("sourceRefs", None)
    return payload


# Recovery contract for the backend's evidence-attach gate. Mirrors
# validateEvidenceAttachJustification in systems (routes: observations with
# media-bearing sourceRefs, work-costing attach with a sourceRef). Without
# this in-face guidance the model's observed failure mode is dropping the
# photo-bearing message ids to get past the gate (stage-1 finding 2, 2026-07-20).
_ATTACH_UNJUSTIFIED_RECOVERY = (
    "Retry the SAME write keeping ALL cited sourceRefs — never drop photo or "
    "media message ids to get past this gate; dropping evidence to pass "
    "validation is forbidden. Add or fix a top-level justification object in "
    "the same payload: {\"kind\": one of identifier_match | thread_continuation "
    "| operator_directive, \"identifier\": {\"type\": job_no | block_unit, "
    "\"value\": the job number or block/unit EXACTLY as it appears in the "
    "source material}, \"source\": one of caption | image_content | "
    "thread_ref}. The identifier value must appear verbatim in the cited "
    "message text/caption/thread; for source=image_content also pass what is "
    "visible in a top-level image_content string. If no truthful justification "
    "exists, the evidence does not belong on this case — hold it via operation "
    "tgg_attention_raise instead of attaching without it."
)


def _shape_attach_unjustified_result(result: Any) -> Any:
    """Append recovery guidance to ATTACH_UNJUSTIFIED backend rejections."""
    try:
        if not isinstance(result, dict):
            return result
        error = result.get("error")
        if isinstance(error, Mapping):
            code = str(error.get("code") or "")
        else:
            code = str(error or "")
        if "ATTACH_UNJUSTIFIED" not in code:
            return result
        shaped = dict(result)
        shaped["recovery"] = _ATTACH_UNJUSTIFIED_RECOVERY
        return shaped
    except Exception:
        return result


_PA_BUSINESS_PAYLOAD_SCHEMA = {
    "type": "object",
    "description": "JSON payload to pass through to the configured PA business operation.",
    "additionalProperties": True,
}


PA_BUSINESS_READ_SCHEMA = {
    "name": "pa_business_read",
    "description": (
        "Run an opt-in configured PA business read operation. The tool calls "
        "an external HTTP endpoint or local command and returns JSON; it does "
        "not persist business facts in Hermes state."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "description": "Configured PA business operation name.",
            },
            "payload": _PA_BUSINESS_PAYLOAD_SCHEMA,
        },
        "required": ["operation"],
    },
}


PA_BUSINESS_WRITE_SCHEMA = {
    "name": "pa_business_write",
    "description": (
        "Run an opt-in configured PA business write operation. The tool calls "
        "an external HTTP endpoint or local command and returns JSON; it does "
        "not persist business facts in Hermes state."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "description": "Configured PA business operation name.",
            },
            "payload": _PA_BUSINESS_PAYLOAD_SCHEMA,
        },
        "required": ["operation"],
    },
}


TGG_CASE_LOOKUP_SCHEMA = {
    "name": "tgg_case_lookup",
    "description": "Look up exactly one TGG operator case by real job number, e.g. SK/JOB/2604/2376. Do not use for unit numbers.",
    "parameters": {
        "type": "object",
        "properties": {
            "jobNo": {
                "type": "string",
                "description": "Exact TGG/HDB job number such as SK/JOB/2604/2376.",
            },
        },
        "required": ["jobNo"],
        "additionalProperties": False,
    },
}


TGG_CASE_PHOTOS_SCHEMA = {
    "name": "tgg_case_photos",
    "description": (
        "Retrieve retained image files for one TGG case by exact job number. "
        "Returns a bounded list of validated local image paths derived from "
        "opaque Systems media refs. Known cases with no photos return count 0."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "job_no": {
                "type": "string",
                "description": "Exact job number, e.g. SK/JOB/2604/2376.",
            },
        },
        "required": ["job_no"],
        "additionalProperties": False,
    },
}


TGG_CASE_QUERY_SCHEMA = {
    "name": "tgg_case_query",
    "description": (
        "Answer AGGREGATE / analytical management questions (counts, "
        "breakdowns, oldest/newest, cases missing evidence) by running ONE "
        "read-only SQL SELECT against the TGG case database. The server "
        "enforces read-only: single SELECT statement (WITH ... SELECT is "
        "fine), no writes/PRAGMA/ATTACH/semicolon chains, ~200-row cap with "
        "a truncated flag, results as {columns, rows, rowCount, truncated}. "
        "For a single known case use tgg_case_lookup; for finding a case by "
        "address/unit use tgg_case_search. Schema (SQLite; timestamps are "
        "epoch SECONDS; use datetime(col,'unixepoch','+8 hours') for SGT): "
        "cases(id, job_no, wc_no, zone, report_zone, priority, address, "
        "block, unit, street_name, postcode, contact_name, contact_phone, "
        "problem, state IN "
        "('open','hdb_confirmed','in_progress','completed','closed',"
        "'cancelled','dismissed_not_a_case','disputed'), completed_at, "
        "due_at, job_receipt_date, service_line, type_of_work, job_status, "
        "linkfm_status, feedback, ma_work_coordinator, hdb_officer_name, "
        "normalized_job_no, created_at, updated_at). "
        "case_observations(id, case_id -> cases.id, source, source_ref, "
        "observed_at, fields JSON, confidence, notes, created_at) — evidence "
        "and work items attach here; fields is a JSON blob, work items are "
        "under json_each(fields,'$.work_items') with per-item label/status; "
        "'cases with no evidence' ~= cases with no case_observations rows. "
        "attention_items(id, kind, severity, title, meta, context, case_id, "
        "state, handled_at, handled_by, detected_at, created_at, updated_at). "
        "attention_item_notes(id, attention_item_id, note, author, "
        "created_at). Example: SELECT count(*) AS n FROM cases WHERE "
        "block='314' AND state NOT IN ('closed','completed','cancelled',"
        "'dismissed_not_a_case'). Always alias aggregates, add ORDER BY + "
        "LIMIT for top-N questions, and state in your reply which rows the "
        "answer came from."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": (
                    "One SQLite SELECT statement. No semicolons, no writes — "
                    "the endpoint is read-only and will reject anything else."
                ),
            },
        },
        "required": ["sql"],
        "additionalProperties": False,
    },
}


TGG_SPREADSHEET_JOB_NUMBERS_SCHEMA = {
    "name": "tgg_spreadsheet_job_numbers",
    "description": (
        "Open a spreadsheet that passed the TGG media gate and extract the "
        "unique job-number column. Accepts only macro-free XLSX or text CSV, "
        "revalidates content before reading, and never executes workbook code. "
        "After this tool returns, use the existing tgg_case_query tool to "
        "cross-check the returned job numbers against live case state."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Exact local spreadsheet path from the message.",
            },
        },
        "required": ["path"],
        "additionalProperties": False,
    },
}


TGG_CASE_SEARCH_SCHEMA = {
    "name": "tgg_case_search",
    "description": (
        "Search TGG operator cases. Returns a compact candidate list "
        "{candidates: [{jobNo, address, block, unit, state, problem, "
        "matchBasis}], count} (limit 10). matchBasis names why each candidate "
        "surfaced (unit_exact, unit_exact_block_mismatch, job_no, "
        "block_street_fuzzy, text_like). The search is deliberately generous "
        "(recall) — it returns CANDIDATES with matchBasis; YOU judge which "
        "(if any) is the same job. Multiple plausible candidates or "
        "conflicting evidence → use tgg_clarification_request instead of "
        "guessing. Identity params (jobNo, block, unit — taken from the "
        "message AND its quoted context) are the primary search keys; "
        "free-text search is a last resort when none of them exist. "
        "workType/problem are reasoning hints, only used as search text when "
        "no address or job number exists."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "search": {
                "type": "string",
                "description": "Free text LAST-RESORT fallback — never put work-description prose here when a jobNo, block, or unit is available in the message or its quoted context. If block/street/unit or address is known, keep this to address text only.",
            },
            "address": {"type": "string", "description": "Structured address if known, e.g. 'Blk 350 Anchorvale Rd #11-109'."},
            "block": {"type": "string", "description": "Structured block number if known, e.g. '350'."},
            "street": {"type": "string", "description": "Structured street/name if known, e.g. 'Anchorvale Rd'."},
            "unit": {"type": "string", "description": "Structured unit if known, e.g. '#11-109'. Matches stored units with or without '#'."},
            "jobNo": {"type": "string", "description": "Full or partial job number, e.g. 'SK/JOB/2605/2480' or '2605/2480'. Contains-match; typo'd fragments are fine here."},
            "workType": {"type": "string", "description": "Structured work type/problem if known. Used for reasoning after address candidates return, not mixed into address search."},
            "problem": {"type": "string", "description": "Structured problem/work description if known. Used for reasoning after address candidates return, not mixed into address search."},
            "limit": {"type": "integer", "description": "Maximum candidates to return.", "default": 10},
            "serviceLine": {"type": "string", "description": "Optional service line, usually maintenance or sprucing."},
            "sourceStatus": {"type": "string", "description": "Optional source status filter."},
            "progressStatus": {"type": "string", "description": "Optional progress status filter."},
            "state": {"type": "string", "description": "Optional raw case state filter."},
        },
        "required": [],
        "additionalProperties": False,
    },
}


TGG_MESSAGE_HISTORY_SEARCH_SCHEMA = {
    "name": "tgg_message_history_search",
    "description": (
        "Search the WhatsApp message archive (all group chats, full history) "
        "for prior announcements/reports about a unit, job number, or topic. "
        "Use BEFORE creating any case with a WA/JOB placeholder number — the "
        "real job number is often in an earlier announcement (possibly in a "
        "different chat or with a typo'd street name; prefer block+unit search)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "q": {
                "type": "string",
                "description": "Free text search over message text. Multiple words are ANDed; keep it to 1-3 distinctive words.",
            },
            "block": {"type": "string", "description": "Block number to match in message text, e.g. '446A'."},
            "unit": {"type": "string", "description": "Unit to match in message text, e.g. '#03-326' (matches with or without '#'/spaces)."},
            "jobNo": {"type": "string", "description": "Full or partial job number to match, e.g. 'SK/JOB/2605/2480' or '2605/2480'."},
            "chat_jid": {"type": "string", "description": "Optional: restrict to one chat jid. Usually omit — announcements often live in a different chat."},
            "before_ts": {"type": "integer", "description": "Optional: only return messages sent before this epoch-seconds timestamp."},
            "limit": {"type": "integer", "description": "Maximum messages to return (default 20, max 50)."},
        },
        "required": [],
        "additionalProperties": False,
    },
}


# Client-agnostic alias: deliberately NOT a spread of the tgg_ schema — the
# generic surface carries ONLY client-agnostic params (q, chat_jid, before_ts,
# limit).  Structured client anchors (block/unit/job_no) belong to the
# tgg_-prefixed variant.
MESSAGE_HISTORY_SEARCH_SCHEMA = {
    "name": "message_history_search",
    "description": (
        "Search the message archive (all chats, full history) for prior "
        "messages about a topic. Free-text search; restrict to one chat or a "
        "time window when needed."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "q": {
                "type": "string",
                "description": "Free text search over message text. Multiple words are ANDed; keep it to 1-3 distinctive words.",
            },
            "chat_jid": {"type": "string", "description": "Optional: restrict to one chat jid. Usually omit."},
            "before_ts": {"type": "integer", "description": "Optional: only return messages sent before this epoch-seconds timestamp."},
            "limit": {"type": "integer", "description": "Maximum messages to return (default 20, max 50)."},
        },
        "required": [],
        "additionalProperties": False,
    },
}


TGG_CLARIFICATION_REQUEST_SCHEMA = {
    "name": "tgg_clarification_request",
    "description": (
        "Record a clarification for the OPERATOR when case-matching evidence "
        "is ambiguous: multiple plausible candidate cases, a completed case "
        "matching new same-shape work, or a report whose unit/job cannot be "
        "resolved. Shape: propose and confirm, not an open question — state "
        "your read, the action you will take by default, and invite "
        "correction (e.g. \"we'll add it to the existing case, let me know if "
        "you want me to record otherwise\"). The proposed default must never "
        "be opening a new case. The clarification is recorded for the "
        "operator to review later; it is NEVER sent to any WhatsApp chat and "
        "no answer will arrive this turn. After calling: proceed on your "
        "stated default without assuming a different answer, and do NOT "
        "create a placeholder case for the work you just asked about — the "
        "clarification IS the record. Do not use this when evidence is clear "
        "(over-asking is noise). Max one clarification per case decision."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": (
                    "Propose-and-confirm text naming the unit/address and "
                    "candidate job(s): your one-line read, the default action "
                    "you will take, and an invite to correct — not an "
                    "open-ended question."
                ),
            },
            "candidate_job_nos": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Job numbers of the candidate cases under consideration.",
            },
            "evidence_message_refs": {
                "type": "array",
                "items": {"type": "string"},
                "description": "WhatsApp message refs/ids that triggered the question.",
            },
            "context": {
                "type": "string",
                "description": "Short factual context: what was reported, what the candidates show, what is missing.",
            },
        },
        "required": ["question"],
        "additionalProperties": False,
    },
}


# Client-agnostic alias: same recording semantics, but the candidate list is
# the agnostic ``candidate_refs`` (no client-shaped candidate_job_nos param).
CLARIFICATION_REQUEST_SCHEMA = {
    "name": "clarification_request",
    "description": TGG_CLARIFICATION_REQUEST_SCHEMA["description"],
    "parameters": {
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": (
                    "Propose-and-confirm text naming the entity/candidates "
                    "involved: your one-line read, the default action you "
                    "will take, and an invite to correct — not an open-ended "
                    "question."
                ),
            },
            "candidate_refs": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Identifiers of the candidate records under consideration.",
            },
            "evidence_message_refs": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Message refs/ids that triggered the question.",
            },
            "context": {
                "type": "string",
                "description": "Short factual context: what was reported, what the candidates show, what is missing.",
            },
        },
        "required": ["question"],
        "additionalProperties": False,
    },
}


TGG_CASE_UPDATE_STATE_SCHEMA = {
    "name": "tgg_case_update_state",
    "description": (
        "Mark a TGG case COMPLETED from worker-report evidence. Use ONLY when "
        "the case's scope is clearly what the report says is done (e.g. case "
        "says pipe leak, report says 'epoxy applied, done') — cite the report "
        "messages as evidence_message_refs. If the report covers only part of "
        "the scope, or the scope is unclear, do NOT complete: record a "
        "tgg_case_observation and ask via tgg_clarification_request ('is this "
        "completed?') instead. Never complete on ambiguity. Only "
        "state='completed' is accepted in v1."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "job_no": {
                "type": "string",
                "description": "Exact job number of the case to mark completed, e.g. SK/JOB/2604/2376.",
            },
            "state": {
                "type": "string",
                "enum": ["completed"],
                "description": "Target state. Only 'completed' is accepted (v1).",
            },
            "evidence_message_refs": {
                "type": "array",
                "items": {"type": "string"},
                "description": "WhatsApp message refs/ids evidencing the completion (the worker report).",
            },
            "observed_at": {
                "type": "string",
                "description": (
                    "When the completion was observed, as epoch SECONDS "
                    "(integer), e.g. 1779700000 — this is what the backend "
                    "expects. ISO-8601 strings (e.g. "
                    "'2026-05-26T11:02:58+08:00') are also accepted and "
                    "coerced to epoch seconds; naive timestamps are treated "
                    "as SGT."
                ),
            },
        },
        "required": ["job_no", "state"],
        "additionalProperties": False,
    },
}


TGG_CASE_OBSERVATION_SCHEMA = {
    "name": "tgg_case_observation",
    "description": "Record WhatsApp evidence or worker updates against an existing TGG case. Requires a real job number; dry-run mode validates without writing.",
    "parameters": {
        "type": "object",
        "properties": {
            "jobNo": {"type": "string", "description": "Exact job number to attach the observation to."},
            "source": {"type": "string", "description": "Observation source, e.g. whatsapp."},
            "observedAt": {"type": "string", "description": "Observed time in SGT or ISO format."},
            "notes": {"type": "string", "description": "Short factual observation notes."},
            "confidence": {"type": "string", "description": "Evidence confidence, e.g. observed, high, low."},
            "fields": {"type": "object", "description": "Structured extracted facts.", "additionalProperties": True},
            "messageText": {"type": "string", "description": "Original message text or bundled message summary."},
            "senderName": {"type": "string", "description": "WhatsApp sender name/id."},
            "chatName": {"type": "string", "description": "WhatsApp group/chat name."},
            "sourceRefs": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "description": (
                    "Required source WhatsApp message IDs/refs. Cite the "
                    "message(s) that support this observation; do not supply "
                    "media refs or photo counts."
                ),
            },
        },
        "required": ["jobNo", "source", "observedAt", "notes", "confidence", "sourceRefs"],
        "additionalProperties": False,
    },
}


TGG_CASE_CREATE_SCHEMA = {
    "name": "tgg_case_create",
    "description": (
        "Create a new TGG case from an HDB job sheet after search finds no "
        "matching existing case. Creation requires an HDB job number — "
        "creates without jobNo are not allowed. A worker report with no job "
        "sheet is never a new case: record it with tgg_case_observation "
        "against a matched case, or hold it via tgg_clarification_request. "
        "Dry-run mode validates without writing."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "jobNo": {
                "type": "string",
                "description": (
                    "The HDB job number stated in the message (e.g. 'Job no: "
                    "PG/JOB/2605/0973'). REQUIRED — pass it EXACTLY as "
                    "written; the case is created under this number. Cases "
                    "enter the ledger only from HDB job sheets; there is no "
                    "placeholder path for creates without a job number."
                ),
            },
            "confirmNoJobNo": {
                "type": "boolean",
                "description": (
                    "Set true ONLY when the OPERATOR has explicitly "
                    "instructed you to record this as a case despite no HDB "
                    "job number. Never set it on your own judgment — a "
                    "worker report without a job sheet is an observation or "
                    "a clarification, not a case."
                ),
            },
            "zone": {"type": "string", "description": "TGG zone if known."},
            "serviceLine": {"type": "string", "description": "maintenance or sprucing."},
            "address": {"type": "string", "description": "Case address."},
            "problem": {"type": "string", "description": "Problem or work description."},
            "source": {"type": "string", "description": "Source, e.g. whatsapp."},
            "observedAt": {"type": "string", "description": "Observed time in SGT or ISO format."},
            "receivedAt": {
                "type": "integer",
                "description": (
                    "Job-sheet receipt timestamp as epoch seconds. Optional "
                    "when receiptDate carries the exact date-only source."
                ),
            },
            "receiptDate": {
                "type": "string",
                "description": (
                    "Exact receipt date string from the job sheet when it "
                    "states a date without a time. Hermes normalizes this "
                    "source to midnight Asia/Singapore and computes dueAt."
                ),
            },
            "evidence": {"type": "object", "description": "Evidence used to decide this is new.", "additionalProperties": True},
        },
        "required": ["zone", "address", "problem", "source"],
        "additionalProperties": True,
    },
}


registry.register(
    name="pa_business_read",
    toolset="pa-business",
    schema=PA_BUSINESS_READ_SCHEMA,
    handler=_handle_business_read,
    check_fn=_bridge_available,
)

registry.register(
    name="pa_business_write",
    toolset="pa-business",
    schema=PA_BUSINESS_WRITE_SCHEMA,
    handler=_handle_business_write,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_lookup",
    toolset="pa-business",
    schema=TGG_CASE_LOOKUP_SCHEMA,
    handler=_handle_tgg_case_lookup,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_photos",
    toolset="pa-business",
    schema=TGG_CASE_PHOTOS_SCHEMA,
    handler=_handle_tgg_case_photos,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_query",
    toolset="pa-business",
    schema=TGG_CASE_QUERY_SCHEMA,
    handler=_handle_tgg_case_query,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_spreadsheet_job_numbers",
    toolset="pa-business",
    schema=TGG_SPREADSHEET_JOB_NUMBERS_SCHEMA,
    handler=_handle_tgg_spreadsheet_job_numbers,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_search",
    toolset="pa-business",
    schema=TGG_CASE_SEARCH_SCHEMA,
    handler=_handle_tgg_case_search,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_message_history_search",
    toolset="pa-business",
    schema=TGG_MESSAGE_HISTORY_SEARCH_SCHEMA,
    handler=_handle_tgg_message_history_search,
    check_fn=_bridge_available,
)

registry.register(
    name="message_history_search",
    toolset="pa-business",
    schema=MESSAGE_HISTORY_SEARCH_SCHEMA,
    handler=_handle_generic_message_history_search,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_clarification_request",
    toolset="pa-business",
    schema=TGG_CLARIFICATION_REQUEST_SCHEMA,
    handler=_handle_tgg_clarification_request,
    check_fn=_bridge_available,
)

registry.register(
    name="clarification_request",
    toolset="pa-business",
    schema=CLARIFICATION_REQUEST_SCHEMA,
    handler=_handle_generic_clarification_request,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_update_state",
    toolset="pa-business",
    schema=TGG_CASE_UPDATE_STATE_SCHEMA,
    handler=_handle_tgg_case_update_state,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_observation",
    toolset="pa-business",
    schema=TGG_CASE_OBSERVATION_SCHEMA,
    handler=_handle_tgg_case_observation,
    check_fn=_bridge_available,
)

registry.register(
    name="tgg_case_create",
    toolset="pa-business",
    schema=TGG_CASE_CREATE_SCHEMA,
    handler=_handle_tgg_case_create,
    check_fn=_bridge_available,
)
