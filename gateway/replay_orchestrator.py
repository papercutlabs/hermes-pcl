"""PA replay run orchestrator.

This module owns the run lifecycle around the native Hermes replay primitive:
prepare an isolated target through a provider, run Hermes replay with immutable
run/target provenance, mechanically verify the result, and only then request a
provider promotion/rollback.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional

from hermes_constants import get_hermes_home

from gateway.replay import ReplayPlan, canonical_digest, canonical_json


RUN_MANIFEST_VERSION = 1
NON_DELIVERING_REPLAY_DELIVERY_MODES = {"capture", "drop"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _epoch() -> float:
    return time.time()


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return str(value)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(
            payload, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_hex_manifest(value: Any) -> str:
    """Digest format used by the systems-pcl provider: bare sha256 hex."""
    import hashlib

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _safe_id_part(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "_.:-" else "-" for ch in value)


def tenant_confirmation_token(prefix: str, tenant: str) -> str:
    normalized = "".join(
        ch if ch.isalnum() else "_" for ch in str(tenant).strip().upper()
    ).strip("_")
    if not normalized:
        raise ValueError("tenant is required")
    return f"{prefix}_{normalized}_TARGET"


def mint_replay_run_id(prefix: str = "pa-replay") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}-{uuid.uuid4().hex[:8]}"


def _get_nested(mapping: Mapping[str, Any], *path: str, default: Any = None) -> Any:
    cur: Any = mapping
    for key in path:
        if not isinstance(cur, Mapping) or key not in cur:
            return default
        cur = cur[key]
    return cur


class ReplayRunState(str, Enum):
    INITIALIZED = "initialized"
    PREPARING_TARGET = "preparing_target"
    PREPARED = "prepared"
    RUNNING_AGENT_REPLAY = "running_agent_replay"
    REPLAYED = "replayed"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    PROMOTING = "promoting"
    PROMOTED = "promoted"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    DIRTY = "dirty"
    FAILED = "failed"


TERMINAL_STATES = {
    ReplayRunState.PROMOTED.value,
    ReplayRunState.ROLLED_BACK.value,
    ReplayRunState.DIRTY.value,
    ReplayRunState.FAILED.value,
}


class ReplayOrchestratorError(RuntimeError):
    """Base error for replay orchestration failures."""


class ReplayStateError(ReplayOrchestratorError):
    """Invalid state transition or unsafe state."""


class ReplayVerifyError(ReplayOrchestratorError):
    """Mechanical verify gate failed."""


class ReplayProviderError(ReplayOrchestratorError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        code: str | None = None,
        payload: Any = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.payload = payload


@dataclass(frozen=True)
class VerifyGateConfig:
    """Mechanical verify gate thresholds.

    Defaults are deliberately strict: zero unexpected outbound, zero failed
    turns, zero tool errors, and provider invariants must pass.
    """

    tool_error_budget: int = 0
    allowed_outbound_kinds: tuple[str, ...] = ()
    expected_turn_count: Optional[int] = None
    min_turn_count: Optional[int] = None
    require_provider_invariants: bool = True

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "VerifyGateConfig":
        data = data or {}
        allowed = (
            data.get("allowed_outbound_kinds") or data.get("allowedOutboundKinds") or ()
        )
        if isinstance(allowed, str):
            allowed_tuple = tuple(
                part.strip() for part in allowed.split(",") if part.strip()
            )
        else:
            allowed_tuple = tuple(str(part) for part in allowed if str(part))
        expected = data.get("expected_turn_count", data.get("expectedTurnCount"))
        min_turns = data.get("min_turn_count", data.get("minTurnCount"))
        budget = data.get("tool_error_budget", data.get("toolErrorBudget", 0))
        return cls(
            tool_error_budget=int(budget or 0),
            allowed_outbound_kinds=allowed_tuple,
            expected_turn_count=int(expected) if expected is not None else None,
            min_turn_count=int(min_turns) if min_turns is not None else None,
            require_provider_invariants=bool(
                data.get(
                    "require_provider_invariants",
                    data.get("requireProviderInvariants", True),
                )
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "tool_error_budget": self.tool_error_budget,
            "allowed_outbound_kinds": list(self.allowed_outbound_kinds),
            "expected_turn_count": self.expected_turn_count,
            "min_turn_count": self.min_turn_count,
            "require_provider_invariants": self.require_provider_invariants,
        }


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    actual: Any = None
    expected: Any = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        out = {"name": self.name, "ok": self.ok, "actual": self.actual}
        if self.expected is not None:
            out["expected"] = self.expected
        if self.detail:
            out["detail"] = self.detail
        return out


@dataclass(frozen=True)
class ReplayTargetProviderConfig:
    provider_url: str
    admin_token: str
    tenant: str
    timeout_seconds: float = 30.0


class ReplayTargetProviderClient:
    """HTTP client for the systems-pcl replay target provider contract."""

    def __init__(self, config: ReplayTargetProviderConfig):
        if not config.provider_url:
            raise ValueError("provider_url is required")
        if not config.admin_token:
            raise ValueError("provider admin token is required")
        if not config.tenant:
            raise ValueError("tenant is required")
        self.config = config

    def _url(self, path: str, *, query: Mapping[str, Any] | None = None) -> str:
        import urllib.parse

        base = self.config.provider_url.rstrip("/")
        url = f"{base}{path}"
        params = {"tenant": self.config.tenant}
        for key, value in (query or {}).items():
            if value is not None:
                params[key] = value
        return f"{url}?{urllib.parse.urlencode(params)}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
    ) -> Any:
        import httpx

        headers = {
            "Authorization": f"Bearer {self.config.admin_token}",
            "Accept": "application/json",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            with httpx.Client(
                timeout=self.config.timeout_seconds, follow_redirects=True
            ) as client:
                response = client.request(
                    method,
                    self._url(path, query=query),
                    json=dict(body or {}) if body is not None else None,
                    headers=headers,
                )
        except (
            httpx.HTTPError
        ) as exc:  # pragma: no cover - exact subclasses are environment-specific
            raise ReplayProviderError(f"provider request failed: {exc}") from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise ReplayProviderError(
                f"provider returned non-json status={response.status_code}",
                status_code=response.status_code,
                payload=response.text,
            ) from exc
        if response.status_code >= 400 or not payload.get("ok"):
            error = payload.get("error") if isinstance(payload, Mapping) else None
            code = error.get("code") if isinstance(error, Mapping) else None
            message = (
                error.get("message")
                if isinstance(error, Mapping)
                else f"provider status={response.status_code}"
            )
            raise ReplayProviderError(
                str(message),
                status_code=response.status_code,
                code=code,
                payload=payload,
            )
        return payload.get("data")

    def prepare(
        self, *, run_id: str, source_data_dir: str, target_data_dir: str, base_url: str
    ) -> dict[str, Any]:
        return dict(
            self._request(
                "POST",
                "/api/operator/replay-target/prepare",
                body={
                    "runId": run_id,
                    "sourceDataDir": source_data_dir,
                    "targetDataDir": target_data_dir,
                    "baseUrl": base_url,
                },
            )
        )

    def descriptor(self, *, run_id: str, data_dir: str) -> dict[str, Any]:
        return dict(
            self._request(
                "GET",
                f"/api/operator/replay-target/descriptor/{run_id}",
                query={"dataDir": data_dir},
            )
        )

    def verify(self, *, run_id: str, data_dir: str) -> dict[str, Any]:
        return dict(
            self._request(
                "POST",
                "/api/operator/replay-target/verify",
                body={"runId": run_id, "dataDir": data_dir},
            )
        )

    def mark_dirty(self, *, run_id: str, data_dir: str, reason: str) -> dict[str, Any]:
        return dict(
            self._request(
                "POST",
                "/api/operator/replay-target/dirty",
                body={"runId": run_id, "dataDir": data_dir, "reason": reason},
            )
        )

    def promote(
        self, *, run_id: str, target_data_dir: str, prod_data_dir: str
    ) -> dict[str, Any]:
        return dict(
            self._request(
                "POST",
                "/api/operator/replay-target/promote",
                body={
                    "runId": run_id,
                    "targetDataDir": target_data_dir,
                    "prodDataDir": prod_data_dir,
                    "confirm": tenant_confirmation_token(
                        "SWAP", self.config.tenant
                    ),
                },
            )
        )

    def rollback(self, *, run_id: str, promotion_manifest_path: str) -> dict[str, Any]:
        return dict(
            self._request(
                "POST",
                "/api/operator/replay-target/rollback",
                body={
                    "runId": run_id,
                    "promotionManifestPath": promotion_manifest_path,
                },
            )
        )


@dataclass
class ReplayRunManifest:
    run_id: str
    run_dir: Path
    manifest_path: Path
    state: ReplayRunState = ReplayRunState.INITIALIZED
    created_at: str = field(default_factory=_utc_now)
    updated_at: str = field(default_factory=_utc_now)
    fresh_baseline: bool = True
    dirty_reason: str | None = None
    resumed_from_attempt_id: str | None = None
    provider: dict[str, Any] = field(default_factory=dict)
    target: dict[str, Any] = field(default_factory=dict)
    attempts: list[dict[str, Any]] = field(default_factory=list)
    verify: dict[str, Any] = field(default_factory=dict)
    promotion: dict[str, Any] = field(default_factory=dict)
    rollback: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def create(
        cls, *, run_id: str | None = None, out_dir: str | Path | None = None
    ) -> "ReplayRunManifest":
        run_id = run_id or mint_replay_run_id()
        root = (
            Path(out_dir).expanduser() if out_dir else get_hermes_home() / "replay-runs"
        )
        run_dir = root / _safe_id_part(run_id)
        return cls(
            run_id=run_id, run_dir=run_dir, manifest_path=run_dir / "run-manifest.json"
        )

    @classmethod
    def load(cls, path: str | Path) -> "ReplayRunManifest":
        manifest_path = Path(path).expanduser()
        data = _load_json(manifest_path)
        run_dir = Path(data.get("run_dir") or manifest_path.parent)
        return cls(
            run_id=str(data["run_id"]),
            run_dir=run_dir,
            manifest_path=manifest_path,
            state=ReplayRunState(
                str(data.get("state") or ReplayRunState.INITIALIZED.value)
            ),
            created_at=str(data.get("created_at") or _utc_now()),
            updated_at=str(data.get("updated_at") or _utc_now()),
            fresh_baseline=bool(data.get("fresh_baseline", True)),
            dirty_reason=data.get("dirty_reason"),
            resumed_from_attempt_id=data.get("resumed_from_attempt_id"),
            provider=dict(data.get("provider") or {}),
            target=dict(data.get("target") or {}),
            attempts=list(data.get("attempts") or []),
            verify=dict(data.get("verify") or {}),
            promotion=dict(data.get("promotion") or {}),
            rollback=dict(data.get("rollback") or {}),
            errors=list(data.get("errors") or []),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": RUN_MANIFEST_VERSION,
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "manifest_path": str(self.manifest_path),
            "state": self.state.value,
            "terminal": self.state.value in TERMINAL_STATES,
            "fresh_baseline": self.fresh_baseline,
            "dirty_reason": self.dirty_reason,
            "resumed_from_attempt_id": self.resumed_from_attempt_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "provider": self.provider,
            "target": self.target,
            "attempts": self.attempts,
            "verify": self.verify,
            "promotion": self.promotion,
            "rollback": self.rollback,
            "errors": self.errors,
        }

    def save(self) -> None:
        self.updated_at = _utc_now()
        _write_json_atomic(self.manifest_path, self.to_dict())

    def transition(self, state: ReplayRunState) -> None:
        rollback_escape = (
            self.state is ReplayRunState.PROMOTED
            and state is ReplayRunState.ROLLING_BACK
        )
        if (
            self.state.value in TERMINAL_STATES
            and state is not self.state
            and not rollback_escape
        ):
            raise ReplayStateError(f"run is terminal: state={self.state.value}")
        self.state = state
        self.save()

    def append_error(self, phase: str, exc: BaseException) -> None:
        self.errors.append({
            "phase": phase,
            "type": type(exc).__name__,
            "message": str(exc),
            "at": _utc_now(),
        })
        self.save()


class PAReplayOrchestrator:
    def __init__(
        self,
        *,
        provider_client: ReplayTargetProviderClient,
        runner_factory: Callable[[], Any] | None = None,
        manifest: ReplayRunManifest | None = None,
        out_dir: str | Path | None = None,
        run_id: str | None = None,
    ):
        self.provider_client = provider_client
        self.runner_factory = runner_factory
        self.manifest = manifest or ReplayRunManifest.create(
            run_id=run_id, out_dir=out_dir
        )
        self.manifest.provider.update({
            "kind": "systems-pcl",
            "provider_url": provider_client.config.provider_url,
            "tenant": provider_client.config.tenant,
        })
        self.manifest.save()

    @classmethod
    def from_manifest(
        cls,
        manifest_path: str | Path,
        *,
        provider_client: ReplayTargetProviderClient,
        runner_factory: Callable[[], Any] | None = None,
    ) -> "PAReplayOrchestrator":
        return cls(
            provider_client=provider_client,
            runner_factory=runner_factory,
            manifest=ReplayRunManifest.load(manifest_path),
        )

    def prepare_target(
        self, *, source_data_dir: str, target_data_dir: str, target_base_url: str
    ) -> dict[str, Any]:
        self._require_state({ReplayRunState.INITIALIZED.value}, "prepare_target")
        self.manifest.transition(ReplayRunState.PREPARING_TARGET)
        self.manifest.provider.update({
            "target_base_url": target_base_url,
            "source_data_dir": source_data_dir,
            "target_data_dir": target_data_dir,
        })
        try:
            prepared = self.provider_client.prepare(
                run_id=self.manifest.run_id,
                source_data_dir=source_data_dir,
                target_data_dir=target_data_dir,
                base_url=target_base_url,
            )
            descriptor = dict(prepared.get("descriptor") or {})
            baseline = dict(
                prepared.get("baselineManifest")
                or prepared.get("baseline_manifest")
                or {}
            )
            descriptor_digest = str(
                prepared.get("descriptorDigest")
                or prepared.get("descriptor_digest")
                or ""
            )
            baseline_digest = str(
                prepared.get("baselineDigest") or prepared.get("baseline_digest") or ""
            )
            self.manifest.target = {
                "prepared_at": _utc_now(),
                "run_id": prepared.get("runId")
                or prepared.get("run_id")
                or self.manifest.run_id,
                "target_id": prepared.get("targetId")
                or prepared.get("target_id")
                or descriptor.get("targetId"),
                "target_data_dir": prepared.get("targetDataDir")
                or prepared.get("target_data_dir")
                or target_data_dir,
                "descriptor": descriptor,
                "descriptor_digest": descriptor_digest,
                "baseline_manifest": baseline,
                "baseline_digest": baseline_digest,
                "service_token_ref": prepared.get("serviceTokenRef")
                or prepared.get("service_token_ref"),
                "raw_prepare_result": prepared,
            }
            self._write_artifact("target-prepare.json", self.manifest.target)
            self.manifest.transition(ReplayRunState.PREPARED)
            return self.manifest.target
        except Exception as exc:
            self.manifest.append_error("prepare_target", exc)
            self.manifest.state = ReplayRunState.FAILED
            self.manifest.save()
            raise

    def build_plan(self, base_plan: ReplayPlan) -> ReplayPlan:
        if self.manifest.state not in {
            ReplayRunState.PREPARED,
            ReplayRunState.RUNNING_AGENT_REPLAY,
            ReplayRunState.REPLAYED,
            ReplayRunState.VERIFIED,
        }:
            raise ReplayStateError(
                f"cannot build replay plan from state={self.manifest.state.value}"
            )
        attempt_id = f"attempt-{uuid.uuid4().hex[:12]}"
        descriptor = dict(self.manifest.target.get("descriptor") or {})
        baseline = dict(self.manifest.target.get("baseline_manifest") or {})
        return replace(
            base_plan,
            run_id=self.manifest.run_id,
            attempt_id=attempt_id,
            replay_namespace=f"agent:replay:{self.manifest.run_id}",
            target_descriptor_manifest=descriptor,
            target_baseline_manifest=baseline,
        )

    def run_agent_replay(self, base_plan: ReplayPlan) -> dict[str, Any]:
        self._require_state({ReplayRunState.PREPARED.value}, "run_agent_replay")
        if self.manifest.attempts:
            # A second run against the same target is a resumed/dirty investigation,
            # not a promotable fresh-baseline rebuild.
            self.manifest.fresh_baseline = False
        self.manifest.transition(ReplayRunState.RUNNING_AGENT_REPLAY)
        plan = self.build_plan(base_plan)
        self._write_artifact("replay-plan.json", plan.to_dict())
        try:
            result = self._run_gateway_replay(plan)
            result_dict = _result_to_dict(result)
            session_db_path = _session_db_path_from_runner(
                getattr(self, "_last_runner", None)
            )
            attempt_record = {
                "attempt_id": plan.attempt_id,
                "run_id": plan.run_id,
                "started_at": _utc_now(),
                "plan": plan.to_dict(),
                "plan_digest": canonical_digest(plan.to_dict()),
                "result": result_dict,
                "result_digest": canonical_digest(result_dict),
                "session_db_path": session_db_path,
                "fresh_baseline": self.manifest.fresh_baseline,
            }
            attempt_path = self._write_artifact(
                f"attempt-{plan.attempt_id}.json", attempt_record
            )
            attempt_record["artifact_path"] = str(attempt_path)
            self.manifest.attempts.append(attempt_record)
            self.manifest.transition(ReplayRunState.REPLAYED)
            return attempt_record
        except Exception as exc:
            self.manifest.append_error("run_agent_replay", exc)
            self.mark_dirty(
                reason=f"agent replay failed: {type(exc).__name__}: {exc}",
                mark_provider=True,
            )
            raise

    def _run_gateway_replay(self, plan: ReplayPlan) -> Any:
        if self.runner_factory is None:
            from gateway.run import GatewayRunner

            runner = GatewayRunner()
        else:
            runner = self.runner_factory()
        self._last_runner = runner
        result = runner.replay(plan)
        if hasattr(result, "__await__"):
            return asyncio.run(result)
        return result

    def verify(
        self,
        gate: VerifyGateConfig | None = None,
        *,
        session_db_path: str | Path | None = None,
    ) -> dict[str, Any]:
        self._require_state(
            {ReplayRunState.REPLAYED.value, ReplayRunState.VERIFIED.value}, "verify"
        )
        gate = gate or VerifyGateConfig()
        self.manifest.transition(ReplayRunState.VERIFYING)
        checks: list[CheckResult] = []
        latest_attempt = self._latest_attempt()
        result = dict(latest_attempt.get("result") or {})
        attempt = dict(
            result.get("attempt")
            or _get_nested(result, "execution_report", "attempt", default={})
            or {}
        )
        corpus_manifest = (
            _attempt_manifest(attempt, "corpus")
            or _get_nested(latest_attempt, "plan", "corpus_manifest", default={})
            or {}
        )
        corpus_count = _as_int(corpus_manifest.get("message_count"))
        processed = _as_int(result.get("processed"))
        checks.append(
            CheckResult(
                "corpus-parity",
                ok=corpus_count is not None and processed == corpus_count,
                actual={"processed": processed, "corpus_message_count": corpus_count},
                expected={"processed": corpus_count},
            )
        )

        metrics = self._load_execution_metrics(
            latest_attempt, session_db_path=session_db_path
        )
        expected_turn_count = gate.expected_turn_count
        if expected_turn_count is None:
            expected_turn_count = _as_int(corpus_manifest.get("expected_turn_count"))
        min_turn_count = gate.min_turn_count
        if min_turn_count is None:
            min_turn_count = 1 if (corpus_count or 0) > 0 else 0
        turn_count = _as_int(metrics.get("turn_count"))
        if expected_turn_count is not None:
            turn_ok = turn_count == expected_turn_count
            turn_expected: Any = expected_turn_count
        else:
            turn_ok = turn_count is not None and turn_count >= min_turn_count
            turn_expected = {"min_turn_count": min_turn_count}
        coverage_actual: dict[str, Any] = {"turn_count": turn_count}
        expected_ids = _message_ids_from_plan(latest_attempt.get("plan") or {})
        if expected_ids and metrics.get("covered_message_refs") is not None:
            covered = set(str(v) for v in metrics.get("covered_message_refs") or [])
            missing = [mid for mid in expected_ids if mid not in covered]
            coverage_actual["missing_message_refs"] = missing
            turn_ok = turn_ok and not missing
            turn_expected = {
                "turn_count": turn_expected,
                "all_message_refs_covered": True,
            }
        checks.append(
            CheckResult(
                "processed-turn-coverage",
                ok=bool(turn_ok),
                actual=coverage_actual,
                expected=turn_expected,
            )
        )

        failed_turns = _as_int(metrics.get("failed_turn_count"), default=0)
        checks.append(
            CheckResult(
                "no-failed-turns",
                ok=failed_turns == 0,
                actual=failed_turns,
                expected=0,
            )
        )

        outbound_entries = [
            entry
            for entry in (result.get("outbound") or [])
            if isinstance(entry, Mapping)
        ]
        captured_outbound = [
            entry
            for entry in outbound_entries
            if str(entry.get("delivery_mode") or "").strip().lower() == "capture"
        ]
        dropped_outbound = [
            entry
            for entry in outbound_entries
            if str(entry.get("delivery_mode") or "").strip().lower() == "drop"
        ]
        escaped_outbound = [
            entry
            for entry in outbound_entries
            if str(entry.get("delivery_mode") or "").strip().lower()
            not in NON_DELIVERING_REPLAY_DELIVERY_MODES
        ]
        allowed_kinds = set(gate.allowed_outbound_kinds)
        unexpected_outbound = [
            entry
            for entry in escaped_outbound
            if str(entry.get("kind") or "") not in allowed_kinds
        ]
        allowed_escaped_outbound = [
            entry
            for entry in escaped_outbound
            if str(entry.get("kind") or "") in allowed_kinds
        ]
        checks.append(
            CheckResult(
                "zero-unexpected-outbound",
                ok=len(escaped_outbound) == 0,
                actual={
                    "captured_count": len(captured_outbound),
                    "captured": captured_outbound[:5],
                    "dropped_count": len(dropped_outbound),
                    "dropped": dropped_outbound[:5],
                    "escaped_count": len(escaped_outbound),
                    "escaped": escaped_outbound[:5],
                    "unexpected_count": len(unexpected_outbound),
                    "unexpected": unexpected_outbound[:5],
                    "allowed_escaped_count": len(allowed_escaped_outbound),
                    "allowed_escaped": allowed_escaped_outbound[:5],
                },
                expected={"escaped_count": 0},
                detail=(
                    "Captured/dropped replay outbounds "
                    "(delivery_mode=capture|drop) are reported but non-failing. "
                    "Any other delivery mode is treated as escaped and "
                    "hard-fails this gate."
                ),
            )
        )

        tool_error_count = _as_int(metrics.get("tool_error_count"), default=0)
        checks.append(
            CheckResult(
                "tool-error-budget",
                ok=tool_error_count <= gate.tool_error_budget,
                actual={
                    "tool_error_count": tool_error_count,
                    "examples": metrics.get("tool_error_examples") or [],
                },
                expected={"tool_error_budget": gate.tool_error_budget},
            )
        )

        checks.extend(
            self._digest_checks(attempt=attempt, latest_attempt=latest_attempt)
        )

        provider_verify = None
        if gate.require_provider_invariants:
            target_data_dir = str(
                self.manifest.target.get("target_data_dir")
                or self.manifest.provider.get("target_data_dir")
                or ""
            )
            try:
                provider_verify = self.provider_client.verify(
                    run_id=self.manifest.run_id, data_dir=target_data_dir
                )
                provider_ok = bool(provider_verify.get("ok"))
            except Exception as exc:
                provider_verify = {
                    "ok": False,
                    "error": {"type": type(exc).__name__, "message": str(exc)},
                }
                provider_ok = False
            checks.append(
                CheckResult(
                    "provider-invariants",
                    ok=provider_ok,
                    actual=provider_verify,
                    expected={"ok": True},
                )
            )

        ok = all(check.ok for check in checks)
        report = {
            "ok": ok,
            "run_id": self.manifest.run_id,
            "attempt_id": latest_attempt.get("attempt_id"),
            "gate_config": gate.to_dict(),
            "checks": [check.to_dict() for check in checks],
            "provider_verify": provider_verify,
            "verified_at": _utc_now(),
        }
        self.manifest.verify = report
        self._write_artifact("verify-report.json", report)
        if ok:
            self.manifest.transition(ReplayRunState.VERIFIED)
            return report
        self.mark_dirty(reason="mechanical verify gate failed", mark_provider=True)
        raise ReplayVerifyError(
            "mechanical verify gate failed: "
            + ", ".join(check.name for check in checks if not check.ok)
        )

    def promote(self, *, prod_data_dir: str) -> dict[str, Any]:
        self._require_state({ReplayRunState.VERIFIED.value}, "promote")
        self._assert_promotable()
        self.manifest.transition(ReplayRunState.PROMOTING)
        try:
            result = self.provider_client.promote(
                run_id=self.manifest.run_id,
                target_data_dir=str(self.manifest.target.get("target_data_dir")),
                prod_data_dir=prod_data_dir,
            )
            self.manifest.promotion = {
                "ok": bool(result.get("ok")),
                "provider_result": result,
                "promoted_at": _utc_now(),
                "prod_data_dir": prod_data_dir,
            }
            self._write_artifact("promote-result.json", self.manifest.promotion)
            self.manifest.transition(ReplayRunState.PROMOTED)
            return self.manifest.promotion
        except Exception as exc:
            self.manifest.append_error("promote", exc)
            self.manifest.state = ReplayRunState.FAILED
            self.manifest.save()
            raise

    def rollback(self, *, promotion_manifest_path: str | None = None) -> dict[str, Any]:
        self._require_state({ReplayRunState.PROMOTED.value}, "rollback")
        promotion_manifest_path = promotion_manifest_path or str(
            _get_nested(
                self.manifest.promotion, "provider_result", "manifestPath", default=""
            )
        )
        if not promotion_manifest_path:
            raise ReplayStateError("promotion manifest path is required for rollback")
        self.manifest.transition(ReplayRunState.ROLLING_BACK)
        try:
            result = self.provider_client.rollback(
                run_id=self.manifest.run_id,
                promotion_manifest_path=promotion_manifest_path,
            )
            self.manifest.rollback = {
                "ok": bool(result.get("ok")),
                "provider_result": result,
                "rolled_back_at": _utc_now(),
                "promotion_manifest_path": promotion_manifest_path,
            }
            self._write_artifact("rollback-result.json", self.manifest.rollback)
            self.manifest.transition(ReplayRunState.ROLLED_BACK)
            return self.manifest.rollback
        except Exception as exc:
            self.manifest.append_error("rollback", exc)
            self.manifest.state = ReplayRunState.FAILED
            self.manifest.save()
            raise

    def mark_dirty(self, *, reason: str, mark_provider: bool = True) -> dict[str, Any]:
        if self.manifest.state.value in {
            ReplayRunState.PROMOTED.value,
            ReplayRunState.ROLLED_BACK.value,
        }:
            raise ReplayStateError(
                f"cannot mark dirty from state={self.manifest.state.value}"
            )
        provider_result: dict[str, Any] | None = None
        if mark_provider and self.manifest.target.get("target_data_dir"):
            try:
                provider_result = self.provider_client.mark_dirty(
                    run_id=self.manifest.run_id,
                    data_dir=str(self.manifest.target.get("target_data_dir")),
                    reason=reason,
                )
            except Exception as exc:
                provider_result = {
                    "ok": False,
                    "error": {"type": type(exc).__name__, "message": str(exc)},
                }
        self.manifest.dirty_reason = reason
        self.manifest.state = ReplayRunState.DIRTY
        self.manifest.verify = self.manifest.verify or {}
        self.manifest.verify.setdefault("dirty_reason", reason)
        if provider_result is not None:
            self.manifest.verify["provider_dirty_result"] = provider_result
        self.manifest.save()
        return {
            "ok": True,
            "run_id": self.manifest.run_id,
            "state": "dirty",
            "reason": reason,
            "provider_result": provider_result,
        }

    def _digest_checks(
        self, *, attempt: Mapping[str, Any], latest_attempt: Mapping[str, Any]
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []
        target = self.manifest.target
        descriptor = dict(target.get("descriptor") or {})
        baseline = dict(target.get("baseline_manifest") or {})
        descriptor_digest = str(target.get("descriptor_digest") or "")
        baseline_digest = str(target.get("baseline_digest") or "")
        checks.append(
            CheckResult(
                "descriptor-digest-match",
                ok=bool(descriptor_digest)
                and _sha256_hex_manifest(descriptor) == descriptor_digest,
                actual=_sha256_hex_manifest(descriptor) if descriptor else None,
                expected=descriptor_digest,
            )
        )
        checks.append(
            CheckResult(
                "baseline-digest-match",
                ok=bool(baseline_digest)
                and _sha256_hex_manifest(baseline) == baseline_digest,
                actual=_sha256_hex_manifest(baseline) if baseline else None,
                expected=baseline_digest,
            )
        )
        for name in (
            "corpus",
            "config_overlay",
            "target_descriptor",
            "target_baseline",
            "code",
            "replay_policy",
            "plan",
        ):
            manifest_value = _attempt_manifest(attempt, name)
            digest_value = attempt.get(f"{name}_digest")
            if name == "plan" and not manifest_value:
                manifest_value = latest_attempt.get("plan") or {}
                digest_value = latest_attempt.get("plan_digest")
            if manifest_value:
                checks.append(
                    CheckResult(
                        f"attempt-{name}-digest-match",
                        ok=canonical_digest(manifest_value) == digest_value,
                        actual=canonical_digest(manifest_value),
                        expected=digest_value,
                    )
                )
        code_manifest = _attempt_manifest(attempt, "code")
        checks.append(
            CheckResult(
                "code-digest-present",
                ok=bool(code_manifest and attempt.get("code_digest")),
                actual={
                    "code_manifest": code_manifest,
                    "code_digest": attempt.get("code_digest"),
                },
                expected="code manifest and digest",
            )
        )
        return checks

    def _load_execution_metrics(
        self,
        latest_attempt: Mapping[str, Any],
        *,
        session_db_path: str | Path | None = None,
    ) -> dict[str, Any]:
        result = dict(latest_attempt.get("result") or {})
        summary = dict(
            _get_nested(result, "execution_report", "summary", default={}) or {}
        )
        metrics: dict[str, Any] = {
            "turn_count": _as_int(summary.get("turn_count")),
            "failed_turn_count": _as_int(summary.get("failed_turn_count"), default=0),
            "tool_error_count": _as_int(summary.get("tool_error_count"), default=0),
            "tool_error_examples": summary.get("tool_error_examples") or [],
        }
        db_path = session_db_path or latest_attempt.get("session_db_path")
        if not db_path:
            return metrics
        path = Path(str(db_path)).expanduser()
        if not path.exists():
            return metrics
        try:
            from hermes_state import SessionDB

            db = SessionDB(db_path=path)
            try:
                turns = db.list_pa_turns(
                    replay_run_id=self.manifest.run_id, limit=100000
                )
            finally:
                db.close()
        except Exception:
            return metrics
        attempt_id = latest_attempt.get("attempt_id")
        if attempt_id:
            turns = [
                turn for turn in turns if turn.get("replay_attempt_id") == attempt_id
            ]
        tool_errors: list[dict[str, Any]] = []
        covered_refs: set[str] = set()
        failed_turns = 0
        for turn in turns:
            if turn.get("turn_status") == "failed":
                failed_turns += 1
            refs = turn.get("message_refs")
            if isinstance(refs, list):
                covered_refs.update(str(ref) for ref in refs)
            for tc in turn.get("tool_calls") or []:
                if _tool_call_has_error(tc):
                    tool_errors.append({
                        "turn_id": turn.get("turn_id"),
                        "tool_name": tc.get("tool_name"),
                        "result": tc.get("result"),
                    })
        metrics.update({
            "turn_count": len(turns),
            "failed_turn_count": failed_turns,
            "covered_message_refs": sorted(covered_refs),
            "tool_error_count": len(tool_errors),
            "tool_error_examples": tool_errors[:5],
        })
        return metrics

    def _latest_attempt(self) -> dict[str, Any]:
        if not self.manifest.attempts:
            raise ReplayStateError("run has no replay attempt")
        return dict(self.manifest.attempts[-1])

    def _assert_promotable(self) -> None:
        if self.manifest.state is not ReplayRunState.VERIFIED:
            raise ReplayStateError(
                f"run is not verified: state={self.manifest.state.value}"
            )
        if not self.manifest.fresh_baseline:
            raise ReplayStateError(
                "fresh-baseline-only promote refused: run is not fresh"
            )
        if self.manifest.dirty_reason:
            raise ReplayStateError(
                f"dirty run cannot promote: {self.manifest.dirty_reason}"
            )
        if self.manifest.resumed_from_attempt_id:
            raise ReplayStateError("resumed attempts cannot promote")
        if len(self.manifest.attempts) != 1:
            raise ReplayStateError("promote requires exactly one fresh replay attempt")
        if not self.manifest.verify.get("ok"):
            raise ReplayStateError("mechanical verify gate has not passed")

    def _require_state(self, allowed: set[str], action: str) -> None:
        if self.manifest.state.value not in allowed:
            raise ReplayStateError(
                f"cannot {action} from state={self.manifest.state.value}; expected one of {sorted(allowed)}"
            )

    def _write_artifact(self, name: str, payload: Mapping[str, Any]) -> Path:
        path = self.manifest.run_dir / name
        _write_json_atomic(path, payload)
        return path


def _attempt_manifest(attempt: Mapping[str, Any], name: str) -> dict[str, Any]:
    for key in (f"{name}_manifest", f"{name}_manifest_json"):
        value = attempt.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _message_ids_from_plan(plan: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    for message in plan.get("messages") or []:
        if isinstance(message, Mapping):
            mid = (
                message.get("messageId")
                or message.get("message_id")
                or message.get("id")
            )
            if mid is not None:
                ids.append(str(mid))
    return ids


def _tool_call_has_error(tc: Mapping[str, Any]) -> bool:
    result = tc.get("result")
    if result is None and isinstance(tc.get("result_json"), str):
        try:
            result = json.loads(str(tc.get("result_json")))
        except Exception:
            result = tc.get("result_json")
    if isinstance(result, Mapping):
        if result.get("success") is False or result.get("ok") is False:
            return True
        if result.get("error") or result.get("errors"):
            return True
        status = result.get("status")
        if isinstance(status, str) and status.lower() in {"error", "failed", "failure"}:
            return True
    return False


def _result_to_dict(result: Any) -> dict[str, Any]:
    if hasattr(result, "to_dict"):
        return dict(result.to_dict())
    if isinstance(result, Mapping):
        return dict(result)
    if hasattr(result, "__dict__"):
        return dict(result.__dict__)
    raise TypeError("replay runner returned an unsupported result shape")


def _session_db_path_from_runner(runner: Any) -> str | None:
    session_db = getattr(runner, "_session_db", None)
    path = getattr(session_db, "db_path", None)
    return str(path) if path else None


def provider_config_from_env(
    *,
    provider_url: str | None = None,
    admin_token: str | None = None,
    tenant: str,
    timeout_seconds: float = 30.0,
) -> ReplayTargetProviderConfig:
    return ReplayTargetProviderConfig(
        provider_url=provider_url
        or os.environ.get("PS_REPLAY_PROVIDER_URL")
        or os.environ.get("SYSTEMS_PCL_REPLAY_PROVIDER_URL")
        or "",
        admin_token=admin_token
        or os.environ.get("PS_REPLAY_PROVIDER_ADMIN_TOKEN")
        or "",
        tenant=str(tenant or "").strip(),
        timeout_seconds=timeout_seconds,
    )
