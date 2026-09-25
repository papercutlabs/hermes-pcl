#!/usr/bin/env python3
"""Fail closed when Christopher's deployment artifacts drift from the spec."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

# Authored selector additions on top of the recovered June brain. The build
# layer (build_runtime_slots.py) inserts each addition directly after its
# anchor selector; the deployment spec's allowlists and the active
# constitution must both carry recovered + exactly these, nothing else.
# 2026-07-21 demo end-state (teren demo rulings): the ops-test ingest group.
AUTHORED_SELECTOR_ADDITIONS: list[dict[str, Any]] = [
    {
        "job_type": "tgg_ops_ingest",
        "match": {
            "source.platform": "whatsapp",
            "source.chat_id": "120363429226022766@g.us",
        },
        "anchor_job_type": "tgg_ops_ingest",
        "anchor_chat_id": "120363423568509280@g.us",
    }
]


EXPECTED_API_VERSION = "pa.papercutlabs.com/v1"
EXPECTED_KIND = "ClientAgentDeployment"

# slot id -> model. Suffixed slots pin an explicit reasoning effort.
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
    "gpt-5.6-luna-xhigh": "gpt-5.6-luna",
    "gpt-5.6-terra-medium": "gpt-5.6-terra",
    "gpt-5.6-terra-high": "gpt-5.6-terra",
    "gpt-6-sol-medium": "gpt-6-sol",
}
SLOT_REASONING_EFFORT = {
    "gpt-5.6-luna-low": "low",
    "gpt-5.6-luna-xhigh": "xhigh",
    "gpt-5.6-terra-medium": "medium",
    "gpt-5.6-terra-high": "high",
    "gpt-6-sol-medium": "medium",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} must contain a YAML mapping")
    return data


def _resolve(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if path != root and root not in path.parents:
        raise RuntimeError(f"spec path escapes app root: {relative}")
    if not path.is_file():
        raise RuntimeError(f"spec path does not exist: {relative}")
    return path


def _selector_chats(
    constitution: dict[str, Any], *, platform: str, job_type: str
) -> list[str]:
    return [
        str(selector["match"]["source.chat_id"])
        for selector in constitution.get("selectors", [])
        if selector.get("job_type") == job_type
        and selector.get("match", {}).get("source.platform") == platform
    ]


def _parse_checksums(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        digest, relative = raw.split(None, 1)
        result[relative.strip()] = digest
    return result


def validate(app_root: Path, spec_path: Path) -> dict[str, Any]:
    app_root = app_root.resolve()
    spec_path = spec_path.resolve()
    document = _load_yaml(spec_path)
    if document.get("apiVersion") != EXPECTED_API_VERSION:
        raise RuntimeError("deployment apiVersion mismatch")
    if document.get("kind") != EXPECTED_KIND:
        raise RuntimeError("deployment kind mismatch")
    if document.get("metadata", {}).get("client") != "tgg":
        raise RuntimeError("deployment client must be tgg")
    if document.get("metadata", {}).get("agent") != "christopher":
        raise RuntimeError("deployment agent must be christopher")

    spec = document["spec"]
    if spec["processing"]["enabled"] is not False:
        raise RuntimeError("processing must remain disabled for this deployment")
    if spec["processing"]["activationIncludedInThisDeployment"] is not False:
        raise RuntimeError("the deployment must not include processing activation")
    expected_activation_primitive = (
        "deploy/tgg/christopher/scripts/activate_processing.py"
    )
    if spec["processing"].get("activationPrimitive") != expected_activation_primitive:
        raise RuntimeError("processing activation primitive path drifted")
    _resolve(app_root, expected_activation_primitive)
    ps_token = next(
        (row for row in spec["env"]["refs"] if row.get("name") == "CHRISTOPHER_TGG_PS_SERVICE_TOKEN"),
        None,
    )
    if ps_token != {
        "name": "CHRISTOPHER_TGG_PS_SERVICE_TOKEN",
        "source": "/etc/systems-papercut-labs/tgg.env#CHRISTOPHER_TGG_PS_SERVICE_TOKEN",
        "materializer": "deploy/tgg/christopher/scripts/processing_activation_transaction.py",
        "verifier": (
            "GET /api/operator/auth/identity must return tenant=tgg, agent=christopher, "
            "and all operator scopes; then GET /api/operator/cases/count must pass"
        ),
        "requiredWhen": "processing-enabled",
    }:
        raise RuntimeError("processing PS service-token materialization contract drifted")
    if (
        spec["processing"]["effectiveEnableRule"]
        != "pa.enabled AND processing-gate.enabled"
    ):
        raise RuntimeError(
            "production activation must require config and the root gate"
        )
    if spec["runtime"]["groupSessionsPerUser"] is not False:
        raise RuntimeError("group_sessions_per_user judgment must remain false")
    if spec["channels"]["whatsapp"]["hermesBuiltInGatewayEnabled"] is not False:
        raise RuntimeError("Hermes must not connect its built-in WhatsApp gateway")
    if spec["channels"]["whatsapp"]["consumer"]["mode"] != "durable-jsonl-tail":
        raise RuntimeError("WhatsApp consumer must use the durable JSONL tail")
    retention = spec["channels"]["whatsapp"].get("mediaRetention")
    expected_retention = {
        "enabled": True,
        "sourceGate": "pa.enabled-and-processing-gate",
        "sourceRoots": ["/var/lib/tgg-capture/whatsapp/media"],
        "mediaRoot": "/home/pclaw/.systems-pcl/data/media",
        "convergenceOperation": "tgg_media_retention",
        "retrievalOperation": "tgg_case_media",
        "retrievalTool": "tgg_case_photos",
        "ordering": (
            "retain-and-converge-while-business-pending-before-lane-selection-"
            "with-claimed-chat-idempotent-safety-net"
        ),
        "failureDisposition": (
            "retry-up-to-max-attempts-then-quarantine-full-envelope-and-"
            "failure-history-and-bypass-for-business-processing"
        ),
        "maxAttempts": 5,
        "retryIntervalSeconds": 60,
        "activationBacklogPreflight": "all-image-paths-from-current-cursor-must-resolve",
        "minimumRealVolumeFreeBytes": 5368709120,
        "pathContract": "opaque-media-refs-resolved-under-configured-root",
        "delivery": {
            "route": "/send-media",
            "destination": "current-triggering-management-chat-only",
            "durableKey": "chat-id-plus-current-inbound-anchor-plus-media-identity-plus-ordinal",
            "unknownOutcome": "durable-undelivered-no-blind-retry",
            "staleOrNonManagement": "suppress",
        },
        "rollback": "disable-retention-and-media-send-preserve-retained-data-and-cursors",
    }
    if retention != expected_retention:
        raise RuntimeError("WhatsApp media-retention contract drifted")
    expected_status_media_fields = [
        "retention_total",
        "retention_failures",
        "retention_attempts",
        "retention_pending",
        "retention_complete",
        "retention_bypassed",
        "retention_held",
        "retention_quarantined",
        "retention_quarantine_status",
        "retention_quarantine_message_ids",
        "retention_hold",
        "media_root_count",
        "media_root_bytes",
        "media_volume_free_percent",
        "media_volume_free_bytes",
        "media_volume_total_bytes",
    ]
    if (
        spec["channels"]["whatsapp"]["consumer"].get("statusMediaFields")
        != expected_status_media_fields
    ):
        raise RuntimeError("consumer media-status contract drifted")
    if spec["channels"]["whatsapp"]["consumer"].get("retentionBatchSize") != 25:
        raise RuntimeError("consumer retention batch bound drifted")
    if spec["channels"]["whatsapp"]["prohibitedConsumerSurface"]["path"] != "/messages":
        raise RuntimeError("the destructive bridge route must stay prohibited")

    brain = spec["brain"]
    recovered_config_path = _resolve(app_root, brain["recoveredConfig"]["path"])
    recovered_constitution_path = _resolve(
        app_root, brain["recoveredConstitution"]["path"]
    )
    recovered_config_hash = _sha256(recovered_config_path)
    recovered_constitution_hash = _sha256(recovered_constitution_path)
    if recovered_config_hash != brain["recoveredConfig"]["sha256"]:
        raise RuntimeError("recovered June config hash mismatch")
    if recovered_constitution_hash != brain["recoveredConstitution"]["sha256"]:
        raise RuntimeError("recovered June constitution hash mismatch")
    composite = hashlib.sha256(
        (
            f"config {recovered_config_hash}\n"
            f"constitution {recovered_constitution_hash}\n"
        ).encode("utf-8")
    ).hexdigest()
    if not str(spec["brainRef"]).endswith(f"@sha256:{composite}"):
        raise RuntimeError("brainRef composite hash mismatch")

    recovered_config = _load_yaml(recovered_config_path)
    recovered_constitution = _load_yaml(recovered_constitution_path)
    active_config = _load_yaml(app_root / "deploy/tgg/christopher/config.yaml")
    active_constitution = _load_yaml(
        app_root / "deploy/tgg/christopher/christopher_tgg_constitution.yaml"
    )
    engine = spec["engine"]
    default_slot = engine["defaultSlot"]
    allowed_slots = list(engine["allowedSlots"])
    if default_slot != "gpt-5.4-mini":
        raise RuntimeError("default engine slot must remain gpt-5.4-mini")
    if allowed_slots != list(SLOT_MODELS):
        raise RuntimeError("engine slot set drifted")

    slot_manifest_path = _resolve(app_root, engine["slotManifest"])
    checksums = _parse_checksums(slot_manifest_path)
    slot_hashes: dict[str, dict[str, str]] = {}
    for slot in allowed_slots:
        model = SLOT_MODELS[slot]
        effort = SLOT_REASONING_EFFORT.get(slot)
        slot_root = app_root / "deploy/tgg/christopher/runtime-slots" / slot
        config_path = slot_root / "config.yaml"
        constitution_path = slot_root / "christopher_tgg_constitution.yaml"
        config = _load_yaml(config_path)
        constitution = _load_yaml(constitution_path)
        if config["pa"]["enabled"] is not False:
            raise RuntimeError(f"slot {slot} enables PA")
        if config.get("memory") != {
            "memory_enabled": True,
            "user_profile_enabled": False,
            "memory_char_limit": 2200,
        }:
            raise RuntimeError(f"slot {slot} memory injection config drifted")
        if config.get("python_sandbox", {}).get("datasets", {}).get("documents") != {
            "type": "path",
            "path": "/var/lib/tgg-capture/whatsapp/media/documents",
            "description": "Documents uploaded in WhatsApp chats, ORIGINAL filenames matching the saved-at path shown in the message (read-only)",
        }:
            raise RuntimeError(f"slot {slot} documents dataset drifted")
        if config["pa"].get("media_retention") != {
            "enabled": True,
            "media_root": "/home/pclaw/.systems-pcl/data/media/tgg/hermes",
            "media_ref_prefix": "/media/tgg/hermes",
            "source_roots": ["/var/lib/tgg-capture/whatsapp/media"],
            "operation": "tgg_media_retention",
            "min_free_bytes": 5368709120,
            "max_attempts": 5,
            "retry_interval_seconds": 60,
        }:
            raise RuntimeError(f"slot {slot} media-retention config drifted")
        if config["platforms"]["whatsapp"]["enabled"] is not False:
            raise RuntimeError(f"slot {slot} enables the built-in WhatsApp gateway")
        if config["group_sessions_per_user"] is not False:
            raise RuntimeError(f"slot {slot} changed group_sessions_per_user")
        if config.get("timezone") != "Asia/Singapore":
            raise RuntimeError(f"slot {slot} timezone mismatch")
        if config.get("session_reset", {}).get("mode") != "none":
            raise RuntimeError(f"slot {slot} session_reset mode is not none")
        if config["model"]["provider"] != engine["provider"]:
            raise RuntimeError(f"slot {slot} provider mismatch")
        if config["model"]["default"] != model:
            raise RuntimeError(f"slot {slot} model mismatch")
        if effort is None:
            if "reasoning_effort" in config.get("agent", {}):
                raise RuntimeError(f"slot {slot} sets an unexpected reasoning effort")
        elif config.get("agent", {}).get("reasoning_effort") != effort:
            raise RuntimeError(f"slot {slot} reasoning effort mismatch")
        if constitution["runtime"] != {
            "provider": engine["provider"],
            "model": model,
        }:
            raise RuntimeError(f"slot {slot} constitution engine mismatch")
        relative_config = f"{slot}/config.yaml"
        relative_constitution = f"{slot}/christopher_tgg_constitution.yaml"
        config_hash = _sha256(config_path)
        constitution_hash = _sha256(constitution_path)
        if checksums.get(relative_config) != config_hash:
            raise RuntimeError(f"slot manifest mismatch for {relative_config}")
        if checksums.get(relative_constitution) != constitution_hash:
            raise RuntimeError(f"slot manifest mismatch for {relative_constitution}")
        slot_hashes[slot] = {
            "config": config_hash,
            "constitution": constitution_hash,
        }

    default_root = app_root / "deploy/tgg/christopher/runtime-slots" / default_slot
    if (app_root / "deploy/tgg/christopher/config.yaml").read_bytes() != (
        default_root / "config.yaml"
    ).read_bytes():
        raise RuntimeError("root deployment config differs from the default slot")
    if (
        app_root / "deploy/tgg/christopher/christopher_tgg_constitution.yaml"
    ).read_bytes() != (default_root / "christopher_tgg_constitution.yaml").read_bytes():
        raise RuntimeError("root constitution differs from the default slot")

    whatsapp = spec["channels"]["whatsapp"]
    expected_dms = [
        str(value)
        for value in recovered_config["platforms"]["whatsapp"]["extra"]["allow_from"]
    ]
    expected_outbound = list(
        recovered_config["platforms"]["whatsapp"]["extra"]["outbound_allowed_chats"]
    )
    expected_ops = _selector_chats(
        recovered_constitution,
        platform="whatsapp",
        job_type="tgg_ops_ingest",
    ) + [
        addition["match"]["source.chat_id"]
        for addition in AUTHORED_SELECTOR_ADDITIONS
        if addition["job_type"] == "tgg_ops_ingest"
        and addition["match"]["source.platform"] == "whatsapp"
    ]
    expected_management = _selector_chats(
        recovered_constitution,
        platform="whatsapp",
        job_type="tgg_management",
    )
    if whatsapp["allowlists"]["directMessages"] != expected_dms:
        raise RuntimeError("WhatsApp DM allowlist drifted from the recovered brain")
    if whatsapp["allowlists"]["operationsChats"] != expected_ops:
        raise RuntimeError("WhatsApp operations selectors drifted")
    if whatsapp["allowlists"]["managementChats"] != expected_management:
        raise RuntimeError("WhatsApp management selectors drifted")
    if whatsapp["allowlists"]["outboundAllowedChats"] != expected_outbound:
        raise RuntimeError("WhatsApp outbound allowlist drifted")
    if (
        active_config["platforms"]["whatsapp"]["extra"]["allow_from"]
        != recovered_config["platforms"]["whatsapp"]["extra"]["allow_from"]
    ):
        raise RuntimeError("active config changed the recovered DM allowlist")
    expected_selectors = list(recovered_constitution["selectors"])
    for addition in AUTHORED_SELECTOR_ADDITIONS:
        anchor_index = next(
            index
            for index, selector in enumerate(expected_selectors)
            if selector.get("job_type") == addition["anchor_job_type"]
            and selector.get("match", {}).get("source.platform")
            == addition["match"]["source.platform"]
            and str(selector.get("match", {}).get("source.chat_id"))
            == addition["anchor_chat_id"]
        )
        expected_selectors.insert(
            anchor_index + 1,
            {"job_type": addition["job_type"], "match": dict(addition["match"])},
        )
    if active_constitution["selectors"] != expected_selectors:
        raise RuntimeError(
            "active default constitution changed recovered selectors beyond the authored additions"
        )

    telegram = spec["channels"]["telegram"]["selectorChats"]
    if telegram["operations"] != _selector_chats(
        recovered_constitution,
        platform="telegram",
        job_type="tgg_ops_ingest",
    ):
        raise RuntimeError("Telegram operations selectors drifted")
    if telegram["management"] != _selector_chats(
        recovered_constitution,
        platform="telegram",
        job_type="tgg_management",
    ):
        raise RuntimeError("Telegram management selectors drifted")

    unit_sources = {
        entry["name"]: _resolve(app_root, entry["source"])
        for entry in spec["units"]["owned"]
    }
    expected_units = {
        "christopher-tgg-hermes.service",
        "christopher-tgg-hermes-health.service",
        "christopher-tgg-hermes-health.timer",
        "christopher-tgg-report-weekly.service",
        "christopher-tgg-report-weekly.timer",
        "christopher-tgg-report-monthly.service",
        "christopher-tgg-report-monthly.timer",
        "christopher-tgg-nightly-whatsapp.service",
        "christopher-tgg-nightly-whatsapp.timer",
        "christopher-tgg-retention-cleanup.service",
        "christopher-tgg-retention-cleanup.timer",
    }
    if set(unit_sources) != expected_units:
        raise RuntimeError("owned Hermes unit set drifted")
    consumer_unit = unit_sources["christopher-tgg-hermes.service"].read_text(
        encoding="utf-8"
    )
    if "/messages" in consumer_unit:
        raise RuntimeError("consumer unit references the destructive /messages route")
    if whatsapp["durableSource"]["path"] not in consumer_unit:
        raise RuntimeError("consumer unit does not declare the durable capture source")
    if spec["processing"]["gatePath"] not in consumer_unit:
        raise RuntimeError("consumer unit does not enforce the root processing gate")
    if "Environment=TGG_REPLY_BRIDGE_URL=http://127.0.0.1:3011" not in consumer_unit:
        raise RuntimeError("consumer unit does not bind the guarded reply bridge for cron delivery")
    consumer_spec = whatsapp["consumer"]
    if (
        f"--case-db {consumer_spec['caseDbPath']}" not in consumer_unit
        or "--source-before-image-dir "
        f"{consumer_spec['sourceEvidenceBeforeImageDir']}" not in consumer_unit
    ):
        raise RuntimeError(
            "consumer unit does not explicitly bind source projection storage"
        )

    manifest_path = _resolve(app_root, spec["deploy"]["manifestRef"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    included = set(manifest.get("include", []))
    required_runtime_files = {
        "gateway/durable_jsonl_consumer.py",
        "gateway/platforms/base.py",
        "gateway/platforms/helpers.py",
        "gateway/platforms/whatsapp.py",
        "run_agent.py",
        "model_tools.py",
        "deploy/tgg/christopher/client-agent-deployment.yaml",
        "deploy/tgg/christopher/scripts/build_pa_agent_manifest.py",
        "deploy/tgg/christopher/scripts/activate_processing.py",
        "deploy/tgg/christopher/scripts/processing_activation_transaction.py",
        "deploy/tgg/christopher/scripts/validate_deployment_spec.py",
        "deploy/tgg/christopher/scripts/verify_runtime.sh",
        "deploy/tgg/christopher/scripts/run_scheduled_report.py",
        "deploy/tgg/christopher/scripts/run_nightly_whatsapp.py",
        "deploy/tgg/christopher/plugins/report-operations/__init__.py",
        "deploy/tgg/christopher/plugins/report-operations/plugin.yaml",
        "deploy/tgg/christopher/systemd/christopher-tgg-report-weekly.service",
        "deploy/tgg/christopher/systemd/christopher-tgg-report-weekly.timer",
        "deploy/tgg/christopher/systemd/christopher-tgg-report-monthly.service",
        "deploy/tgg/christopher/systemd/christopher-tgg-report-monthly.timer",
        "deploy/tgg/christopher/systemd/christopher-tgg-nightly-whatsapp.service",
        "deploy/tgg/christopher/systemd/christopher-tgg-nightly-whatsapp.timer",
    }
    missing_runtime_files = sorted(required_runtime_files - included)
    if missing_runtime_files:
        raise RuntimeError(
            f"deploy manifest omitted required runtime files: {missing_runtime_files}"
        )
    for relative in included:
        _resolve(app_root, relative)
    manifest_services = {entry["name"] for entry in manifest.get("services", [])}
    if manifest_services != {"christopher-tgg-hermes.service"}:
        raise RuntimeError(
            "deploy manifest may restart only the Christopher Hermes service"
        )
    if any(
        forbidden in json.dumps(manifest)
        for forbidden in (
            "tgg-capture-whatsapp-bridge.service",
            "christopher-tgg-systems.service",
            "singpass-pair-server.service",
        )
    ):
        raise RuntimeError("deploy manifest crossed the service ownership boundary")

    check_path = _resolve(
        app_root, spec["reconciliation"]["stateChangeCheck"]["definition"]
    )
    check_wrapper = json.loads(check_path.read_text(encoding="utf-8"))
    checks = check_wrapper.get("checks", [])
    if len(checks) != 1:
        raise RuntimeError(
            "deployment must declare exactly one runtime invariant check"
        )
    check = checks[0]
    if (
        check.get("logical_key")
        != spec["reconciliation"]["stateChangeCheck"]["logicalKey"]
    ):
        raise RuntimeError("runtime invariant check logical key drifted from the spec")
    if check.get("red_owner_agent_override") != "edna":
        raise RuntimeError("runtime invariant check must page Edna on red")
    if check.get("checker_error_direction") != "fail_closed":
        raise RuntimeError("runtime invariant check must fail closed")

    return {
        "ok": True,
        "spec": str(spec_path),
        "brain_ref": spec["brainRef"],
        "default_slot": default_slot,
        "allowed_slots": allowed_slots,
        "slot_hashes": slot_hashes,
        "processing_enabled": False,
        "group_sessions_per_user": False,
        "owned_units": sorted(expected_units),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-root", required=True)
    parser.add_argument("--spec", required=True)
    args = parser.parse_args()
    result = validate(Path(args.app_root), Path(args.spec))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
