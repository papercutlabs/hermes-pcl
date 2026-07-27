#!/usr/bin/env python3
"""Configure Christopher/TGG asymmetric WhatsApp outbound policy.

This is a deploy-time helper for the pa-agent manifest.  It patches only
runtime-local state on the TGG VPS:

- HERMES_HOME .env files: disable the old global outbound kill switch and set
  the explicit per-chat outbound allowlist.
- HERMES_HOME config.yaml: expose the allowlist to the gateway-launched bridge
  and remove the old single-job pin so constitution selectors can route mgmt
  vs ops chats.
- Christopher constitution: keep ops silent, enable mgmt replies, and replace
  placeholder WhatsApp selectors with the real chat ids supplied by deploy.
- outbound-policy.env: non-secret env file consumed by the standalone v2 bridge
  systemd unit.

Any chat not supplied as --management-chat remains outbound-blocked by the
bridge allowlist.  Any chat not supplied as --ops-chat or --management-chat has
no PA job selector and therefore no TGG-specific behavior.
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
from pathlib import Path
from typing import Iterable

try:
    import yaml
except Exception as exc:  # pragma: no cover - deployment host owns PyYAML
    raise SystemExit(f"PyYAML is required to patch Christopher runtime YAML: {exc}") from exc


DEFAULT_ENV_FILES = [
    Path("/home/pclaw/.hermes-christopher-tgg/.env"),
    Path("/home/pclaw/.hermes-christopher-tgg-state/.env"),
]
DEFAULT_CONFIG_FILES = [
    Path("/home/pclaw/.hermes-christopher-tgg/config.yaml"),
    Path("/home/pclaw/.hermes-christopher-tgg-state/config.yaml"),
]
DEFAULT_CONSTITUTION = Path("/home/pclaw/.hermes-christopher-tgg/christopher_tgg_constitution.yaml")
DEFAULT_POLICY_ENV = Path("/home/pclaw/.hermes-christopher-tgg/outbound-policy.env")


def _backup(path: Path) -> None:
    if not path.exists():
        return
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.name}.bak-{stamp}-stream2-outbound")
    backup.write_bytes(path.read_bytes())


def _normalize_many(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        for part in str(value).replace("\n", ",").split(","):
            item = part.strip()
            if not item:
                continue
            if item not in seen:
                seen.add(item)
                out.append(item)
    return out


def _set_env_file(path: Path, updates: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    original_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    _backup(path)
    seen: set[str] = set()
    new_lines: list[str] = []
    for line in original_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            new_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            new_lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")
    path.write_text("\n".join(new_lines).rstrip() + "\n", encoding="utf-8")


def _read_env_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(path)
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _check_env_file(path: Path, expected: dict[str, str]) -> None:
    values = _read_env_values(path)
    for key, value in expected.items():
        actual = values.get(key)
        # These bridge env keys are optional on the Hermes process. When absent,
        # the live boundary is still checked below in policy.env, config, and the
        # constitution; do not force an .env mutation merely to satisfy this check.
        if key in {"WHATSAPP_OUTBOUND_DISABLED", "WHATSAPP_OUTBOUND_ALLOWED_CHATS"} and actual is None:
            continue
        if actual != value:
            raise AssertionError(f"{path}: expected {key}={value!r}, found {actual!r}")


def _write_policy_env(path: Path, allowed_chats: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _backup(path)
    path.write_text(
        "# Non-secret Christopher/TGG outbound policy. Managed by pa-agent deploy.\n"
        "WHATSAPP_OUTBOUND_DISABLED=false\n"
        f"WHATSAPP_OUTBOUND_ALLOWED_CHATS={','.join(allowed_chats)}\n",
        encoding="utf-8",
    )


def _check_policy_env(path: Path, allowed_chats: list[str]) -> None:
    _check_env_file(
        path,
        {
            "WHATSAPP_OUTBOUND_DISABLED": "false",
            "WHATSAPP_OUTBOUND_ALLOWED_CHATS": ",".join(allowed_chats),
        },
    )


def _check_systemd_process_env(service: str, expected: dict[str, str]) -> None:
    pid = subprocess.check_output(
        ["systemctl", "show", service, "--property=MainPID", "--value"],
        text=True,
    ).strip()
    if not pid or pid == "0":
        raise AssertionError(f"{service}: no active MainPID")
    env_path = Path("/proc") / pid / "environ"
    values: dict[str, str] = {}
    for raw in env_path.read_bytes().split(b"\0"):
        if not raw or b"=" not in raw:
            continue
        key, value = raw.split(b"=", 1)
        values[key.decode(errors="replace")] = value.decode(errors="replace")
    for key, value in expected.items():
        actual = values.get(key)
        if actual != value:
            raise AssertionError(f"{service}: expected process env {key}={value!r}, found {actual!r}")


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(path)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"{path}: expected YAML mapping")
    return loaded


def _dump_yaml(path: Path, data: dict) -> None:
    _backup(path)
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True).replace("null\n", "\n"),
        encoding="utf-8",
    )


def _patch_config(path: Path, allowed_chats: list[str]) -> None:
    data = _load_yaml(path)
    whatsapp = data.setdefault("whatsapp", {})
    if not isinstance(whatsapp, dict):
        whatsapp = {}
        data["whatsapp"] = whatsapp
    whatsapp["outbound_allowed_chats"] = list(allowed_chats)

    pa = data.get("pa")
    if isinstance(pa, dict):
        # Let constitution selectors choose ops vs management per chat.
        pa.pop("job_type", None)

    platforms = data.setdefault("platforms", {})
    if not isinstance(platforms, dict):
        platforms = {}
        data["platforms"] = platforms
    wa_platform = platforms.setdefault("whatsapp", {})
    if not isinstance(wa_platform, dict):
        wa_platform = {}
        platforms["whatsapp"] = wa_platform
    extra = wa_platform.setdefault("extra", {})
    if not isinstance(extra, dict):
        extra = {}
        wa_platform["extra"] = extra
    extra.pop("pa_job_type", None)
    extra["outbound_allowed_chats"] = list(allowed_chats)

    _dump_yaml(path, data)


def _check_config(path: Path, allowed_chats: list[str]) -> None:
    data = _load_yaml(path)
    whatsapp = data.get("whatsapp") if isinstance(data.get("whatsapp"), dict) else {}
    if whatsapp.get("outbound_allowed_chats") != allowed_chats:
        raise AssertionError(f"{path}: whatsapp.outbound_allowed_chats mismatch")
    pa = data.get("pa") if isinstance(data.get("pa"), dict) else {}
    if "job_type" in pa:
        raise AssertionError(f"{path}: pa.job_type still pins one job; selectors cannot route mgmt vs ops")
    platforms = data.get("platforms") if isinstance(data.get("platforms"), dict) else {}
    wa_platform = platforms.get("whatsapp") if isinstance(platforms.get("whatsapp"), dict) else {}
    extra = wa_platform.get("extra") if isinstance(wa_platform.get("extra"), dict) else {}
    if "pa_job_type" in extra:
        raise AssertionError(f"{path}: platforms.whatsapp.extra.pa_job_type still pins one job")
    if extra.get("outbound_allowed_chats") != allowed_chats:
        raise AssertionError(f"{path}: platforms.whatsapp.extra.outbound_allowed_chats mismatch")


def _selector(job_type: str, chat_id: str) -> dict:
    return {
        "job_type": job_type,
        "match": {
            "source.platform": "whatsapp",
            "source.chat_id": chat_id,
        },
    }


def _patch_constitution(path: Path, management_chats: list[str], ops_chats: list[str]) -> None:
    data = _load_yaml(path)
    briefs = data.setdefault("job_briefs", {})
    if not isinstance(briefs, dict):
        raise ValueError(f"{path}: job_briefs must be a mapping")
    for job in ("tgg_ops_ingest", "tgg_management"):
        if job not in briefs or not isinstance(briefs[job], dict):
            raise ValueError(f"{path}: missing job_briefs.{job}")
        policy = briefs[job].setdefault("response_policy", {})
        if not isinstance(policy, dict):
            policy = {}
            briefs[job]["response_policy"] = policy
        policy["never_send_replies"] = job == "tgg_ops_ingest"

    preserved: list[dict] = []
    for selector in data.get("selectors") or []:
        if not isinstance(selector, dict):
            continue
        match = selector.get("match") if isinstance(selector.get("match"), dict) else {}
        if match.get("source.platform") == "whatsapp" and selector.get("job_type") in {"tgg_ops_ingest", "tgg_management"}:
            continue
        preserved.append(selector)

    selectors = []
    selectors.extend(_selector("tgg_ops_ingest", chat) for chat in ops_chats)
    selectors.extend(_selector("tgg_management", chat) for chat in management_chats)
    selectors.extend(preserved)
    data["selectors"] = selectors

    _dump_yaml(path, data)


def _check_constitution(path: Path, management_chats: list[str], ops_chats: list[str]) -> None:
    data = _load_yaml(path)
    briefs = data.get("job_briefs") if isinstance(data.get("job_briefs"), dict) else {}
    ops_policy = ((briefs.get("tgg_ops_ingest") or {}).get("response_policy") or {})
    management_policy = ((briefs.get("tgg_management") or {}).get("response_policy") or {})
    if ops_policy.get("never_send_replies") is not True:
        raise AssertionError(f"{path}: tgg_ops_ingest must keep never_send_replies=true")
    if management_policy.get("never_send_replies") is not False:
        raise AssertionError(f"{path}: tgg_management must set never_send_replies=false")
    selectors = data.get("selectors") or []

    def has(job_type: str, chat_id: str) -> bool:
        return any(
            isinstance(selector, dict)
            and selector.get("job_type") == job_type
            and isinstance(selector.get("match"), dict)
            and selector["match"].get("source.platform") == "whatsapp"
            and selector["match"].get("source.chat_id") == chat_id
            for selector in selectors
        )

    for chat_id in management_chats:
        if not has("tgg_management", chat_id):
            raise AssertionError(f"{path}: missing tgg_management selector for {chat_id}")
    for chat_id in ops_chats:
        if not has("tgg_ops_ingest", chat_id):
            raise AssertionError(f"{path}: missing tgg_ops_ingest selector for {chat_id}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--management-chat", action="append", required=True, help="WhatsApp chat JID allowed to receive Christopher mgmt replies; repeatable/comma-safe")
    parser.add_argument("--ops-chat", action="append", default=[], help="WhatsApp ops chat JID routed to silent ops ingest; repeatable/comma-safe")
    parser.add_argument("--env-file", action="append", type=Path, default=[], help="Env file to patch. Defaults to Christopher Hermes env files.")
    parser.add_argument("--config-file", action="append", type=Path, default=[], help="Gateway config YAML to patch. Defaults to Christopher Hermes config files.")
    parser.add_argument("--constitution-file", type=Path, default=DEFAULT_CONSTITUTION)
    parser.add_argument("--policy-env-file", type=Path, default=DEFAULT_POLICY_ENV)
    parser.add_argument("--check-only", action="store_true", help="Verify the expected policy is already applied; do not mutate files.")
    parser.add_argument("--check-v2-service-env", help="During --check-only, also verify the named systemd service MainPID has the outbound policy env.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    management_chats = _normalize_many(args.management_chat)
    ops_chats = _normalize_many(args.ops_chat)
    if not management_chats:
        raise SystemExit("at least one --management-chat is required")
    if not ops_chats:
        raise SystemExit("at least one --ops-chat is required so ops ingestion remains explicit")

    allowed = management_chats
    env_updates = {
        "WHATSAPP_OUTBOUND_DISABLED": "false",
        "WHATSAPP_OUTBOUND_ALLOWED_CHATS": ",".join(allowed),
    }

    env_files = args.env_file or DEFAULT_ENV_FILES
    config_files = args.config_file or DEFAULT_CONFIG_FILES

    if args.check_only:
        for path in env_files:
            _check_env_file(path, env_updates)
        _check_policy_env(args.policy_env_file, allowed)
        for path in config_files:
            _check_config(path, allowed)
        _check_constitution(args.constitution_file, management_chats, ops_chats)
        if args.check_v2_service_env:
            _check_systemd_process_env(args.check_v2_service_env, env_updates)
        print("Christopher/TGG outbound policy verified")
        print(f"management_allowed={','.join(management_chats)}")
        print(f"ops_silent_count={len(ops_chats)}")
        return

    for path in env_files:
        _set_env_file(path, env_updates)
    _write_policy_env(args.policy_env_file, allowed)
    for path in config_files:
        _patch_config(path, allowed)
    _patch_constitution(args.constitution_file, management_chats, ops_chats)

    print("configured Christopher/TGG outbound policy")
    print(f"management_allowed={','.join(management_chats)}")
    print(f"ops_silent_count={len(ops_chats)}")


if __name__ == "__main__":
    main()
