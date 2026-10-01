#!/usr/bin/env python3
"""Atomically materialize a pre-staged Christopher engine slot."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import grp
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


# slot id -> model. Suffixed slots pin an explicit reasoning effort.
SLOT_MODELS = {
    "gpt-5.4-mini": "gpt-5.4-mini",
    "gpt-5.6-luna": "gpt-5.6-luna",
    "gpt-5.6-luna-low": "gpt-5.6-luna",
    "gpt-5.6-luna-xhigh": "gpt-5.6-luna",
    "gpt-5.6-terra-medium": "gpt-5.6-terra",
    "gpt-5.6-terra-high": "gpt-5.6-terra",
    "gpt-6-sol-medium": "gpt-6-sol",
    "gpt-6.1-sol-medium": "gpt-6.1-sol",
}
SLOT_REASONING_EFFORT = {
    "gpt-5.6-luna-low": "low",
    "gpt-5.6-luna-xhigh": "xhigh",
    "gpt-5.6-terra-medium": "medium",
    "gpt-5.6-terra-high": "high",
    "gpt-6-sol-medium": "medium",
    "gpt-6.1-sol-medium": "medium",
}
ALLOWED_SLOTS = tuple(SLOT_MODELS)
DEFAULT_SLOT = ALLOWED_SLOTS[0]
CAPABILITY_ID = "christopher-tgg"
CAPABILITY_COMPATIBILITY_CONTRACT = "christopher-tgg-host-config/v1"
CAPABILITY_BASE_FILES = frozenset({
    "christopher_tgg_constitution.yaml",
    "plugins/tgg-whatsapp-evidence/__init__.py",
    "plugins/tgg-whatsapp-evidence/plugin.yaml",
})
CAPABILITY_NIGHTLY_PLUGIN_FILES = frozenset({
    "plugins/tgg-nightly-whatsapp/__init__.py",
    "plugins/tgg-nightly-whatsapp/plugin.yaml",
})
CAPABILITY_NIGHTLY_LAUNCHER_FILES = frozenset({
    "scripts/run-nightly-whatsapp.py",
})
CAPABILITY_PER_CASE_PLUGIN_FILES = frozenset({
    "plugins/tgg-per-case-whatsapp/__init__.py",
    "plugins/tgg-per-case-whatsapp/plugin.yaml",
})
CAPABILITY_PER_CASE_HELPER_FILES = frozenset({
    "scripts/per-case-whatsapp-engine.mjs",
})
SHARED_MANAGEMENT_CHAT_IDS = frozenset({
    "120363426509183563@g.us",
    "120363407903158826@g.us",
})
CAPABILITY_SKILL_SLUG = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
PROVIDER_PROFILES = frozenset({"openai-direct-primary", "openai-codex"})
DEFAULT_PROVIDER_PROFILE = "openai-direct-primary"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _file_identity(path: Path) -> dict[str, int | str]:
    value = path.stat()
    return {
        "sha256": _sha256(path),
        "mode": value.st_mode & 0o777,
        "uid": value.st_uid,
        "gid": value.st_gid,
    }


def _expected_hashes(slots_root: Path) -> dict[str, str]:
    expected: dict[str, str] = {}
    for raw in (slots_root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, relative = raw.split(None, 1)
        expected[relative.strip()] = digest
    return expected


def _atomic_copy(source: Path, target: Path, *, mode: int, uid: int, gid: int) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent, prefix=f".{target.name}.", delete=False
    ) as handle:
        tmp = Path(handle.name)
        handle.write(source.read_bytes())
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(tmp, mode)
        os.chown(tmp, uid, gid)
        os.replace(tmp, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if tmp.exists():
            tmp.unlink()


def _select_slot(slot_file: Path, requested: str | None) -> str:
    if requested:
        selected = requested.strip()
    elif slot_file.exists():
        selected = slot_file.read_text(encoding="utf-8").strip()
    else:
        selected = DEFAULT_SLOT
    if selected not in ALLOWED_SLOTS:
        raise RuntimeError(f"invalid engine slot {selected!r}; allowed={ALLOWED_SLOTS}")
    return selected


def _atomic_write_json(path: Path, payload: dict, *, mode: int, uid: int, gid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write((json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temporary, mode)
        os.chown(temporary, uid, gid)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_write_yaml(path: Path, payload: dict, *, mode: int, uid: int, gid: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(yaml.safe_dump(payload, sort_keys=False).encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temporary, mode)
        os.chown(temporary, uid, gid)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_provider_profile(profile_file: Path) -> tuple[str, str | None]:
    if not profile_file.exists():
        return DEFAULT_PROVIDER_PROFILE, None
    try:
        payload = json.loads(profile_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"invalid provider profile state at {profile_file}: {exc}") from exc
    provider = str(payload.get("provider") or "").strip() if isinstance(payload, dict) else ""
    label = payload.get("credential_label") if isinstance(payload, dict) else None
    if provider not in PROVIDER_PROFILES:
        raise RuntimeError(f"invalid provider profile {provider!r}")
    if label is not None and not isinstance(label, str):
        raise RuntimeError("provider credential_label must be a string or null")
    label = label.strip() if isinstance(label, str) else None
    if provider == "openai-codex" and not label:
        raise RuntimeError("openai-codex requires an explicit credential_label")
    if provider != "openai-codex" and label:
        raise RuntimeError(f"{provider} does not accept credential_label")
    return provider, label


def _apply_provider_profile(
    config_path: Path, constitution_path: Path, *, provider: str,
    credential_label: str | None, model: str, uid: int, gid: int,
) -> None:
    """Overlay accountable provider/account selection onto copied source files."""
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))
    original_config = json.loads(json.dumps(config))
    original_constitution = json.loads(json.dumps(constitution))
    config["model"]["provider"] = provider
    if credential_label:
        config["model"]["credential_label"] = credential_label
    else:
        config["model"].pop("credential_label", None)
    constitution["runtime"] = {"provider": provider, "model": model}
    for brief in constitution.get("job_briefs", {}).values():
        if isinstance(brief.get("runtime"), dict):
            brief["runtime"] = {"provider": provider, "model": model}
    if config != original_config:
        _atomic_write_yaml(config_path, config, mode=0o640, uid=uid, gid=gid)
    if constitution != original_constitution:
        _atomic_write_yaml(constitution_path, constitution, mode=0o644, uid=uid, gid=gid)


def _apply_constitution_runtime(
    constitution_path: Path,
    *,
    provider: str,
    model: str,
    uid: int,
    gid: int,
) -> None:
    """Overlay runtime identity onto instructions without touching host config."""
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))
    original = json.loads(json.dumps(constitution))
    constitution["runtime"] = {"provider": provider, "model": model}
    for brief in constitution.get("job_briefs", {}).values():
        if isinstance(brief, dict):
            brief["runtime"] = {"provider": provider, "model": model}
    if constitution != original:
        _atomic_write_yaml(constitution_path, constitution, mode=0o644, uid=uid, gid=gid)


def _validate_live_runtime_config(config_path: Path, slot: str) -> tuple[str, str | None]:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    model = config.get("model") or {}
    agent = config.get("agent") or {}
    pa = config.get("pa") or {}
    provider = model.get("provider")
    credential_label = model.get("credential_label")
    if provider not in PROVIDER_PROFILES:
        raise RuntimeError(f"live host provider is invalid: {provider!r}")
    if provider == "openai-codex":
        if not isinstance(credential_label, str) or not credential_label.strip():
            raise RuntimeError("live openai-codex config requires credential_label")
        credential_label = credential_label.strip()
    elif credential_label is not None:
        raise RuntimeError("live direct provider must not carry credential_label")
    if model.get("default") != SLOT_MODELS[slot]:
        raise RuntimeError("live host model does not match selected engine slot")
    effort = agent.get("reasoning_effort")
    if effort != SLOT_REASONING_EFFORT.get(slot):
        raise RuntimeError("live host reasoning effort does not match selected engine slot")
    if not isinstance(pa.get("enabled"), bool):
        raise RuntimeError("live host processing state must be boolean")
    if config.get("group_sessions_per_user") is not False:
        raise RuntimeError("live host group session contract invalid")
    if (config.get("platforms") or {}).get("whatsapp", {}).get("enabled") is not False:
        raise RuntimeError("live host generic WhatsApp adapter must remain disabled")
    return provider, credential_label


def _apply_slot_runtime_contract(
    config_path: Path,
    constitution_path: Path,
    *,
    slot_root: Path,
    uid: int,
    gid: int,
) -> None:
    """Overlay runtime-owned fields onto an external capability release.

    A capability owns Christopher's instructions, tools, and selectors.  The
    selected engine slot owns model, reasoning effort, and the media-retention
    safety contract.  Keeping those fields out of capability ownership lets a
    provider/engine or disk-policy change take effect without rebuilding an
    otherwise identical capability snapshot.
    """
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))
    original_config = json.loads(json.dumps(config))
    original_constitution = json.loads(json.dumps(constitution))
    slot_config = yaml.safe_load((slot_root / "config.yaml").read_text(encoding="utf-8"))
    slot_constitution = yaml.safe_load(
        (slot_root / "christopher_tgg_constitution.yaml").read_text(encoding="utf-8")
    )
    config["model"] = slot_config["model"]
    # The slot owns every auxiliary task that runs on the main model, so an
    # engine switch moves compression, session search, titles and vision too.
    for task, slot_task in (slot_config.get("auxiliary") or {}).items():
        live_task = (config.get("auxiliary") or {}).get(task)
        if (
            isinstance(live_task, dict)
            and isinstance(slot_task, dict)
            and live_task.get("provider") == "main"
            and "model" in slot_task
        ):
            live_task["model"] = slot_task["model"]
    config["pa"]["media_retention"] = slot_config["pa"]["media_retention"]
    config["agent"].pop("reasoning_effort", None)
    if "reasoning_effort" in slot_config["agent"]:
        config["agent"]["reasoning_effort"] = slot_config["agent"]["reasoning_effort"]
    constitution["runtime"] = slot_constitution["runtime"]
    for brief in constitution.get("job_briefs", {}).values():
        if isinstance(brief.get("runtime"), dict):
            brief["runtime"] = dict(slot_constitution["runtime"])
    if config != original_config:
        _atomic_write_yaml(config_path, config, mode=0o640, uid=uid, gid=gid)
    if constitution != original_constitution:
        _atomic_write_yaml(constitution_path, constitution, mode=0o644, uid=uid, gid=gid)


def _bind_live_constitution_path(
    config_path: Path,
    constitution_path: Path,
    *,
    uid: int,
    gid: int,
) -> None:
    """Make PA read the engine-overlaid live constitution."""
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["pa"]["constitution_path"] = str(constitution_path)
    _atomic_write_yaml(config_path, config, mode=0o640, uid=uid, gid=gid)


def _validate_runtime_pair(config_path: Path, constitution_path: Path, slot: str) -> None:
    model = SLOT_MODELS[slot]
    effort = SLOT_REASONING_EFFORT.get(slot)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))
    assert config["pa"]["enabled"] is False
    assert config["group_sessions_per_user"] is False
    assert config["timezone"] == "Asia/Singapore"
    assert config["session_reset"] == {"mode": "none"}
    assert config["platforms"]["whatsapp"]["enabled"] is False
    assert config["model"]["provider"] == "openai-direct-primary"
    assert config["model"]["default"] == model
    if effort is None:
        assert "reasoning_effort" not in config["agent"]
    else:
        assert config["agent"]["reasoning_effort"] == effort
    assert constitution["runtime"] == {
        "provider": "openai-direct-primary",
        "model": model,
    }


def _validate_slot(slot_root: Path, slot: str) -> None:
    _validate_runtime_pair(
        slot_root / "config.yaml",
        slot_root / "christopher_tgg_constitution.yaml",
        slot,
    )


def _parse_sums(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        digest, relative = raw.split(None, 1)
        relative = relative.strip()
        if relative in values:
            raise RuntimeError(f"duplicate capability checksum entry: {relative}")
        values[relative] = digest
    return values


def _capability_skill_files(files: dict[str, str]) -> dict[str, set[str]]:
    """Validate manifest-pinned capability paths without hardcoding components."""
    for relative in (name for name in files if not name.startswith("skills/")):
        parts = relative.split("/")
        valid = relative in CAPABILITY_BASE_FILES
        if len(parts) == 3 and parts[0] == "plugins":
            allowed_plugin_files = {"__init__.py", "plugin.yaml"}
            if parts[1] == "tgg-whatsapp-evidence":
                allowed_plugin_files.add("post_compaction.py")
            valid = bool(CAPABILITY_SKILL_SLUG.fullmatch(parts[1])) and parts[2] in allowed_plugin_files
        elif len(parts) == 2 and parts[0] == "scripts":
            valid = bool(re.fullmatch(r"[a-z0-9][a-z0-9._-]*\.(?:py|mjs)", parts[1]))
        if not valid:
            raise RuntimeError(f"external capability file path is not allowed: {relative}")

    grouped: dict[str, set[str]] = {}
    for relative in files:
        if not relative.startswith("skills/"):
            continue
        parts = relative.split("/")
        if (
            len(parts) < 3
            or any(not part or part in {".", ".."} for part in parts)
            or not CAPABILITY_SKILL_SLUG.fullmatch(parts[1])
        ):
            raise RuntimeError("external capability skill path is invalid")
        skill_relative = "/".join(parts[2:])
        if skill_relative not in {"SKILL.md", "agents/openai.yaml"} and not (
            len(parts) >= 4 and parts[2] in {"assets", "references"}
        ):
            raise RuntimeError(f"external capability skill file is not allowed: {relative}")
        grouped.setdefault(parts[1], set()).add(skill_relative)

    for slug, skill_files in grouped.items():
        if "SKILL.md" not in skill_files:
            raise RuntimeError(f"external capability skill is missing SKILL.md: {slug}")
    return grouped


def _validate_capability_skills(release_root: Path, grouped: dict[str, set[str]]) -> None:
    for slug, skill_files in grouped.items():
        skill_path = release_root / "skills" / slug / "SKILL.md"
        text = skill_path.read_text(encoding="utf-8")
        if not text.startswith("---\n"):
            raise RuntimeError(f"external capability skill frontmatter is invalid: {slug}")
        try:
            _opening, frontmatter, _body = text.split("---", 2)
            metadata = yaml.safe_load(frontmatter)
        except (ValueError, yaml.YAMLError) as exc:
            raise RuntimeError(
                f"external capability skill frontmatter is invalid: {slug}"
            ) from exc
        if (
            not isinstance(metadata, dict)
            or metadata.get("name") != slug
            or not isinstance(metadata.get("description"), str)
            or not metadata["description"].strip()
        ):
            raise RuntimeError(f"external capability skill metadata is invalid: {slug}")
        if "agents/openai.yaml" in skill_files:
            try:
                agent_metadata = yaml.safe_load(
                    (release_root / "skills" / slug / "agents" / "openai.yaml").read_text(
                        encoding="utf-8"
                    )
                )
            except yaml.YAMLError as exc:
                raise RuntimeError(
                    f"external capability skill agent metadata is invalid: {slug}"
                ) from exc
            if not isinstance(agent_metadata, dict):
                raise RuntimeError(
                    f"external capability skill agent metadata is invalid: {slug}"
                )


def _capability_compatibility(
    manifest: dict[str, Any],
    release_root: Path,
    hermes_home: Path,
    current: Path,
    files: dict[str, str],
    skill_files: dict[str, set[str]],
) -> dict[str, Any]:
    compatibility = manifest.get("compatibility")
    if (
        not isinstance(compatibility, dict)
        or set(compatibility) != {"contract", "plugins"}
        or compatibility.get("contract") != CAPABILITY_COMPATIBILITY_CONTRACT
    ):
        raise RuntimeError("external capability compatibility contract invalid")
    requirements = compatibility.get("plugins")
    if not isinstance(requirements, list) or not requirements:
        raise RuntimeError("external capability compatibility plugins invalid")
    config_path = hermes_home / "config.yaml"
    if not config_path.is_file() or config_path.is_symlink():
        raise RuntimeError("live host config is missing")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    enabled_plugins = (config.get("plugins") or {}).get("enabled")
    if not isinstance(enabled_plugins, list):
        raise RuntimeError("live host enabled plugin list is invalid")
    plugin_ids: list[str] = []
    tool_ids: list[str] = []
    bundled_plugins = {
        relative.split("/")[1]
        for relative in files
        if relative.startswith("plugins/") and len(relative.split("/")) == 3
    }
    declared_bundled: set[str] = set()
    for requirement in requirements:
        if not isinstance(requirement, dict) or set(requirement) != {"id", "tools"}:
            raise RuntimeError("external capability plugin requirement invalid")
        plugin_id = requirement.get("id")
        required_tools = requirement.get("tools")
        if (
            not isinstance(plugin_id, str)
            or not plugin_id
            or not isinstance(required_tools, list)
            or not required_tools
            or any(not isinstance(tool, str) or not tool for tool in required_tools)
            or len(required_tools) != len(set(required_tools))
        ):
            raise RuntimeError("external capability plugin requirement invalid")
        plugin_ids.append(plugin_id)
        tool_ids.extend(required_tools)
        if enabled_plugins.count(plugin_id) != 1:
            raise RuntimeError(f"live host required plugin missing: {plugin_id}")
        bundled_descriptor = release_root / "plugins" / plugin_id / "plugin.yaml"
        host_descriptor = hermes_home / "plugins" / plugin_id / "plugin.yaml"
        descriptor_path = bundled_descriptor if bundled_descriptor.is_file() else host_descriptor
        if not descriptor_path.is_file():
            raise RuntimeError(f"required plugin descriptor missing: {plugin_id}")
        descriptor = yaml.safe_load(descriptor_path.read_text(encoding="utf-8")) or {}
        provided = descriptor.get("provides_tools")
        if descriptor.get("name") != plugin_id or not isinstance(provided, list):
            raise RuntimeError(f"required plugin descriptor invalid: {plugin_id}")
        missing_tools = sorted(set(required_tools) - set(provided))
        if missing_tools:
            raise RuntimeError(
                f"required plugin tools missing: {plugin_id}:{','.join(missing_tools)}"
            )
        if bundled_descriptor.is_file():
            declared_bundled.add(plugin_id)
    if len(plugin_ids) != len(set(plugin_ids)) or len(tool_ids) != len(set(tool_ids)):
        raise RuntimeError("external capability compatibility identity duplicate")
    if declared_bundled != bundled_plugins:
        raise RuntimeError("external capability bundled plugin declaration mismatch")
    if skill_files:
        external_dirs = (config.get("skills") or {}).get("external_dirs")
        expected = str(current / "skills")
        if not isinstance(external_dirs, list) or expected not in external_dirs:
            raise RuntimeError("live host capability skills path missing")
    expected_constitution = hermes_home / "christopher_tgg_constitution.yaml"
    if Path((config.get("pa") or {}).get("constitution_path", "")) != expected_constitution:
        raise RuntimeError("live host constitution path is incompatible")
    return {
        "contract": CAPABILITY_COMPATIBILITY_CONTRACT,
        "requirements": requirements,
        "bundled_plugins": sorted(bundled_plugins),
    }
def _external_capability(runtime_root: Path, hermes_home: Path, slot: str) -> dict[str, Any] | None:
    capability_root = runtime_root / "capabilities" / CAPABILITY_ID
    current = capability_root / "current"
    if not current.exists() and not current.is_symlink():
        return None
    if not current.is_symlink():
        raise RuntimeError("external capability current pointer must be a symlink")
    releases_root = (capability_root / "releases").resolve(strict=True)
    release_root = current.resolve(strict=True)
    if not release_root.is_relative_to(releases_root) or release_root.parent != releases_root:
        raise RuntimeError("external capability target escapes the releases directory")
    manifest_path = release_root / "manifest.json"
    sums_path = release_root / "SHA256SUMS"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != "christopher-tgg-capability-release/v1":
        raise RuntimeError("external capability manifest schema mismatch")
    if manifest.get("release_id") != release_root.name:
        raise RuntimeError("external capability release id/path mismatch")
    if manifest.get("audience") != "production":
        raise RuntimeError("external capability must be a production release")
    if manifest.get("canary") is not None:
        raise RuntimeError("external capability cannot restrict management selectors")
    files = manifest.get("files")
    if not isinstance(files, dict):
        raise RuntimeError("external capability file set mismatch")
    skill_files = _capability_skill_files(files)
    sums = _parse_sums(sums_path)
    if sums.get("manifest.json") != _sha256(manifest_path):
        raise RuntimeError("external capability manifest checksum mismatch")
    for relative, expected in files.items():
        path = release_root / relative
        if (
            not path.is_file()
            or not path.resolve().is_relative_to(release_root)
            or _sha256(path) != expected
            or sums.get(relative) != expected
        ):
            raise RuntimeError(f"external capability checksum mismatch: {relative}")
    _validate_capability_skills(release_root, skill_files)
    constitution_path = release_root / "christopher_tgg_constitution.yaml"
    constitution = yaml.safe_load(constitution_path.read_text(encoding="utf-8"))
    compatibility = _capability_compatibility(
        manifest,
        release_root,
        hermes_home,
        current,
        files,
        skill_files,
    )
    # Christopher has one production runtime. Both the real and test
    # management chats are permanent selectors; testing is traffic sent to
    # the test chat, never a restricted shadow/canary release.
    management_chat_ids = {
        selector.get("match", {}).get("source.chat_id")
        for selector in constitution.get("selectors", [])
        if selector.get("job_type") == "tgg_management"
        and selector.get("match", {}).get("source.platform") == "whatsapp"
    }
    if not SHARED_MANAGEMENT_CHAT_IDS.issubset(management_chat_ids):
        raise RuntimeError("external capability shared management selector missing")
    plugin_names = sorted({
        relative.split("/")[1]
        for relative in files
        if relative.startswith("plugins/") and len(relative.split("/")) == 3
    })
    if "tgg-whatsapp-evidence" not in plugin_names:
        raise RuntimeError("external WhatsApp evidence plugin missing")
    for name in plugin_names:
        expected_files = {
            f"plugins/{name}/__init__.py", f"plugins/{name}/plugin.yaml",
        }
        if not expected_files <= set(files):
            raise RuntimeError(f"external capability plugin incomplete: {name}")
    includes_per_case_plugin = "tgg-per-case-whatsapp" in plugin_names
    includes_per_case_helpers = CAPABILITY_PER_CASE_HELPER_FILES.issubset(files)
    if includes_per_case_plugin != includes_per_case_helpers:
        raise RuntimeError("external per-case component is incomplete")
    plugin_sources = {
        name: release_root / "plugins" / name for name in plugin_names
    }
    plugin_source = plugin_sources["tgg-whatsapp-evidence"]
    return {
        "release_root": release_root,
        "release_id": manifest["release_id"],
        "manifest_sha256": _sha256(manifest_path),
        "constitution_path": constitution_path,
        "compatibility": compatibility,
        "plugin_source": plugin_source,
        "plugin_link": hermes_home / "plugins" / "tgg-whatsapp-evidence",
        "plugin_sources": plugin_sources,
        "plugin_links": {
            name: hermes_home / "plugins" / name for name in plugin_sources
        },
    }


def _atomic_symlink(target: Path, link: Path, *, uid: int, gid: int) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    temporary = link.parent / f".{link.name}.{os.getpid()}.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(target)
    os.lchown(temporary, uid, gid)
    try:
        os.replace(temporary, link)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-root", required=True)
    parser.add_argument("--hermes-home", required=True)
    parser.add_argument("--slot", choices=ALLOWED_SLOTS)
    parser.add_argument("--provider-profile", choices=sorted(PROVIDER_PROFILES))
    parser.add_argument("--credential-label")
    parser.add_argument(
        "--preserve-host-config",
        action="store_true",
        help="validate the live config and materialize only runtime-owned state",
    )
    args = parser.parse_args()

    app_root = Path(args.app_root).resolve()
    hermes_home = Path(args.hermes_home).resolve()
    slots_root = app_root / "deploy" / "tgg" / "christopher" / "runtime-slots"
    runtime_root = hermes_home / "runtime"
    slot_file = runtime_root / "engine-slot"
    profile_file = runtime_root / "provider-profile.json"
    selected = _select_slot(slot_file, args.slot)
    slot_root = slots_root / selected
    _validate_slot(slot_root, selected)

    expected = _expected_hashes(slots_root)
    for name in ("config.yaml", "christopher_tgg_constitution.yaml"):
        relative = f"{selected}/{name}"
        actual = _sha256(slot_root / name)
        if expected.get(relative) != actual:
            raise RuntimeError(
                f"slot hash mismatch for {relative}: expected={expected.get(relative)} actual={actual}"
            )

    capability = _external_capability(runtime_root, hermes_home, selected)
    live_config_path = hermes_home / "config.yaml"
    config_source = live_config_path if capability else slot_root / "config.yaml"
    constitution_source = (
        capability["constitution_path"]
        if capability
        else slot_root / "christopher_tgg_constitution.yaml"
    )

    user = pwd.getpwnam("pclaw")
    group = grp.getgrnam("pclaw")
    host_config_before: dict[str, int | str] | None = None
    if args.preserve_host_config:
        if args.provider_profile or args.credential_label:
            raise RuntimeError(
                "provider selection is not allowed while preserving host config"
            )
        host_config_before = _file_identity(live_config_path)
        provider, credential_label = _validate_live_runtime_config(
            live_config_path,
            selected,
        )
        declared_provider, declared_label = _read_provider_profile(profile_file)
        if (provider, credential_label) != (declared_provider, declared_label):
            raise RuntimeError("live host config and provider profile disagree")
    else:
        provider = credential_label = None
    if args.provider_profile:
        if args.provider_profile == "openai-codex" and not (args.credential_label or "").strip():
            raise RuntimeError("--credential-label is required for openai-codex")
        if args.provider_profile != "openai-codex" and args.credential_label:
            raise RuntimeError("--credential-label is only valid for openai-codex")
        _atomic_write_json(
            profile_file,
            {"version": 1, "provider": args.provider_profile,
             "credential_label": (args.credential_label or "").strip() or None},
            mode=0o640, uid=0, gid=group.gr_gid,
        )
    if not args.preserve_host_config:
        provider, credential_label = _read_provider_profile(profile_file)
        # Explicit engine/provider switches may rewrite their owned fields,
        # while boot/deploy callers use --preserve-host-config below.
        live_processing_enabled = False
        if live_config_path.is_file():
            pa_match = re.search(
                r"^pa:\s*(?:#.*)?\n((?:[ \t].*\n|\n)*)",
                live_config_path.read_text(encoding="utf-8"),
                flags=re.MULTILINE,
            )
            if pa_match and re.search(
                r"^  enabled:\s*true\s*(?:#.*)?$", pa_match.group(1), flags=re.MULTILINE
            ):
                live_processing_enabled = True
        _atomic_copy(
            config_source,
            live_config_path,
            mode=0o640,
            uid=0,
            gid=group.gr_gid,
        )
        # With an external capability the source is the live config itself,
        # which already carries its processing state; only a repo slot copy
        # (processing forced off) needs the live key put back.
        if live_processing_enabled and config_source != live_config_path:
            config_text = live_config_path.read_text(encoding="utf-8")
            patched, count = re.subn(
                r"(^pa:\s*(?:#.*)?\n  enabled:)\s*false(\s*(?:#.*)?$)",
                r"\1 true\2",
                config_text,
                flags=re.MULTILINE,
            )
            if count != 1:
                raise RuntimeError(
                    f"expected exactly one pa.enabled to re-apply the live processing key, found {count}"
                )
            patch_tmp = hermes_home / f".config.{os.getpid()}.tmp"
            patch_tmp.write_text(patched, encoding="utf-8")
            os.chmod(patch_tmp, 0o640)
            os.chown(patch_tmp, 0, group.gr_gid)
            os.replace(patch_tmp, live_config_path)
    _atomic_copy(
        constitution_source,
        hermes_home / "christopher_tgg_constitution.yaml",
        mode=0o644,
        uid=0,
        gid=group.gr_gid,
    )
    if capability and not args.preserve_host_config:
        _apply_slot_runtime_contract(
            live_config_path,
            hermes_home / "christopher_tgg_constitution.yaml",
            slot_root=slot_root,
            uid=0,
            gid=group.gr_gid,
        )
    if args.preserve_host_config:
        _apply_constitution_runtime(
            hermes_home / "christopher_tgg_constitution.yaml",
            provider=provider,
            model=SLOT_MODELS[selected],
            uid=0,
            gid=group.gr_gid,
        )
    else:
        _apply_provider_profile(
            live_config_path, hermes_home / "christopher_tgg_constitution.yaml",
            provider=provider, credential_label=credential_label, model=SLOT_MODELS[selected],
            uid=0, gid=group.gr_gid,
        )
    if capability and not args.preserve_host_config:
        _bind_live_constitution_path(
            live_config_path,
            hermes_home / "christopher_tgg_constitution.yaml",
            uid=0,
            gid=group.gr_gid,
        )
    if capability:
        for plugin_name, plugin_link in capability["plugin_links"].items():
            if plugin_link.exists() and not plugin_link.is_symlink():
                raise RuntimeError(
                    "external capability plugin destination exists and is not a symlink; "
                    "the installer must preserve it before activation"
                )
            _atomic_symlink(
                capability["plugin_sources"][plugin_name],
                plugin_link,
                uid=user.pw_uid,
                gid=group.gr_gid,
            )
    runtime_root.mkdir(parents=True, exist_ok=True)
    slot_tmp = runtime_root / f".engine-slot.{os.getpid()}.tmp"
    slot_tmp.write_text(f"{selected}\n", encoding="utf-8")
    os.chmod(slot_tmp, 0o640)
    os.chown(slot_tmp, 0, group.gr_gid)
    os.replace(slot_tmp, slot_file)
    os.chown(hermes_home, user.pw_uid, group.gr_gid)
    if host_config_before is not None and _file_identity(live_config_path) != host_config_before:
        raise RuntimeError("live host config changed during runtime materialization")

    receipt = {
        "version": 2,
        "selected_at": datetime.now(timezone.utc).isoformat(),
        "slot": selected,
        "provider": provider,
        "credential_label": credential_label,
        "model": SLOT_MODELS[selected],
        "reasoning_effort": SLOT_REASONING_EFFORT.get(selected),
        "config_sha256": _sha256(hermes_home / "config.yaml"),
        "constitution_sha256": _sha256(
            hermes_home / "christopher_tgg_constitution.yaml"
        ),
        "configuration_source": "external-capability" if capability else "repo-engine-slot",
        "host_config_authoritative": args.preserve_host_config,
        "capability_release_id": capability["release_id"] if capability else None,
        "capability_manifest_sha256": capability["manifest_sha256"] if capability else None,
    }
    receipt_path = runtime_root / "engine-slot-receipt.json"
    receipt_path.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
    os.chmod(receipt_path, 0o640)
    os.chown(receipt_path, 0, group.gr_gid)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
