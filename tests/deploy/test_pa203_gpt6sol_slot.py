"""PA-203 round 2: the gpt-6-sol-medium engine slot validates everywhere.

Christopher's engine is chosen by a named slot, and every deployment check
keeps its own copy of the allowed list.  The terra slots stay so a switch
back is always possible.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy/tgg/christopher"
SCRIPTS = DEPLOY / "scripts"
SLOT = "gpt-6-sol-medium"
ROLLBACK_SLOT = "gpt-5.6-terra-medium"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"pa203_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_slot_is_built_with_every_auxiliary_task_on_gpt6sol():
    builder = _load("build_runtime_slots")
    assert builder.SLOTS[SLOT] == {"model": "gpt-6-sol", "reasoning_effort": "medium"}
    assert ROLLBACK_SLOT in builder.SLOTS
    config = yaml.safe_load((DEPLOY / "runtime-slots" / SLOT / "config.yaml").read_text())
    constitution = yaml.safe_load(
        (DEPLOY / "runtime-slots" / SLOT / "christopher_tgg_constitution.yaml").read_text()
    )
    assert config["model"]["default"] == "gpt-6-sol"
    assert config["agent"]["reasoning_effort"] == "medium"
    assert {task["model"] for task in config["auxiliary"].values()} == {"gpt-6-sol"}
    assert constitution["runtime"]["model"] == "gpt-6-sol"
    assert {
        brief["runtime"]["model"]
        for brief in constitution["job_briefs"].values()
        if isinstance(brief.get("runtime"), dict)
    } <= {"gpt-6-sol"}


def test_slot_files_match_their_checksums():
    sums = {
        relative: digest
        for digest, relative in (
            line.split(None, 1)
            for line in (DEPLOY / "runtime-slots/SHA256SUMS").read_text().splitlines()
        )
    }
    for name in ("config.yaml", "christopher_tgg_constitution.yaml"):
        path = DEPLOY / "runtime-slots" / SLOT / name
        assert sums[f"{SLOT}/{name}"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_apply_engine_slot_accepts_the_slot():
    apply = _load("apply_engine_slot")
    assert SLOT in apply.ALLOWED_SLOTS and ROLLBACK_SLOT in apply.ALLOWED_SLOTS
    assert apply.SLOT_MODELS[SLOT] == "gpt-6-sol"
    assert apply.SLOT_REASONING_EFFORT[SLOT] == "medium"
    apply._validate_slot(DEPLOY / "runtime-slots" / SLOT, SLOT)


def test_deployment_spec_validates_with_the_slot():
    completed = subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_deployment_spec.py"),
         "--app-root", str(ROOT), "--spec", str(DEPLOY / "client-agent-deployment.yaml")],
        capture_output=True, text=True, check=True,
    )
    result = json.loads(completed.stdout)
    assert result["ok"] is True
    assert result["allowed_slots"][-3:-1] == ["gpt-5.6-terra-high", SLOT]
    spec = yaml.safe_load((DEPLOY / "client-agent-deployment.yaml").read_text())
    assert SLOT in spec["spec"]["engine"]["allowedSlots"]
    assert spec["spec"]["engine"]["switchCommand"].startswith(
        "/opt/tgg-christopher/runtime/current/"
    )


def test_every_shell_and_smoke_copy_lists_the_slot():
    verify = (SCRIPTS / "verify_runtime.sh").read_text()
    assert verify.count(f'"{SLOT}": "gpt-6-sol",') == 2
    assert verify.count(f'"{SLOT}": "medium",') == 2
    switch = (SCRIPTS / "switch_engine_slot.sh").read_text()
    assert switch.count(f"|{SLOT}") == 2
    smoke = (SCRIPTS / "run_isolated_smoke.py").read_text()
    assert f'"{SLOT}",' in smoke
    manifest = json.loads((DEPLOY / "pa-agent.hermes.manifest.json").read_text())
    text = json.dumps(manifest)
    for name in ("config.yaml", "christopher_tgg_constitution.yaml"):
        assert f"deploy/tgg/christopher/runtime-slots/{SLOT}/{name}" in text


def test_switch_defaults_to_the_production_runtime_and_interpreter():
    switch = (SCRIPTS / "switch_engine_slot.sh").read_text()
    assert 'APP_ROOT="${APP_ROOT:-/opt/tgg-christopher/runtime/current}"' in switch
    assert 'PYTHON="${PYTHON:-/home/pclaw/apps/hermes-pcl/.venv/bin/python}"' in switch
    # The standalone runtime has no .venv; nothing may call one under APP_ROOT.
    assert "$APP_ROOT/.venv" not in switch
    assert switch.count('--python "$PYTHON"') == 2
    unit = (DEPLOY / "systemd/christopher-tgg-hermes.service").read_text()
    assert (
        "ExecStartPre=+/home/pclaw/apps/hermes-pcl/.venv/bin/python "
        "/opt/tgg-christopher/runtime/current/deploy/tgg/christopher/scripts/apply_engine_slot.py"
    ) in unit
    assert "--python" in (SCRIPTS / "verify_release_minimal.py").read_text()


def _live_config(tmp_path: Path) -> Path:
    """A host-shaped live config: the terra slot on the Codex account."""
    config = yaml.safe_load(
        (DEPLOY / "runtime-slots" / ROLLBACK_SLOT / "config.yaml").read_text()
    )
    config["model"]["provider"] = "openai-codex"
    config["model"]["credential_label"] = "openai-codex-oauth-2"
    config["python_sandbox"] = {"workspace": "agent"}
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return path


def _constitution(tmp_path: Path) -> Path:
    path = tmp_path / "christopher_tgg_constitution.yaml"
    path.write_text(
        (DEPLOY / "runtime-slots" / ROLLBACK_SLOT / "christopher_tgg_constitution.yaml").read_text()
    )
    return path


def _switch(apply, config: Path, constitution: Path, slot: str) -> None:
    """The explicit-switch path of apply_engine_slot.main for an external capability."""
    apply._apply_slot_runtime_contract(
        config, constitution, slot_root=DEPLOY / "runtime-slots" / slot,
        uid=-1, gid=-1,
    )
    apply._apply_provider_profile(
        config, constitution, provider="openai-codex",
        credential_label="openai-codex-oauth-2", model=apply.SLOT_MODELS[slot],
        uid=-1, gid=-1,
    )


def test_switch_moves_every_engine_field_and_back(tmp_path, monkeypatch):
    apply = _load("apply_engine_slot")
    monkeypatch.setattr(apply.os, "chown", lambda *_: None)
    config, constitution = _live_config(tmp_path), _constitution(tmp_path)
    before = yaml.safe_load(config.read_text())

    _switch(apply, config, constitution, SLOT)
    live = yaml.safe_load(config.read_text())
    # The boot check (ExecStartPre --preserve-host-config) now accepts it.
    assert apply._validate_live_runtime_config(config, SLOT) == (
        "openai-codex", "openai-codex-oauth-2"
    )
    assert live["model"]["default"] == "gpt-6-sol"
    assert {task["model"] for task in live["auxiliary"].values()} == {"gpt-6-sol"}
    assert live["python_sandbox"] == {"workspace": "agent"}
    briefs = yaml.safe_load(constitution.read_text())["job_briefs"]
    assert briefs["tgg_management"]["runtime"] == {
        "provider": "openai-codex", "model": "gpt-6-sol"
    }

    # Rollback: switching back restores the terra engine fields exactly.
    _switch(apply, config, constitution, ROLLBACK_SLOT)
    assert yaml.safe_load(config.read_text()) == before
    apply._validate_live_runtime_config(config, ROLLBACK_SLOT)


def test_boot_check_refuses_a_hand_edited_model(tmp_path):
    apply = _load("apply_engine_slot")
    config = _live_config(tmp_path)
    data = yaml.safe_load(config.read_text())
    data["model"]["default"] = "gpt-6-sol"
    config.write_text(yaml.safe_dump(data))
    try:
        apply._validate_live_runtime_config(config, ROLLBACK_SLOT)
    except RuntimeError as exc:
        assert re.search("model does not match", str(exc))
    else:  # pragma: no cover
        raise AssertionError("a hand-edited model must fail the boot check")


def test_boot_after_switch_puts_every_brief_on_gpt6sol(tmp_path, monkeypatch):
    """The capability constitution carries no runtime blocks; the runtime writes them.

    An explicit switch rewrites the live config; the restart's
    ExecStartPre (--preserve-host-config) then copies the capability
    constitution and writes the slot model into every job brief.
    """
    apply = _load("apply_engine_slot")
    monkeypatch.setattr(apply.os, "chown", lambda *_: None)
    config = _live_config(tmp_path)
    capability = yaml.safe_load(_constitution(tmp_path).read_text())
    capability.pop("runtime", None)
    for brief in capability["job_briefs"].values():
        brief.pop("runtime", None)
    constitution = tmp_path / "christopher_tgg_constitution.yaml"
    constitution.write_text(yaml.safe_dump(capability, sort_keys=False))

    _switch(apply, config, constitution, SLOT)
    provider, _label = apply._validate_live_runtime_config(config, SLOT)
    apply._apply_constitution_runtime(
        constitution, provider=provider, model=apply.SLOT_MODELS[SLOT], uid=-1, gid=-1
    )
    live = yaml.safe_load(constitution.read_text())
    assert live["runtime"] == {"provider": "openai-codex", "model": "gpt-6-sol"}
    assert {
        json.dumps(brief["runtime"], sort_keys=True) for brief in live["job_briefs"].values()
    } == {json.dumps({"model": "gpt-6-sol", "provider": "openai-codex"}, sort_keys=True)}


def test_explicit_switch_with_processing_enabled_and_external_capability(tmp_path, monkeypatch):
    """The production shape: external capability, processing on.

    The explicit switch used to raise "expected exactly one pa.enabled to
    re-apply the live processing key, found 0", because with an external
    capability it copies the live config onto itself, which already says
    enabled: true.  switch_engine_slot.sh could never switch production.
    """
    from types import SimpleNamespace

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_tgg_external_capability import make_release

    apply = _load("apply_engine_slot")
    home, _runtime, _release = make_release(tmp_path)
    config = yaml.safe_load((home / "config.yaml").read_text())
    config["pa"]["enabled"] = True
    config.setdefault("auxiliary", {})["compression"] = {"provider": "main", "model": "gpt-5.6-terra"}
    (home / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
    monkeypatch.setattr(apply.pwd, "getpwnam", lambda _name: SimpleNamespace(pw_uid=501))
    monkeypatch.setattr(apply.grp, "getgrnam", lambda _name: SimpleNamespace(gr_gid=20))
    monkeypatch.setattr(apply.os, "chown", lambda *_args: None)
    monkeypatch.setattr(apply.os, "lchown", lambda *_args: None)

    def run(*extra: str) -> None:
        monkeypatch.setattr(sys, "argv", [
            "apply_engine_slot.py", "--app-root", str(ROOT), "--hermes-home", str(home), *extra,
        ])
        assert apply.main() == 0

    run("--slot", SLOT)
    run("--preserve-host-config")  # the restart's ExecStartPre
    live = yaml.safe_load((home / "config.yaml").read_text())
    assert live["pa"]["enabled"] is True
    assert live["model"]["default"] == "gpt-6-sol"
    assert live["auxiliary"]["compression"]["model"] == "gpt-6-sol"
    assert (home / "runtime/engine-slot").read_text().strip() == SLOT
    receipt = json.loads((home / "runtime/engine-slot-receipt.json").read_text())
    assert (receipt["model"], receipt["reasoning_effort"]) == ("gpt-6-sol", "medium")

    run("--slot", ROLLBACK_SLOT)
    run("--preserve-host-config")
    back = yaml.safe_load((home / "config.yaml").read_text())
    assert back["model"]["default"] == "gpt-5.6-terra" and back["pa"]["enabled"] is True
    assert back["auxiliary"]["compression"]["model"] == "gpt-5.6-terra"
