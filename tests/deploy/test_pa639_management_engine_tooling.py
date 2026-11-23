"""PA-639: the engine-slot tooling carries the Management chats' engine.

The engine is one extra block, ``pa.management_engine``, written next to the
slot's engine by ``apply_engine_slot.py --management-engine`` and recorded in
``runtime/management-engine``.  The tooling validates it as it validates the
slot, so a wrong model cannot ship silently.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy/tgg/christopher"
SCRIPTS = DEPLOY / "scripts"
SLOT = "gpt-6.1-terra-medium"
OTHER_SLOT = "gpt-6.1-sol-medium"
MANAGEMENT = "gpt-6.2-sol-medium"
BLOCK = {"model": "gpt-6.2-sol", "reasoning_effort": "medium"}


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"pa639_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def host(tmp_path, monkeypatch):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_tgg_external_capability import make_release

    apply = _load("apply_engine_slot")
    home, _runtime, _release = make_release(tmp_path)
    monkeypatch.setattr(apply.pwd, "getpwnam", lambda _name: SimpleNamespace(pw_uid=501))
    monkeypatch.setattr(apply.grp, "getgrnam", lambda _name: SimpleNamespace(gr_gid=20))
    monkeypatch.setattr(apply.os, "chown", lambda *_args: None)
    monkeypatch.setattr(apply.os, "lchown", lambda *_args: None)

    def run(*extra: str) -> None:
        monkeypatch.setattr(sys, "argv", [
            "apply_engine_slot.py", "--app-root", str(ROOT), "--hermes-home", str(home), *extra,
        ])
        assert apply.main() == 0

    def config() -> dict:
        return yaml.safe_load((home / "config.yaml").read_text())

    run("--slot", SLOT)
    live = config()
    live["pa"]["enabled"] = True
    (home / "config.yaml").write_text(yaml.safe_dump(live, sort_keys=False))
    run("--preserve-host-config")
    return SimpleNamespace(apply=apply, home=home, run=run, config=config)


def test_install_with_the_engine_unchanged_writes_no_management_block(host):
    """Step 1 of the install: the new runtime starts with no management engine."""
    assert "management_engine" not in host.config()["pa"]
    assert not (host.home / "runtime/management-engine").exists() or (
        (host.home / "runtime/management-engine").read_text().strip() == "none"
    )
    receipt = json.loads((host.home / "runtime/engine-slot-receipt.json").read_text())
    assert receipt["management_engine"] == "none"


def test_apply_sets_the_block_state_and_receipt_and_leaves_the_slot_engine(host):
    before = host.config()
    host.run("--slot", SLOT, "--management-engine", MANAGEMENT)
    host.run("--preserve-host-config")  # the restart's ExecStartPre accepts it
    live = host.config()
    assert live["pa"]["management_engine"] == BLOCK
    assert live["model"] == before["model"]
    assert live["agent"]["reasoning_effort"] == "medium"
    assert live["pa"]["enabled"] is True
    assert (host.home / "runtime/management-engine").read_text().strip() == MANAGEMENT
    receipt = json.loads((host.home / "runtime/engine-slot-receipt.json").read_text())
    assert (receipt["model"], receipt["management_engine"]) == ("gpt-6.1-terra", MANAGEMENT)
    constitution = yaml.safe_load((host.home / "christopher_tgg_constitution.yaml").read_text())
    assert {b["runtime"]["model"] for b in constitution["job_briefs"].values() if "runtime" in b} <= {
        "gpt-6.1-terra"
    }


def test_a_later_slot_switch_keeps_the_management_engine_and_none_removes_it(host):
    host.run("--slot", SLOT, "--management-engine", MANAGEMENT)
    host.run("--slot", OTHER_SLOT)
    assert host.config()["pa"]["management_engine"] == BLOCK
    host.run("--preserve-host-config")
    host.run("--slot", SLOT, "--management-engine", "none")
    assert "management_engine" not in host.config()["pa"]
    assert (host.home / "runtime/management-engine").read_text().strip() == "none"
    host.run("--preserve-host-config")


def test_boot_refuses_a_hand_edited_or_unrecorded_block(host):
    host.run("--slot", SLOT, "--management-engine", MANAGEMENT)
    live = host.config()
    live["pa"]["management_engine"] = {"model": "gpt-6.2-astra", "reasoning_effort": "medium"}
    (host.home / "config.yaml").write_text(yaml.safe_dump(live, sort_keys=False))
    with pytest.raises(RuntimeError, match="management engine"):
        host.run("--preserve-host-config")
    live["pa"]["management_engine"] = {**BLOCK, "reasoning_effort": "high"}
    (host.home / "config.yaml").write_text(yaml.safe_dump(live, sort_keys=False))
    with pytest.raises(RuntimeError, match="management engine"):
        host.run("--preserve-host-config")
    # A block with nothing recorded for it is refused too.
    (host.home / "runtime/management-engine").write_text("none\n")
    live["pa"]["management_engine"] = BLOCK
    (host.home / "config.yaml").write_text(yaml.safe_dump(live, sort_keys=False))
    with pytest.raises(RuntimeError, match="management engine"):
        host.run("--preserve-host-config")


def test_unknown_engines_and_preserve_with_a_selection_are_refused(host):
    with pytest.raises(SystemExit):
        host.run("--slot", SLOT, "--management-engine", "gpt-6.2-astra-medium")
    with pytest.raises(RuntimeError, match="not allowed while preserving"):
        host.run("--preserve-host-config", "--management-engine", MANAGEMENT)


def test_deployment_spec_validation_and_every_copy_of_the_table_agree():
    apply = _load("apply_engine_slot")
    validate = _load("validate_deployment_spec")
    assert apply.MANAGEMENT_ENGINES == {MANAGEMENT: BLOCK}
    assert validate.MANAGEMENT_ENGINES == apply.MANAGEMENT_ENGINES
    completed = subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_deployment_spec.py"),
         "--app-root", str(ROOT), "--spec", str(DEPLOY / "client-agent-deployment.yaml")],
        capture_output=True, text=True, check=True,
    )
    assert json.loads(completed.stdout)["ok"] is True
    spec = yaml.safe_load((DEPLOY / "client-agent-deployment.yaml").read_text())
    assert spec["spec"]["engine"]["allowedManagementEngines"] == [MANAGEMENT]
    minimal = (SCRIPTS / "verify_release_minimal.py").read_text()
    assert f'"{MANAGEMENT}": {BLOCK!r}'.replace("'", '"') in minimal


def test_a_drifted_management_set_in_the_spec_fails_validation(tmp_path):
    spec_path = tmp_path / "spec.yaml"
    spec = yaml.safe_load((DEPLOY / "client-agent-deployment.yaml").read_text())
    spec["spec"]["engine"]["allowedManagementEngines"] = [MANAGEMENT, "gpt-6.2-astra-medium"]
    spec_path.write_text(yaml.safe_dump(spec, sort_keys=False))
    completed = subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_deployment_spec.py"),
         "--app-root", str(ROOT), "--spec", str(spec_path)],
        capture_output=True, text=True,
    )
    assert completed.returncode != 0
    assert "management engine set drifted" in completed.stderr + completed.stdout


def test_switch_script_takes_an_optional_known_management_engine():
    text = (SCRIPTS / "switch_engine_slot.sh").read_text()
    assert f"{MANAGEMENT}|none" in text
    # The rollback restores the previous management engine with the previous slot.
    assert '--management-engine "$old_management"' in text
    for args, code in ((["a", "b", "c"], 2), (["gpt-6.1-terra-medium", "gpt-6.2-astra-medium"], 2)):
        completed = subprocess.run(["bash", str(SCRIPTS / "switch_engine_slot.sh"), *args],
                                   capture_output=True, text=True)
        assert completed.returncode == code
    assert re.search(r"usage: .* \[gpt-6.2-sol-medium\|none\]", subprocess.run(
        ["bash", str(SCRIPTS / "switch_engine_slot.sh")], capture_output=True, text=True).stderr)
