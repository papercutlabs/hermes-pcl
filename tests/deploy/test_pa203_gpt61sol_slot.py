"""PA-203: the gpt-6.1-sol-medium engine slot validates everywhere.

Added exactly as gpt-6-sol-medium was (test_pa203_gpt6sol_slot.py).  The
gpt-6-sol and terra slots stay, so a switch back is always possible.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy/tgg/christopher"
SCRIPTS = DEPLOY / "scripts"
SLOT = "gpt-6.1-sol-medium"
MODEL = "gpt-6.1-sol"
ROLLBACK_SLOT = "gpt-6-sol-medium"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"pa203_61_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_slot_is_built_with_every_auxiliary_task_on_the_slot_model():
    builder = _load("build_runtime_slots")
    assert builder.SLOTS[SLOT] == {"model": MODEL, "reasoning_effort": "medium"}
    assert ROLLBACK_SLOT in builder.SLOTS
    config = yaml.safe_load((DEPLOY / "runtime-slots" / SLOT / "config.yaml").read_text())
    constitution = yaml.safe_load(
        (DEPLOY / "runtime-slots" / SLOT / "christopher_tgg_constitution.yaml").read_text()
    )
    assert config["model"]["default"] == MODEL
    assert config["agent"]["reasoning_effort"] == "medium"
    assert {task["model"] for task in config["auxiliary"].values()} == {MODEL}
    assert constitution["runtime"]["model"] == MODEL


def test_slot_files_match_their_checksums_and_differ_from_gpt6sol_only_by_model():
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
        old = (DEPLOY / "runtime-slots" / ROLLBACK_SLOT / name).read_text()
        assert path.read_text() == old.replace("gpt-6-sol", MODEL)


def test_every_table_accepts_the_slot():
    apply = _load("apply_engine_slot")
    assert SLOT in apply.ALLOWED_SLOTS and ROLLBACK_SLOT in apply.ALLOWED_SLOTS
    assert (apply.SLOT_MODELS[SLOT], apply.SLOT_REASONING_EFFORT[SLOT]) == (MODEL, "medium")
    apply._validate_slot(DEPLOY / "runtime-slots" / SLOT, SLOT)
    completed = subprocess.run(
        [sys.executable, str(SCRIPTS / "validate_deployment_spec.py"),
         "--app-root", str(ROOT), "--spec", str(DEPLOY / "client-agent-deployment.yaml")],
        capture_output=True, text=True, check=True,
    )
    result = json.loads(completed.stdout)
    assert result["ok"] is True and result["allowed_slots"][-2:] == [ROLLBACK_SLOT, SLOT]
    verify = (SCRIPTS / "verify_runtime.sh").read_text()
    assert verify.count(f'"{SLOT}": "{MODEL}",') == 2
    assert verify.count(f'"{SLOT}": "medium",') == 2
    assert (SCRIPTS / "switch_engine_slot.sh").read_text().count(f"|{SLOT}") == 2
    assert f'"{SLOT}",' in (SCRIPTS / "run_isolated_smoke.py").read_text()
    text = (DEPLOY / "pa-agent.hermes.manifest.json").read_text()
    for name in ("config.yaml", "christopher_tgg_constitution.yaml"):
        assert f"deploy/tgg/christopher/runtime-slots/{SLOT}/{name}" in text


def test_production_shape_switch_and_back(tmp_path, monkeypatch):
    """External capability, processing on: gpt-6-sol -> gpt-6.1-sol -> gpt-6-sol."""
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

    run("--slot", ROLLBACK_SLOT)
    config = yaml.safe_load((home / "config.yaml").read_text())
    config["pa"]["enabled"] = True
    config.setdefault("auxiliary", {})["compression"] = {"provider": "main", "model": "gpt-6-sol"}
    (home / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
    run("--preserve-host-config")
    before = yaml.safe_load((home / "config.yaml").read_text())

    run("--slot", SLOT)
    run("--preserve-host-config")  # the restart's ExecStartPre
    live = yaml.safe_load((home / "config.yaml").read_text())
    assert live["pa"]["enabled"] is True
    assert live["model"]["default"] == MODEL
    assert live["auxiliary"]["compression"]["model"] == MODEL
    assert (home / "runtime/engine-slot").read_text().strip() == SLOT
    receipt = json.loads((home / "runtime/engine-slot-receipt.json").read_text())
    assert (receipt["model"], receipt["reasoning_effort"]) == (MODEL, "medium")
    constitution = yaml.safe_load((home / "christopher_tgg_constitution.yaml").read_text())
    assert constitution["runtime"]["model"] == MODEL
    assert {b["runtime"]["model"] for b in constitution["job_briefs"].values() if "runtime" in b} <= {MODEL}

    run("--slot", ROLLBACK_SLOT)
    run("--preserve-host-config")
    assert yaml.safe_load((home / "config.yaml").read_text()) == before
