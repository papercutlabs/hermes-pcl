from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
DEPLOY_ROOT = ROOT / "deploy" / "tgg" / "christopher"
VERIFY = DEPLOY_ROOT / "scripts" / "verify_runtime.sh"


def _status(
    *,
    state: str,
    enabled: bool,
    retention_held: int = 0,
    retention_hold: str | None = None,
    **overrides: object,
) -> dict[str, object]:
    value: dict[str, object] = {
        "state": state,
        "processing_enabled": enabled,
        "config_enabled": enabled,
        "gate_enabled": enabled,
        "retention_held": retention_held,
        "retention_hold": retention_hold,
    }
    value.update(overrides)
    return value


@pytest.mark.parametrize(
    ("enabled", "gate_enabled", "status", "expected_ok"),
    [
        pytest.param(True, True, _status(state="running", enabled=True), True, id="running"),
        pytest.param(
            False, False, _status(state="standby", enabled=False), True, id="standby"
        ),
        pytest.param(
            True,
            True,
            _status(
                state="held-pending",
                enabled=True,
                retention_held=1,
                retention_hold="mandatory media has no capture path",
            ),
            True,
            id="held-pending-with-retention-evidence",
        ),
        pytest.param(
            True,
            True,
            _status(
                state="held-pending",
                enabled=True,
                retention_hold="mandatory media has no capture path",
            ),
            False,
            id="held-pending-without-held-count",
        ),
        pytest.param(
            True,
            True,
            _status(state="held-pending", enabled=True, retention_held=1),
            False,
            id="held-pending-without-error",
        ),
        pytest.param(
            False,
            False,
            _status(
                state="held-pending",
                enabled=False,
                retention_held=1,
                retention_hold="mandatory media has no capture path",
            ),
            False,
            id="disabled-cannot-be-held-pending",
        ),
        pytest.param(
            True,
            True,
            _status(
                state="running",
                enabled=True,
                retention_held=1,
                retention_hold="mandatory media has no capture path",
            ),
            False,
            id="running-cannot-have-retention-error",
        ),
        pytest.param(
            False,
            False,
            _status(
                state="standby",
                enabled=False,
                processing_enabled=True,
            ),
            False,
            id="status-flags-cannot-contradict-config",
        ),
        pytest.param(
            True,
            False,
            _status(state="running", enabled=True),
            False,
            id="processing-gate-cannot-contradict-config",
        ),
    ],
)
def test_consumer_status_contract(
    tmp_path: Path,
    enabled: bool,
    gate_enabled: bool,
    status: dict[str, object],
    expected_ok: bool,
) -> None:
    config_path = tmp_path / "config.yaml"
    gate_path = tmp_path / "processing-gate.json"
    status_path = tmp_path / "capture-consumer-status.json"
    config_path.write_text(yaml.safe_dump({"pa": {"enabled": enabled}}))
    gate_path.write_text(json.dumps({"enabled": gate_enabled}))
    status_path.write_text(json.dumps(status))

    result = subprocess.run(
        [
            str(VERIFY),
            "--verify-status-contract",
            str(config_path),
            str(gate_path),
            str(status_path),
        ],
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "VERIFY_PYTHON": sys.executable},
    )

    assert (result.returncode == 0) is expected_ok, result.stderr


def test_fatal_held_producer_shape_reports_fatal_message(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    gate_path = tmp_path / "processing-gate.json"
    status_path = tmp_path / "capture-consumer-status.json"
    config_path.write_text(yaml.safe_dump({"pa": {"enabled": True}}))
    gate_path.write_text(json.dumps({"enabled": True}))
    status_path.write_text(
        json.dumps(
            _status(
                state="held",
                enabled=True,
                processing_enabled=False,
                retention_held=1,
                retention_hold="mandatory media has no capture path",
            )
        )
    )

    result = subprocess.run(
        [
            str(VERIFY),
            "--verify-status-contract",
            str(config_path),
            str(gate_path),
            str(status_path),
        ],
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "VERIFY_PYTHON": sys.executable},
    )

    assert result.returncode != 0
    assert "fatal consumer state: held" in result.stderr


def test_deploy_selects_canonical_manifest_and_current_units() -> None:
    assert not (ROOT / "pa-agent.manifest.json").exists()

    spec = yaml.safe_load((DEPLOY_ROOT / "client-agent-deployment.yaml").read_text())
    manifest_ref = spec["spec"]["deploy"]["manifestRef"]
    assert manifest_ref == "deploy/tgg/christopher/pa-agent.hermes.manifest.json"

    deploy_script = (DEPLOY_ROOT / "scripts" / "deploy_runtime.sh").read_text()
    assert 'd["spec"]["deploy"]["manifestRef"]' in deploy_script
    assert '--manifest "$manifest_path"' in deploy_script
    assert "output_quality_eval.py" in deploy_script
    assert '"--trigger",\n            "deploy"' in deploy_script
    assert '"--maker-session-id",' in deploy_script
    assert 'f"deploy:{head_sha}"' in deploy_script
    assert "set +e" in deploy_script
    assert '"evaluator_ok": evaluator_ok' in deploy_script
    assert '"evaluator_ok":%s' in deploy_script

    verify_script = VERIFY.read_text()
    assert "systemctl is-active --quiet systems-papercut-labs.service" in verify_script
    assert "christopher-tgg-systems.service" not in verify_script
    assert "singpass-pair-server.service" not in verify_script
    assert (
        'APP_ROOT="$APP_ROOT" VERIFY_PYTHON="$APP_ROOT/.venv/bin/python" \\\n'
        '  "$0" --verify-status-contract \\\n'
        '  "$HERMES_HOME/config.yaml" \\\n'
        '  "$RUNTIME_ROOT/processing-gate.json" \\\n'
        '  "$RUNTIME_ROOT/capture-consumer-status.json"'
    ) in verify_script
    main_pid_offset = verify_script.index(
        'main_pid="$(systemctl show -p MainPID --value '
        'christopher-tgg-hermes.service)"'
    )
    assert verify_script.index(
        'if python3 - "$RUNTIME_ROOT/capture-consumer-status.json"'
    ) < main_pid_offset
    assert verify_script.index("exit 34") < main_pid_offset
    assert '"state": status["state"]' in verify_script
    assert '"retention_held": status["retention_held"]' in verify_script
    assert '"retention_hold": status["retention_hold"]' in verify_script

    manifest = json.loads((ROOT / manifest_ref).read_text())
    assert [service["name"] for service in manifest["services"]] == [
        "christopher-tgg-hermes.service"
    ]
    manifest_text = json.dumps(manifest)
    assert "christopher-tgg-systems.service" not in manifest_text
    assert "singpass-pair-server.service" not in manifest_text
    assert manifest["verifyHooks"] == [
        {
            "name": "christopher-hermes-full-consumer-check",
            "command": (
                "/home/pclaw/apps/hermes-pcl/deploy/tgg/christopher/"
                "scripts/verify_runtime.sh --full"
            ),
            "timeoutMs": 600000,
        }
    ]
