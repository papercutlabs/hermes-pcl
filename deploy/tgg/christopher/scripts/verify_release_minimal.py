#!/usr/bin/env python3
"""Critical Christopher release gate; business diagnostics run separately."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pwd
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import yaml


FIVE_GIB = 5 * 1024**3
SERVICE = "christopher-tgg-hermes.service"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, capture_output=True, text=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-root", default="/home/pclaw/apps/hermes-pcl")
    parser.add_argument("--hermes-home", default="/home/pclaw/.hermes-christopher-tgg")
    parser.add_argument("--test-root", default="/home/pclaw/.hermes-christopher-tgg-test")
    parser.add_argument(
        "--python",
        help="service interpreter; defaults to <app-root>/.venv/bin/python "
        "(the standalone /opt runtime has no virtualenv, so pass it there)",
    )
    args = parser.parse_args()

    app = Path(args.app_root).resolve(strict=True)
    home = Path(args.hermes_home).resolve(strict=True)
    runtime = home / "runtime"
    test_root = Path(args.test_root).resolve()
    deploy = app / "deploy/tgg/christopher"
    python = Path(args.python) if args.python else app / ".venv/bin/python"

    assert run(["systemctl", "is-active", SERVICE]).stdout.strip() == "active"
    pid = int(run(["systemctl", "show", "-p", "MainPID", "--value", SERVICE]).stdout)
    assert pid > 0
    status_lines = Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines()
    uid_line = next(line for line in status_lines if line.startswith("Uid:"))
    assert int(uid_line.split()[1]) == pwd.getpwnam("pclaw").pw_uid

    config = yaml.safe_load((home / "config.yaml").read_text()) or {}
    gate = json.loads((runtime / "processing-gate.json").read_text())
    consumer = json.loads((runtime / "capture-consumer-status.json").read_text())
    enabled = config["pa"]["enabled"]
    assert isinstance(enabled, bool)
    assert gate["enabled"] is enabled
    assert consumer["processing_enabled"] is enabled

    engine = json.loads((runtime / "engine-slot-receipt.json").read_text())
    profile_path = runtime / "provider-profile.json"
    profile = (
        json.loads(profile_path.read_text())
        if profile_path.exists()
        else {"provider": "openai-direct-primary", "credential_label": None}
    )
    provider = profile["provider"]
    credential_label = profile.get("credential_label")
    assert engine["provider"] == provider == config["model"]["provider"]
    assert engine["model"] == config["model"]["default"]
    assert engine.get("reasoning_effort") == config.get("agent", {}).get("reasoning_effort")
    if provider == "openai-codex":
        assert credential_label and config["model"]["credential_label"] == credential_label
        run(
            [
                "runuser", "-u", "pclaw", "--", "env", f"HERMES_HOME={home}",
                str(python),
                str(deploy / "scripts/verify_codex_auth.py"),
                "--hermes-home", str(home),
                "--credential-label", credential_label,
                "--service-user", "pclaw",
            ]
        )

    current = runtime / "capabilities/christopher-tgg/current"
    release = current.resolve(strict=True)
    manifest_path = release / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert engine["capability_release_id"] == manifest["release_id"]
    assert engine["capability_manifest_sha256"] == sha256(manifest_path)
    assert "tgg-whatsapp-evidence" in config["plugins"]["enabled"]
    plugin = home / "plugins/tgg-whatsapp-evidence"
    assert plugin.is_symlink() and plugin.resolve(strict=True).is_dir()

    retention = config["pa"]["media_retention"]
    assert retention["min_free_bytes"] == FIVE_GIB
    assert "min_free_percent" not in retention
    media_root = Path(retention["media_root"]).resolve(strict=True)
    free_bytes = shutil.disk_usage(media_root).free
    assert free_bytes >= retention["min_free_bytes"], (free_bytes, retention["min_free_bytes"])

    test_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    report = test_root / f"release-ready-{uuid.uuid4().hex}.json"
    smoke = run(
        [
            "runuser", "-u", "pclaw", "--", "env",
            f"HERMES_HOME={home}", f"PYTHONPATH={app}",
            str(python),
            str(deploy / "scripts/run_isolated_smoke.py"),
            "--app-root", str(app), "--live-home", str(home),
            "--test-root", str(test_root),
            "--slot-file", str(runtime / "engine-slot"),
            "--report", str(report),
        ]
    )
    result = json.loads(report.read_text())
    turn = result["result"]
    assert result["ok"] is True and result["mode"] == "fixture-only"
    assert result["external_outbound_sent"] == 0
    assert result["client_mutation_requests"] == 0
    assert turn["processed"] == 1 and turn["turn_id"]
    assert turn["provider"] == provider and turn["model"] == engine["model"]
    outbound = turn["captured_outbound"]
    assert len(outbound) == 1
    assert outbound[0]["kwargs"]["content"].strip() == "READY"

    print(
        json.dumps(
            {
                "ok": True,
                "service": SERVICE,
                "pid": pid,
                "processing_enabled": enabled,
                "provider": provider,
                "credential_label": credential_label,
                "model": engine["model"],
                "reasoning_effort": engine.get("reasoning_effort"),
                "capability_release_id": manifest["release_id"],
                "free_bytes": free_bytes,
                "min_free_bytes": retention["min_free_bytes"],
                "ready_turn_id": turn["turn_id"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
