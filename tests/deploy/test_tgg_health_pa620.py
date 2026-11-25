"""PA-620: the quick health check must match the shipped state and stay read-only."""
from __future__ import annotations

import importlib.util
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "deploy" / "tgg" / "christopher" / "scripts"
VERIFY = SCRIPTS / "verify_runtime.sh"
CODEX_AUTH = SCRIPTS / "verify_codex_auth.py"


def _monthly_block() -> str:
    source = VERIFY.read_text()
    match = re.search(
        r'if [^\n]*christopher-tgg-report-monthly\.timer; then\n.*?\nfi\n', source, re.S
    )
    assert match, "monthly timer check not found in verify_runtime.sh"
    return match.group(0)


def _run_monthly_check(tmp_path: Path, enabled: bool) -> subprocess.CompletedProcess:
    stub = tmp_path / "bin"
    stub.mkdir(exist_ok=True)
    systemctl = stub / "systemctl"
    systemctl.write_text(f"#!/bin/sh\nexit {0 if enabled else 1}\n")
    systemctl.chmod(0o755)
    script = tmp_path / "check.sh"
    script.write_text("set -euo pipefail\n" + _monthly_block() + "echo passed\n")
    return subprocess.run(
        ["bash", str(script)],
        env={**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"},
        capture_output=True, text=True,
    )


def test_monthly_timer_not_enabled_fails_with_35(tmp_path):
    # Tests the extracted monthly block of verify_runtime.sh, not the whole
    # script, which needs a full host runtime.
    result = _run_monthly_check(tmp_path, enabled=False)
    assert result.returncode == 35
    assert "monthly report timer must be enabled" in result.stderr


def test_monthly_timer_enabled_passes(tmp_path):
    result = _run_monthly_check(tmp_path, enabled=True)
    assert result.returncode == 0, result.stderr
    assert "passed" in result.stdout


def test_weekly_timer_check_is_unchanged():
    source = VERIFY.read_text()
    assert (
        'if systemctl is-enabled --quiet christopher-tgg-report-weekly.timer; then\n'
        '  echo "weekly report timer must ship disabled" >&2\n'
        '  exit 35\n'
    ) in source


# ---- verify_codex_auth.py ----

class _Entry:
    def __init__(self, label, access="a-secret", refresh="r-secret", runtime=None):
        self.label = label
        self.access_token = access
        self.refresh_token = refresh
        self.runtime_api_key = runtime


class _FakePool:
    """Pool double: any refresh/select/persist path fails the test."""
    def __init__(self, entries):
        self._entries = entries

    def entries(self):
        return list(self._entries)

    def select_label(self, *_a, **_k):
        raise AssertionError("select_label must not be called")

    def _available_entries(self, *_a, **_k):
        raise AssertionError("refresh path must not be called")

    def _persist(self, *_a, **_k):
        raise AssertionError("persist must not be called")


def _run_auth(monkeypatch, tmp_path, entries, label="christopher", capsys=None, owner_ok=True):
    home = tmp_path / "home"
    home.mkdir(exist_ok=True)
    auth = home / "auth.json"
    auth.write_text("{}")
    auth.chmod(0o600)
    monkeypatch.setenv("HERMES_HOME", str(home))

    pool = _FakePool(entries)
    calls = {"load_pool": 0}

    def load_pool(_provider):
        calls["load_pool"] += 1
        return pool

    # Fake modules; from_dict is the identity so tests pass entries through.
    cp = types.ModuleType("agent.credential_pool")
    cp.load_pool = load_pool
    cp.PooledCredential = types.SimpleNamespace(from_dict=lambda _p, payload: payload)
    agent_pkg = types.ModuleType("agent")
    agent_pkg.credential_pool = cp
    auth_mod = types.ModuleType("hermes_cli.auth")
    auth_mod.read_credential_pool = lambda _provider: list(entries)
    cli_pkg = types.ModuleType("hermes_cli")
    cli_pkg.auth = auth_mod
    monkeypatch.setitem(sys.modules, "agent", agent_pkg)
    monkeypatch.setitem(sys.modules, "agent.credential_pool", cp)
    monkeypatch.setitem(sys.modules, "hermes_cli", cli_pkg)
    monkeypatch.setitem(sys.modules, "hermes_cli.auth", auth_mod)

    spec = importlib.util.spec_from_file_location("verify_codex_auth_under_test", CODEX_AUTH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    me = pwd.getpwuid(os.getuid()).pw_name
    monkeypatch.setattr(sys, "argv", [
        "verify_codex_auth.py", "--hermes-home", str(home),
        "--credential-label", label, "--service-user", me,
    ])
    return module, calls


def test_complete_entry_passes_without_select_or_refresh(monkeypatch, tmp_path, capsys):
    module, calls = _run_auth(monkeypatch, tmp_path, [_Entry(" Christopher ")])
    assert module.main() == 0
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["ok"] is True
    assert payload["credential_label"] == " Christopher "
    assert set(payload) == {
        "ok", "provider", "credential_label", "auth_store", "auth_store_owner",
        "auth_store_mode", "has_access_token", "has_refresh_token",
    }
    assert "a-secret" not in out and "r-secret" not in out
    assert calls["load_pool"] == 0, "load_pool re-seeds and persists; must not be used"


def test_runtime_api_key_alone_counts_as_access(monkeypatch, tmp_path, capsys):
    module, _ = _run_auth(monkeypatch, tmp_path, [_Entry("christopher", access=None, runtime="k-secret")])
    assert module.main() == 0
    assert "k-secret" not in capsys.readouterr().out


def test_missing_refresh_token_fails(monkeypatch, tmp_path):
    module, _ = _run_auth(monkeypatch, tmp_path, [_Entry("christopher", refresh=None)])
    with pytest.raises(RuntimeError, match="incomplete"):
        module.main()


def test_missing_access_and_runtime_key_fails(monkeypatch, tmp_path):
    module, _ = _run_auth(monkeypatch, tmp_path, [_Entry("christopher", access=None)])
    with pytest.raises(RuntimeError, match="incomplete"):
        module.main()


def test_zero_or_two_label_matches_fail(monkeypatch, tmp_path):
    module, _ = _run_auth(monkeypatch, tmp_path, [_Entry("other")])
    with pytest.raises(RuntimeError, match="exactly one"):
        module.main()
    module, _ = _run_auth(monkeypatch, tmp_path, [_Entry("christopher"), _Entry("CHRISTOPHER")])
    with pytest.raises(RuntimeError, match="exactly one"):
        module.main()


def test_script_source_never_selects_or_loads_pool():
    source = CODEX_AUTH.read_text()
    assert "select_label(" not in source.replace("Never call pool.select_label()", "")
    assert "load_pool(" not in source.replace("load_pool() is not read-only", "")
