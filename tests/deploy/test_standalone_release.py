from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[2] / "deploy/tgg/christopher/scripts/standalone_release.py"
spec = importlib.util.spec_from_file_location("standalone_release", SCRIPT)
assert spec and spec.loader
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def capability(path: Path, release_id: str = "r1") -> None:
    path.mkdir(parents=True)
    (path / "plugin.py").write_text("x = 1\n")
    (path / "manifest.json").write_text(json.dumps({"release_id": release_id, "runtime": {"hermes_commit": "abc1234"}, "files": {"plugin.py": release.sha256(path / "plugin.py")}}))


def runtime(path: Path, revision: str = "abc1234") -> None:
    path.mkdir(parents=True)
    (path / ".git-revision").write_text(revision + "\n")
    (path / "gateway.py").write_text("pass\n")
    unit = path / "deploy/tgg/christopher/systemd/christopher-tgg-hermes.service"
    unit.parent.mkdir(parents=True)
    unit.write_text(f"[Service]\n# {revision}\n")


def runtime_manifest(path: Path) -> Path:
    manifest = path / "runtime-manifest.json"
    manifest.write_text(json.dumps({"include": ["gateway.py", "deploy/tgg/christopher/systemd/christopher-tgg-hermes.service"]}))
    return manifest


def repository_guard(commit: str) -> dict[str, str]:
    return {
        "canonical_repository_url": release.CANONICAL_REPOSITORY_URL,
        "protected_ref": release.PROTECTED_MAIN_REF,
        "verified_main_head": commit,
        "runtime_commit": commit,
        "verified_at": "2026-08-26T00:00:00+00:00",
    }


def preserve_installed_bundle(tmp_path: Path, *, commit: str, release_id: str = "preserve") -> tuple[Path, Path]:
    """Create the smallest capability-only bundle accepted by ``apply``."""
    capability_root = tmp_path / "incoming-capability"
    capability(capability_root, release_id)
    (capability_root / "plugins/tgg").mkdir(parents=True)
    (capability_root / "plugins/tgg/plugin.py").write_text("x = 2\n")
    manifest = json.loads((capability_root / "manifest.json").read_text())
    manifest["runtime"]["hermes_commit"] = commit
    manifest["files"]["plugins/tgg/plugin.py"] = release.sha256(
        capability_root / "plugins/tgg/plugin.py"
    )
    (capability_root / "manifest.json").write_text(json.dumps(manifest))

    bundle = tmp_path / "preserve-bundle"
    bundle.mkdir()
    capability_archive = bundle / "capability.tgz"
    release_json = {
        "schema": release.SCHEMA,
        "runtime_mode": "preserve_installed",
        "runtime_commit": commit,
        "runtime_files": {},
        "runtime_sha256": None,
        "capability_release_id": release_id,
        "capability_manifest_sha256": release.sha256(capability_root / "manifest.json"),
        "capability_files": release.file_inventory(capability_root),
        "capability_sha256": release.archive_tree(capability_root, capability_archive),
        "provider": "p",
        "model": "m",
        "reasoning_effort": "r",
        "capability_declared_runtime_commit": commit,
        "repository_guard": {"mode": "preserve_installed", "runtime_commit": commit},
    }
    (bundle / "release.json").write_text(json.dumps(release_json))
    return bundle, capability_root


def installed_release_fixture(tmp_path: Path, *, runtime_commit: str) -> tuple[Path, Path, Path, Path]:
    """Seed the existing runtime/capability pointers required by ``apply``."""
    root, home = tmp_path / "root", tmp_path / "home"
    installed_runtime = root / "runtime/releases/installed"
    old_capability = root / "capability/releases/old"
    old_home_capability = home / "runtime/capabilities/christopher-tgg/releases/old"
    runtime(installed_runtime, runtime_commit)
    capability(old_capability, "old")
    capability(old_home_capability, "old-home")
    (old_home_capability / "plugins/tgg").mkdir(parents=True)
    release.replace_pointer(root / "runtime/current", installed_runtime)
    release.replace_pointer(root / "capability/current", old_capability)
    release.replace_pointer(home / "runtime/capabilities/christopher-tgg/current", old_home_capability)
    release.replace_pointer(home / "plugins/tgg", old_home_capability / "plugins/tgg")
    return root, home, installed_runtime, old_capability


def rollback_security_fixture(tmp_path: Path) -> tuple[dict, object, dict[str, str | None]]:
    root, home = tmp_path / "root", tmp_path / "home"
    old_runtime = root / "runtime/releases/old"
    active_runtime = root / "runtime/releases/active"
    home_releases = home / "runtime/capabilities/christopher-tgg/releases"
    old_capability = home_releases / "r140"
    active_capability = home_releases / "active-opt"
    old_home_capability = home_releases / "r148"
    active_home_capability = home_releases / "active-home"
    runtime(old_runtime, "old")
    runtime(active_runtime, "active")
    capability(old_capability, "r140")
    capability(active_capability, "active")
    capability(old_home_capability, "r148")
    capability(active_home_capability, "active")
    for target in (old_home_capability, active_home_capability):
        (target / "plugins/tgg").mkdir(parents=True)
    release.replace_pointer(root / "runtime/current", active_runtime)
    release.replace_pointer(root / "capability/current", active_capability)
    release.replace_pointer(
        home / "runtime/capabilities/christopher-tgg/current", active_home_capability
    )
    release.replace_pointer(home / "plugins/tgg", active_home_capability / "plugins/tgg")
    (home / "config.yaml").write_text("pa:\n  enabled: false\n")
    unit = tmp_path / "christopher.service"
    unit.write_text("[Service]\n# active\n")
    receipt = {
        "schema": release.SCHEMA,
        "status": "committed",
        "before": {
            "runtime": str(old_runtime),
            "capability": str(old_capability),
            "home_capability": str(old_home_capability),
            "plugins": {"tgg": str(old_home_capability / "plugins/tgg")},
        },
    }
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps(receipt))
    args = type("A", (), {
        "receipt": str(receipt_path),
        "root": str(root),
        "hermes_home": str(home),
        "systemd_unit": str(unit),
    })()
    snapshot = {
        "runtime": release.pointer_target(root / "runtime/current"),
        "capability": release.pointer_target(root / "capability/current"),
        "home_capability": release.pointer_target(
            home / "runtime/capabilities/christopher-tgg/current"
        ),
        "plugin": release.pointer_target(home / "plugins/tgg"),
        "unit": unit.read_text(),
    }
    return receipt, args, snapshot


def test_prepare_builds_separately_hashed_payloads(tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    rt, cap, bundle = tmp_path / "runtime", tmp_path / "capability", tmp_path / "bundle"
    runtime(rt); capability(cap)
    monkeypatch.setattr(release, "verify_prepare_repository", lambda _runtime, **_kwargs: repository_guard("abc1234"))
    assert release.main(["prepare", "--runtime", str(rt), "--runtime-manifest", str(runtime_manifest(rt)), "--capability", str(cap), "--out", str(bundle), "--provider", "openai-codex", "--model", "x", "--reasoning-effort", "medium"]) == 0
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["runtime_commit"] == "abc1234"
    assert emitted["capability_release_id"] == "r1"
    assert release.sha256(bundle / "runtime.tgz") == emitted["runtime_sha256"]
    assert release.sha256(bundle / "capability.tgz") == emitted["capability_sha256"]


def test_prepare_break_glass_builds_audited_payload_but_does_not_authorize_apply(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, main = "b" * 40, "a" * 40
    rt, cap, bundle = tmp_path / "runtime", tmp_path / "capability", tmp_path / "bundle"
    runtime(rt, candidate)
    capability(cap)
    manifest = json.loads((cap / "manifest.json").read_text())
    manifest["runtime"]["hermes_commit"] = candidate
    (cap / "manifest.json").write_text(json.dumps(manifest))
    audit = {
        "break_glass": True,
        "reason": "scoped TGG continuation repair",
        "actor": "release-owner",
        "canonical_repository_url": release.CANONICAL_REPOSITORY_URL,
        "protected_ref": release.PROTECTED_MAIN_REF,
        "runtime_commit": candidate,
        "observed_protected_main_head": main,
        "observed_at": "2026-09-15T05:00:00+00:00",
        "repository_reconciliation_required": True,
    }
    observed: dict[str, object] = {}

    def verify(_runtime, **kwargs):
        observed.update(kwargs)
        return audit

    monkeypatch.setattr(release, "verify_prepare_repository", verify)
    assert release.main([
        "prepare", "--runtime", str(rt), "--runtime-manifest", str(runtime_manifest(rt)),
        "--capability", str(cap), "--out", str(bundle), "--provider", "openai-codex",
        "--model", "gpt-5.6-terra", "--reasoning-effort", "medium",
        "--break-glass", "--reason", "scoped TGG continuation repair",
    ]) == 0
    emitted = json.loads(capsys.readouterr().out)
    assert observed == {
        "break_glass": True,
        "reason": "scoped TGG continuation repair",
    }
    assert emitted["runtime_commit"] == candidate
    assert emitted["repository_guard"] == audit
    assert (bundle / "runtime.tgz").is_file()
    monkeypatch.setattr(release, "resolve_protected_main_head", lambda: main)
    with pytest.raises(release.ReleaseError, match="protected main changed"):
        release.verify_apply_repository(
            emitted, break_glass=False, reason=None,
        )


def test_prepare_preserve_installed_runtime_skips_runtime_payload_and_pins_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    commit = "a" * 40
    cap, bundle = tmp_path / "capability", tmp_path / "bundle"
    capability(cap, "capability-only")
    manifest = json.loads((cap / "manifest.json").read_text())
    manifest["runtime"]["hermes_commit"] = commit
    (cap / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(
        release,
        "verify_prepare_repository",
        lambda _runtime: (_ for _ in ()).throw(AssertionError("must not inspect/archive runtime")),
    )

    assert release.main([
        "prepare", "--preserve-installed-runtime", "--capability", str(cap), "--out", str(bundle),
        "--provider", "p", "--model", "m", "--reasoning-effort", "r",
    ]) == 0

    emitted = json.loads(capsys.readouterr().out)
    assert emitted["runtime_mode"] == "preserve_installed"
    assert emitted["runtime_commit"] == commit
    assert emitted["capability_declared_runtime_commit"] == commit
    assert emitted["runtime_files"] == {}
    assert emitted["runtime_sha256"] is None
    assert not (bundle / "runtime.tgz").exists()
    assert (bundle / "capability.tgz").is_file()


def test_extract_rejects_modified_payload(tmp_path: Path) -> None:
    payload = tmp_path / "payload"; runtime(payload)
    archive = tmp_path / "payload.tgz"
    expected = release.archive_tree(payload, archive)
    archive.write_bytes(archive.read_bytes() + b"tamper")
    with pytest.raises(release.ReleaseError, match="hash mismatch"):
        release.extract_verified(archive, expected, tmp_path / "out")


def test_capability_accepts_only_verified_sha256sums_sidecar(tmp_path: Path) -> None:
    cap = tmp_path / "cap"; capability(cap)
    manifest = json.loads((cap / "manifest.json").read_text())
    (cap / "SHA256SUMS").write_text(f"{manifest['files']['plugin.py']}  plugin.py\n{release.sha256(cap / 'manifest.json')}  manifest.json\n")
    assert release.required_capability_files(cap, manifest) == manifest["files"]


def test_capability_runtime_prefix_must_match() -> None:
    assert release.declared_runtime_compatibility({"runtime": {"hermes_commit": "a9cb3d0af7"}}, "a9cb3d0af7bbbb") == "a9cb3d0af7"
    with pytest.raises(release.ReleaseError, match="mismatch"):
        release.declared_runtime_compatibility({"runtime": {"hermes_commit": "deadbee"}}, "a9cb3d0af7bbbb")


def test_prepare_repository_requires_clean_exact_fresh_main(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def checked(argv, *, cwd=None):
        calls.append(argv)
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]:
            return "true"
        if argv[:2] == ["git", "status"]:
            return ""
        if argv[-1] == "HEAD":
            return "a" * 40
        if argv[1] == "fetch":
            return ""
        if argv[-1] == "FETCH_HEAD":
            return "a" * 40
        raise AssertionError(argv)

    monkeypatch.setattr(release, "_checked_output", checked)
    evidence = release.verify_prepare_repository(tmp_path)
    assert evidence["verified_main_head"] == "a" * 40
    assert evidence["runtime_commit"] == "a" * 40
    fetch = next(argv for argv in calls if argv[1] == "fetch")
    assert release.CANONICAL_REPOSITORY_URL in fetch
    assert release.PROTECTED_MAIN_REF in fetch


@pytest.mark.parametrize(("runtime_head", "main_head"), [("b" * 40, "a" * 40), ("9" * 40, "a" * 40)])
def test_prepare_repository_refuses_unmerged_or_stale_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, runtime_head: str, main_head: str) -> None:
    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return ""
        if argv[-1] == "HEAD": return runtime_head
        if argv[1] == "fetch": return ""
        if argv[-1] == "FETCH_HEAD": return main_head
        raise AssertionError(argv)
    monkeypatch.setattr(release, "_checked_output", checked)
    with pytest.raises(release.ReleaseError, match="not the freshly verified"):
        release.verify_prepare_repository(tmp_path)


def test_prepare_repository_refuses_dirty_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return " M tools/pa_business_tools.py"
        raise AssertionError(argv)
    monkeypatch.setattr(release, "_checked_output", checked)
    with pytest.raises(release.ReleaseError, match="not clean"):
        release.verify_prepare_repository(tmp_path)


def test_prepare_repository_break_glass_records_candidate_main_and_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate, main = "b" * 40, "a" * 40

    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return ""
        if argv[-1] == "HEAD": return candidate
        if argv[1] == "fetch": return ""
        if argv[-1] == "FETCH_HEAD": return main
        raise AssertionError(argv)

    monkeypatch.setattr(release, "_checked_output", checked)
    monkeypatch.setenv("SUDO_USER", "release-owner")
    result = release.verify_prepare_repository(
        tmp_path, break_glass=True, reason="scoped TGG continuation repair",
    )
    assert result == {
        "break_glass": True,
        "reason": "scoped TGG continuation repair",
        "actor": "release-owner",
        "canonical_repository_url": release.CANONICAL_REPOSITORY_URL,
        "protected_ref": release.PROTECTED_MAIN_REF,
        "runtime_commit": candidate,
        "observed_protected_main_head": main,
        "observed_at": result["observed_at"],
        "repository_reconciliation_required": True,
    }


def test_prepare_repository_break_glass_still_refuses_dirty_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return " M gateway/run.py"
        raise AssertionError(argv)

    monkeypatch.setattr(release, "_checked_output", checked)
    with pytest.raises(release.ReleaseError, match="not clean"):
        release.verify_prepare_repository(
            tmp_path, break_glass=True, reason="scoped TGG continuation repair",
        )


def test_prepare_repository_break_glass_requires_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return ""
        if argv[-1] == "HEAD": return "b" * 40
        if argv[1] == "fetch": return ""
        if argv[-1] == "FETCH_HEAD": return "a" * 40
        raise AssertionError(argv)

    monkeypatch.setattr(release, "_checked_output", checked)
    with pytest.raises(release.ReleaseError, match="non-empty"):
        release.verify_prepare_repository(tmp_path, break_glass=True, reason="  ")


def test_prepare_repository_remote_failure_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def checked(argv, *, cwd=None):
        if argv[:3] == ["git", "rev-parse", "--is-inside-work-tree"]: return "true"
        if argv[:2] == ["git", "status"]: return ""
        if argv[-1] == "HEAD": return "a" * 40
        if argv[1] == "fetch": raise release.ReleaseError("repository verification failed")
        raise AssertionError(argv)
    monkeypatch.setattr(release, "_checked_output", checked)
    with pytest.raises(release.ReleaseError, match="verification failed"):
        release.verify_prepare_repository(tmp_path)


def test_apply_repository_requires_bundle_runtime_and_current_main_match(monkeypatch: pytest.MonkeyPatch) -> None:
    commit = "a" * 40
    bundle = {"runtime_commit": commit, "repository_guard": repository_guard(commit)}
    monkeypatch.setattr(release, "resolve_protected_main_head", lambda: commit)
    result = release.verify_apply_repository(bundle, break_glass=False, reason=None)
    assert result["break_glass"] is False
    assert result["observed_protected_main_head"] == commit


def test_apply_repository_refuses_main_advancing_after_prepare(monkeypatch: pytest.MonkeyPatch) -> None:
    commit = "a" * 40
    monkeypatch.setattr(release, "resolve_protected_main_head", lambda: "b" * 40)
    with pytest.raises(release.ReleaseError, match="protected main changed"):
        release.verify_apply_repository(
            {"runtime_commit": commit, "repository_guard": repository_guard(commit)},
            break_glass=False,
            reason=None,
        )


def test_bundle_cannot_replace_host_pinned_repository(monkeypatch: pytest.MonkeyPatch) -> None:
    commit = "a" * 40
    guard = repository_guard(commit)
    guard["canonical_repository_url"] = "https://attacker.invalid/repo.git"
    monkeypatch.setattr(release, "resolve_protected_main_head", lambda: commit)
    with pytest.raises(release.ReleaseError, match="host-pinned"):
        release.verify_apply_repository(
            {"runtime_commit": commit, "repository_guard": guard},
            break_glass=False,
            reason=None,
        )


def test_apply_repository_remote_failure_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        release,
        "resolve_protected_main_head",
        lambda: (_ for _ in ()).throw(release.ReleaseError("lookup failed")),
    )
    with pytest.raises(release.ReleaseError, match="lookup failed"):
        release.verify_apply_repository(
            {"runtime_commit": "a" * 40, "repository_guard": repository_guard("a" * 40)},
            break_glass=False,
            reason=None,
        )


def test_break_glass_requires_reason_and_records_audit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(release, "resolve_protected_main_head", lambda: "b" * 40)
    monkeypatch.setattr(release.getpass, "getuser", lambda: "root")
    with pytest.raises(release.ReleaseError, match="non-empty"):
        release.verify_apply_repository(
            {"runtime_commit": "a" * 40}, break_glass=True, reason="  "
        )
    result = release.verify_apply_repository(
        {"runtime_commit": "a" * 40},
        break_glass=True,
        reason="urgent runtime recovery",
    )
    assert result == {
        "break_glass": True,
        "reason": "urgent runtime recovery",
        "actor": "root",
        "runtime_commit": "a" * 40,
        "observed_protected_main_head": "b" * 40,
        "observed_at": result["observed_at"],
        "repository_reconciliation_required": True,
    }


def test_receipt_rollback_is_exempt_from_fresh_main_lookup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, home = tmp_path / "root", tmp_path / "home"
    old_runtime = root / "runtime/releases/old"
    home_releases = home / "runtime/capabilities/christopher-tgg/releases"
    old_capability = home_releases / "r140"
    old_home_capability = home_releases / "r148"
    runtime(old_runtime, "old")
    capability(old_capability, "r140")
    capability(old_home_capability, "r148")
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text("pa:\n  enabled: false\n")
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({
        "schema": release.SCHEMA,
        "status": "committed",
        "before": {
            "runtime": str(old_runtime),
            "capability": str(old_capability),
            "home_capability": str(old_home_capability),
            "plugins": {},
        }
    }))
    monkeypatch.setattr(
        release,
        "resolve_protected_main_head",
        lambda: (_ for _ in ()).throw(AssertionError("rollback must not resolve main")),
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(release, "command", lambda argv: calls.append(argv) or "")
    unit_path = tmp_path / "christopher.service"
    args = type("A", (), {
        "receipt": str(receipt),
        "root": str(root),
        "hermes_home": str(home),
        "systemd_unit": str(unit_path),
    })()

    assert release.rollback(args) == 0
    assert release.pointer_target(root / "runtime/current") == str(old_runtime)
    assert release.pointer_target(root / "capability/current") == str(old_capability)
    assert release.pointer_target(
        home / "runtime/capabilities/christopher-tgg/current"
    ) == str(old_home_capability)
    assert ["systemctl", "restart", release.SERVICE] in calls


def test_receipt_rollback_requires_independent_home_capability_target(
    tmp_path: Path,
) -> None:
    receipt, args, _snapshot = rollback_security_fixture(tmp_path)
    receipt["before"].pop("home_capability")
    Path(args.receipt).write_text(json.dumps(receipt))

    with pytest.raises(release.ReleaseError, match="home capability rollback target"):
        release.rollback(args)


@pytest.mark.parametrize(
    "case",
    [
        "schema",
        "status",
        "runtime-type",
        "runtime-outside",
        "capability-outside",
        "home-capability-outside",
        "plugins-shape",
        "plugin-key",
        "plugin-outside",
    ],
)
def test_untrusted_rollback_receipt_refuses_before_any_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    receipt, args, snapshot = rollback_security_fixture(tmp_path)
    before = receipt["before"]
    outside = tmp_path / "outside" / case
    outside.mkdir(parents=True)
    if case == "schema":
        receipt["schema"] = "wrong/v1"
    elif case == "status":
        receipt["status"] = "rolled_back"
    elif case == "runtime-type":
        before["runtime"] = 42
    elif case == "runtime-outside":
        before["runtime"] = str(outside)
    elif case == "capability-outside":
        before["capability"] = str(outside)
    elif case == "home-capability-outside":
        before["home_capability"] = str(outside)
    elif case == "plugins-shape":
        before["plugins"] = []
    elif case == "plugin-key":
        before["plugins"] = {"../tgg": None}
    elif case == "plugin-outside":
        before["plugins"] = {"tgg": str(outside)}
    Path(args.receipt).write_text(json.dumps(receipt))
    service_calls: list[list[str]] = []
    monkeypatch.setattr(
        release, "command", lambda argv: service_calls.append(argv) or ""
    )

    with pytest.raises(release.ReleaseError):
        release.rollback(args)

    root, home = Path(args.root), Path(args.hermes_home)
    assert release.pointer_target(root / "runtime/current") == snapshot["runtime"]
    assert release.pointer_target(root / "capability/current") == snapshot["capability"]
    assert release.pointer_target(
        home / "runtime/capabilities/christopher-tgg/current"
    ) == snapshot["home_capability"]
    assert release.pointer_target(home / "plugins/tgg") == snapshot["plugin"]
    assert Path(args.systemd_unit).read_text() == snapshot["unit"]
    assert service_calls == []
    assert not (root / "release-activity.lock").exists()


def test_exclusive_release_lock_refuses_shared_consumer_lock(tmp_path: Path) -> None:
    activity = tmp_path / "activity.lock"
    child = subprocess.Popen([sys.executable, "-c", "import fcntl,sys,time; f=open(sys.argv[1],'a+'); fcntl.flock(f,fcntl.LOCK_SH); print('locked', flush=True); time.sleep(5)", str(activity)], stdout=subprocess.PIPE, text=True)
    assert child.stdout and child.stdout.readline().strip() == "locked"
    with pytest.raises(release.ReleaseError, match="processing"):
        with release.ExclusiveActivityLock(activity):
            pass
    child.terminate(); child.wait(timeout=2)


def test_processing_rows_reads_only_processing(tmp_path: Path) -> None:
    import sqlite3
    inbox = tmp_path / "inbox.db"
    with sqlite3.connect(inbox) as db:
        db.execute("CREATE TABLE ingress_events(status TEXT)")
        db.executemany("INSERT INTO ingress_events VALUES(?)", [("pending",), ("processing",), ("processing",)])
    assert release.processing_rows(inbox) == 2


def test_systemctl_status_accepts_inactive_exit_three(monkeypatch: pytest.MonkeyPatch) -> None:
    class Result:
        def __init__(self, state, code): self.stdout, self.returncode = state + "\n", code
    monkeypatch.setattr(release.subprocess, "run", lambda argv, **kwargs: Result("inactive", 3) if "is-active" in argv else Result("linked", 0))
    assert release.systemctl_status("nightly.timer", "is-active") == {"state": "inactive", "returncode": 3}


def test_control_state_preserves_an_active_nightly_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    (home / "runtime").mkdir(parents=True)
    (home / "config.yaml").write_text("pa:\n  enabled: true\n")
    (home / "runtime/processing-gate.json").write_text('{"enabled":true}\n')
    monkeypatch.setattr(
        release, "systemctl_status",
        lambda _unit, verb: {"state": "active", "returncode": 0}
        if verb == "is-active" else {"state": "enabled", "returncode": 0},
    )
    controls = release.control_state(home)
    assert controls["timer_active"] == {"state": "active", "returncode": 0}
    assert controls["timer_enabled"] == {"state": "enabled", "returncode": 0}


def test_control_state_refuses_a_failed_nightly_timer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    home = tmp_path / "home"
    (home / "runtime").mkdir(parents=True)
    (home / "config.yaml").write_text("pa:\n  enabled: true\n")
    (home / "runtime/processing-gate.json").write_text('{"enabled":true}\n')
    monkeypatch.setattr(
        release, "systemctl_status",
        lambda _unit, verb: {"state": "failed", "returncode": 3}
        if verb == "is-active" else {"state": "enabled", "returncode": 0},
    )
    with pytest.raises(release.ReleaseError, match="timer baseline is failed"):
        release.control_state(home)


def test_restart_clears_start_limit_before_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(release, "command", lambda argv: calls.append(argv) or "")
    release.restart_service()
    assert calls == [["systemctl", "reset-failed", release.SERVICE], ["systemctl", "restart", release.SERVICE]]


def test_host_config_gate_and_timer_must_all_remain_fixed() -> None:
    before = {
        "config_sha256": "old", "gate_sha256": "gate", "gate_enabled": True,
        "timer_active": {"state": "inactive", "returncode": 3},
        "timer_enabled": {"state": "linked", "returncode": 0},
    }
    after = {**before, "config_sha256": "new"}
    assert not release.operational_controls_unchanged(before, after)
    after = dict(before)
    after["gate_enabled"] = False
    assert not release.operational_controls_unchanged(before, after)


def test_focused_verify_rejects_host_config_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, home = tmp_path / "root", tmp_path / "home"
    runtime_release = root / "runtime/releases/abcdef0"
    capability_release = root / "capability/releases/newcap"
    runtime(runtime_release, "abcdef0")
    capability(capability_release, "newcap")
    release.replace_pointer(root / "runtime/current", runtime_release)
    release.replace_pointer(root / "capability/current", capability_release)
    home.mkdir(parents=True)
    (home / "config.yaml").write_text("pa:\n  enabled: false\n")
    before = {
        "config_sha256": "before-config",
        "gate_sha256": "gate",
        "gate_enabled": False,
        "timer_active": {"state": "inactive", "returncode": 3},
        "timer_enabled": {"state": "linked", "returncode": 0},
    }
    after = {**before, "config_sha256": "mutated-config"}
    monkeypatch.setattr(release, "control_state", lambda _home: after)
    monkeypatch.setattr(release, "command", lambda _argv: "active")
    expected = {
        "runtime_commit": "abcdef0",
        "capability_release_id": "newcap",
        "provider": "provider",
        "model": "model",
        "reasoning_effort": "medium",
    }

    with pytest.raises(release.ReleaseError, match="changed host config"):
        release.focused_verify(root, home, expected, before)


def test_release_tree_is_made_read_only_without_losing_execute_bits(tmp_path: Path) -> None:
    root = tmp_path / "release"
    script = root / "bin" / "run"
    script.parent.mkdir(parents=True)
    script.write_text("#!/bin/sh\n")
    script.chmod(0o755)
    data = root / "config.json"
    data.write_text("{}\n")
    data.chmod(0o644)

    release.make_immutable_tree(root)

    assert script.stat().st_mode & 0o777 == 0o555
    assert data.stat().st_mode & 0o777 == 0o444
    assert root.stat().st_mode & 0o777 == 0o555


def test_plugin_pointer_directory_repairs_root_only_mode(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    plugins = home / "plugins"
    plugins.mkdir(mode=0o600)

    repaired = release.ensure_plugin_pointer_directory(home)

    assert repaired == {
        "path": str(plugins),
        "uid": home.stat().st_uid,
        "gid": home.stat().st_gid,
        "mode": "0o750",
    }
    assert release.verify_plugin_pointer_directory(home) == repaired


def test_vision_receipt_tree_repairs_root_only_entries(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    receipts = tmp_path / "systems/vision-receipts"
    digest = receipts / ("a" * 64)
    digest.mkdir(parents=True, mode=0o700)
    receipt = digest / "receipt.json"
    receipt.write_text("{}\n")
    receipts.chmod(0o700)
    receipt.chmod(0o600)
    (home / "config.yaml").write_text(
        "pa:\n  vision_inspection_receipts:\n    enabled: true\n"
        f"    receipt_root: {receipts}\n"
    )

    repaired = release.ensure_vision_receipt_tree(home)

    assert repaired == {
        "enabled": True,
        "path": str(receipts),
        "uid": home.stat().st_uid,
        "gid": home.stat().st_gid,
        "directory_mode": "0o750",
        "file_mode": "0o640",
        "directories": 2,
        "files": 1,
    }
    assert release.verify_vision_receipt_tree(home) == repaired
    assert digest.stat().st_mode & 0o777 == 0o750
    assert receipt.stat().st_mode & 0o777 == 0o640


def test_vision_receipt_tree_rejects_symlink_entries(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    (receipts / "escape").symlink_to(tmp_path)
    (home / "config.yaml").write_text(
        "pa:\n  vision_inspection_receipts:\n    enabled: true\n"
        f"    receipt_root: {receipts}\n"
    )

    with pytest.raises(release.ReleaseError, match="unsafe entry"):
        release.ensure_vision_receipt_tree(home)


def test_apply_flips_all_pointers_and_rolls_back_on_verify_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root, home = tmp_path / "root", tmp_path / "home"
    old_runtime = root / "runtime/releases/old"
    home_releases = home / "runtime/capabilities/christopher-tgg/releases"
    old_cap, old_home_cap = home_releases / "r140", home_releases / "r148"
    runtime(old_runtime, "old"); capability(old_cap, "oldcap")
    capability(old_home_cap, "r148")
    (old_cap / "plugins/tgg").mkdir(parents=True)
    (old_home_cap / "plugins/tgg").mkdir(parents=True)
    root.joinpath("runtime").mkdir(parents=True, exist_ok=True); root.joinpath("capability").mkdir(parents=True, exist_ok=True)
    release.replace_pointer(root / "runtime/current", old_runtime); release.replace_pointer(root / "capability/current", old_cap)
    release.replace_pointer(home / "runtime/capabilities/christopher-tgg/current", old_home_cap)
    release.replace_pointer(home / "plugins/tgg", old_home_cap / "plugins/tgg")
    (home / "runtime").mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text("pa:\n  enabled: false\n")
    (home / "runtime/processing-gate.json").write_text('{"enabled": false}')
    rt, cap, bundle = tmp_path / "rt", tmp_path / "cap", tmp_path / "bundle"
    runtime(rt, "abcdef0"); capability(cap, "newcap"); (cap / "plugins/tgg").mkdir(parents=True)
    (cap / "plugins/new-plugin").mkdir(parents=True)
    # Plugin content must be represented in the release manifest.
    (cap / "plugins/tgg/plugin.py").write_text("x=2\n")
    (cap / "plugins/new-plugin/plugin.py").write_text("x=3\n")
    manifest = json.loads((cap / "manifest.json").read_text()); manifest["runtime"]["hermes_commit"] = "abcdef0"; manifest["files"]["plugins/tgg/plugin.py"] = release.sha256(cap / "plugins/tgg/plugin.py"); manifest["files"]["plugins/new-plugin/plugin.py"] = release.sha256(cap / "plugins/new-plugin/plugin.py"); (cap / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(release, "verify_prepare_repository", lambda _runtime, **_kwargs: repository_guard("abcdef0"))
    assert release.main(["prepare", "--runtime", str(rt), "--runtime-manifest", str(runtime_manifest(rt)), "--capability", str(cap), "--out", str(bundle), "--provider", "p", "--model", "m", "--reasoning-effort", "r"]) == 0
    monkeypatch.setattr(release, "verify_apply_repository", lambda *_args, **_kwargs: {"break_glass": False})
    monkeypatch.setattr(release, "command", lambda argv: "inactive" if "is-active" in argv else "disabled")
    class Result:
        def __init__(self, state, code): self.stdout, self.returncode = state + "\n", code
    monkeypatch.setattr(release.subprocess, "run", lambda argv, **kwargs: Result("inactive", 3) if "is-active" in argv else Result("linked", 0))
    observed: dict[str, str | None] = {}
    def fail_verify(current_root, current_home, *_args):
        observed["runtime"] = release.pointer_target(current_root / "runtime/current")
        observed["plugin"] = release.pointer_target(current_home / "plugins/tgg")
        observed["unit"] = unit_path.read_text()
        raise release.ReleaseError("bad verify")
    monkeypatch.setattr(release, "focused_verify", fail_verify)
    unit_path = tmp_path / "christopher.service"; unit_path.write_text("[Service]\n# old\n")
    args = type("A", (), {"bundle": str(bundle), "root": str(root), "hermes_home": str(home), "systemd_unit": str(unit_path)})()
    with pytest.raises(release.ReleaseError, match="bad verify"):
        release.apply(args)
    assert release.pointer_target(root / "runtime/current") == str(old_runtime)
    assert release.pointer_target(root / "capability/current") == str(old_cap)
    assert release.pointer_target(
        home / "runtime/capabilities/christopher-tgg/current"
    ) == str(old_home_cap)
    assert release.pointer_target(home / "plugins/tgg") == str(old_home_cap / "plugins/tgg")
    assert not (home / "plugins/new-plugin").exists()
    assert unit_path.read_text() == "[Service]\n# old\n"
    approved = home / "runtime/capabilities/christopher-tgg/releases/newcap"
    assert observed == {"runtime": str(root / "runtime/releases/abcdef0"), "plugin": str(approved / "plugins/tgg"), "unit": "[Service]\n# abcdef0\n"}
    assert str(approved).startswith(str(home / "runtime/capabilities/christopher-tgg/releases"))
    rollback_receipt = next((root / "transactions").rglob("receipt.json"))
    before = json.loads(rollback_receipt.read_text())["before"]
    assert before["capability"] == str(old_cap)
    assert before["home_capability"] == str(old_home_cap)


def test_apply_preserve_installed_runtime_refuses_mismatched_runtime_without_replacing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed_commit, declared_commit = "b" * 40, "a" * 40
    root, home, installed_runtime, old_capability = installed_release_fixture(
        tmp_path, runtime_commit=installed_commit
    )
    bundle, _capability_root = preserve_installed_bundle(tmp_path, commit=declared_commit)
    unit_path = tmp_path / "christopher.service"
    unit_path.write_text("[Service]\n# installed\n")
    monkeypatch.setattr(release, "verify_apply_repository", lambda *_args, **_kwargs: {"mode": "preserve_installed"})
    monkeypatch.setattr(
        release, "ensure_plugin_pointer_directory",
        lambda _home: (_ for _ in ()).throw(AssertionError("must not normalize plugins before identity proof")),
    )
    monkeypatch.setattr(
        release, "ensure_vision_receipt_tree",
        lambda _home: (_ for _ in ()).throw(AssertionError("must not normalize vision receipts before identity proof")),
    )
    monkeypatch.setattr(release, "control_state", lambda _home: {"controls": "unchanged"})
    monkeypatch.setattr(release, "restart_service", lambda: (_ for _ in ()).throw(AssertionError("must not restart")))

    args = type("A", (), {
        "bundle": str(bundle), "root": str(root), "hermes_home": str(home),
        "systemd_unit": str(unit_path),
    })()
    with pytest.raises(release.ReleaseError, match="installed runtime identity"):
        release.apply(args)

    assert release.pointer_target(root / "runtime/current") == str(installed_runtime)
    assert release.pointer_target(root / "capability/current") == str(old_capability)
    assert unit_path.read_text() == "[Service]\n# installed\n"


def test_apply_preserve_installed_runtime_keeps_runtime_pointer_and_unit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commit = "a" * 40
    root, home, installed_runtime, _old_capability = installed_release_fixture(
        tmp_path, runtime_commit=commit
    )
    bundle, _capability_root = preserve_installed_bundle(tmp_path, commit=commit)
    unit_path = tmp_path / "christopher.service"
    unit_path.write_text("[Service]\n# installed\n")
    commands: list[list[str]] = []
    monkeypatch.setattr(release, "verify_apply_repository", lambda *_args, **_kwargs: {"mode": "preserve_installed"})
    monkeypatch.setattr(release, "ensure_plugin_pointer_directory", lambda _home: {})
    monkeypatch.setattr(release, "ensure_vision_receipt_tree", lambda _home: {"enabled": False})
    monkeypatch.setattr(release, "control_state", lambda _home: {"controls": "unchanged"})
    monkeypatch.setattr(release, "command", lambda argv: commands.append(argv) or "")
    monkeypatch.setattr(release, "restart_service", lambda: commands.append(["restart"]))
    monkeypatch.setattr(
        release, "focused_verify", lambda *_args: {"service": "active", "runtime_commit": commit}
    )

    args = type("A", (), {
        "bundle": str(bundle), "root": str(root), "hermes_home": str(home),
        "systemd_unit": str(unit_path),
    })()
    assert release.apply(args) == 0

    assert release.pointer_target(root / "runtime/current") == str(installed_runtime)
    assert unit_path.read_text() == "[Service]\n# installed\n"
    assert ["systemctl", "daemon-reload"] in commands
    assert ["restart"] in commands
    assert release.pointer_target(root / "capability/current") == str(
        home / "runtime/capabilities/christopher-tgg/releases/preserve"
    )
