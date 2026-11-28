"""Tests for the local-only PA-129 ACL cutover transaction helper."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import pwd
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).parents[2]
    / "deploy"
    / "tgg"
    / "christopher"
    / "scripts"
    / "sandbox_reader_acl.py"
)
SPEC = importlib.util.spec_from_file_location("sandbox_reader_acl", SCRIPT)
assert SPEC and SPEC.loader
cutover = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cutover)

APPLY_ENGINE_SLOT = SCRIPT.with_name("apply_engine_slot.py")
APPLY_SPEC = importlib.util.spec_from_file_location("apply_engine_slot", APPLY_ENGINE_SLOT)
assert APPLY_SPEC and APPLY_SPEC.loader
apply_engine_slot = importlib.util.module_from_spec(APPLY_SPEC)
APPLY_SPEC.loader.exec_module(apply_engine_slot)


def test_runtime_manifest_carries_reader_module_and_cutover_helper():
    manifest = json.loads(
        (
            Path(__file__).parents[2]
            / "deploy"
            / "tgg"
            / "christopher"
            / "pa-agent.hermes.manifest.json"
        ).read_text(encoding="utf-8")
    )


@pytest.mark.skipif(
    platform.system() != "Linux"
    or os.geteuid() != 0
    or shutil.which("setfacl") is None,
    reason="requires root Linux ACL fixture",
)
def test_home_acl_preserves_actual_root_atomic_copy_for_service_group(tmp_path):
    producer = pwd.getpwnam("pclaw")
    reader = pwd.getpwnam("tggcapture")
    assert producer.pw_uid == 996 and producer.pw_gid == 987
    assert reader.pw_uid == 999 and reader.pw_gid == 988

    os.chown(tmp_path, producer.pw_uid, producer.pw_gid)
    os.chmod(tmp_path, 0o711)
    home = tmp_path / "home"
    home.mkdir()
    os.chown(home, producer.pw_uid, producer.pw_gid)
    source = tmp_path / "config-source.yaml"
    source.write_text(
        "python_sandbox:\n  reader_identity: tggcapture\n",
        encoding="utf-8",
    )

    cutover.preserve_reader_home_access(home, str(reader.pw_uid))
    target = home / "config.yaml"
    apply_engine_slot._atomic_copy(
        source,
        target,
        mode=0o644,
        uid=0,
        gid=producer.pw_gid,
    )

    def run_as(account, code):
        def drop_identity():
            os.setgroups([account.pw_gid])
            os.setgid(account.pw_gid)
            os.setuid(account.pw_uid)

        return subprocess.run(
            [str(Path(sys.executable)), "-c", code],
            check=False,
            capture_output=True,
            text=True,
            env={
                "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
                "PYTHONPATH": str(Path(__file__).parents[2]),
                "HERMES_HOME": str(home),
            },
            preexec_fn=drop_identity,
        )

    producer_result = run_as(
        producer,
        "from hermes_cli.config import ensure_hermes_home; "
        "ensure_hermes_home(); "
        "open(__import__('os').environ['HERMES_HOME'] + '/config.yaml').read()",
    )
    assert producer_result.returncode == 0, producer_result.stderr
    reader_result = run_as(
        reader,
        "open(__import__('os').environ['HERMES_HOME'] + '/config.yaml').read()",
    )
    assert reader_result.returncode != 0
    assert "PermissionError" in reader_result.stderr
    assert "tools/python_sandbox_access.py" in manifest["include"]
    assert (
        "deploy/tgg/christopher/scripts/sandbox_reader_acl.py"
        in manifest["include"]
    )


def test_scope_refuses_top_level_symlink(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (home / "linked").symlink_to(outside)

    with pytest.raises(cutover.CutoverError, match="symlinks"):
        cutover._scope(home)


def test_apply_denies_existing_private_siblings_before_home_traversal(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    workspace_root = home / "sandbox_workspaces"
    workspace = workspace_root / ("s_" + "a" * 64)
    work = workspace / "work"
    work.mkdir(parents=True)
    private = home / "config.yaml"
    private.write_text("private", encoding="utf-8")
    root_private = workspace_root / "private.db"
    owner_private = workspace / "metadata.json"
    root_private.write_text("private", encoding="utf-8")
    owner_private.write_text("private", encoding="utf-8")
    payload = {"acl_sha256": "digest", "reader_uid": "999"}
    events = []
    monkeypatch.setattr(cutover, "_load_receipt", lambda _path: payload)
    monkeypatch.setattr(
        cutover,
        "_validate_receipt_scope",
        lambda _payload, **_kwargs: (
            home,
            "999",
            [private, workspace_root],
            workspace_root,
        ),
    )
    monkeypatch.setattr(cutover, "_snapshot", lambda _home: ("acl", []))
    monkeypatch.setattr(
        cutover.hashlib,
        "sha256",
        lambda _value: type("Digest", (), {"hexdigest": lambda self: "digest"})(),
    )
    monkeypatch.setattr(cutover, "_executable", lambda _name: "/usr/bin/setfacl")
    monkeypatch.setattr(
        cutover,
        "_run",
        lambda command, **_kwargs: events.append(("command", command)) or "",
    )
    monkeypatch.setattr(
        cutover,
        "preserve_reader_home_access",
        lambda *_args: events.append(("home", None)),
    )
    monkeypatch.setattr(
        cutover,
        "grant_reader_work_access",
        lambda *_args: events.append(("work", None)),
    )
    monkeypatch.setattr(
        cutover,
        "preflight_reader_work_access",
        lambda *_args: events.append(("preflight", None)),
    )

    assert cutover.apply(argparse.Namespace(receipt=str(tmp_path / "receipt"))) == 0

    assert events[0][0] == "preflight"
    assert events[1][0] == "command"
    assert str(private) in events[1][1]
    assert str(root_private) in events[1][1]
    assert str(owner_private) in events[1][1]
    assert events[2][0] == "home"
    assert events[3][0] == "work"


def test_prepare_writes_owner_only_receipt(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    receipt = tmp_path / "receipts" / "acl.json"
    monkeypatch.setattr(cutover, "_reader_uid", lambda _value: "999")
    monkeypatch.setattr(cutover, "_snapshot", lambda _home: ("acl\n", []))

    assert cutover.prepare(
        argparse.Namespace(home=str(home), reader="tggcapture", receipt=str(receipt))
    ) == 0
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600


def test_apply_restores_preimage_when_work_grant_fails(tmp_path, monkeypatch):
    home = tmp_path / "home"
    work = home / "sandbox_workspaces" / ("s_" + "a" * 64) / "work"
    work.mkdir(parents=True)
    payload = {"acl_sha256": "digest", "reader_uid": "999"}
    events = []
    monkeypatch.setattr(cutover, "_load_receipt", lambda _path: payload)
    monkeypatch.setattr(
        cutover,
        "_validate_receipt_scope",
        lambda _payload, **_kwargs: (
            home,
            "999",
            [home / "sandbox_workspaces"],
            home / "sandbox_workspaces",
        ),
    )
    monkeypatch.setattr(cutover, "_snapshot", lambda _home: ("acl", []))
    monkeypatch.setattr(
        cutover.hashlib,
        "sha256",
        lambda _value: type("Digest", (), {"hexdigest": lambda self: "digest"})(),
    )
    monkeypatch.setattr(cutover, "_executable", lambda _name: "/usr/bin/setfacl")
    monkeypatch.setattr(cutover, "_run", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(cutover, "preflight_reader_work_access", lambda *_args: None)
    monkeypatch.setattr(cutover, "preserve_reader_home_access", lambda *_args: None)
    monkeypatch.setattr(
        cutover,
        "grant_reader_work_access",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("setfacl failed")),
    )
    monkeypatch.setattr(
        cutover,
        "_restore_preimage",
        lambda _payload: events.append("restored") or [],
    )

    with pytest.raises(cutover.CutoverError, match="preimage restored"):
        cutover.apply(argparse.Namespace(receipt=str(tmp_path / "receipt")))
    assert events == ["restored"]


def test_rollback_accepts_new_paths_and_reports_deleted_preimage(
    tmp_path, monkeypatch, capsys
):
    home = tmp_path / "home"
    work = home / "sandbox_workspaces" / ("s_" + "a" * 64) / "work"
    work.mkdir(parents=True)
    new_private = home / "new-private.db"
    new_private.write_bytes(b"private")
    new_work = work / "new.xlsx"
    new_work.write_bytes(b"new")
    old_missing = str(work / "old.xlsx")
    payload = {"home": str(home), "reader_uid": "999"}
    removed = []
    monkeypatch.setattr(cutover, "_load_receipt", lambda _path: payload)

    def validate(_payload, **kwargs):
        assert kwargs == {"require_unchanged_children": False}
        return home, "999", [], home / "sandbox_workspaces"

    monkeypatch.setattr(cutover, "_validate_receipt_scope", validate)
    monkeypatch.setattr(
        cutover,
        "_remove_current_reader_entries",
        lambda actual_home, uid: removed.append((actual_home, uid)),
    )
    monkeypatch.setattr(cutover, "_restore_preimage", lambda _payload: [old_missing])

    assert cutover.rollback(argparse.Namespace(receipt=str(tmp_path / "receipt"))) == 0
    assert removed == [(home, "999")]
    output = json.loads(capsys.readouterr().out)
    assert output["missing_preimage_paths_skipped"] == [old_missing]

    current_paths, _directories = cutover._current_policy_paths(home)
    assert new_private in current_paths
    assert new_work in current_paths
