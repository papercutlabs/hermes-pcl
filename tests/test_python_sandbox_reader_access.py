"""Focused contract tests for the optional sandbox work reader policy."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

import tools.python_sandbox_access as access
import tools.python_sandbox_tool as sandbox

WORKSPACE = "s_" + "a" * 64


def _linux_account(monkeypatch, *, uid: int = 999, name: str = "tggcapture") -> None:
    monkeypatch.setattr(access.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        access.pwd,
        "getpwnam",
        lambda value: SimpleNamespace(pw_uid=uid)
        if value == name
        else (_ for _ in ()).throw(KeyError(value)),
    )
    monkeypatch.setattr(
        access.pwd,
        "getpwuid",
        lambda value: SimpleNamespace(pw_uid=uid)
        if value == uid
        else (_ for _ in ()).throw(KeyError(value)),
    )


def test_reader_policy_is_default_off_without_identity(monkeypatch):
    monkeypatch.setattr(
        access.pwd,
        "getpwnam",
        lambda _value: pytest.fail("unset policy must not inspect accounts"),
    )
    assert access.configured_reader_uid({}) is None
    assert access.configured_reader_uid({"reader_identity": None}) is None


@pytest.mark.parametrize(
    "value",
    ["", " tggcapture", "tggcapture ", "capture:rw", "-1", "0999", True, []],
)
def test_reader_identity_validation_refuses_ambiguous_values(monkeypatch, value):
    _linux_account(monkeypatch)
    with pytest.raises(access.SandboxReaderPolicyError):
        access.configured_reader_uid({"reader_identity": value})


def test_reader_identity_resolves_name_or_canonical_uid(monkeypatch):
    _linux_account(monkeypatch)
    assert access.configured_reader_uid({"reader_identity": "tggcapture"}) == "999"
    assert access.configured_reader_uid({"reader_identity": 999}) == "999"
    assert access.configured_reader_uid({"reader_identity": "999"}) == "999"


def test_home_policy_preserves_reader_traversal_and_denies_new_siblings(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    home.mkdir()
    calls = []
    monkeypatch.setattr(access, "_setfacl", lambda *args: calls.append(args))

    access.preserve_reader_home_access(home, "999")

    assert len(calls) == 1
    acl = calls[0][1]
    assert "u:999:--x" in acl
    assert "d:u:999:---" in acl
    assert "d:g::r-x" in acl and "d:m::r-x" in acl
    assert "g::---" in acl and "o::---" in acl
    assert str(home) == calls[0][-1]


def test_work_policy_grants_only_configured_reader_and_covers_nested_files(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    workspace_root = home / "sandbox_workspaces"
    work = workspace_root / WORKSPACE / "work"
    nested = work / "one" / "two"
    nested.mkdir(parents=True)
    root_sibling = workspace_root / "private.db"
    owner_sibling = work.parent / "metadata.json"
    root_sibling.write_bytes(b"private")
    owner_sibling.write_bytes(b"private")
    (work / "old.xlsx").write_bytes(b"old")
    (nested / "new.xlsx").write_bytes(b"new")
    calls = []
    monkeypatch.setattr(access, "_setfacl", lambda *args: calls.append(args))

    access.grant_reader_work_access(home, work, "999")

    rendered = [" ".join(call) for call in calls]
    assert all("998" not in call for call in rendered)
    assert all(str(root_sibling) not in call for call in rendered)
    assert all(str(owner_sibling) not in call for call in rendered)
    ancestor_grants = [
        call
        for call in rendered
        if "u:999:--x,d:u:999:---" in call
    ]
    assert len(ancestor_grants) == 2
    assert any("u:999:r--" in call and "old.xlsx" in call and "new.xlsx" in call for call in rendered)
    defaults = [call for call in rendered if "u:999:r-x,d:u:999:r-x" in call]
    assert defaults and str(nested) in defaults[0]


def test_work_policy_refuses_symlink_escape_before_any_acl_change(tmp_path, monkeypatch):
    home = tmp_path / "home"
    work = home / "sandbox_workspaces" / WORKSPACE / "work"
    work.mkdir(parents=True)
    outside = tmp_path / "private.txt"
    outside.write_text("private", encoding="utf-8")
    (work / "escape").symlink_to(outside)
    calls = []
    monkeypatch.setattr(access, "_setfacl", lambda *args: calls.append(args))

    with pytest.raises(access.SandboxReaderPolicyError, match="symlink"):
        access.grant_reader_work_access(home, work, "999")
    assert calls == []


def test_workspace_publication_applies_policy_before_files_are_visible_across_runs(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    destination = home / "sandbox_workspaces" / WORKSPACE / "work"
    destination.mkdir(parents=True)
    (destination / "old.txt").write_text("retained", encoding="utf-8")
    observed = []

    def record_policy(_home: Path, tree: Path, uid: str) -> None:
        observed.append(
            (
                uid,
                tree.name,
                sorted(path.name for path in tree.rglob("*") if path.is_file()),
                sorted(path.name for path in destination.rglob("*") if path.is_file()),
            )
        )
        assert destination.exists()

    monkeypatch.setattr(sandbox, "grant_reader_work_access", record_policy)
    for index in (1, 2):
        base = sandbox._work_manifest(destination)
        source = tmp_path / f"source-{index}"
        shutil.copytree(destination, source)
        (source / "nested").mkdir(exist_ok=True)
        (source / "nested" / f"file-{index}.txt").write_text(
            f"run-{index}", encoding="utf-8"
        )
        sandbox._publish_workspace(
            source,
            destination,
            base,
            run_id=f"r_0000000{index}",
            hermes_home=home,
            reader_uid="999",
        )

    staged = [entry for entry in observed if entry[1].startswith(".work-")]
    assert [entry[2] for entry in staged] == [["file-1.txt"], ["file-2.txt"]]
    # Each new file is opened while still staged, before it is visible.
    assert "file-1.txt" not in staged[0][3]
    assert "file-2.txt" not in staged[1][3]
    assert all(entry[0] == "999" for entry in observed)
    assert (destination / "old.txt").read_text(encoding="utf-8") == "retained"
    assert (destination / "nested" / "file-1.txt").read_text(encoding="utf-8") == "run-1"
    assert (destination / "nested" / "file-2.txt").read_text(encoding="utf-8") == "run-2"


def test_workspace_publication_acl_failure_keeps_previous_publication(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    destination = home / "sandbox_workspaces" / WORKSPACE / "work"
    destination.mkdir(parents=True)
    (destination / "current.txt").write_text("current", encoding="utf-8")
    base = sandbox._work_manifest(destination)
    source = tmp_path / "source"
    source.mkdir()
    (source / "candidate.txt").write_text("candidate", encoding="utf-8")

    def fail_policy(*_args, **_kwargs):
        raise access.SandboxReaderPolicyError("sandbox reader ACL update failed")

    monkeypatch.setattr(sandbox, "grant_reader_work_access", fail_policy)
    with pytest.raises(access.SandboxReaderPolicyError):
        sandbox._publish_workspace(
            source,
            destination,
            base,
            run_id="r_00000001",
            hermes_home=home,
            reader_uid="999",
        )

    assert (destination / "current.txt").read_text(encoding="utf-8") == "current"
    assert not (destination / "candidate.txt").exists()
    assert not list(destination.parent.glob(".work-*.tmp"))


def test_invalid_reader_config_returns_error_before_workspace_creation(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sandbox,
        "configured_reader_uid",
        lambda _config: (_ for _ in ()).throw(
            access.SandboxReaderPolicyError("reader is invalid")
        ),
    )
    monkeypatch.setattr(sandbox, "get_hermes_home", lambda: tmp_path)

    response = json.loads(
        sandbox._run_python_sandbox(
            "print(1)",
            [],
            None,
            None,
            config={"reader_identity": "bad"},
            session_id="owner",
        )
    )

    assert response == {"status": "error", "error": "reader is invalid", "result": None}
    assert not (tmp_path / "sandbox_runs").exists()
