"""Contract tests for the anti-drift plane manifest path grammar."""

from __future__ import annotations

import importlib.util
from pathlib import Path


_REPO = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "plane_lint", _REPO / "scripts" / "plane_lint.py"
)
assert _SPEC and _SPEC.loader
_PLANE_LINT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_PLANE_LINT)


def test_wildcard_client_directory_owns_full_subtree() -> None:
    assert _PLANE_LINT.under("deploy/acme/agent/config.yaml", ["deploy/*/"])
    assert not _PLANE_LINT.under("deploy/shared_tool.py", ["deploy/*/"])


def test_root_manifest_pattern_does_not_match_nested_manifests() -> None:
    assert _PLANE_LINT.under("runtime.manifest.json", ["*.manifest.json"])
    assert not _PLANE_LINT.under(
        "deploy/acme/runtime.manifest.json", ["*.manifest.json"]
    )


def test_client_plane_wins_when_shared_parent_is_scanned(tmp_path: Path) -> None:
    (tmp_path / "deploy" / "acme").mkdir(parents=True)
    (tmp_path / "deploy" / "shared_tool.py").write_text("shared\n")
    (tmp_path / "deploy" / "acme" / "config.py").write_text("client\n")
    manifest = {
        "sharedPlanePaths": ["deploy/"],
        "clientPlanePaths": ["deploy/*/"],
    }

    assert list(_PLANE_LINT.iter_shared_files(str(tmp_path), manifest)) == [
        "deploy/shared_tool.py"
    ]
