#!/usr/bin/env python3
"""plane_lint.py — anti-drift plane lint (PA de-fusion Phase 0.2, L2 mechanical gate).

Enforces the plane boundary declared in ``plane-manifest.json`` at the repo
root. Two deterministic checks, zero judgment, zero recall:

  (a) import-direction — shared-plane source files must not import
      client-plane packages/modules. Python imports are parsed with ``ast``
      for ``.py`` files; ES import/export/require specifiers are matched for
      ``.ts/.tsx/.js/.jsx/.mjs/.cjs`` files (relative paths and manifest
      ``importAliases`` such as ``@/`` are resolved repo-relative). Files
      listed in ``loaderSeamExceptions`` are exempt from this check only —
      that is the single designed seam through which the platform may load
      client packs.

  (b) client-token — client tokens from ``clientTokenRegistry`` must not
      appear in shared-plane file contents or file paths.

Token matching rule (documented per spec):
  * Case-insensitive occurrence scan, then boundary validation:
  * "Word-ish" boundaries: an occurrence counts when each alphanumeric end
    of the token is bounded by (i) a non-``[A-Za-z0-9]`` character or
    string start/end — underscore, dash, slash, dot all count as
    boundaries — OR (ii) a camelCase transition (the character inside the
    match is lowercase and the adjacent character outside is uppercase, or
    vice versa on the leading side). So ``tgg`` matches ``tgg_case_search``,
    ``TGG_DEMO``, ``deploy/tgg/``, ``tggView`` and ``registerTggRoutes``;
    ``hdb`` matches ``hdb_confirmed``; ``ilinked`` matches
    ``iLinkedReconciliation`` — but ``mtu`` does NOT match ``mtual`` or
    ``azimuth``-class embeddings, and ``hdb`` does NOT match ``shdbx``.
    (Plain ``\\b`` would treat ``_`` as a word char and miss the ``tgg_*``
    identifier class that dominates the audit, so boundaries are
    letter/digit-only on purpose.)
  * Known residual: an all-caps token immediately followed by another
    all-caps run (``MTUSIZE``) has no detectable boundary and is missed;
    the audit corpus contains no such form.
  * A token whose first/last character is itself non-alphanumeric anchors
    only on its alphanumeric side(s): ``bor_`` is a prefix token (matches
    ``bor_tables``); ``SK/JOB`` matches inside ``SK/JOB/2604/2376``.

Plane classification: a file is client-plane if it sits under any
``clientPlanePaths`` entry; otherwise it is shared-plane if it sits under
any ``sharedPlanePaths`` entry (client wins — this is how "``deploy/``
excluding ``deploy/tgg/``" is expressed). Files under neither plane are not
scanned.

Modes:
  --write-baseline   Write ``plane-lint-baseline.json`` capturing ALL
                     current violations, keyed stably by file+check+detail
                     (no line numbers — keys survive unrelated edits).
  (default)          WARN mode: print violations NOT in the baseline,
                     always exit 0.
  --strict           Exit 1 when any non-baselined violation exists.

Stdlib only, python3. The same script runs unmodified in every repo that
carries a ``plane-manifest.json``.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

BASELINE_NAME = "plane-lint-baseline.json"
MANIFEST_NAME = "plane-manifest.json"

SKIP_DIR_NAMES = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "build",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "website",
    "optional-skills",
}

ES_EXTENSIONS = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}
PY_EXTENSIONS = {".py"}

ES_IMPORT_RES = [
    # import x from '...'; import {a} from "..."; export {a} from '...'
    re.compile(r"""(?:^|\s)(?:import|export)\s+[^;'"]*?\sfrom\s+['"]([^'"]+)['"]"""),
    # side-effect import: import '...'
    re.compile(r"""(?:^|\s)import\s+['"]([^'"]+)['"]"""),
    # dynamic import('...') / require('...')
    re.compile(r"""(?:import|require)\s*\(\s*['"]([^'"]+)['"]"""),
]


def norm(path: str) -> str:
    return path.replace(os.sep, "/")


def load_manifest(root: str) -> dict:
    manifest_path = os.path.join(root, MANIFEST_NAME)
    if not os.path.isfile(manifest_path):
        sys.stderr.write(f"plane-lint: no {MANIFEST_NAME} at {root}\n")
        sys.exit(2)
    with open(manifest_path, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    for field in ("sharedPlanePaths", "clientPlanePaths", "clientTokenRegistry"):
        if field not in manifest:
            sys.stderr.write(f"plane-lint: manifest missing required field {field!r}\n")
            sys.exit(2)
    # Manifest self-verification: declared paths must exist (a path moving
    # without a manifest update is a loud config error, not a silent pass).
    missing = []
    for entry in manifest["sharedPlanePaths"]:
        # Wildcards declare a class of paths (for example repo-root manifests)
        # and may legitimately have zero current members. Literal paths must exist.
        if any(ch in entry for ch in "*?["):
            continue
        if not os.path.exists(os.path.join(root, entry.rstrip("/"))):
            missing.append(entry)
    if missing:
        sys.stderr.write(
            "plane-lint: manifest sharedPlanePaths do not exist on disk: "
            + ", ".join(missing)
            + "\n"
        )
        sys.exit(2)
    return manifest


def under(rel: str, prefixes: list[str]) -> bool:
    """True if repo-relative file path `rel` sits under any prefix entry.

    Entries ending with '/' are directories; others match as exact file or
    directory prefix.
    """
    for prefix in prefixes:
        directory_pattern = norm(prefix).endswith("/")
        p = norm(prefix).rstrip("/")
        if any(ch in p for ch in "*?["):
            # A wildcard basename without a slash is a repo-root file pattern.
            if "/" not in p:
                if "/" not in rel and fnmatch.fnmatchcase(rel, p):
                    return True
                continue
            # Directory patterns own the matched directory and its full subtree.
            if directory_pattern and fnmatch.fnmatchcase(rel, p + "/**"):
                return True
            if not directory_pattern and fnmatch.fnmatchcase(rel, p):
                return True
            continue
        if rel == p or rel.startswith(p + "/"):
            return True
    return False


def is_binary(path: str) -> bool:
    try:
        with open(path, "rb") as fh:
            chunk = fh.read(8192)
    except OSError:
        return True
    if b"\x00" in chunk:
        return True
    try:
        chunk.decode("utf-8")
    except UnicodeDecodeError:
        # Not valid utf-8 in the first chunk — treat as binary-ish, skip.
        return True
    return False


def iter_shared_files(root: str, manifest: dict):
    shared = manifest["sharedPlanePaths"]
    client = manifest["clientPlanePaths"]
    seen = set()
    for entry in shared:
        raw_entry = entry.rstrip("/")
        if any(ch in raw_entry for ch in "*?["):
            candidates = sorted(glob.glob(os.path.join(root, raw_entry)))
        else:
            candidates = [os.path.join(root, raw_entry)]
        for abs_entry in candidates:
            if os.path.isfile(abs_entry):
                rel = norm(os.path.relpath(abs_entry, root))
                if rel not in seen and not under(rel, client):
                    seen.add(rel)
                    yield rel
                continue
            for dirpath, dirnames, filenames in os.walk(abs_entry):
                dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIR_NAMES)
                for name in sorted(filenames):
                    abs_path = os.path.join(dirpath, name)
                    rel = norm(os.path.relpath(abs_path, root))
                    if rel in seen or under(rel, client):
                        continue
                    seen.add(rel)
                    yield rel


def token_occurrences(text: str, token: str, regex: re.Pattern) -> list[int]:
    """Boundary-validated match start offsets of `token` in `text`.

    See the module docstring for the word-ish + camelCase boundary rule.
    """
    starts = []
    for m in re.finditer(regex, text):
        s, e = m.start(), m.end()
        ok_lead = True
        if token[0].isalnum() and s > 0:
            prev, first = text[s - 1], text[s]
            ok_lead = (not prev.isalnum()) or (prev.islower() and first.isupper())
        ok_trail = True
        if token[-1].isalnum() and e < len(text):
            nxt, last = text[e], text[e - 1]
            ok_trail = (not nxt.isalnum()) or (last.islower() and nxt.isupper())
        if ok_lead and ok_trail:
            starts.append(s)
    return starts


def build_token_matchers(manifest: dict):
    matchers = []
    for client, tokens in sorted(manifest["clientTokenRegistry"].items()):
        for token in tokens:
            matchers.append((client, token, re.compile(re.escape(token), re.IGNORECASE)))
    return matchers


def client_module_prefixes(manifest: dict) -> list[str]:
    """Client-plane path patterns as Python module patterns."""
    prefixes = []
    for entry in manifest["clientPlanePaths"]:
        mod = entry.strip("/").replace("/", ".")
        if mod:
            prefixes.append(mod)
    return prefixes


def python_imported_modules(rel: str, text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    mods = []
    pkg_parts = norm(os.path.dirname(rel)).split("/") if os.path.dirname(rel) else []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import — resolve against file location
                base = pkg_parts[: len(pkg_parts) - (node.level - 1)]
                mod = ".".join(base + ([node.module] if node.module else []))
                if mod:
                    mods.append(mod)
                # `from . import sibling` — the siblings are the modules
                if not node.module:
                    mods.extend(
                        ".".join(base + [alias.name]) for alias in node.names
                    )
            elif node.module:
                mods.append(node.module)
    return mods


def resolve_es_specifier(rel: str, spec: str, aliases: dict) -> str | None:
    """Resolve an ES import specifier to a repo-relative path, or None if external."""
    for alias, target in aliases.items():
        if spec == alias.rstrip("/"):
            return norm(target.rstrip("/"))
        if spec.startswith(alias):
            return norm(os.path.normpath(target + spec[len(alias):]))
    if spec.startswith("."):
        return norm(os.path.normpath(os.path.join(os.path.dirname(rel), spec)))
    return None  # bare package specifier — external


def es_import_specifiers(text: str) -> list[str]:
    specs = []
    for regex in ES_IMPORT_RES:
        specs.extend(regex.findall(text))
    return specs


def collect_violations(root: str, manifest: dict) -> list[dict]:
    violations = []
    matchers = build_token_matchers(manifest)
    seam_exceptions = set(manifest.get("loaderSeamExceptions", []))
    aliases = manifest.get("importAliases", {})
    py_client_prefixes = client_module_prefixes(manifest)
    client_paths = manifest["clientPlanePaths"]

    for rel in iter_shared_files(root, manifest):
        abs_path = os.path.join(root, rel)
        if is_binary(abs_path):
            continue
        with open(abs_path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()

        # (b) client-token — contents and file path, one violation per file+token
        for client, token, regex in matchers:
            in_path = bool(token_occurrences(rel, token, regex))
            content_hits = token_occurrences(text, token, regex)
            if not in_path and not content_hits:
                continue
            where = []
            if in_path:
                where.append("path")
            if content_hits:
                first_line = text.count("\n", 0, content_hits[0]) + 1
                where.append(f"content x{len(content_hits)}, first at line {first_line}")
            violations.append(
                {
                    "file": rel,
                    "check": "client-token",
                    "detail": f"{client}:{token}",
                    "note": "; ".join(where),
                }
            )

        # (a) import-direction
        if rel in seam_exceptions:
            continue
        ext = os.path.splitext(rel)[1]
        if ext in PY_EXTENSIONS:
            for mod in python_imported_modules(rel, text):
                for prefix in py_client_prefixes:
                    if (
                        mod == prefix
                        or mod.startswith(prefix + ".")
                        or fnmatch.fnmatchcase(mod, prefix)
                        or fnmatch.fnmatchcase(mod, prefix + ".**")
                    ):
                        violations.append(
                            {
                                "file": rel,
                                "check": "import-direction",
                                "detail": mod,
                                "note": "python import of client-plane module",
                            }
                        )
                        break
        elif ext in ES_EXTENSIONS:
            for spec in es_import_specifiers(text):
                resolved = resolve_es_specifier(rel, spec, aliases)
                if resolved is not None and under(resolved, client_paths):
                    violations.append(
                        {
                            "file": rel,
                            "check": "import-direction",
                            "detail": spec,
                            "note": f"resolves to {resolved}",
                        }
                    )

    # de-duplicate on stable key, keep first note
    dedup = {}
    for v in violations:
        dedup.setdefault(violation_key(v), v)
    return [dedup[k] for k in sorted(dedup)]


def violation_key(v: dict) -> str:
    return f"{v['file']}::{v['check']}::{v['detail']}"


def load_baseline(root: str) -> dict:
    path = os.path.join(root, BASELINE_NAME)
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh).get("entries", {})


def write_baseline(root: str, violations: list[dict]) -> None:
    entries = {
        violation_key(v): {"note": v["note"]} for v in violations
    }
    payload = {
        "tool": "plane_lint.py",
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(entries),
        "entries": {k: entries[k] for k in sorted(entries)},
    }
    path = os.path.join(root, BASELINE_NAME)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    print(f"plane-lint: wrote baseline with {len(entries)} entries -> {norm(os.path.relpath(path))}")


def summarize(violations: list[dict]) -> dict:
    counts = {}
    for v in violations:
        counts[v["check"]] = counts.get(v["check"], 0) + 1
    return counts


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=None, help="repo root (default: script's parent's parent, else cwd)")
    parser.add_argument("--write-baseline", action="store_true", help="capture all current violations as the baseline")
    parser.add_argument("--strict", action="store_true", help="exit 1 on non-baselined violations")
    args = parser.parse_args(argv)

    if args.root:
        root = os.path.abspath(args.root)
    else:
        script_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        root = script_root if os.path.isfile(os.path.join(script_root, MANIFEST_NAME)) else os.getcwd()

    manifest = load_manifest(root)
    violations = collect_violations(root, manifest)

    if args.write_baseline:
        write_baseline(root, violations)
        counts = summarize(violations)
        for check, n in sorted(counts.items()):
            print(f"  {check}: {n}")
        return 0

    baseline = load_baseline(root)
    new = [v for v in violations if violation_key(v) not in baseline]
    suppressed = len(violations) - len(new)
    stale = sorted(set(baseline) - {violation_key(v) for v in violations})

    if new:
        print(f"plane-lint: {len(new)} violation(s) NOT in baseline:")
        for v in new:
            print(f"  {v['file']} :: {v['check']} :: {v['detail']}  ({v['note']})")
    counts = summarize(violations)
    print(
        "plane-lint: total {t} (".format(t=len(violations))
        + ", ".join(f"{c}={n}" for c, n in sorted(counts.items()))
        + f"), suppressed by baseline: {suppressed}, new: {len(new)}"
    )
    if stale:
        print(f"plane-lint: {len(stale)} baseline entr(y/ies) no longer fire (burn-down candidates)")
    if new and args.strict:
        return 1
    if not new:
        print("plane-lint: OK" + (" (strict)" if args.strict else " (warn mode)"))
    elif not args.strict:
        print("plane-lint: WARN mode — exiting 0 despite new violations")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
