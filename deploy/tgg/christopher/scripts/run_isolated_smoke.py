#!/usr/bin/env python3
"""Run a consumer-layer Christopher turn in a fixture-only Hermes home."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import threading
import time
import uuid
from io import BytesIO
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import yaml
from dotenv import load_dotenv
from openpyxl import Workbook


ALLOWED_SECRET_KEYS = {
    "OPENAI_API_KEY",
}


class _OperatorStub(BaseHTTPRequestHandler):
    requests_total = 0
    mutation_requests = 0
    report_scenario = "none"
    request_paths: list[str] = []
    workbooks: dict[str, bytes] = {}
    case_db: Path | None = None
    case_queries: list[dict[str, object]] = []

    def _read_json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    @classmethod
    def _run_case_query(cls, sql: str) -> dict[str, object]:
        if cls.case_db is None:
            raise RuntimeError("case query requested without --case-db")
        normalized = sql.strip()
        first_word = normalized.split(None, 1)[0].upper() if normalized else ""
        forbidden = (
            "PRAGMA",
            "ATTACH",
            "DETACH",
            "INSERT",
            "UPDATE",
            "DELETE",
            "REPLACE",
            "CREATE",
            "ALTER",
            "DROP",
            "VACUUM",
        )
        upper = normalized.upper()
        if first_word not in {"SELECT", "WITH"} or any(
            re.search(rf"\b{token}\b", upper) for token in forbidden
        ):
            raise ValueError("only read-only SELECT or WITH queries are allowed")
        started = time.monotonic()
        uri = f"file:{cls.case_db.as_posix()}?mode=ro&immutable=1"
        with sqlite3.connect(uri, uri=True) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.execute(normalized)
            columns = [item[0] for item in (cursor.description or [])]
            fetched = cursor.fetchmany(201)
        truncated = len(fetched) > 200
        rows = [dict(row) for row in fetched[:200]]
        receipt = {
            "sql_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
            "row_count": len(rows),
            "truncated": truncated,
        }
        cls.case_queries.append(receipt)
        return {
            "ok": True,
            "data": {
                "columns": columns,
                "rows": rows,
                "rowCount": len(rows),
                "truncated": truncated,
                "elapsedMs": round((time.monotonic() - started) * 1000, 3),
            },
        }

    @staticmethod
    def _workbook(tabs: list[str]) -> bytes:
        buffer = BytesIO()
        workbook = Workbook()
        workbook.remove(workbook.active)
        for tab in tabs:
            sheet = workbook.create_sheet(tab)
            sheet.append(["Job No.", "Status", "Remarks"])
            sheet.append(["AM/JOB/2608/0001", "Completed", "Fixture row"])
        workbook.save(buffer)
        return buffer.getvalue()

    @classmethod
    def _cached_workbook(cls, file_name: str, tabs: list[str]) -> bytes:
        if file_name not in cls.workbooks:
            cls.workbooks[file_name] = cls._workbook(tabs)
        return cls.workbooks[file_name]

    def _reply(self) -> None:
        type(self).requests_total += 1
        type(self).request_paths.append(self.path)
        request_path = urlsplit(self.path).path
        if self.command in {"POST", "PATCH", "PUT", "DELETE"} and request_path != "/api/operator/query":
            type(self).mutation_requests += 1
        if request_path == "/api/operator/query" and self.command == "POST":
            try:
                payload = self._read_json_body()
                body = json.dumps(
                    self._run_case_query(str(payload.get("sql") or ""))
                ).encode()
            except Exception as exc:
                body = json.dumps({"ok": False, "error": str(exc)}).encode()
        elif self.path.startswith("/api/operator/report-cycle/"):
            report_workbook = self._cached_workbook("report.xlsx", ["Report"])
            digest = hashlib.sha256(report_workbook).hexdigest()
            zones = ("AMK", "HG", "PG", "SK")
            endpoint = urlsplit(self.path).path.rsplit("/", 1)[-1]
            if endpoint == "fetch-sources":
                master_tabs = (
                    [
                        "AMK",
                        "HG",
                        "PG",
                        "SK",
                    ]
                    if self.report_scenario == "clean"
                    else ["AMK"]
                )
                source_specs = (
                    (
                        ("master", "master.xlsx", master_tabs),
                        ("closure", "closure.xlsx", ["3 August until 8 August 26"]),
                    )
                    if self.report_scenario == "clean"
                    else (("master", "master.xlsx", master_tabs),)
                )
                result = {
                    "fetch_id": "fixture-fetch",
                    "sources": [
                        {
                            "name": name,
                            "hash": hashlib.sha256(
                                self._cached_workbook(file_name, sheet_tabs)
                            ).hexdigest(),
                            "bytes": len(self._cached_workbook(file_name, sheet_tabs)),
                            "fetched_at": "2026-08-03T08:00:00+08:00",
                            "sheet_tabs": sheet_tabs,
                            "ref": (
                                f"http://127.0.0.1:{self.server.server_port}"
                                f"/fixture-files/{file_name}"
                            ),
                            "file_name": file_name,
                        }
                        for name, file_name, sheet_tabs in source_specs
                    ],
                    "preview_rows": [{"zone": zone, "row_count": 1} for zone in zones],
                }
            elif endpoint == "preview-reconcile":
                result = {
                    "run_id": "fixture-run",
                    "delta": {
                        "new_cases": 1,
                        "canonical_updates": 2,
                        "source_selection_updates": 4,
                        "work_items_inserted": 0,
                    },
                    "structured": {
                        "casesInBuild": 4,
                        "casesEvaluated": 4,
                        "casesCreated": 1,
                        "casesCanonicalChanged": 2,
                        "casesSourceSelectionChanged": 4,
                        "workItemsInserted": 0,
                    },
                    "warnings": {"structured_identity": []},
                }
            elif endpoint == "apply-reconcile":
                result = {
                    "applied": {
                        "new_cases": 1,
                        "canonical_updates": 2,
                        "source_selection_updates": 4,
                        "work_items_inserted": 0,
                    },
                    "backup": {"path": "/fixture/backup", "hash": digest, "verified": True},
                    "audit_batch_id": "fixture-audit",
                }
            elif endpoint == "generate":
                reports = []
                for zone in zones:
                    payload = self._cached_workbook(f"{zone}.xlsx", [zone])
                    reports.append({"zone": zone, "ref": f"fixture-{zone}", "hash": hashlib.sha256(payload).hexdigest()})
                result = {"run_id": "fixture-run", "verdict": "pass", "checks": {"verifier": "pass"}, "reports": reports}
            elif endpoint == "get-reports":
                files = []
                for zone in zones:
                    payload = self._cached_workbook(f"{zone}.xlsx", [zone])
                    files.append({"zone": zone, "ref": f"http://127.0.0.1:{self.server.server_port}/fixture-files/{zone}.xlsx", "hash": hashlib.sha256(payload).hexdigest(), "file_name": f"{zone}.xlsx"})
                result = {"files": files, "receipt": {"window": "auto", "verifier": "pass"}}
            elif endpoint == "status":
                result = {"stage": "generated", "ok": True, "populations_touched": {"cases": 4}}
            else:
                result = {"error": "unknown report endpoint"}
            body = json.dumps(result).encode()
        else:
            body = json.dumps({"ok": True, "data": [], "cases": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = _reply
    do_POST = _reply
    do_PATCH = _reply
    do_PUT = _reply
    do_DELETE = _reply

    def log_message(self, _format: str, *_args) -> None:
        return

    def do_GET(self):
        if self.path.startswith("/fixture-files/"):
            type(self).requests_total += 1
            type(self).request_paths.append(self.path)
            file_name = Path(urlsplit(self.path).path).name
            body = type(self).workbooks.get(file_name)
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._reply()


def _rewrite_http_urls(value, *, base_url: str):
    if isinstance(value, dict):
        return {
            key: _rewrite_http_urls(item, base_url=base_url)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite_http_urls(item, base_url=base_url) for item in value]
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        original = urlsplit(value)
        base = urlsplit(base_url)
        return urlunsplit((base.scheme, base.netloc, original.path, original.query, ""))
    return value


def _copy_test_env(source: Path, target: Path) -> None:
    lines = []
    for raw in source.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.removeprefix("export ").split("=", 1)[0].strip()
        if key in ALLOWED_SECRET_KEYS:
            lines.append(stripped.removeprefix("export "))
    if not any(line.startswith("OPENAI_API_KEY=") for line in lines):
        raise RuntimeError("live Hermes env has no OPENAI_API_KEY")
    lines.append("CHRISTOPHER_TGG_PS_SERVICE_TOKEN=fixture-only")
    lines.append("TGG_REPORT_OPS_TOKEN=fixture-only")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    target.chmod(stat.S_IRUSR | stat.S_IWUSR)
    # Plugin discovery happens when the Hermes runtime modules are imported
    # below.  Loading the fixture env after that import is too late: plugin
    # availability checks would hide every report tool from the model even
    # though the credential exists on disk.  Export the isolated fixture env
    # first; it contains only the allow-listed provider keys plus a synthetic
    # report token and never touches the live process environment upstream.
    load_dotenv(target, override=True)


def _prepare_home(
    run_root: Path,
    *,
    slot_root: Path,
    live_config: Path,
    live_constitution: Path,
    live_auth: Path,
    live_env: Path,
    live_memory: Path,
    soul_path: Path,
    stub_url: str,
    case_db: Path | None,
) -> Path:
    run_root.mkdir(parents=True, mode=0o700)
    config = yaml.safe_load((slot_root / "config.yaml").read_text(encoding="utf-8"))
    selected = yaml.safe_load(live_config.read_text(encoding="utf-8"))
    config["model"] = selected["model"]
    config["agent"].pop("reasoning_effort", None)
    if "reasoning_effort" in selected.get("agent", {}):
        config["agent"]["reasoning_effort"] = selected["agent"]["reasoning_effort"]
    config["pa"]["enabled"] = True
    config["pa"]["constitution_path"] = str(
        run_root / "christopher_tgg_constitution.yaml"
    )
    config["platforms"]["whatsapp"]["enabled"] = False
    config["platforms"]["whatsapp"]["extra"]["session_path"] = str(
        run_root / "whatsapp-v2" / "session"
    )
    fixture_media = run_root / "media"
    fixture_media.mkdir(parents=True, exist_ok=True)
    config["pa"]["report_operations"]["download_root"] = str(
        fixture_media / "report-runs"
    )
    config["pa"]["report_operations"]["base_url"] = stub_url
    config["pa"]["report_operations"]["allowed_download_hosts"] = [
        f"127.0.0.1:{urlsplit(stub_url).port}"
    ]
    config["pa"]["report_operations"]["allow_insecure_downloads"] = True
    config["pa"]["media_retention"]["media_root"] = str(fixture_media)
    config["pa"]["media_retention"]["source_roots"] = [str(fixture_media)]
    config["python_sandbox"]["media_retention"]["root"] = str(fixture_media)
    config["python_sandbox"]["datasets"]["media"]["path"] = str(fixture_media)
    if case_db is not None:
        config["python_sandbox"]["datasets"]["cases"]["path"] = str(case_db)
    config["pa"]["overlay"]["client"]["business_bridge"] = _rewrite_http_urls(
        config["pa"]["overlay"]["client"]["business_bridge"], base_url=stub_url
    )
    for operation in config["pa"]["overlay"]["client"]["business_bridge"][
        "operations"
    ].values():
        if operation.get("type") != "http":
            continue
        if not str(operation.get("url", "")).startswith(f"{stub_url}/"):
            raise RuntimeError(
                "fixture left a business-bridge HTTP URL outside the stub"
            )
    (run_root / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )
    # Retain the deployment-verification selectors from the slot fixture, but
    # overlay every runtime choice from the materialized live constitution.
    # Copying the live constitution wholesale omits the synthetic verification
    # chat; copying the slot unchanged silently switches a Codex probe to the
    # direct provider.
    constitution = yaml.safe_load(
        (slot_root / "christopher_tgg_constitution.yaml").read_text(
            encoding="utf-8"
        )
    )
    selected_constitution = yaml.safe_load(
        live_constitution.read_text(encoding="utf-8")
    )
    constitution["runtime"] = selected_constitution["runtime"]
    selected_briefs = selected_constitution.get("job_briefs", {})
    for name, brief in constitution.get("job_briefs", {}).items():
        selected_brief = selected_briefs.get(name, {})
        if "runtime" in selected_brief:
            brief["runtime"] = selected_brief["runtime"]
    (run_root / "christopher_tgg_constitution.yaml").write_text(
        yaml.safe_dump(constitution, sort_keys=False), encoding="utf-8"
    )
    shutil.copyfile(soul_path, run_root / "SOUL.md")
    if not live_memory.is_file() or not live_memory.read_text(encoding="utf-8").strip():
        raise RuntimeError(f"live Hermes memory is missing or empty: {live_memory}")
    memory_dir = run_root / "memories"
    memory_dir.mkdir(mode=0o700)
    shutil.copyfile(live_memory, memory_dir / "MEMORY.md")
    (memory_dir / "MEMORY.md").chmod(0o600)
    plugin_source = soul_path.parent / "plugins" / "report-operations"
    shutil.copytree(plugin_source, run_root / "plugins" / "report-operations")
    _copy_test_env(live_env, run_root / ".env")
    if live_auth.is_file():
        shutil.copyfile(live_auth, run_root / "auth.json")
        (run_root / "auth.json").chmod(0o600)
    return run_root / "config.yaml"


def _verify_memory_in_session_jsonl(run_root: Path, live_memory: Path) -> Path:
    """Prove a fresh gateway transcript carried live durable memory."""
    memory_text = live_memory.read_text(encoding="utf-8")
    probe = next(
        (
            line.strip()
            for line in memory_text.splitlines()
            if len(line.strip()) >= 24 and not line.lstrip().startswith("#")
        ),
        memory_text[:128].strip(),
    )
    if not probe:
        raise RuntimeError("live Hermes memory has no usable verification probe")
    session_files = sorted((run_root / "sessions").glob("*.jsonl"))
    if len(session_files) != 1:
        raise RuntimeError(f"expected one fresh gateway session JSONL, found {len(session_files)}")
    rows = [json.loads(line) for line in session_files[0].read_text(encoding="utf-8").splitlines() if line.strip()]
    prompts = [row.get("system_prompt", "") for row in rows if row.get("role") == "session_meta"]
    if len(prompts) != 1 or probe not in prompts[0]:
        raise RuntimeError("fresh gateway session JSONL omitted live MEMORY.md content")
    return session_files[0]


def _retain_runs(root: Path, *, keep: int, current: Path) -> None:
    candidates = sorted(
        (
            path
            for path in root.iterdir()
            if path.is_dir() and path.name.startswith("smoke-") and path != current
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates[max(0, keep - 1) :]:
        # The prefix and parent checks are the deletion manifest. Nothing
        # outside the smoke-run root can enter this cleanup.
        if path.parent == root and path.name.startswith("smoke-"):
            shutil.rmtree(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-root", required=True)
    parser.add_argument("--live-home", required=True)
    parser.add_argument("--test-root", required=True)
    parser.add_argument("--slot-file", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument(
        "--case-db",
        help=(
            "Optional production-sized SQLite copy. The local operator stub opens "
            "it immutable/read-only and the sandbox receives it as the cases dataset."
        ),
    )
    # Optional fixture overrides. The defaults are the deploy-verification
    # probe (a management chat asked to reply READY); overriding them lets the
    # same isolated rig exercise a real turn shape — e.g. a management Q&A —
    # without touching the live service.
    parser.add_argument("--chat-id", default="120363409954029949@g.us")
    parser.add_argument("--chat-name", default="Christopher Deployment Verification")
    parser.add_argument(
        "--body",
        default="Synthetic deployment verification. Reply with exactly READY.",
    )
    parser.add_argument(
        "--report-ops-scenario",
        choices=("none", "clean", "corrupt"),
        default="none",
        help="Exercise the real report judgment chain against a local API stub.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    app_root = Path(args.app_root).resolve()
    live_home = Path(args.live_home).resolve()
    test_root = Path(args.test_root).resolve()
    slot = Path(args.slot_file).read_text(encoding="utf-8").strip()
    if slot not in {
        "gpt-5.4-mini",
        "gpt-5.6-luna",
        "gpt-5.6-luna-low",
        "gpt-5.6-luna-xhigh",
        "gpt-5.6-terra-medium",
        "gpt-5.6-terra-high",
        "gpt-6-sol-medium",
    }:
        raise RuntimeError(f"invalid engine slot {slot!r}")
    slot_root = app_root / "deploy" / "tgg" / "christopher" / "runtime-slots" / slot
    if not slot_root.is_dir():
        raise RuntimeError(f"missing engine slot directory {slot_root}")

    test_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    test_root.chmod(0o700)
    run_root = test_root / f"smoke-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    fixture_path = run_root / "fixture.jsonl"
    report_path = Path(args.report).resolve()
    if report_path != test_root and test_root not in report_path.parents:
        raise RuntimeError("report path must stay inside test root")
    case_db = Path(args.case_db).resolve() if args.case_db else None
    if case_db is not None and not case_db.is_file():
        raise RuntimeError(f"case database does not exist: {case_db}")

    _OperatorStub.requests_total = 0
    _OperatorStub.mutation_requests = 0
    _OperatorStub.request_paths = []
    _OperatorStub.workbooks = {}
    _OperatorStub.case_db = case_db
    _OperatorStub.case_queries = []
    _OperatorStub.report_scenario = args.report_ops_scenario
    server = ThreadingHTTPServer(("127.0.0.1", 0), _OperatorStub)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        stub_url = f"http://127.0.0.1:{server.server_port}"
        config_path = _prepare_home(
            run_root,
            slot_root=slot_root,
            live_config=live_home / "config.yaml",
            live_constitution=live_home / "christopher_tgg_constitution.yaml",
            live_auth=live_home / "auth.json",
            live_env=live_home / ".env",
            live_memory=live_home / "memories" / "MEMORY.md",
            soul_path=app_root / "deploy" / "tgg" / "christopher" / "SOUL.md",
            stub_url=stub_url,
            case_db=case_db,
        )
        fixture = {
            "messageId": f"synthetic-{uuid.uuid4().hex}",
            "chatId": args.chat_id,
            "senderId": "synthetic-verifier",
            "senderName": "Synthetic Verifier",
            "chatName": args.chat_name,
            "isGroup": True,
            "body": args.body,
            "hasMedia": False,
            "mediaType": None,
            "mediaUrls": [],
            "mentionedIds": [],
            "timestamp": int(time.time()),
            "fromMe": False,
            "historySync": False,
        }
        fixture_path.write_text(json.dumps(fixture) + "\n", encoding="utf-8")

        # HERMES_HOME must be set before importing Hermes runtime modules.
        os.environ["HERMES_HOME"] = str(run_root)
        os.environ["HERMES_QUIET"] = "1"
        from gateway import durable_jsonl_consumer as consumer

        run_id = f"deploy-smoke-{uuid.uuid4().hex[:12]}"
        namespace = argparse.Namespace(
            test_root=str(run_root),
            source=str(fixture_path),
            cursor=str(run_root / "consumer" / "cursor.json"),
            inbox=str(run_root / "consumer" / "inbox.db"),
            config=str(config_path),
            state_db=str(run_root / "state.db"),
            report=str(run_root / "consumer-report.json"),
            run_id=run_id,
            max_records=10,
        )
        rc = asyncio.run(consumer.run_fixture(namespace))
        if rc != 0:
            raise RuntimeError(f"consumer fixture returned {rc}")
        report = json.loads((run_root / "consumer-report.json").read_text())
        memory_session = _verify_memory_in_session_jsonl(
            run_root, live_home / "memories" / "MEMORY.md"
        )
        report["memory_injected"] = True
        report["memory_session_jsonl"] = str(memory_session)
        # Every business-bridge HTTP URL was rewritten to the fixture stub
        # above. Mutation-shaped calls can therefore exercise the constitution
        # without reaching client state; report them separately from real
        # client mutation requests.
        report["client_mutation_requests"] = 0
        report["operator_stub_mutation_attempts"] = _OperatorStub.mutation_requests
        report["operator_stub_requests_total"] = _OperatorStub.requests_total
        report["external_outbound_sent"] = 0
        report["report_ops_scenario"] = args.report_ops_scenario
        report["report_ops_request_paths"] = list(_OperatorStub.request_paths)
        report["case_db"] = str(case_db) if case_db is not None else None
        report["case_db_bytes"] = case_db.stat().st_size if case_db is not None else None
        report["case_queries"] = list(_OperatorStub.case_queries)
        if args.report_ops_scenario == "clean":
            expected = [
                "/api/operator/report-cycle/status?tenant=tgg",
                "/api/operator/report-cycle/fetch-sources?tenant=tgg",
                "/api/operator/report-cycle/preview-reconcile?tenant=tgg",
                "/api/operator/report-cycle/apply-reconcile?tenant=tgg",
                "/api/operator/report-cycle/generate?tenant=tgg",
                "/api/operator/report-cycle/get-reports?tenant=tgg",
            ]
            seen = [path for path in _OperatorStub.request_paths if path.startswith("/api/operator/report-cycle/")]
            if seen != expected:
                raise RuntimeError(f"clean report chain mismatch: {seen}")
            body = json.dumps(report["result"].get("captured_outbound") or [])
            if body.count("MEDIA:") < 4:
                raise RuntimeError("clean report chain did not draft four attachments")
        elif args.report_ops_scenario == "corrupt":
            seen = [path for path in _OperatorStub.request_paths if path.startswith("/api/operator/report-cycle/")]
            if seen != [
                "/api/operator/report-cycle/status?tenant=tgg",
                "/api/operator/report-cycle/fetch-sources?tenant=tgg",
            ]:
                raise RuntimeError(f"corrupt report chain failed to stop after fetch: {seen}")
            captured = report["result"].get("captured_outbound") or []
            drafted = [
                str(item.get("kwargs", {}).get("content") or "").strip()
                for item in captured
                if item.get("kind") == "send"
            ]
            # The judgment fixture proves the agent stopped after inspection;
            # the failure receipt's wording is intentionally model-authored and
            # must not be reduced to a brittle vocabulary allowlist.
            if len(drafted) != 1 or not drafted[0] or "MEDIA:" in drafted[0]:
                raise RuntimeError("corrupt report chain omitted management-chat failure draft")
        report["run_root"] = str(run_root)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, sort_keys=True))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    _retain_runs(test_root, keep=3, current=run_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
