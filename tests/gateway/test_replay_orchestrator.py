from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from gateway.replay import ReplayAttempt, ReplayPlan, canonical_digest
from gateway.replay_orchestrator import (
    PAReplayOrchestrator,
    ReplayProviderError,
    ReplayRunManifest,
    ReplayRunState,
    ReplayStateError,
    ReplayTargetProviderClient,
    ReplayTargetProviderConfig,
    ReplayVerifyError,
    VerifyGateConfig,
    _sha256_hex_manifest,
    tenant_confirmation_token,
)
from hermes_cli.replay import add_replay_parser, add_replay_run_parser
from hermes_state import SessionDB


class FakeProviderClient:
    def __init__(self):
        self.config = SimpleNamespace(provider_url="http://provider.test", tenant="tgg")
        self.calls = []
        self.verify_ok = True
        self.dirty_calls = []
        self.promote_calls = []
        self.rollback_calls = []


    def prepare(
        self, *, run_id: str, source_data_dir: str, target_data_dir: str, base_url: str
    ):
        self.calls.append((
            "prepare",
            run_id,
            source_data_dir,
            target_data_dir,
            base_url,
        ))
        descriptor = {
            "provider": "systems-pcl",
            "tenantSlug": "tgg",
            "targetKind": "tgg-eval-tenant",
            "targetId": f"tgg-eval-{run_id}",
            "runId": run_id,
            "mode": "eval",
            "baseUrl": base_url,
            "writeScope": ["operator:write"],
            "authRef": {"kind": "file", "ref": f"{target_data_dir}/token"},
            "requiredHeaders": {"X-Replay-Run-Id": run_id, "X-PS-Tenant": "tgg"},
            "targetDataDir": target_data_dir,
            "createdAt": "2026-06-18T00:00:00.000Z",
        }
        baseline = {
            "provider": "systems-pcl",
            "tenantSlug": "tgg",
            "snapshotId": f"snap-{run_id}",
            "snapshotDbPath": f"{target_data_dir}/baseline.db",
            "sourceDataDir": source_data_dir,
            "sourceDbPath": f"{source_data_dir}/tenants/tgg.db",
            "snapshotSha256": "a" * 64,
            "snapshotSizeBytes": 123,
            "createdAt": "2026-06-18T00:00:00.000Z",
            "tableCounts": {"cases": 1},
        }
        return {
            "runId": run_id,
            "targetId": descriptor["targetId"],
            "targetDataDir": target_data_dir,
            "descriptor": descriptor,
            "descriptorDigest": _sha256_hex_manifest(descriptor),
            "baselineManifest": baseline,
            "baselineDigest": _sha256_hex_manifest(baseline),
            "serviceTokenRef": {"kind": "file", "ref": f"{target_data_dir}/token"},
        }

    def verify(self, *, run_id: str, data_dir: str):
        self.calls.append(("verify", run_id, data_dir))
        return {
            "ok": self.verify_ok,
            "runId": run_id,
            "targetId": f"tgg-eval-{run_id}",
            "state": "clean" if self.verify_ok else "dirty",
            "checks": [
                {
                    "name": "target-state-clean",
                    "ok": self.verify_ok,
                    "actual": "clean" if self.verify_ok else "dirty",
                }
            ],
        }

    def mark_dirty(self, *, run_id: str, data_dir: str, reason: str):
        self.dirty_calls.append((run_id, data_dir, reason))
        self.calls.append(("dirty", run_id, data_dir, reason))
        return {"ok": True, "runId": run_id, "state": "dirty", "checks": []}

    def promote(self, *, run_id: str, target_data_dir: str, prod_data_dir: str):
        self.promote_calls.append((run_id, target_data_dir, prod_data_dir))
        self.calls.append(("promote", run_id, target_data_dir, prod_data_dir))
        return {
            "ok": True,
            "runId": run_id,
            "promotionId": f"promo-{run_id}",
            "prodDbPath": f"{prod_data_dir}/tenants/tgg.db",
            "backupDbPath": f"{prod_data_dir}/replay-target-provider/backups/previous.db",
            "promotedSha256": "b" * 64,
            "previousSha256": "c" * 64,
            "manifestPath": f"{prod_data_dir}/replay-target-provider/promotions/promo.json",
        }

    def rollback(self, *, run_id: str, promotion_manifest_path: str):
        self.rollback_calls.append((run_id, promotion_manifest_path))
        self.calls.append(("rollback", run_id, promotion_manifest_path))
        return {
            "ok": True,
            "runId": run_id,
            "promotionId": f"promo-{run_id}",
            "prodDbPath": "/non-prod/tenants/tgg.db",
            "restoredSha256": "c" * 64,
            "manifestPath": promotion_manifest_path,
        }


class FakeRunner:
    def __init__(
        self, db_path, *, outbound=None, tool_result=None, turn_status="completed"
    ):
        self._session_db = SessionDB(db_path=db_path)
        self.outbound = outbound or []
        self.tool_result = tool_result if tool_result is not None else {"ok": True}
        self.turn_status = turn_status

    async def replay(self, plan: ReplayPlan):
        attempt = ReplayAttempt.from_plan(plan)
        self._session_db.record_replay_attempt(**attempt.to_db_kwargs())
        message_ids = [m.get("messageId") for m in plan.messages if m.get("messageId")]
        self._session_db.record_pa_turn(
            turn_id=f"turn-{plan.attempt_id}",
            agent_id="christopher",
            chat_id="120363111@g.us",
            session_id=f"sess-{plan.attempt_id}",
            message_refs=message_ids,
            turn_status=self.turn_status,
            replay_run_id=plan.run_id,
            replay_attempt_id=plan.attempt_id,
            tool_calls=[
                {
                    "tool_name": "tgg_case_upsert",
                    "input": {"jobNo": "SK/JOB/2605/0001"},
                    "result": self.tool_result,
                    "call_id": f"call-{plan.attempt_id}",
                }
            ],
        )
        self._session_db.finish_replay_attempt(
            attempt_id=plan.attempt_id, status="completed"
        )
        execution_report = self._session_db.replay_execution_report(
            run_id=plan.run_id, attempt_id=plan.attempt_id
        )
        attempt_dict = attempt.to_dict()
        attempt_dict["status"] = "completed"
        return {
            "run_id": plan.run_id,
            "attempt_id": plan.attempt_id,
            "platform": plan.platform,
            "processed": len(plan.messages),
            "outbound": self.outbound,
            "blocked_commands": [],
            "delivery_mode": plan.delivery_mode,
            "corpus_report": plan.corpus_manifest.get("report") or {},
            "attempt": attempt_dict,
            "execution_report": execution_report,
        }


def _plan():
    return ReplayPlan(
        platform="whatsapp",
        messages=(
            {
                "messageId": "m1",
                "chatId": "120363111@g.us",
                "body": "first",
                "timestamp": 100,
            },
            {
                "messageId": "m2",
                "chatId": "120363111@g.us",
                "body": "second",
                "timestamp": 101,
            },
        ),
        corpus_manifest={"message_count": 2, "messages_digest": "fixture"},
        code_manifest={"repo": "hermes", "git_commit": "abc123"},
    )


def _prepared_orchestrator(tmp_path, *, runner_factory=None, provider=None):
    provider = provider or FakeProviderClient()
    orch = PAReplayOrchestrator(
        provider_client=provider,
        runner_factory=runner_factory,
        out_dir=tmp_path / "runs",
        run_id="run-test-001",
    )
    orch.prepare_target(
        source_data_dir=str(tmp_path / "source-data"),
        target_data_dir=str(tmp_path / "target-data"),
        target_base_url="http://127.0.0.1:5192",
    )
    return orch, provider



def test_tenant_confirmation_tokens_are_derived_not_tgg_constants():
    assert tenant_confirmation_token("SWAP", "finexis") == "SWAP_FINEXIS_TARGET"
    assert (
        tenant_confirmation_token("ORCHESTRATOR", "client-east")
        == "ORCHESTRATOR_CLIENT_EAST_TARGET"
    )
    with pytest.raises(ValueError, match="tenant is required"):
        tenant_confirmation_token("SWAP", "")


def test_provider_promote_derives_confirmation_from_configured_tenant(monkeypatch):
    client = ReplayTargetProviderClient(
        ReplayTargetProviderConfig(
            provider_url="http://provider.test",
            admin_token="secret",
            tenant="finexis",
        )
    )
    captured = {}

    def fake_request(method, path, *, body=None, query=None):
        captured.update({"method": method, "path": path, "body": body})
        return {"ok": True}

    monkeypatch.setattr(client, "_request", fake_request)
    client.promote(
        run_id="run-finexis",
        target_data_dir="/tmp/target",
        prod_data_dir="/tmp/prod",
    )

    assert captured["body"]["confirm"] == "SWAP_FINEXIS_TARGET"


def test_replay_run_cli_requires_explicit_tenant():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_replay_run_parser(subparsers)

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "replay-run",
                "verify",
                "--manifest",
                "/tmp/run.json",
            ]
        )

    args = parser.parse_args(
        [
            "replay-run",
            "verify",
            "--tenant",
            "finexis",
            "--manifest",
            "/tmp/run.json",
        ]
    )
    assert args.tenant == "finexis"


def test_replay_cli_uses_generic_window_flags_and_explicit_pa_context():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_replay_parser(subparsers)

    args = parser.parse_args(
        [
            "replay",
            "--bridge-message-log",
            "/tmp/bridge.db",
            "--chat-id",
            "group",
            "--since",
            "2026-05-24T00:00:00+08:00",
            "--until",
            "2026-05-25T00:00:00+08:00",
            "--tenant",
            "finexis",
            "--agent-id",
            "mtu",
            "--job-type",
            "advisor_ingest",
        ]
    )
    assert (args.since, args.until) == (
        "2026-05-24T00:00:00+08:00",
        "2026-05-25T00:00:00+08:00",
    )
    assert (args.tenant, args.agent_id, args.job_type) == (
        "finexis",
        "mtu",
        "advisor_ingest",
    )


def test_orchestrator_prepare_run_verify_persists_manifest_and_gate(tmp_path):
    runner = FakeRunner(tmp_path / "state.db")
    orch, provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)

    attempt = orch.run_agent_replay(_plan())
    report = orch.verify(VerifyGateConfig(expected_turn_count=1))

    assert orch.manifest.state is ReplayRunState.VERIFIED
    assert orch.manifest.manifest_path.exists()
    saved = ReplayRunManifest.load(orch.manifest.manifest_path)
    assert saved.run_id == "run-test-001"
    assert saved.target["descriptor_digest"] == _sha256_hex_manifest(
        saved.target["descriptor"]
    )
    assert attempt["result_digest"] == canonical_digest(attempt["result"])
    assert report["ok"] is True
    checks = {check["name"]: check for check in report["checks"]}
    assert checks["corpus-parity"]["ok"] is True
    assert checks["processed-turn-coverage"]["ok"] is True
    assert checks["zero-unexpected-outbound"]["ok"] is True
    assert checks["tool-error-budget"]["ok"] is True
    assert checks["provider-invariants"]["ok"] is True
    assert ("verify", "run-test-001", str(tmp_path / "target-data")) in provider.calls


def test_verify_gate_marks_dirty_on_unexpected_outbound(tmp_path):
    runner = FakeRunner(
        tmp_path / "state.db",
        outbound=[{"kind": "send", "delivery_mode": "live", "kwargs": {"content": "leak"}}],
    )
    orch, provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    with pytest.raises(ReplayVerifyError, match="zero-unexpected-outbound"):
        orch.verify(VerifyGateConfig(expected_turn_count=1))

    assert orch.manifest.state is ReplayRunState.DIRTY
    assert orch.manifest.dirty_reason == "mechanical verify gate failed"
    assert provider.dirty_calls
    with pytest.raises(ReplayStateError, match="cannot promote"):
        orch.promote(prod_data_dir=str(tmp_path / "non-prod"))


def test_verify_gate_reports_but_allows_captured_outbound(tmp_path):
    runner = FakeRunner(
        tmp_path / "state.db",
        outbound=[
            {
                "kind": "send",
                "delivery_mode": "capture",
                "kwargs": {"content": "would-be reply"},
            }
        ],
    )
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    report = orch.verify(VerifyGateConfig(expected_turn_count=1))

    checks = {check["name"]: check for check in report["checks"]}
    outbound_check = checks["zero-unexpected-outbound"]
    assert outbound_check["ok"] is True
    assert outbound_check["actual"]["captured_count"] == 1
    assert outbound_check["actual"]["captured"][0]["kwargs"]["content"] == "would-be reply"
    assert outbound_check["actual"]["escaped_count"] == 0
    assert outbound_check["actual"]["unexpected_count"] == 0


def test_verify_gate_reports_but_allows_dropped_outbound(tmp_path):
    runner = FakeRunner(
        tmp_path / "state.db",
        outbound=[
            {
                "kind": "send",
                "delivery_mode": "drop",
                "kwargs": {"content": "discarded reply"},
            }
        ],
    )
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    report = orch.verify(VerifyGateConfig(expected_turn_count=1))

    checks = {check["name"]: check for check in report["checks"]}
    outbound_check = checks["zero-unexpected-outbound"]
    assert outbound_check["ok"] is True
    assert outbound_check["actual"]["dropped_count"] == 1
    assert (
        outbound_check["actual"]["dropped"][0]["kwargs"]["content"]
        == "discarded reply"
    )
    assert outbound_check["actual"]["escaped_count"] == 0
    assert outbound_check["actual"]["unexpected_count"] == 0


def test_verify_gate_hard_fails_escaped_outbound_even_when_kind_allowed(tmp_path):
    runner = FakeRunner(
        tmp_path / "state.db",
        outbound=[
            {
                "kind": "send",
                "delivery_mode": "live",
                "kwargs": {"content": "escaped"},
            }
        ],
    )
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    with pytest.raises(ReplayVerifyError, match="zero-unexpected-outbound"):
        orch.verify(
            VerifyGateConfig(expected_turn_count=1, allowed_outbound_kinds=("send",))
        )

    checks = {
        check["name"]: check for check in orch.manifest.verify.get("checks", [])
    }
    outbound_check = checks["zero-unexpected-outbound"]
    assert outbound_check["ok"] is False
    assert outbound_check["actual"]["escaped_count"] == 1
    assert outbound_check["actual"]["allowed_escaped_count"] == 1


@pytest.mark.parametrize(
    "outbound_entry",
    [
        {"kind": "send", "kwargs": {"content": "missing mode"}},
        {"kind": "send", "delivery_mode": "", "kwargs": {"content": "empty mode"}},
        {"kind": "send", "delivery_mode": "live", "kwargs": {"content": "live send"}},
        {
            "kind": "send",
            "delivery_mode": "unknown",
            "kwargs": {"content": "unknown mode"},
        },
    ],
)
def test_verify_gate_fail_closed_for_missing_empty_or_unknown_delivery_mode(
    tmp_path, outbound_entry
):
    runner = FakeRunner(tmp_path / "state.db", outbound=[outbound_entry])
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    with pytest.raises(ReplayVerifyError, match="zero-unexpected-outbound"):
        orch.verify(VerifyGateConfig(expected_turn_count=1))

    checks = {
        check["name"]: check for check in orch.manifest.verify.get("checks", [])
    }
    outbound_check = checks["zero-unexpected-outbound"]
    assert outbound_check["ok"] is False
    assert outbound_check["actual"]["escaped_count"] == 1


def test_verify_gate_enforces_tool_error_budget(tmp_path):
    runner = FakeRunner(
        tmp_path / "state.db", tool_result={"ok": False, "error": "boom"}
    )
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())

    with pytest.raises(ReplayVerifyError, match="tool-error-budget"):
        orch.verify(VerifyGateConfig(expected_turn_count=1, tool_error_budget=0))

    assert orch.manifest.state is ReplayRunState.DIRTY


def test_promote_and_rollback_call_provider_only_after_verified_fresh_run(tmp_path):
    runner = FakeRunner(tmp_path / "state.db")
    orch, provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())
    orch.verify(VerifyGateConfig(expected_turn_count=1))

    promotion = orch.promote(prod_data_dir=str(tmp_path / "non-prod-prod-dir"))
    assert orch.manifest.state is ReplayRunState.PROMOTED
    assert promotion["provider_result"]["promotionId"] == "promo-run-test-001"
    assert provider.promote_calls == [
        (
            "run-test-001",
            str(tmp_path / "target-data"),
            str(tmp_path / "non-prod-prod-dir"),
        )
    ]

    rollback = orch.rollback()
    assert orch.manifest.state is ReplayRunState.ROLLED_BACK
    assert rollback["provider_result"]["ok"] is True
    assert provider.rollback_calls == [
        (
            "run-test-001",
            f"{tmp_path / 'non-prod-prod-dir'}/replay-target-provider/promotions/promo.json",
        )
    ]


def test_promote_refuses_resumed_or_non_fresh_attempt(tmp_path):
    runner = FakeRunner(tmp_path / "state.db")
    orch, _provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())
    orch.verify(VerifyGateConfig(expected_turn_count=1))
    orch.manifest.fresh_baseline = False
    orch.manifest.save()

    with pytest.raises(ReplayStateError, match="fresh-baseline-only"):
        orch.promote(prod_data_dir=str(tmp_path / "non-prod-prod-dir"))


def test_provider_promote_failure_moves_run_to_terminal_failed(tmp_path):
    runner = FakeRunner(tmp_path / "state.db")
    orch, provider = _prepared_orchestrator(tmp_path, runner_factory=lambda: runner)
    orch.run_agent_replay(_plan())
    orch.verify(VerifyGateConfig(expected_turn_count=1))

    def fail_promote(*, run_id, target_data_dir, prod_data_dir):  # noqa: ARG001
        raise ReplayProviderError(
            "disabled", status_code=403, code="REPLAY_PROMOTE_DISABLED"
        )

    provider.promote = fail_promote

    with pytest.raises(ReplayProviderError):
        orch.promote(prod_data_dir=str(tmp_path / "non-prod-prod-dir"))

    assert orch.manifest.state is ReplayRunState.FAILED
    saved = ReplayRunManifest.load(orch.manifest.manifest_path)
    assert saved.to_dict()["terminal"] is True
    assert saved.errors[-1]["phase"] == "promote"


def test_provider_client_uses_declared_http_routes_and_auth(tmp_path):
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def _send(self, data, status=200):
            raw = json.dumps(data).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length") or "0")
            body = json.loads(self.rfile.read(length) or b"{}")
            seen.append(("POST", self.path, self.headers.get("Authorization"), body))
            if self.path.startswith("/api/operator/replay-target/prepare"):
                self._send({
                    "ok": True,
                    "data": {
                        "runId": body["runId"],
                        "descriptor": {},
                        "descriptorDigest": _sha256_hex_manifest({}),
                        "baselineManifest": {},
                        "baselineDigest": _sha256_hex_manifest({}),
                    },
                })
            elif self.path.startswith("/api/operator/replay-target/verify"):
                self._send({
                    "ok": True,
                    "data": {"ok": True, "runId": body["runId"], "checks": []},
                })
            elif self.path.startswith("/api/operator/replay-target/promote"):
                self._send({
                    "ok": True,
                    "data": {
                        "ok": True,
                        "runId": body["runId"],
                        "manifestPath": "/tmp/promo.json",
                    },
                })
            elif self.path.startswith("/api/operator/replay-target/rollback"):
                self._send({
                    "ok": True,
                    "data": {
                        "ok": True,
                        "runId": body["runId"],
                        "manifestPath": body["promotionManifestPath"],
                    },
                })
            else:
                self._send(
                    {"ok": False, "error": {"code": "NOPE", "message": "no"}},
                    status=404,
                )

        def do_GET(self):  # noqa: N802
            seen.append(("GET", self.path, self.headers.get("Authorization"), None))
            if self.path.startswith("/api/operator/replay-target/descriptor/run-http"):
                self._send({
                    "ok": True,
                    "data": {
                        "descriptor": {},
                        "descriptorDigest": _sha256_hex_manifest({}),
                    },
                })
            else:
                self._send(
                    {"ok": False, "error": {"code": "NOPE", "message": "no"}},
                    status=404,
                )

        def log_message(self, format, *args):  # noqa: A003
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = ReplayTargetProviderClient(
            ReplayTargetProviderConfig(
                provider_url=f"http://127.0.0.1:{server.server_port}",
                admin_token="secret",
                tenant="tgg",
            )
        )
        client.prepare(
            run_id="run-http",
            source_data_dir=str(tmp_path / "source"),
            target_data_dir=str(tmp_path / "target"),
            base_url="http://127.0.0.1:5192",
        )
        client.descriptor(run_id="run-http", data_dir=str(tmp_path / "target"))
        client.verify(run_id="run-http", data_dir=str(tmp_path / "target"))
        client.promote(
            run_id="run-http",
            target_data_dir=str(tmp_path / "target"),
            prod_data_dir=str(tmp_path / "non-prod"),
        )
        client.rollback(run_id="run-http", promotion_manifest_path="/tmp/promo.json")
    finally:
        server.shutdown()
        thread.join(timeout=5)

    paths = [row[1] for row in seen]
    assert any(
        path.startswith("/api/operator/replay-target/prepare?tenant=tgg")
        for path in paths
    )
    assert any(
        path.startswith("/api/operator/replay-target/descriptor/run-http?tenant=tgg")
        for path in paths
    )
    assert any(
        path.startswith("/api/operator/replay-target/verify?tenant=tgg")
        for path in paths
    )
    assert any(
        path.startswith("/api/operator/replay-target/promote?tenant=tgg")
        for path in paths
    )
    assert any(
        path.startswith("/api/operator/replay-target/rollback?tenant=tgg")
        for path in paths
    )
    assert all(row[2] == "Bearer secret" for row in seen)


def test_provider_client_surfaces_provider_error(tmp_path):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            raw = json.dumps({
                "ok": False,
                "error": {"code": "REPLAY_PROMOTE_DISABLED", "message": "disabled"},
            }).encode("utf-8")
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format, *args):  # noqa: A003
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = ReplayTargetProviderClient(
            ReplayTargetProviderConfig(
                provider_url=f"http://127.0.0.1:{server.server_port}",
                admin_token="secret",
                tenant="tgg",
            )
        )
        with pytest.raises(ReplayProviderError) as err:
            client.promote(
                run_id="run-http",
                target_data_dir=str(tmp_path / "target"),
                prod_data_dir=str(tmp_path / "non-prod"),
            )
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert err.value.status_code == 403
    assert err.value.code == "REPLAY_PROMOTE_DISABLED"
