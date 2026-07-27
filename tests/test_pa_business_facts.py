import json
import sys
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tools.pa_business_tools import (
    PABusinessBridgeConfig,
    TenantScopeMismatch,
    execute_business_operation,
    load_business_bridge_config,
    record_agent_action,
    _user_task_allows_ilinked,
)


TGG_PRODUCTION_CONFIG = (
    Path(__file__).parents[1] / "deploy" / "tgg" / "christopher" / "config.yaml"
)


class _FakeBusinessHandler(BaseHTTPRequestHandler):
    received = {}

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        type(self).received = {
            "path": self.path,
            "payload": payload,
            "content_type": self.headers.get("Content-Type"),
            "tgg_token": self.headers.get("X-TGG-Token"),
            "mofex_token": self.headers.get("X-Mofex-Token"),
        }
        body = json.dumps({"ok": True, "echo": payload}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        return


@pytest.fixture
def source_refs_context():
    """Bind gateway turn refs on the task-local production surface."""
    from gateway.session_context import set_session_vars

    tokens = []

    def bind(refs):
        tokens.append(set_session_vars(source_message_refs=json.dumps(refs)))

    yield bind
    for turn_tokens in reversed(tokens):
        for token in reversed(turn_tokens):
            token.var.reset(token)


@pytest.fixture
def fake_business_endpoint():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeBusinessHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/business"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_http_operation_calls_fake_endpoint_and_returns_json(fake_business_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "lookup": {
                    "type": "http",
                    "url": fake_business_endpoint,
                    "method": "POST",
                }
            }
        }
    }

    result = execute_business_operation(config, "lookup", {"case_id": "C-123"})

    assert result["ok"] is True
    assert result["echo"] == {"case_id": "C-123"}
    assert result["status_code"] == 200
    assert _FakeBusinessHandler.received == {
        "path": "/business",
        "payload": {"case_id": "C-123"},
        "content_type": "application/json",
        "tgg_token": None,
        "mofex_token": None,
    }


def test_local_command_operation_returns_json():
    config = {
        "pa_business": {
            "operations": {
                "local_echo": {
                    "type": "command",
                    "command": [
                        sys.executable,
                        "-c",
                        (
                            "import json,sys; "
                            "payload=json.load(sys.stdin); "
                            "print(json.dumps({'ok': True, 'payload': payload}))"
                        ),
                    ],
                }
            }
        }
    }

    result = execute_business_operation(config, "local_echo", {"amount": 42})

    assert result == {"ok": True, "payload": {"amount": 42}}


@pytest.mark.parametrize(
    ("legacy_name", "canonical_name"),
    [
        ("tgg_clarification_request", "tgg_clarification_raise"),
        ("tgg_message_history_search", "message_search"),
        ("tgg_case_update_state", "tgg_case_update"),
    ],
)
def test_legacy_operation_name_resolves_to_canonical_when_registry_is_canonical(
    legacy_name, canonical_name
):
    config = {
        "pa_business": {
            "operations": {
                canonical_name: {
                    "type": "command",
                    "command": [
                        sys.executable,
                        "-c",
                        "import json,sys; print(json.dumps(json.load(sys.stdin)))",
                    ],
                }
            }
        }
    }

    assert execute_business_operation(config, legacy_name, {"ok": True}) == {
        "ok": True
    }


def test_generic_observation_injects_current_turn_source_refs(
    monkeypatch, source_refs_context
):
    import tools.pa_business_tools as pbt

    bridge = load_business_bridge_config(
        {
            "pa_business": {
                "operations": {
                    "tgg_case_observation": {
                        "type": "http",
                        "url": "http://127.0.0.1:1/observations",
                        "method": "POST",
                    }
                }
            }
        }
    )
    captured = {}
    source_refs_context(["wa-current-generic"])
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", lambda: bridge)

    def fake_execute(_bridge, *, operation, payload):
        captured.update(operation=operation, payload=payload)
        return {"ok": True}

    monkeypatch.setattr(pbt, "execute_business_operation", fake_execute)

    result = json.loads(
        pbt._handle_business_call(
            {
                "operation": "tgg_case_observation",
                "payload": {"jobNo": "AM/JOB/2601/1018"},
            }
        )
    )

    assert result["ok"] is True
    assert captured["payload"]["sourceRefs"] == ["wa-current-generic"]


def _observation_bridge():
    return load_business_bridge_config(
        {
            "pa_business": {
                "operations": {
                    "tgg_case_observation": {
                        "type": "http",
                        "url": "http://127.0.0.1:1/observations",
                        "method": "POST",
                    }
                }
            }
        }
    )


def _run_generic_observation(monkeypatch, payload, backend_result=None):
    import tools.pa_business_tools as pbt

    captured = {}
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", _observation_bridge)

    def fake_execute(_bridge, *, operation, payload):
        captured.update(operation=operation, payload=payload)
        return backend_result if backend_result is not None else {"ok": True}

    monkeypatch.setattr(pbt, "execute_business_operation", fake_execute)
    result = json.loads(
        pbt._handle_business_call(
            {"operation": "tgg_case_observation", "payload": payload}
        )
    )
    return result, captured


def test_generic_observation_placeholder_source_refs_bind_real_turn_ids(
    monkeypatch, source_refs_context
):
    """Literal "current_turn" is a placeholder, not a citable id — the tool
    layer must treat it as omitted and bind the gateway's real turn refs
    (stage-1 backprocess finding 1, 2026-07-20)."""
    source_refs_context(["wa-real-1", "wa-real-2"])
    _, captured = _run_generic_observation(
        monkeypatch,
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["current_turn"]},
    )
    assert captured["payload"]["sourceRefs"] == ["wa-real-1", "wa-real-2"]
    assert "source_refs" not in captured["payload"]


def test_generic_observation_mixed_placeholder_keeps_real_refs(monkeypatch):
    monkeypatch.setenv("HERMES_SESSION_SOURCE_MESSAGE_REFS", '["wa-real-1"]')
    _, captured = _run_generic_observation(
        monkeypatch,
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["Current_Turn", "wa-cited-9"]},
    )
    assert captured["payload"]["sourceRefs"] == ["wa-cited-9"]


def test_generic_observation_placeholder_inside_fields_bind_real_turn_ids(
    monkeypatch, source_refs_context
):
    source_refs_context(["wa-real-1"])
    _, captured = _run_generic_observation(
        monkeypatch,
        {
            "jobNo": "AM/JOB/2601/1018",
            "fields": {"source_refs": ["current_turn"], "note_key": "kept"},
        },
    )
    assert captured["payload"]["sourceRefs"] == ["wa-real-1"]
    assert captured["payload"]["fields"] == {"note_key": "kept"}


def test_generic_observation_real_refs_pass_through_untouched(monkeypatch):
    monkeypatch.setenv("HERMES_SESSION_SOURCE_MESSAGE_REFS", '["wa-real-1"]')
    _, captured = _run_generic_observation(
        monkeypatch,
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["wa-cited-1", "wa-cited-2"]},
    )
    assert captured["payload"]["sourceRefs"] == ["wa-cited-1", "wa-cited-2"]


def test_attach_unjustified_error_carries_recovery_guidance(monkeypatch):
    """ATTACH_UNJUSTIFIED rejections must teach the retry: keep ALL sourceRefs
    and supply the justification contract — the observed failure mode is the
    model dropping photo message ids to pass the gate (stage-1 finding 2)."""
    result, _ = _run_generic_observation(
        monkeypatch,
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["wa-cited-1"]},
        backend_result={
            "ok": False,
            "error": {
                "code": "ATTACH_UNJUSTIFIED",
                "message": "justification is required for evidence attachment.",
            },
            "status_code": 400,
        },
    )
    recovery = result.get("recovery") or ""
    assert "keeping ALL cited sourceRefs" in recovery
    assert "identifier_match" in recovery
    assert "thread_continuation" in recovery
    assert "operator_directive" in recovery
    assert "block_unit" in recovery
    assert "image_content" in recovery
    assert "tgg_attention_raise" in recovery


def test_non_attach_errors_get_no_recovery_field(monkeypatch):
    result, _ = _run_generic_observation(
        monkeypatch,
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["wa-cited-1"]},
        backend_result={
            "ok": False,
            "error": {"code": "CASE_NOT_FOUND", "message": "No case."},
            "status_code": 404,
        },
    )
    assert "recovery" not in result


def test_direct_observation_placeholder_source_refs_bind_real_turn_ids(
    monkeypatch, source_refs_context
):
    import tools.pa_business_tools as pbt

    source_refs_context(["wa-real-7"])
    captured = {}
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", _observation_bridge)

    def fake_execute(_bridge, *, operation, payload):
        captured.update(operation=operation, payload=payload)
        return {"ok": True, "data": {"observationId": 99}}

    monkeypatch.setattr(pbt, "execute_business_operation", fake_execute)
    raw = pbt._handle_tgg_case_observation(
        {
            "jobNo": "AM/JOB/2601/1018",
            "notes": "worker photos",
            "sourceRefs": ["current_turn"],
        }
    )
    assert json.loads(raw)["ok"] is True
    assert captured["payload"]["fields"]["source_refs"] == ["wa-real-7"]


def test_tgg_production_config_exposes_searchable_case_operations():
    config = yaml.safe_load(TGG_PRODUCTION_CONFIG.read_text(encoding="utf-8"))
    pa_context = SimpleNamespace(
        constitution=SimpleNamespace(client=config["pa"]["overlay"]["client"])
    )
    bridge = load_business_bridge_config(config, pa_context=pa_context)

    assert "tgg_case_search" in bridge.operations
    assert "job_work_costings" in bridge.operations
    assert "work_costing_lookup" in bridge.operations
    assert "work_costing_ingest_ilinked" in bridge.operations
    assert bridge.operations["tgg_case_search"].method == "GET"
    assert "tgg_message_history_search" not in bridge.operations
    assert "tgg_clarification_request" not in bridge.operations
    assert "tgg_case_update_state" not in bridge.operations
    assert bridge.operations["job_work_costings"].url.endswith("/api/operator/cases/{jobNo}/work-costings")
    assert bridge.media_root == Path(
        "/home/pclaw/.systems-pcl/data/media/tgg/hermes"
    )
    assert bridge.media_ref_prefix == "/media/tgg/hermes"


def test_ilinked_read_operations_require_explicit_current_user_cue():
    assert _user_task_allows_ilinked("case_lookup", "latest note for 0182")
    assert not _user_task_allows_ilinked("ilinked_status", "latest note for 0182")
    assert not _user_task_allows_ilinked("ilinked_lookup", "223A outstanding")
    assert _user_task_allows_ilinked(
        "ilinked_status",
        "",
        {"jobNo": "SK/JOB/2604/0360", "source_system_requested": True},
    )
    assert not _user_task_allows_ilinked(
        "ilinked_status",
        "latest note for 0182",
        {"jobNo": "PG/JOB/2605/0182", "source_system_requested": True},
    )
    assert _user_task_allows_ilinked("ilinked_status", "check iLinked for SK/JOB/2604/0360")
    assert _user_task_allows_ilinked("ilinked_status", "HDB status for SK/JOB/2604/0360")
    assert _user_task_allows_ilinked("ilinked_wc_lookup", "work costing scope for 0360")


def test_tgg_ilinked_lookup_adapter_reads_corpus_exact_job(monkeypatch, tmp_path):
    corpus = tmp_path / "full-import-test"
    tree = corpus / "tree"
    tree.mkdir(parents=True)
    (tree / "leaf-0001-page-first.json").write_text(
        json.dumps(
            {
                "leaf": {"text": "Job (1)"},
                "pageArg": "first",
                "grid": {
                    "ok": True,
                    "headers": [
                        "",
                        "Task Number",
                        "Description",
                        "Task Type",
                        "Location",
                        "Created Date",
                        "Created By",
                        "Sub Status",
                        "Status",
                    ],
                    "rows": [
                        {
                            "cells": [
                                {"text": ""},
                                {"text": "SD/JOB/2605/1008"},
                                {"text": "Kitchen sink leak"},
                                {"text": "Job"},
                                {
                                    "text": "BLK 223A SUMANG LANE, #12-4947 MATILDA EDGE, SINGAPORE 821223"
                                },
                                {"text": "2026-05-25"},
                                {"text": "Sky"},
                                {"text": "Assigned"},
                                {"text": "Open"},
                            ]
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CHRISTOPHER_ILINKED_CORPUS_DIR", str(corpus))

    config = {
        "pa_business": {
            "operations": {
                "ilinked_lookup": {
                    "type": "command",
                    "command": [
                        sys.executable,
                        "-m",
                        "tools.tgg_ilinked_lookup",
                    ],
                    "timeout": 10,
                }
            }
        }
    }

    result = execute_business_operation(
        config, "ilinked_lookup", {"jobNo": "SD/JOB/2605/1008"}
    )

    assert result["ok"] is True
    assert result["confidence"] == "exact"
    assert result["matches"][0]["entry"]["taskNo"] == "SD/JOB/2605/1008"
    assert result["matches"][0]["entry"]["block"] == "223A"
    assert result["matches"][0]["entry"]["unit"] == "12-4947"




def test_tgg_ilinked_wc_lookup_adapter_returns_structured_cost_lines(monkeypatch, tmp_path):
    detail = {
        "ok": True,
        "data": {
            "kind": "work_costing",
            "identifier": "AM/WC/2605/0334",
            "matched_row": {
                "values": {
                    "Work Costing Number": "AM/WC/2605/0334",
                    "Work Costing Description": "Spruce Package B 1 Room",
                    "Job Number": "AM/JOB/2605/0906",
                    "Location": "BLK 420 ANG MO KIO AVENUE 10, #06-1125",
                    "Commencement Date": "01 Jun 2026",
                    "Estimated End Date": "24 Jul 2026",
                    "Estimated WC Amount": "$5,328.93",
                    "Status": "Approved",
                    "Created Date": "18 May 2026",
                    "Last Modified Date": "22 May 2026",
                }
            },
            "tables": [
                {
                    "headers": [],
                    "rows": [
                        [
                            "", "", "", "1", "Service",
                            "Carry Out and Complete Sprucing Works",
                            "PG", "3,678.00", "1.00", "3,678.00",
                            "", "", "", "[1000] - BUILDING WORKS",
                            "SAVILLS PROPERTY MANAGEMENT PTE. LTD.",
                            "D/496/25 - PROVISION OF MANAGING AGENT SE",
                            "AM/WC/2605/0334-WO01",
                        ]
                    ],
                }
            ],
        },
    }
    fixture = tmp_path / "detail-work-costing.json"
    fixture.write_text(json.dumps(detail), encoding="utf-8")
    monkeypatch.setenv("CHRISTOPHER_ILINKED_DETAIL_JSON", str(fixture))

    config = {
        "pa_business": {
            "operations": {
                "ilinked_wc_lookup": {
                    "type": "command",
                    "command": [sys.executable, "-m", "tools.tgg_ilinked_reads", "wc"],
                    "timeout": 10,
                }
            }
        }
    }

    result = execute_business_operation(
        config, "ilinked_wc_lookup", {"job_no": "AM/JOB/2605/0906"}
    )

    assert result["ok"] is True
    data = result["data"]
    assert data["work_costing_id"] == "AM/WC/2605/0334"
    assert data["job_no"] == "AM/JOB/2605/0906"
    assert data["total_estimate"] == "$5,328.93"
    assert data["approval_status"] == "Approved"
    assert data["vendor"] == "SAVILLS PROPERTY MANAGEMENT PTE. LTD."
    assert data["cost_lines"][0]["description"] == "Carry Out and Complete Sprucing Works"


def test_tgg_ilinked_status_adapter_reads_corpus_index(monkeypatch, tmp_path):
    corpus = tmp_path / "full-import-test"
    corpus.mkdir()
    (corpus / "task-index-date-desc.json").write_text(
        json.dumps(
            [
                {
                    "task_key": "ZZ/JOB/2605/0001",
                    "date_iso": "2026-06-30",
                    "leaf_text": "Job (1)",
                    "cells": [
                        "",
                        "ZZ/JOB/2605/0001",
                        "Sprucing scope",
                        "Job",
                        "BLK 1 TEST ROAD #01-01",
                        "20 May 2026",
                        "Sky",
                        "Contractor to Inspect/Repair",
                        "Pending Execution",
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CHRISTOPHER_ILINKED_CORPUS_DIR", str(corpus))

    config = {
        "pa_business": {
            "operations": {
                "ilinked_status": {
                    "type": "command",
                    "command": [sys.executable, "-m", "tools.tgg_ilinked_reads", "status"],
                    "timeout": 10,
                }
            }
        }
    }

    result = execute_business_operation(
        config, "ilinked_status", {"job_no": "ZZ/JOB/2605/0001"}
    )

    assert result["ok"] is True
    data = result["data"]
    assert data["task_no"] == "ZZ/JOB/2605/0001"
    assert data["status"] == "Pending Execution"
    assert data["sub_status"] == "Contractor to Inspect/Repair"
    assert data["end_date"] == "2026-06-30"
    assert data["source"]["type"] == "task-index"


def _agent_action_config(url: str) -> dict:
    return {
        "pa_business": {
            "operations": {
                "agent_action_record": {
                    "type": "http",
                    "url": url,
                    "method": "POST",
                }
            }
        }
    }


def test_record_agent_action_observation_type(fake_business_endpoint):
    ok = record_agent_action(
        agent_id="iris",
        engagement_id="00000000-0000-0000-0000-000000000001",
        action_type="observation",
        payload={"incoming_message": "hello"},
        source="whatsapp",
        turn_id="turn-1",
        config=_agent_action_config(fake_business_endpoint),
    )

    assert ok is True
    assert _FakeBusinessHandler.received["payload"] == {
        "agent_id": "iris",
        "engagement_id": "00000000-0000-0000-0000-000000000001",
        "action_type": "observation",
        "payload": {"incoming_message": "hello"},
        "source": "whatsapp",
        "cost_usd": 0.0,
        "tokens_input": 0,
        "tokens_output": 0,
        "status": "pending",
        "turn_id": "turn-1",
    }


def test_record_agent_action_dry_run_reply_type(fake_business_endpoint):
    ok = record_agent_action(
        agent_id="iris",
        engagement_id="00000000-0000-0000-0000-000000000002",
        action_type="dry-run-reply",
        payload={"reply": "draft only"},
        source="telegram",
        cost_usd=0.123456,
        tokens_input=321,
        tokens_output=45,
        status="dry-run",
        turn_id="turn-2",
        config=_agent_action_config(fake_business_endpoint),
    )

    assert ok is True
    received = _FakeBusinessHandler.received["payload"]
    assert received["action_type"] == "dry-run-reply"
    assert received["status"] == "dry-run"
    assert received["payload"] == {"reply": "draft only"}
    assert received["cost_usd"] == 0.123456
    assert received["tokens_input"] == 321
    assert received["tokens_output"] == 45


def test_record_agent_action_executed_reply_type(fake_business_endpoint):
    ok = record_agent_action(
        agent_id="iris",
        engagement_id="00000000-0000-0000-0000-000000000003",
        action_type="executed-reply",
        payload={"reply": "sent"},
        source="whatsapp",
        status="executed",
        turn_id="turn-3",
        config=_agent_action_config(fake_business_endpoint),
    )

    assert ok is True
    received = _FakeBusinessHandler.received["payload"]
    assert received["action_type"] == "executed-reply"
    assert received["status"] == "executed"
    assert received["payload"] == {"reply": "sent"}


def test_record_agent_action_photo_pair_classified_type(fake_business_endpoint):
    ok = record_agent_action(
        agent_id="bobby",
        engagement_id="00000000-0000-0000-0000-000000000006",
        action_type="photo-pair-classified",
        payload={
            "before": {"file_id": "before-file", "getFile_url": "https://files/before.jpg"},
            "after": {"file_id": "after-file", "getFile_url": "https://files/after.jpg"},
            "confidence": 0.96,
            "classified_at": "2026-05-18T12:00:31Z",
        },
        source="whatsapp",
        status="executed",
        turn_id="turn-photo-pair",
        config=_agent_action_config(fake_business_endpoint),
    )

    assert ok is True
    received = _FakeBusinessHandler.received["payload"]
    assert received["action_type"] == "photo-pair-classified"
    assert received["status"] == "executed"
    assert received["payload"]["before"]["file_id"] == "before-file"
    assert received["payload"]["after"]["getFile_url"] == "https://files/after.jpg"


def test_record_agent_action_fails_soft_when_bridge_unavailable():
    ok = record_agent_action(
        agent_id="iris",
        engagement_id="00000000-0000-0000-0000-000000000004",
        action_type="observation",
        payload={"incoming_message": "hello"},
        config={"pa_business": {"operations": {}}},
    )

    assert ok is False


def test_record_agent_action_agent_id_passed_verbatim(fake_business_endpoint):
    ok = record_agent_action(
        agent_id="Iris V1",
        engagement_id="00000000-0000-0000-0000-000000000005",
        action_type="observation",
        payload={"incoming_message": "hello"},
        config=_agent_action_config(fake_business_endpoint),
    )

    assert ok is True
    assert _FakeBusinessHandler.received["payload"]["agent_id"] == "Iris V1"


def test_nested_pa_business_config_is_supported(fake_business_endpoint):
    config = {
        "pa": {
            "business": {
                "operations": {
                    "lookup": {
                        "type": "http",
                        "url": fake_business_endpoint,
                    }
                }
            }
        }
    }

    result = execute_business_operation(config, "lookup", {"case_id": "C-456"})

    assert result["ok"] is True
    assert result["echo"] == {"case_id": "C-456"}


def test_unknown_operation_fails_loudly():
    bridge = load_business_bridge_config({"pa_business": {"operations": {}}})

    with pytest.raises(ValueError, match="unknown PA business operation"):
        execute_business_operation(bridge, "missing", {})


def test_no_config_means_empty_inactive_bridge():
    bridge = load_business_bridge_config(None)

    assert bridge.operations == {}


def test_runtime_bridge_loads_raw_pa_business_config(monkeypatch, tmp_path, fake_business_endpoint):
    from tools.pa_business_tools import _load_runtime_bridge_config

    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "config.yaml").write_text(
        "\n".join(
            [
                "pa_business:",
                "  operations:",
                "    lookup:",
                "      type: http",
                f"      url: {fake_business_endpoint}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    bridge = _load_runtime_bridge_config()

    assert sorted(bridge.operations) == ["lookup"]


def test_bridge_module_does_not_import_or_call_hermes_state_writers():
    source = Path("tools/pa_business_tools.py").read_text(encoding="utf-8")

    forbidden_fragments = [
        "MemoryManager",
        "memory_tool",
        "state.db",
        "session_db",
        "save_message",
        "add_memory",
        "write_memory",
    ]
    assert not any(fragment in source for fragment in forbidden_fragments)


def test_pa_business_toolset_is_registered_without_all_tools():
    from toolsets import get_toolset, resolve_toolset

    expected = {
        "pa_business_read",
        "pa_business_write",
        "tgg_case_lookup",
        "tgg_case_photos",
        "tgg_case_query",
        "tgg_spreadsheet_job_numbers",
        "tgg_case_search",
        "tgg_message_history_search",
        "message_history_search",
        "tgg_clarification_request",
        "clarification_request",
        "tgg_case_observation",
        "tgg_case_create",
        "tgg_case_update_state",
    }
    toolset = get_toolset("pa-business")
    assert toolset is not None
    assert set(toolset["tools"]) == expected
    assert set(resolve_toolset("pa-business")) == expected
    custom = get_toolset("custom")
    assert custom is not None
    assert set(custom["tools"]) == expected
    assert set(resolve_toolset("custom")) == expected


def _jpeg(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xd8\xff\xe0fixture")


def _xlsx(
    path: Path,
    *,
    macro_enabled: bool = False,
    job_numbers: tuple[str, ...] = (),
) -> None:
    workbook_type = (
        "application/vnd.ms-excel.sheet.macroEnabled.main+xml"
        if macro_enabled
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                f'<Override PartName="/xl/workbook.xml" ContentType="{workbook_type}"/>'
                "</Types>"
            ),
        )
        archive.writestr(
            "xl/workbook.xml",
            (
                '<workbook xmlns="http://schemas.openxmlformats.org/'
                'spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/'
                'officeDocument/2006/relationships"><sheets><sheet name="Sheet1" '
                'sheetId="1" r:id="rId1"/></sheets></workbook>'
            ),
        )
        archive.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/'
                '2006/relationships"><Relationship Id="rId1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/'
                'relationships/worksheet" Target="worksheets/sheet1.xml"/>'
                "</Relationships>"
            ),
        )
        rows = [
            '<row r="1"><c r="A1" t="inlineStr"><is><t>Job No.</t></is></c>'
            '<c r="B1" t="inlineStr"><is><t>Status</t></is></c></row>'
        ]
        rows.extend(
            f'<row r="{index}"><c r="A{index}" t="inlineStr"><is><t>{job}</t>'
            f'</is></c><c r="B{index}" t="inlineStr"><is><t>Open</t></is></c></row>'
            for index, job in enumerate(job_numbers, start=2)
        )
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            (
                '<worksheet xmlns="http://schemas.openxmlformats.org/'
                'spreadsheetml/2006/main"><sheetData>'
                + "".join(rows)
                + "</sheetData></worksheet>"
            ),
        )
        if macro_enabled:
            archive.writestr("xl/vbaProject.bin", b"macro")


def test_tgg_spreadsheet_gate_accepts_xlsx_and_csv_by_content(tmp_path):
    import tools.pa_business_tools as pbt

    workbook = tmp_path / "jobs.xlsx"
    _xlsx(workbook)
    csv_file = tmp_path / "jobs.csv"
    csv_file.write_text("Job No.,Status\nAM/JOB/2607/0001,Open\n", encoding="utf-8")

    assert pbt.validate_tgg_spreadsheet(
        workbook,
        declared_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ).endswith("spreadsheetml.sheet")
    assert pbt.validate_tgg_spreadsheet(
        csv_file, declared_mime="text/csv"
    ) == "text/csv"


def test_tgg_spreadsheet_job_numbers_extracts_xlsx_and_csv(tmp_path):
    import tools.pa_business_tools as pbt

    workbook = tmp_path / "jobs.xlsx"
    _xlsx(
        workbook,
        job_numbers=(
            "AM/JOB/2607/0001",
            "AM/JOB/2607/0002",
            "AM/JOB/2607/0001",
        ),
    )
    csv_file = tmp_path / "jobs.csv"
    csv_file.write_text(
        "Address,Job Number,Status\n"
        "One,AM/JOB/2607/0003,Open\n"
        "Two,not-a-job,Open\n",
        encoding="utf-8",
    )

    assert pbt.extract_tgg_spreadsheet_job_numbers(workbook) == [
        "AM/JOB/2607/0001",
        "AM/JOB/2607/0002",
    ]
    assert pbt.extract_tgg_spreadsheet_job_numbers(csv_file) == [
        "AM/JOB/2607/0003"
    ]


def test_tgg_spreadsheet_tool_hands_numbers_to_existing_cross_check(
    tmp_path, monkeypatch
):
    import tools.pa_business_tools as pbt
    import hermes_cli.config as hermes_config

    workbook = tmp_path / "jobs.xlsx"
    _xlsx(workbook, job_numbers=("AM/JOB/2607/0001", "AM/JOB/2607/0002"))
    monkeypatch.setattr(
        hermes_config,
        "read_raw_config",
        lambda: {
            "pa": {"media_retention": {"source_roots": [str(tmp_path)]}}
        },
    )
    result = json.loads(
        pbt._handle_tgg_spreadsheet_job_numbers({"path": str(workbook)})
    )

    assert result["jobNumbers"] == [
        "AM/JOB/2607/0001",
        "AM/JOB/2607/0002",
    ]
    assert "tgg_case_query" in result["next"]


def test_tgg_spreadsheet_tool_refuses_path_outside_configured_roots(
    tmp_path, monkeypatch
):
    import tools.pa_business_tools as pbt
    import hermes_cli.config as hermes_config

    workbook = tmp_path / "outside" / "jobs.xlsx"
    workbook.parent.mkdir()
    _xlsx(workbook, job_numbers=("AM/JOB/2607/0001",))
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    monkeypatch.setattr(
        hermes_config,
        "read_raw_config",
        lambda: {
            "pa": {"media_retention": {"source_roots": [str(allowed)]}}
        },
    )

    result = json.loads(
        pbt._handle_tgg_spreadsheet_job_numbers({"path": str(workbook)})
    )

    assert result == {
        "error": (
            "INVALID_MEDIA_REF: spreadsheet is unavailable or outside "
            "configured roots"
        )
    }
    assert str(workbook) not in result["error"]


def test_tgg_spreadsheet_gate_refuses_oversized_csv_without_full_read(
    tmp_path, monkeypatch
):
    import tools.pa_business_tools as pbt

    csv_file = tmp_path / "huge.csv"
    csv_file.write_text("Job No.,Status\nAM/JOB/2607/0001,Open\n", encoding="utf-8")
    real_stat = Path.stat

    class Oversized:
        st_mode = 0o100644
        st_size = pbt._TGG_SPREADSHEET_MAX_FILE_BYTES + 1

    monkeypatch.setattr(
        Path,
        "stat",
        lambda self, *args, **kwargs: (
            Oversized() if self == csv_file else real_stat(self, *args, **kwargs)
        ),
    )

    with pytest.raises(ValueError, match="SPREADSHEET_TOO_LARGE"):
        pbt.validate_tgg_spreadsheet(csv_file, declared_mime="text/csv")


@pytest.mark.parametrize("suffix", [".xlsm", ".xltm"])
def test_tgg_spreadsheet_gate_refuses_macro_enabled_extensions(tmp_path, suffix):
    import tools.pa_business_tools as pbt

    workbook = tmp_path / f"jobs{suffix}"
    _xlsx(workbook, macro_enabled=True)
    with pytest.raises(ValueError, match="UNSUPPORTED_MEDIA_TYPE"):
        pbt.validate_tgg_spreadsheet(
            workbook,
            declared_mime="application/vnd.ms-excel.sheet.macroenabled.12",
        )


def test_tgg_spreadsheet_gate_refuses_macro_payload_renamed_xlsx(tmp_path):
    import tools.pa_business_tools as pbt

    workbook = tmp_path / "renamed.xlsx"
    _xlsx(workbook, macro_enabled=True)
    with pytest.raises(ValueError, match="PROVENANCE_DIVERGENCE"):
        pbt.validate_tgg_spreadsheet(
            workbook,
            declared_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def test_tgg_spreadsheet_gate_refuses_executable_renamed_xlsx(tmp_path):
    import tools.pa_business_tools as pbt

    executable = tmp_path / "malware.xlsx"
    executable.write_bytes(b"MZ" + b"\x00" * 100)
    with pytest.raises(ValueError, match="PROVENANCE_DIVERGENCE"):
        pbt.validate_tgg_spreadsheet(
            executable,
            declared_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def test_tgg_spreadsheet_gate_refuses_mime_bytes_mismatch(tmp_path):
    import tools.pa_business_tools as pbt

    csv_file = tmp_path / "jobs.csv"
    csv_file.write_text("Job No.,Status\nAM/JOB/2607/0001,Open\n", encoding="utf-8")
    with pytest.raises(ValueError, match="PROVENANCE_DIVERGENCE"):
        pbt.validate_tgg_spreadsheet(
            csv_file,
            declared_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def test_tgg_case_photo_provenance_gate_is_unchanged(tmp_path):
    import tools.pa_business_tools as pbt

    photo = tmp_path / "photo.jpg"
    _jpeg(photo)
    with pytest.raises(ValueError, match="PROVENANCE_DIVERGENCE"):
        pbt._resolve_case_photo(
            {"ref": "/media/photo.jpg", "mimeType": "image/png"},
            tmp_path,
        )


def test_tgg_case_photos_resolves_opaque_refs_under_configured_root(
    monkeypatch, tmp_path
):
    import tools.pa_business_tools as pbt

    root = tmp_path / "systems-media"
    photo = root / "retained" / "one.jpg"
    _jpeg(photo)
    bridge = PABusinessBridgeConfig(operations={}, media_root=root)
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", lambda: bridge)
    monkeypatch.setattr(
        pbt,
        "execute_business_operation",
        lambda *_a, **_kw: {
            "ok": True,
            "data": {
                "media": [
                    {"ref": "/media/retained/one.jpg", "mimeType": "image/jpeg"},
                    {"ref": "/media/retained/one.jpg", "mimeType": "image/jpeg"},
                    {"ref": "/media/retained/note.txt", "mimeType": "text/plain"},
                ]
            },
        },
    )
    (root / "retained" / "note.txt").write_text("not an image")

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": "SK/JOB/2604/2376"}))

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["photos"] == [
        {"media_ref": "/media/retained/one.jpg", "image_path": str(photo.resolve())}
    ]


def test_tgg_case_photos_maps_configured_prefix_directly_to_media_root(
    monkeypatch, tmp_path
):
    import tools.pa_business_tools as pbt

    root = tmp_path / "media" / "tgg" / "hermes"
    photo = root / "d7ba5f99ed0a259185f5c07e_0.jpg"
    _jpeg(photo)
    bridge = PABusinessBridgeConfig(
        operations={},
        media_root=root,
        media_ref_prefix="/media/tgg/hermes",
    )
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", lambda: bridge)
    monkeypatch.setattr(
        pbt,
        "execute_business_operation",
        lambda *_a, **_kw: {
            "ok": True,
            "data": {
                "media": [
                    {
                        "ref": "/media/tgg/hermes/d7ba5f99ed0a259185f5c07e_0.jpg",
                        "mimeType": "image/jpeg",
                    }
                ]
            },
        },
    )

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": "SK/JOB/2606/2372"}))

    assert result["ok"] is True
    assert result["photos"] == [
        {
            "media_ref": "/media/tgg/hermes/d7ba5f99ed0a259185f5c07e_0.jpg",
            "image_path": str(photo.resolve()),
        }
    ]


def test_tgg_case_photos_refuses_ref_outside_configured_prefix(monkeypatch, tmp_path):
    import tools.pa_business_tools as pbt

    root = tmp_path / "media" / "tgg" / "hermes"
    root.mkdir(parents=True)
    bridge = PABusinessBridgeConfig(
        operations={},
        media_root=root,
        media_ref_prefix="/media/tgg/hermes",
    )
    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", lambda: bridge)
    monkeypatch.setattr(
        pbt,
        "execute_business_operation",
        lambda *_a, **_kw: {
            "ok": True,
            "data": {"media": [{"ref": "/media/other/photo.jpg"}]},
        },
    )

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": "SK/JOB/2606/2372"}))

    assert "error" in result
    assert "INVALID_MEDIA_REF" in result["error"]


def test_tgg_case_photos_known_case_without_media_is_graceful(monkeypatch, tmp_path):
    import tools.pa_business_tools as pbt

    root = tmp_path / "systems-media"
    root.mkdir()
    monkeypatch.setattr(
        pbt,
        "_load_runtime_bridge_config",
        lambda: PABusinessBridgeConfig(operations={}, media_root=root),
    )
    monkeypatch.setattr(
        pbt,
        "execute_business_operation",
        lambda *_a, **_kw: {"ok": True, "data": {"files": [], "count": 0}},
    )

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": "SK/JOB/2604/2376"}))

    assert result == {
        "ok": True,
        "jobNo": "SK/JOB/2604/2376",
        "photos": [],
        "count": 0,
        "message": "no retained case photos",
    }


@pytest.mark.parametrize("job_no", ["123", "42", "/tmp/photo.jpg", "SK/JOB/1/2"])
def test_tgg_case_photos_accepts_only_real_job_numbers(job_no):
    import tools.pa_business_tools as pbt

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": job_no}))
    assert "error" in result
    assert "INVALID_JOB_NO" in result["error"]


def test_tgg_case_photos_refuses_traversal_without_path_disclosure(monkeypatch, tmp_path):
    import tools.pa_business_tools as pbt

    root = tmp_path / "systems-media"
    root.mkdir()
    outside = tmp_path / "secret.jpg"
    _jpeg(outside)
    monkeypatch.setattr(
        pbt,
        "_load_runtime_bridge_config",
        lambda: PABusinessBridgeConfig(operations={}, media_root=root),
    )
    monkeypatch.setattr(
        pbt,
        "execute_business_operation",
        lambda *_a, **_kw: {"ok": True, "files": [{"ref": "/media/../secret.jpg"}]},
    )

    result = json.loads(pbt._handle_tgg_case_photos({"job_no": "SK/JOB/2604/2376"}))

    assert "error" in result
    assert "INVALID_MEDIA_REF" in result["error"]
    assert str(outside) not in result["error"]


def test_tenant_scoped_http_operation_injects_client_auth(fake_business_endpoint):
    from agent.pa_constitution import resolve_context

    constitution = {
        "id": "bobby",
        "agent_name": "Bobby",
        "identity": {"role": "assistant"},
        "client": {
            "name": "TGG",
            "tenant": "tgg",
            "business_bridge": {
                "auth": {
                    "type": "header",
                    "header": "X-TGG-Token",
                    "token": "tgg-secret",
                },
                "operations": {
                    "update_case": {
                        "type": "http",
                        "tenant": "tgg",
                        "url": fake_business_endpoint,
                    }
                },
            },
        },
        "job_briefs": {
            "ops": {"title": "Ops", "purpose": "Ops", "instructions": ["Do ops."]}
        },
    }
    pa_context = resolve_context({"constitution": constitution, "job_type": "ops"}, {})

    result = execute_business_operation(
        {"pa_business": {"operations": {}}},
        "update_case",
        {"case_id": "C-789"},
        pa_context=pa_context,
    )

    assert result["ok"] is True
    assert _FakeBusinessHandler.received["tgg_token"] == "tgg-secret"


class _PathParamHandler(BaseHTTPRequestHandler):
    """Records path + payload for any method, useful for path-param tests."""

    last_request: dict = {}

    def _record(self, body_bytes: bytes) -> None:
        try:
            payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
        except json.JSONDecodeError:
            payload = {"__raw": body_bytes.decode("utf-8", errors="replace")}
        type(self).last_request = {
            "method": self.command,
            "path": self.path,
            "payload": payload,
            "authorization": self.headers.get("Authorization"),
            "ps_tenant": self.headers.get("X-PS-Tenant"),
        }
        body = json.dumps({"ok": True, "echoPath": self.path}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._record(b"")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self._record(self.rfile.read(length))

    def do_PATCH(self):
        length = int(self.headers.get("Content-Length", "0"))
        self._record(self.rfile.read(length))

    def log_message(self, _format, *_args):
        return


@pytest.fixture
def path_param_endpoint():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _PathParamHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_path_param_interpolation_get(path_param_endpoint):
    """GET with one path param + remaining payload becomes query string."""
    config = {
        "pa_business": {
            "operations": {
                "case_lookup": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases/{{jobNo}}",
                    "path_params": ["jobNo"],
                    "headers": {"X-PS-Tenant": "tgg"},
                }
            }
        }
    }

    result = execute_business_operation(
        config, "case_lookup", {"jobNo": "AM/JOB/2605/0112"}
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "GET"
    # Slashes in jobNo must be percent-encoded (safe="").
    assert last["path"] == "/api/operator/cases/AM%2FJOB%2F2605%2F0112"
    assert last["payload"] == {}
    assert last["ps_tenant"] == "tgg"


def test_case_search_accepts_model_query_alias(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "tgg_case_search": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases",
                }
            }
        }
    }

    result = execute_business_operation(
        config, "tgg_case_search", {"query": "350 Anchorvale Rd", "limit": 10}
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "GET"
    assert "search=350+Anchorvale+Rd" in last["path"]
    assert "query=" not in last["path"]
    assert "limit=10" in last["path"]


def test_case_search_accepts_structured_fields_as_single_query(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "tgg_case_search": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases",
                }
            }
        }
    }

    result = execute_business_operation(
        config,
        "tgg_case_search",
        {
            "block": "350",
            "street": "Anchorvale Rd",
            "unit": "11-109",
            "workType": "window grille install",
            "zone": "SK",
            "limit": 10,
        },
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "GET"
    assert "search=Blk+350+Anchorvale+Rd+%2311-109" in last["path"]
    assert "window+grille+install" not in last["path"]
    # Structured anchors are PRESERVED — the API runs the tiered candidate
    # search on them (unit_exact > job_no > block_street_fuzzy > text_like).
    assert "block=350" in last["path"]
    assert "unit=11-109" in last["path"]
    # Street feeds the composed search text only; work/zone never pass through.
    assert "street=" not in last["path"]
    assert "workType=" not in last["path"]
    assert "zone=SK" not in last["path"]
    assert "limit=10" in last["path"]


def test_case_search_uses_work_text_only_when_no_address(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "tgg_case_search": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases",
                }
            }
        }
    }

    result = execute_business_operation(
        config,
        "tgg_case_search",
        {
            "workType": "window grille install",
            "limit": 10,
        },
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "GET"
    assert "search=window+grille+install" in last["path"]
    assert "workType=" not in last["path"]


def test_path_param_interpolation_patch_keeps_remaining_payload_in_body(path_param_endpoint):
    """PATCH with path param: jobNo goes in URL, remaining payload becomes JSON body."""
    config = {
        "pa_business": {
            "operations": {
                "case_state_update": {
                    "type": "http",
                    "method": "PATCH",
                    "url": f"{path_param_endpoint}/api/operator/cases/{{jobNo}}/state",
                    "path_params": ["jobNo"],
                }
            }
        }
    }

    result = execute_business_operation(
        config,
        "case_state_update",
        {"jobNo": "BS/JOB/2605/0087", "state": "completed"},
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "PATCH"
    assert last["path"] == "/api/operator/cases/BS%2FJOB%2F2605%2F0087/state"
    # jobNo was popped from payload; only state remains in the body.
    assert last["payload"] == {"state": "completed"}


def test_case_create_operation_posts_body_without_path_params(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "case_create": {
                    "type": "http",
                    "method": "POST",
                    "url": f"{path_param_endpoint}/api/operator/cases/create",
                    "headers": {"X-PS-Tenant": "tgg"},
                }
            }
        }
    }

    result = execute_business_operation(
        config,
        "case_create",
        {
            "zone": "B15",
            "address": "Blk 771 #04-18",
            "problem": "Ceiling leak",
            "source": "whatsapp:tgg-ops:test",
            "photos": ["p1", "p2"],
        },
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "POST"
    assert last["path"] == "/api/operator/cases/create"
    assert last["payload"]["address"] == "Blk 771 #04-18"
    assert last["payload"]["photos"] == ["p1", "p2"]
    assert last["ps_tenant"] == "tgg"


def test_path_param_missing_from_payload_fails_loudly(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "case_lookup": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases/{{jobNo}}",
                    "path_params": ["jobNo"],
                }
            }
        }
    }
    with pytest.raises(ValueError, match="requires path_param 'jobNo'"):
        execute_business_operation(config, "case_lookup", {})


def test_clarification_request_posts_to_clarifications_endpoint(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "tgg_clarification_request": {
                    "type": "http",
                    "method": "POST",
                    "url": f"{path_param_endpoint}/api/operator/clarifications",
                }
            }
        }
    }

    result = execute_business_operation(
        config,
        "tgg_clarification_request",
        {
            "question": "Is the 182 Rivervale grille report the same job as SK/JOB/2603/1728?",
            "candidate_job_nos": ["SK/JOB/2603/1728"],
            "evidence_message_refs": ["wa:12345"],
            "context": "Completed case matches new same-shape work.",
        },
    )

    assert result["ok"] is True
    last = _PathParamHandler.last_request
    assert last["method"] == "POST"
    assert last["path"].startswith("/api/operator/clarifications")
    assert last["payload"]["question"].startswith("Is the 182 Rivervale")
    assert last["payload"]["candidate_job_nos"] == ["SK/JOB/2603/1728"]
    assert last["payload"]["evidence_message_refs"] == ["wa:12345"]


def test_case_lookup_rejects_unit_as_job_number(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "case_lookup": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases/{{jobNo}}",
                    "path_params": ["jobNo"],
                }
            }
        }
    }
    with pytest.raises(ValueError, match="INVALID_JOB_NO"):
        execute_business_operation(config, "case_lookup", {"jobNo": "11-109"})


def test_path_param_without_placeholder_fails_loudly(path_param_endpoint):
    config = {
        "pa_business": {
            "operations": {
                "broken": {
                    "type": "http",
                    "method": "GET",
                    "url": f"{path_param_endpoint}/api/operator/cases",
                    "path_params": ["jobNo"],
                }
            }
        }
    }
    with pytest.raises(ValueError, match="URL has no \\{jobNo\\} placeholder"):
        execute_business_operation(config, "broken", {"jobNo": "SK/JOB/2604/2376"})


def test_wrong_tenant_operation_fails_loudly(fake_business_endpoint):
    from agent.pa_constitution import resolve_context

    constitution = {
        "id": "bobby",
        "agent_name": "Bobby",
        "identity": {"role": "assistant"},
        "client": {
            "name": "TGG",
            "tenant": "tgg",
            "business_bridge": {
                "auth": {
                    "type": "header",
                    "header": "X-TGG-Token",
                    "token": "tgg-secret",
                },
                "operations": {
                    "mofex_lookup": {
                        "type": "http",
                        "tenant": "mofex",
                        "url": fake_business_endpoint,
                    }
                },
            },
        },
        "job_briefs": {
            "ops": {"title": "Ops", "purpose": "Ops", "instructions": ["Do ops."]}
        },
    }
    pa_context = resolve_context({"constitution": constitution, "job_type": "ops"}, {})

    with pytest.raises(TenantScopeMismatch, match="TENANT_SCOPE_MISMATCH"):
        execute_business_operation(
            {"pa_business": {"operations": {}}},
            "mofex_lookup",
            {"case_id": "M-1"},
            pa_context=pa_context,
        )


# ── generic-vs-tgg tool param hygiene + completion verb (v6) ──────────────


@pytest.fixture
def captured_reads(monkeypatch):
    """Capture (operation, payload) from the read handlers without HTTP."""
    import tools.pa_business_tools as pbt

    captured = []

    def _fake_read(operation, payload):
        captured.append((operation, dict(payload)))
        return json.dumps({"ok": True})

    monkeypatch.setattr(pbt, "_handle_tgg_read", _fake_read)
    return captured


@pytest.fixture
def captured_writes(monkeypatch):
    """Capture (operation, payload) from the write handlers without HTTP."""
    import tools.pa_business_tools as pbt

    captured = []

    def _fake_write(operation, payload):
        captured.append((operation, dict(payload)))
        return json.dumps({"ok": True})

    monkeypatch.setattr(pbt, "_handle_tgg_write", _fake_write)
    return captured


def test_generic_message_history_search_carries_agnostic_params_only(
    captured_reads, monkeypatch
):
    import tools.pa_business_tools as pbt

    monkeypatch.delenv("HERMES_PA_HISTORY_BEFORE_TS", raising=False)
    pbt._handle_generic_message_history_search(
        {
            "q": "epoxy leak",
            "chat_jid": "123@g.us",
            "before_ts": 1760000000,
            "limit": 10,
            # Client-shaped params a model might pass anyway — must be DROPPED
            "block": "350",
            "unit": "#11-109",
            "jobNo": "SK/JOB/2604/2376",
        }
    )
    op, payload = captured_reads[0]
    assert op == "tgg_message_history_search"
    assert payload == {
        "q": "epoxy leak",
        "chat_jid": "123@g.us",
        "before_ts": 1760000000,
        "limit": 10,
    }


def test_generic_message_history_search_schema_is_agnostic():
    from tools.pa_business_tools import (
        MESSAGE_HISTORY_SEARCH_SCHEMA,
        TGG_MESSAGE_HISTORY_SEARCH_SCHEMA,
    )

    generic = set(MESSAGE_HISTORY_SEARCH_SCHEMA["parameters"]["properties"])
    assert generic == {"q", "chat_jid", "before_ts", "limit"}
    tgg = set(TGG_MESSAGE_HISTORY_SEARCH_SCHEMA["parameters"]["properties"])
    assert {"block", "unit", "jobNo"} <= tgg


def test_before_ts_clamped_by_replay_future_cap(captured_reads, monkeypatch):
    import tools.pa_business_tools as pbt

    monkeypatch.setenv("HERMES_PA_HISTORY_BEFORE_TS", "1700000000")
    pbt._handle_generic_message_history_search(
        {"q": "leak", "before_ts": 1760000000}
    )
    _, payload = captured_reads[0]
    assert payload["before_ts"] == 1700000000  # cap wins over later agent value

    pbt._handle_tgg_message_history_search({"q": "leak", "before_ts": 1600000000})
    _, payload2 = captured_reads[1]
    assert payload2["before_ts"] == 1600000000  # earlier agent value kept


def test_generic_clarification_request_maps_candidate_refs(captured_writes):
    import tools.pa_business_tools as pbt

    pbt._handle_generic_clarification_request(
        {
            "question": "Same job?",
            "candidate_refs": ["SK/JOB/2603/1728"],
            "evidence_message_refs": ["wa:1"],
            "context": "ambiguous",
        }
    )
    op, payload = captured_writes[0]
    assert op == "tgg_clarification_request"
    assert payload["candidate_job_nos"] == ["SK/JOB/2603/1728"]
    assert payload["question"] == "Same job?"


def test_generic_clarification_request_schema_is_agnostic():
    from tools.pa_business_tools import CLARIFICATION_REQUEST_SCHEMA

    props = set(CLARIFICATION_REQUEST_SCHEMA["parameters"]["properties"])
    assert props == {"question", "candidate_refs", "evidence_message_refs", "context"}


def test_case_update_state_maps_contract_payload(captured_writes):
    import tools.pa_business_tools as pbt

    out = pbt._handle_tgg_case_update_state(
        {
            "job_no": "SK/JOB/2604/2376",
            "state": "completed",
            "evidence_message_refs": ["wa:9", "wa:10"],
            "observed_at": "2026-06-10T14:00:00+08:00",
        }
    )
    assert json.loads(out)["ok"] is True
    op, payload = captured_writes[0]
    assert op == "tgg_case_update_state"
    assert payload == {
        "jobNo": "SK/JOB/2604/2376",
        "state": "completed",
        "evidenceMessageRefs": ["wa:9", "wa:10"],
        # ISO-8601 input is coerced to epoch seconds at the tool boundary
        # (the backend expects epoch; see the observed_at coercion tests).
        "observedAt": 1781071200,
    }


def test_case_update_state_coerces_iso_observed_at_to_epoch(captured_writes):
    """REPRO of the sk-day26-v6 wasted retry: christopher sent observed_at as
    ISO-8601 ('2026-05-26T11:02:58+08:00'), the backend rejected it, and the
    agent burned a second call resending epoch. The tool must coerce ISO to
    epoch seconds BEFORE posting. FAILS on pre-fix code (ISO string passed
    through verbatim)."""
    import tools.pa_business_tools as pbt

    pbt._handle_tgg_case_update_state(
        {
            "job_no": "SK/JOB/2601/2304",
            "state": "completed",
            "observed_at": "2026-05-26T11:02:58+08:00",
        }
    )
    _, payload = captured_writes[0]
    assert payload["observedAt"] == 1779764578
    assert isinstance(payload["observedAt"], int)


def test_case_update_state_naive_iso_observed_at_assumed_sgt(captured_writes):
    """Naive ISO timestamps are treated as SGT (TGG operates Asia/Singapore)."""
    import tools.pa_business_tools as pbt

    pbt._handle_tgg_case_update_state(
        {
            "job_no": "SK/JOB/2601/2304",
            "state": "completed",
            "observed_at": "2026-05-26T11:02:58",
        }
    )
    _, payload = captured_writes[0]
    assert payload["observedAt"] == 1779764578


def test_case_update_state_epoch_observed_at_passthrough(captured_writes):
    """Epoch input (int or numeric string) lands as an int; unparseable text
    passes through unchanged so the backend stays the rejection authority."""
    import tools.pa_business_tools as pbt

    pbt._handle_tgg_case_update_state(
        {"job_no": "SK/JOB/2601/2304", "state": "completed", "observed_at": 1779764578}
    )
    pbt._handle_tgg_case_update_state(
        {"job_no": "SK/JOB/2601/2304", "state": "completed", "observed_at": "1779764578"}
    )
    pbt._handle_tgg_case_update_state(
        {"job_no": "SK/JOB/2601/2304", "state": "completed", "observed_at": "around noon"}
    )
    assert captured_writes[0][1]["observedAt"] == 1779764578
    assert captured_writes[1][1]["observedAt"] == 1779764578
    assert captured_writes[2][1]["observedAt"] == "around noon"


def test_case_update_state_schema_says_epoch_seconds():
    """The schema description must steer the model to epoch seconds (the
    'SGT or ISO format' description caused the ISO-first attempt)."""
    from tools.pa_business_tools import TGG_CASE_UPDATE_STATE_SCHEMA

    desc = TGG_CASE_UPDATE_STATE_SCHEMA["parameters"]["properties"]["observed_at"][
        "description"
    ]
    assert "epoch" in desc.lower()
    assert "seconds" in desc.lower()


def test_case_update_state_rejects_non_completed(captured_writes):
    import tools.pa_business_tools as pbt

    out = pbt._handle_tgg_case_update_state(
        {"job_no": "SK/JOB/2604/2376", "state": "cancelled"}
    )
    assert "only accepts state='completed'" in out
    assert captured_writes == []

    out2 = pbt._handle_tgg_case_update_state({"state": "completed"})
    assert "requires job_no" in out2
    assert captured_writes == []


def test_case_update_state_operation_in_production_config():
    raw = yaml.safe_load(TGG_PRODUCTION_CONFIG.read_text(encoding="utf-8"))
    pa_context = SimpleNamespace(
        constitution=SimpleNamespace(client=raw["pa"]["overlay"]["client"])
    )
    bridge = load_business_bridge_config(raw, pa_context=pa_context)
    op = bridge.operations["tgg_case_update"]
    assert op.method == "PATCH"
    assert op.url.endswith("/api/operator/cases/{jobNo}/state")


class TestCaseCreateJobNoContract:
    """jobNo alias coercion + omission gate (PG day-26 WA-mint regression)."""

    def _create(self, monkeypatch, args):
        import tools.pa_business_tools as pbt
        captured = {}

        def fake_write(op, payload):
            captured.update(payload)
            return '{"ok": true}'

        monkeypatch.setattr(pbt, "_handle_tgg_write", fake_write)
        result = pbt._handle_tgg_case_create(args)
        return result, captured

    def test_reported_job_no_alias_coerced(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "reportedJobNo": "PG/JOB/2605/0973",
            "receivedAt": 1_784_476_800,
        })
        assert captured.get("jobNo") == "PG/JOB/2605/0973"
        assert "reportedJobNo" not in captured

    def test_omitted_job_no_with_evidence_token_bounces(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "evidence": {"messageText": "Job no: PG/JOB/2605/0980\nRemarks: y"},
        })
        assert "JOB_NO_OMITTED" in result
        assert "PG/JOB/2605/0980" in result
        assert not captured  # write never happened

    def test_confirm_no_job_no_escape_hatch(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "confirmNoJobNo": True,
            "receivedAt": 1_784_476_800,
            "evidence": {"messageText": "previous job SK/JOB/2603/1709 not attended"},
        })
        assert "JOB_NO_OMITTED" not in result
        assert "confirmNoJobNo" not in captured

    def test_create_without_job_no_bounces(self, monkeypatch):
        """No jobNo + no confirmNoJobNo = corrective error: cases enter the
        ledger only from HDB job sheets (no WA placeholder minting)."""
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "evidence": {"messageText": "tenant reports leak, no job sheet"},
        })
        assert "JOB_NO_REQUIRED" in result
        assert "tgg_case_observation" in result
        assert "tgg_clarification_request" in result
        assert not captured  # write never happened

    def test_operator_instructed_no_job_no_create_passes(self, monkeypatch):
        """confirmNoJobNo is the explicit-operator-instruction escape hatch."""
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "confirmNoJobNo": True,
            "receivedAt": 1_784_476_800,
            "evidence": {"messageText": "tenant reports leak, no job sheet"},
        })
        assert "JOB_NO_REQUIRED" not in result
        assert "JOB_NO_OMITTED" not in result
        assert "confirmNoJobNo" not in captured

    def test_create_schema_requires_hdb_job_no(self):
        from tools.pa_business_tools import TGG_CASE_CREATE_SCHEMA

        desc = TGG_CASE_CREATE_SCHEMA["description"]
        assert "HDB job number" in desc
        assert "not allowed" in desc
        job_no_desc = TGG_CASE_CREATE_SCHEMA["parameters"]["properties"]["jobNo"]["description"]
        assert "minted" not in job_no_desc
        confirm_desc = TGG_CASE_CREATE_SCHEMA["parameters"]["properties"]["confirmNoJobNo"]["description"]
        assert "operator" in confirm_desc.lower()

    def test_explicit_job_no_passes_through(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "address": "Blk 1 Test St #01-01", "problem": "x",
            "jobNo": "SK/JOB/2605/2564",
            "receivedAt": 1_784_476_800,
        })
        assert captured.get("jobNo") == "SK/JOB/2605/2564"

    def test_date_only_receipt_is_byte_identical_across_repeat_runs(
        self, monkeypatch
    ):
        args = {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
            "receiptDate": "20 July 2026",
        }
        first_result, first = self._create(monkeypatch, args)
        second_result, second = self._create(monkeypatch, args)

        assert json.loads(first_result)["ok"] is True
        assert first["receivedAt"] == 1_784_476_800
        assert first["dueAt"] == 1_787_068_800
        assert "receiptDate" not in first
        assert json.dumps(first, sort_keys=True).encode() == json.dumps(
            second, sort_keys=True
        ).encode()
        assert first_result.encode() == second_result.encode()

    def test_invalid_explicit_receipt_date_fails_closed(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
            "receivedAt": 1_784_509_200,
            "receiptDate": "sometime last Monday",
        })
        assert "receiptDate must be a date only" in result
        assert captured == {}

    def test_explicit_receipt_source_precedes_broader_evidence_text(
        self, monkeypatch
    ):
        result, captured = self._create(monkeypatch, {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
            "receiptDate": "20 July 2026",
            "evidence": {
                "messageText": "Previous case receipt date: 1 June 2026."
            },
        })
        assert json.loads(result)["ok"] is True
        assert captured["receivedAt"] == 1_784_476_800
        assert captured["dueAt"] == 1_787_068_800

    def test_explicit_evidence_receipt_date_overrides_model_epoch(
        self, monkeypatch
    ):
        result, captured = self._create(monkeypatch, {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
            "receivedAt": 1_784_509_200,
            "evidence": {
                "receipt_date": "20 July 2026",
                "messageText": "Previous case receipt date: 1 June 2026.",
            },
        })
        assert json.loads(result)["ok"] is True
        assert captured["receivedAt"] == 1_784_476_800
        assert captured["dueAt"] == 1_787_068_800

    def test_generic_case_create_preparation_uses_same_runtime_contract(self):
        import tools.pa_business_tools as pbt

        prepared = pbt._prepare_case_create_receipt({
            "receivedAt": 1_784_509_200,
            "receipt_date": "20 July 2026",
            "dueAt": 42,
        })
        assert isinstance(prepared, dict)
        assert prepared["receivedAt"] == 1_784_476_800
        assert prepared["dueAt"] == 1_787_068_800
        assert "receipt_date" not in prepared

    def test_runtime_overwrites_model_due_at(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
            "receiptDate": "20 July 2026",
            "dueAt": 42,
        })
        assert json.loads(result)["ok"] is True
        assert captured["dueAt"] == 1_787_068_800

    def test_create_requires_epoch_or_date_source(self, monkeypatch):
        result, captured = self._create(monkeypatch, {
            "jobNo": "AM/JOB/2607/9901",
            "address": "Blk 1 Test St #01-01",
            "problem": "x",
        })
        assert "requires receivedAt" in result
        assert captured == {}

    def test_schema_exposes_receipt_date_without_model_due_at(self):
        import tools.pa_business_tools as pbt

        parameters = pbt.TGG_CASE_CREATE_SCHEMA["parameters"]
        assert parameters["properties"]["receiptDate"]["type"] == "string"
        assert parameters["properties"]["receivedAt"]["type"] == "integer"
        assert "dueAt" not in parameters["properties"]

    def test_generated_runtime_slots_carry_receipt_date_instruction(self):
        slots_root = (
            Path(__file__).parents[1]
            / "deploy"
            / "tgg"
            / "christopher"
            / "runtime-slots"
        )
        for slot in ("gpt-5.4-mini", "gpt-5.6-luna", "gpt-5.6-luna-low"):
            constitution = yaml.safe_load(
                (slots_root / slot / "christopher_tgg_constitution.yaml").read_text()
            )
            joined = "\n".join(
                constitution["job_briefs"]["tgg_ops_ingest"]["instructions"]
            )
            assert "exact date-only source string in receiptDate" in joined
            assert "Do not calculate midnight or dueAt" in joined
            assert "runtime mechanically normalizes receiptDate" in joined
