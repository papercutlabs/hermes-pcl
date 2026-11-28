"""Per-chat business-operation scoping.

Christopher runs one process across TGG's ingest chats and its management
chats. The selector picks the job brief; the brief's ``business_operations``
block decides which configured operations survive into the runtime registry for
that chat. Management carries the full case-shaped registry; ingest cannot read
case photos or invoke the runtime-only media-retention convergence operation.
"""

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from agent.pa_constitution import (
    _load_business_operations,
    load_constitution_data,
    resolve_context,
)
from tools.pa_business_tools import (
    OperationNotPermitted,
    execute_business_operation,
    load_business_bridge_config,
)


DEPLOY_ROOT = Path(__file__).parents[1] / "deploy" / "tgg" / "christopher"
TGG_CONFIG = DEPLOY_ROOT / "config.yaml"
TGG_CONSTITUTION = DEPLOY_ROOT / "christopher_tgg_constitution.yaml"

# A real management chat from the deployed selector list (the rung-3a demo
# group) and a real ingest chat, so the tests fail if the selectors move.
MGMT_CHAT_ID = "120363426509183563@g.us"
INGEST_CHAT_ID = "120363403088884777@g.us"

# Case-shaped operations management is explicitly authorized to use.
MANAGEMENT_CASE_OPERATIONS = (
    "tgg_case_create",
    "tgg_case_observation",
    "tgg_case_update",
    "tgg_case_wc_attach",
    "tgg_clarification_raise",
)

# The management brief is deliberately an explicit allow-list. Keep this as a
# set rather than a count: a new bridge operation is an authority decision,
# not merely another item in a total.
MANAGEMENT_OPERATION_SET = {
    "agent_action_record",
    "agent_config_read",
    "ilinked_lookup",
    "ilinked_status",
    "ilinked_wc_lookup",
    "job_work_costings",
    "message_search",
    "tgg_attention_annotate",
    "tgg_attention_list",
    "tgg_attention_raise",
    "tgg_attention_read",
    "tgg_case_create",
    "tgg_case_list",
    "tgg_case_lookup",
    "tgg_case_media",
    "tgg_case_observation",
    "tgg_case_query",
    "tgg_case_search",
    "tgg_case_update",
    "tgg_case_wc_attach",
    "tgg_clarification_raise",
    "tgg_human_resolution_apply_case_update",
    "tgg_human_resolution_create",
    "tgg_human_resolution_document_append",
    "tgg_human_resolution_document_context",
    "work_costing_ingest_ilinked",
    "work_costing_lookup",
    "work_costing_upsert",
}


def _config():
    return yaml.safe_load(TGG_CONFIG.read_text(encoding="utf-8"))


def _resolve(chat_id: str):
    """Resolve a job brief the way the runtime does: platform + chat_id."""
    config = _config()
    pa_config = dict(config["pa"])
    # The deployed config ships with the processing gate closed and points at an
    # absolute on-host constitution path; neither is reachable from a test host.
    pa_config["enabled"] = True
    pa_config.pop("constitution_path", None)
    pa_config["constitution"] = load_constitution_data(
        yaml.safe_load(TGG_CONSTITUTION.read_text(encoding="utf-8")),
        source=str(TGG_CONSTITUTION),
    )
    resolved = resolve_context(
        pa_config,
        {"source": {"platform": "whatsapp", "chat_id": chat_id}},
    )
    assert resolved is not None, chat_id
    return resolved


def _bridge(chat_id: str):
    return load_business_bridge_config(_config(), pa_context=_resolve(chat_id))


def _unscoped_bridge():
    """The client's full registry, resolved without any job brief.

    The operations live under the client overlay, so a bare ``pa_context=None``
    yields an empty registry rather than an unscoped one — this reference has
    the client but no brief, which is what "unscoped" actually means here.
    """
    config = _config()
    return load_business_bridge_config(
        config,
        pa_context=SimpleNamespace(
            constitution=SimpleNamespace(client=config["pa"]["overlay"]["client"])
        ),
    )


def test_cron_pa_job_type_resolves_management_business_registry(monkeypatch):
    """A scheduled management job gets the same dedicated read surface."""
    import tools.pa_business_tools as business_tools
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.registry import registry

    config = _config()
    config["pa"] = dict(config["pa"])
    config["pa"]["enabled"] = True
    config["pa"].pop("constitution_path", None)
    config["pa"]["constitution"] = load_constitution_data(
        yaml.safe_load(TGG_CONSTITUTION.read_text(encoding="utf-8")),
        source=str(TGG_CONSTITUTION),
    )
    monkeypatch.setattr("hermes_cli.config.read_raw_config", lambda: config)
    executed = []
    monkeypatch.setattr(
        business_tools,
        "execute_business_operation",
        lambda bridge, operation, payload: executed.append((bridge, operation, payload)) or {"ok": True},
    )
    tokens = set_session_vars(platform="", chat_id="", pa_job_type="tgg_management")
    try:
        entry = registry.get_entry("tgg_case_query")
        assert entry is not None
        assert entry.check_fn() is True
        response = entry.handler({"sql": "SELECT 1"})
    finally:
        clear_session_vars(tokens)

    assert '"ok": true' in response
    assert executed[0][1] == "tgg_case_query"
    assert executed[0][2] == {"sql": "SELECT 1"}


@pytest.mark.parametrize("entrance", ["dedicated", "pa_business_read"])
def test_large_case_query_becomes_session_sandbox_artifact(
    entrance, tmp_path, monkeypatch
):
    import json
    import tools.pa_business_tools as business_tools
    import hermes_constants
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.python_sandbox_tool import _workspace_key

    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    bridge = SimpleNamespace(
        operations={"tgg_case_query": SimpleNamespace(path_params=())}
    )
    monkeypatch.setattr(
        business_tools, "_load_runtime_bridge_config", lambda: bridge
    )
    rows = [[index, "x" * 120] for index in range(200)]
    monkeypatch.setattr(
        business_tools, "execute_business_operation",
        lambda *_args, **_kwargs: {"ok": True, "columns": ["n", "value"], "rows": rows},
    )
    tokens = set_session_vars(
        session_key="stable-report-owner",
        session_id="compressed-report-session",
    )
    try:
        if entrance == "dedicated":
            raw = business_tools._handle_tgg_case_query({"sql": "SELECT * FROM t"})
        else:
            raw = business_tools._handle_business_read(
                {
                    "operation": "tgg_case_query",
                    "payload": {"sql": "SELECT * FROM t"},
                }
            )
        response = json.loads(raw)
    finally:
        clear_session_vars(tokens)

    assert response["sandbox_artifact"].startswith("/work/pa-query-")
    assert "datasets omitted" in response["sandbox_artifact_usage"]
    assert "do not delegate" in response["sandbox_artifact_usage"]
    path = tmp_path / "sandbox_workspaces" / _workspace_key("stable-report-owner") / "work" / response["sandbox_artifact"].split("/")[-1]
    artifact_bytes = path.read_bytes()
    assert hashlib.sha256(artifact_bytes).hexdigest() == response["sandbox_artifact_sha256"]
    assert json.loads(artifact_bytes)["rows"] == rows


def test_large_case_query_applies_configured_reader_policy(tmp_path, monkeypatch):
    import json
    import tools.pa_business_tools as business_tools
    import tools.python_sandbox_access as reader_access
    import tools.python_sandbox_tool as sandbox_tool
    import hermes_constants
    from gateway.session_context import clear_session_vars, set_session_vars

    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        sandbox_tool, "_load_config", lambda: {"reader_identity": "tggcapture"}
    )
    monkeypatch.setattr(reader_access, "configured_reader_uid", lambda _config: "999")
    granted = []
    monkeypatch.setattr(
        reader_access,
        "grant_reader_work_access",
        lambda home, work, uid: granted.append((home, work, uid)),
    )
    rows = [[index, "x" * 120] for index in range(200)]
    tokens = set_session_vars(session_key="stable-report-owner")
    try:
        result = business_tools._materialize_large_tgg_query_result(
            "tgg_case_query",
            {"ok": True, "columns": ["n", "value"], "rows": rows},
        )
    finally:
        clear_session_vars(tokens)

    assert result["sandbox_artifact"].startswith("/work/pa-query-")
    assert granted == [
        (
            tmp_path,
            tmp_path
            / "sandbox_workspaces"
            / sandbox_tool._workspace_key("stable-report-owner")
            / "work",
            "999",
        )
    ]


def test_small_generic_case_query_stays_inline(tmp_path, monkeypatch):
    import json
    import tools.pa_business_tools as business_tools
    import hermes_constants
    from gateway.session_context import clear_session_vars, set_session_vars

    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    bridge = SimpleNamespace(
        operations={"tgg_case_query": SimpleNamespace(path_params=())}
    )
    monkeypatch.setattr(
        business_tools, "_load_runtime_bridge_config", lambda: bridge
    )
    monkeypatch.setattr(
        business_tools,
        "execute_business_operation",
        lambda *_args, **_kwargs: {"ok": True, "columns": ["n"], "rows": [[1]]},
    )
    tokens = set_session_vars(session_id="small-query-session")
    try:
        response = json.loads(
            business_tools._handle_business_read(
                {
                    "operation": "tgg_case_query",
                    "payload": {"sql": "SELECT 1"},
                }
            )
        )
    finally:
        clear_session_vars(tokens)

    assert response == {"ok": True, "columns": ["n"], "rows": [[1]]}
    assert not (tmp_path / "sandbox_workspaces").exists()


# ── loader validation ────────────────────────────────────────────────────────


def test_absent_block_leaves_operations_unscoped():
    assert _load_business_operations({}, "src") == {}
    assert _load_business_operations(None, "src") == {}


def test_allowed_list_is_deduped_in_author_order():
    loaded = _load_business_operations({"allowed": ["b", "a", "b"]}, "src")
    assert loaded == {"allowed": ("b", "a")}


def test_empty_allowlist_is_rejected():
    # An empty allowlist would silently deny every operation; the author almost
    # certainly meant to omit the key.
    with pytest.raises(ValueError, match="must not be empty"):
        _load_business_operations({"allowed": []}, "src")


def test_unknown_scoping_key_is_rejected():
    with pytest.raises(ValueError, match="unknown keys"):
        _load_business_operations({"allow": ["x"]}, "src")


def test_denied_list_is_supported():
    loaded = _load_business_operations({"denied": ["x"]}, "src")
    assert loaded == {"denied": ("x",)}


def test_business_operations_participate_in_the_job_hash():
    """A scope change must move the behavior hash, or deploys can't detect it."""
    base = {
        "id": "x",
        "agent_name": "X",
        "identity": {},
        "client": {},
        "job_briefs": {
            "j": {"title": "t", "purpose": "p", "instructions": ["i"]},
        },
        "selectors": [],
    }
    scoped = {
        **base,
        "job_briefs": {
            "j": {
                "title": "t",
                "purpose": "p",
                "instructions": ["i"],
                "business_operations": {"allowed": ["a"]},
            },
        },
    }
    unscoped_hash = load_constitution_data(base, source="s").job_briefs["j"].hash
    scoped_hash = load_constitution_data(scoped, source="s").job_briefs["j"].hash
    assert unscoped_hash != scoped_hash


# ── registry scoping against the deployed artifacts ──────────────────────────


def test_management_chat_cannot_reach_ingest_write_operations():
    bridge = _bridge(MGMT_CHAT_ID)
    for operation in MANAGEMENT_CASE_OPERATIONS:
        assert operation in bridge.operations, operation


def test_management_chat_keeps_the_reads_its_instructions_depend_on():
    bridge = _bridge(MGMT_CHAT_ID)
    for operation in (
        "tgg_case_lookup",
        "tgg_case_search",
        "tgg_case_list",
        "message_search",
        "job_work_costings",
        "work_costing_lookup",
        "ilinked_lookup",
        "ilinked_status",
    ):
        assert operation in bridge.operations, operation


def test_management_chat_keeps_attention_write_and_observability():
    bridge = _bridge(MGMT_CHAT_ID)
    # The attention note is the single permitted write; record_event's backing
    # operation must survive or the brief's observability rule cannot be met.
    assert "tgg_attention_raise" in bridge.operations
    assert "agent_action_record" in bridge.operations


def test_management_chat_resolves_the_new_attention_operations():
    """Management gains the read/annotate mechanism the eval fix depends on."""
    bridge = _bridge(MGMT_CHAT_ID)
    for operation in ("tgg_attention_list", "tgg_attention_read", "tgg_attention_annotate"):
        assert operation in bridge.operations, operation


def test_management_chat_allows_case_photos_but_not_retention_primitive():
    bridge = _bridge(MGMT_CHAT_ID)
    assert "tgg_case_media" in bridge.operations
    assert "tgg_media_retention" not in bridge.operations
    assert "tgg_media_retention" in bridge.denied_operations


def test_management_operation_registry_is_exactly_authorized():
    bridge = _bridge(MGMT_CHAT_ID)
    assert set(bridge.operations) == MANAGEMENT_OPERATION_SET


def test_ingest_chat_resolves_the_new_attention_reads():
    ingest = _bridge(INGEST_CHAT_ID)
    for operation in ("tgg_attention_list", "tgg_attention_read"):
        assert operation in ingest.operations, operation


def test_ingest_chat_does_not_resolve_attention_annotate():
    """Ingest is unscoped-by-default but explicitly denies the mgmt-only write."""
    ingest = _bridge(INGEST_CHAT_ID)
    assert "tgg_attention_annotate" not in ingest.operations
    assert "tgg_attention_annotate" in ingest.denied_operations


def test_ingest_chat_denies_case_media_and_retention_primitive():
    ingest = _bridge(INGEST_CHAT_ID)
    for operation in ("tgg_case_media", "tgg_media_retention"):
        assert operation not in ingest.operations
        assert operation in ingest.denied_operations


def test_ingest_chat_still_resolves_its_write_operations():
    """Proof the denied-only block did not accidentally allow-filter the brief."""
    ingest = _bridge(INGEST_CHAT_ID)
    for operation in MANAGEMENT_CASE_OPERATIONS:
        assert operation in ingest.operations, operation


def test_scoping_does_not_invent_operations():
    """Every permitted name must exist in the client's configured registry."""
    mgmt = _bridge(MGMT_CHAT_ID)
    unscoped = _unscoped_bridge()
    assert set(mgmt.operations) <= set(unscoped.operations)
    assert set(mgmt.operations) | mgmt.denied_operations == set(unscoped.operations)
    for operation in ("tgg_attention_list", "tgg_attention_read", "tgg_attention_annotate"):
        assert operation in unscoped.operations, operation


# ── execution refusal ────────────────────────────────────────────────────────


def test_denied_operation_refuses_at_execution():
    bridge = _bridge(INGEST_CHAT_ID)
    with pytest.raises(OperationNotPermitted) as excinfo:
        execute_business_operation(
            bridge,
            operation="tgg_case_media",
            payload={"jobNo": "SK/JOB/2606/2372"},
        )
    assert excinfo.value.code == "OPERATION_NOT_PERMITTED"
    # The refusal names what IS available so the model can self-correct.
    assert "tgg_case_search" in str(excinfo.value)


def test_denied_operation_is_distinguishable_from_a_typo():
    bridge = _bridge(INGEST_CHAT_ID)
    with pytest.raises(ValueError) as denied:
        execute_business_operation(bridge, operation="tgg_case_media", payload={})
    with pytest.raises(ValueError) as unknown:
        execute_business_operation(bridge, operation="tgg_case_bogus", payload={})
    assert isinstance(denied.value, OperationNotPermitted)
    assert not isinstance(unknown.value, OperationNotPermitted)
    assert "unknown PA business operation" in str(unknown.value)


def test_legacy_case_update_alias_remains_available_to_management(monkeypatch):
    import tools.pa_business_tools as pbt

    monkeypatch.setattr(pbt, "_execute_http_operation", lambda *_a, **_kw: {"ok": True})
    assert execute_business_operation(
        _bridge(MGMT_CHAT_ID),
        operation="tgg_case_update_state",
        payload={"jobNo": "SK/JOB/2606/2372"},
    ) == {"ok": True}


def test_permitted_operation_passes_the_scope_check(monkeypatch):
    """Scoping must not break the allowed path.

    Transport is stubbed — these tests never reach the client's live systems API.
    """
    import tools.pa_business_tools as pbt

    seen = {}

    def fake_http(op, payload, bridge_config):
        seen.update(operation=op.name, payload=payload)
        return {"ok": True}

    monkeypatch.setattr(pbt, "_execute_http_operation", fake_http)
    result = execute_business_operation(
        _bridge(MGMT_CHAT_ID),
        operation="tgg_case_lookup",
        payload={"jobNo": "SK/JOB/2606/2372"},
    )
    assert result == {"ok": True}
    assert seen["operation"] == "tgg_case_lookup"


# ── the deployed constitution itself ─────────────────────────────────────────


def test_deployed_management_brief_declares_the_scope():
    resolved = _resolve(MGMT_CHAT_ID)
    assert resolved.job_type == "tgg_management"
    scope = resolved.job_brief.business_operations
    assert set(scope) == {"allowed"}
    for operation in MANAGEMENT_CASE_OPERATIONS + ("tgg_case_media",):
        assert operation in scope["allowed"], operation
    assert "tgg_media_retention" not in scope["allowed"]


def test_every_management_selector_shares_the_scope():
    """The scope is class-scoped to the brief, not pinned to one chat id."""
    constitution = load_constitution_data(
        yaml.safe_load(TGG_CONSTITUTION.read_text(encoding="utf-8")),
        source=str(TGG_CONSTITUTION),
    )
    mgmt_chats = [
        selector["match"]["source.chat_id"]
        for selector in constitution.selectors
        if selector["job_type"] == "tgg_management"
        and selector["match"].get("source.platform") == "whatsapp"
    ]
    assert len(mgmt_chats) > 1, mgmt_chats
    assert MGMT_CHAT_ID in mgmt_chats
    for chat_id in mgmt_chats:
        bridge = _bridge(chat_id)
        for operation in MANAGEMENT_CASE_OPERATIONS + ("tgg_case_media",):
            assert operation in bridge.operations, (chat_id, operation)
