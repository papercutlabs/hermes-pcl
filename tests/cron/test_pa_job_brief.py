from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cron.jobs import create_job, get_job
from cron.scheduler import _merge_cron_pa_toolsets, run_job
from gateway.session_context import clear_session_vars, set_session_vars
from tools.cronjob_tools import cronjob


FIXTURE = Path(__file__).parents[1] / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"
READONLY_INVESTIGATION_TOOLSET = "tgg-management-readonly-investigation"


def _pa_context(*, job_type="tgg_management", enabled=(), disabled=()):
    return SimpleNamespace(
        job_type=job_type,
        job_brief=SimpleNamespace(
            enabled_toolsets=list(enabled),
            disabled_toolsets=list(disabled),
        ),
    )


def test_explicit_tgg_readonly_delegation_survives_cron_default():
    enabled = [READONLY_INVESTIGATION_TOOLSET, "delegation"]

    actual_enabled, disabled = _merge_cron_pa_toolsets(
        None,
        ["cronjob", "messaging", "clarify", "delegation"],
        _pa_context(enabled=enabled),
    )

    assert actual_enabled == enabled
    assert disabled == ["cronjob", "messaging", "clarify"]


def test_explicit_tgg_readonly_delegation_brief_disable_still_wins():
    _, disabled = _merge_cron_pa_toolsets(
        None,
        ["delegation"],
        _pa_context(
            enabled=[READONLY_INVESTIGATION_TOOLSET, "delegation"],
            disabled=["delegation"],
        ),
    )

    assert disabled == ["delegation"]


def test_tgg_delegation_without_readonly_declaration_keeps_cron_default():
    _, disabled = _merge_cron_pa_toolsets(
        None,
        ["delegation"],
        _pa_context(enabled=["delegation"]),
    )

    assert disabled == ["delegation"]


@pytest.mark.parametrize(
    "pa_context",
    [
        None,
        _pa_context(
            job_type="tgg_ops_ingest",
            enabled=[READONLY_INVESTIGATION_TOOLSET, "delegation"],
        ),
    ],
)
def test_non_management_or_unbound_cron_keeps_delegation_disabled(pa_context):
    _, disabled = _merge_cron_pa_toolsets(None, ["delegation"], pa_context)

    assert disabled == ["delegation"]


@pytest.fixture()
def tmp_cron_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    return tmp_path


def _write_config(tmp_path: Path, constitution_path: Path = FIXTURE) -> None:
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "model": "test-model",
                "pa": {
                    "enabled": True,
                    "constitution_path": str(constitution_path),
                },
            }
        ),
        encoding="utf-8",
    )


def _write_readonly_management_constitution(tmp_path: Path) -> Path:
    constitution = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    management = constitution["job_briefs"]["tgg_management"]
    management["enabled_toolsets"] = [
        *management["enabled_toolsets"],
        READONLY_INVESTIGATION_TOOLSET,
        "delegation",
    ]
    path = tmp_path / "readonly-management-constitution.yaml"
    path.write_text(yaml.safe_dump(constitution), encoding="utf-8")
    return path


def test_create_job_stores_pa_job_type(tmp_cron_dir):
    job = create_job(
        prompt="daily TGG status",
        schedule="every 1h",
        pa_job_type="tgg_management",
    )

    assert job["pa_job_type"] == "tgg_management"
    assert get_job(job["id"])["pa_job_type"] == "tgg_management"


def test_cron_tool_inherits_active_pa_job_type_without_exposing_it(tmp_cron_dir):
    tokens = set_session_vars(pa_job_type="tgg_management")
    try:
        response = yaml.safe_load(
            cronjob(
                action="create",
                prompt="send the current report",
                schedule="every 1h",
                name="management report",
            )
        )
    finally:
        clear_session_vars(tokens)

    assert response["success"] is True
    # The operational boundary is persisted, but it is not a model-facing
    # field in cronjob's response schema.
    assert "pa_job_type" not in response["job"]
    assert get_job(response["job_id"])["pa_job_type"] == "tgg_management"


def test_cron_tool_non_pa_creator_remains_unbound(tmp_cron_dir):
    response = yaml.safe_load(
        cronjob(
            action="create",
            prompt="send the current report",
            schedule="every 1h",
        )
    )

    assert response["success"] is True
    assert get_job(response["job_id"])["pa_job_type"] is None


def test_run_job_selects_pa_brief_and_restricts_toolsets(tmp_path):
    _write_config(tmp_path)
    fake_db = MagicMock()
    job = {
        "id": "pa-cron-job",
        "name": "TGG management heartbeat",
        "prompt": "summarize management state",
        "enabled_toolsets": ["terminal"],
        "pa_job_type": "tgg_management",
    }

    with patch("cron.scheduler._hermes_home", tmp_path), \
         patch("cron.scheduler._resolve_origin", return_value=None), \
         patch("dotenv.load_dotenv"), \
         patch("hermes_state.SessionDB", return_value=fake_db), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch(
             "hermes_cli.runtime_provider.resolve_runtime_provider",
             return_value={
                 "api_key": "test-key",
                 "base_url": "https://example.invalid/v1",
                 "provider": "openrouter",
                 "api_mode": "chat_completions",
             },
         ), \
         patch("run_agent.AIAgent") as mock_agent_cls:
        mock_agent = MagicMock()
        mock_agent.run_conversation.return_value = {
            "completed": True,
            "final_response": "management summary",
        }
        mock_agent_cls.return_value = mock_agent

        success, _output, final_response, error = run_job(job)

    assert success is True
    assert final_response == "management summary"
    assert error is None
    kwargs = mock_agent_cls.call_args.kwargs
    assert kwargs["enabled_toolsets"] == ["memory", "file", "web", "custom"]
    assert kwargs["disabled_toolsets"] == ["cronjob", "messaging", "clarify", "delegation", "shell"]
    assert "TGG Management Brief" in kwargs["ephemeral_system_prompt"]
    fake_db.record_pa_behavior_event.assert_called_once()
    recorded = fake_db.record_pa_behavior_event.call_args.kwargs
    assert recorded["constitution_id"] == "bobby_tgg"
    assert recorded["job_type"] == "tgg_management"
    assert recorded["session_source"] == "cron"


def test_run_job_constructor_honors_explicit_readonly_management_delegation(tmp_path):
    constitution_path = _write_readonly_management_constitution(tmp_path)
    _write_config(tmp_path, constitution_path)
    fake_db = MagicMock()
    job = {
        "id": "pa-readonly-cron-job",
        "name": "TGG read-only management investigation",
        "prompt": "investigate the selected cases without changing them",
        "pa_job_type": "tgg_management",
    }

    with patch("cron.scheduler._hermes_home", tmp_path), \
         patch("cron.scheduler._resolve_origin", return_value=None), \
         patch("dotenv.load_dotenv"), \
         patch("hermes_state.SessionDB", return_value=fake_db), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch(
             "hermes_cli.runtime_provider.resolve_runtime_provider",
             return_value={
                 "api_key": "test-key",
                 "base_url": "https://provider.invalid/v1",
                 "provider": "openrouter",
                 "api_mode": "chat_completions",
             },
         ), \
         patch("run_agent.AIAgent") as mock_agent_cls:
        mock_agent = MagicMock()
        mock_agent.run_conversation.return_value = {
            "completed": True,
            "final_response": "management investigation complete",
        }
        mock_agent_cls.return_value = mock_agent

        success, _output, final_response, error = run_job(job)

    assert success is True
    assert final_response == "management investigation complete"
    assert error is None
    kwargs = mock_agent_cls.call_args.kwargs
    assert kwargs["enabled_toolsets"][-2:] == [
        READONLY_INVESTIGATION_TOOLSET,
        "delegation",
    ]
    assert kwargs["disabled_toolsets"] == [
        "cronjob",
        "messaging",
        "clarify",
        "shell",
    ]


def test_native_child_context_copy_preserves_pa_operation_scope_not_parent_prompt():
    import tools.pa_business_tools as business_tools
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.delegate_tool import _run_single_child

    constitution = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    constitution["job_briefs"]["tgg_management"]["business_operations"] = {
        "allowed": ["read_case"],
    }
    config = {
        "pa": {"enabled": True, "constitution": constitution},
        "pa_business": {
            "operations": {
                "read_case": {
                    "type": "command",
                    "command": ["/usr/bin/false"],
                },
                "write_case": {
                    "type": "command",
                    "command": ["/usr/bin/false"],
                },
            }
        },
    }
    observed = {}
    child = MagicMock()
    child._credential_pool = None
    child._delegate_role = "leaf"
    child._subagent_id = None
    child.ephemeral_system_prompt = "focused child prompt"
    child.model = "test-model"
    child.session_prompt_tokens = 0
    child.session_completion_tokens = 0
    child.session_estimated_cost_usd = 0.0
    child.session_reasoning_tokens = 0
    child.tool_progress_callback = None

    def inspect_context(**_kwargs):
        pa_context = business_tools._runtime_pa_context(config)
        bridge = business_tools.load_business_bridge_config(
            config,
            pa_context=pa_context,
        )
        observed.update(
            job_type=pa_context.job_type,
            operations=sorted(bridge.operations),
            denied=sorted(bridge.denied_operations),
            child_prompt=child.ephemeral_system_prompt,
        )
        return {
            "final_response": "done",
            "completed": True,
            "interrupted": False,
            "api_calls": 0,
            "messages": [],
        }

    child.run_conversation.side_effect = inspect_context
    parent = SimpleNamespace(
        _current_task_id=None,
        _touch_activity=lambda _description: None,
    )
    tokens = set_session_vars(platform="cron", pa_job_type="tgg_management")
    try:
        result = _run_single_child(
            task_index=0,
            goal="Inspect the selected management cases",
            child=child,
            parent_agent=parent,
        )
    finally:
        clear_session_vars(tokens)

    assert result["status"] == "completed"
    assert observed == {
        "job_type": "tgg_management",
        "operations": ["read_case"],
        "denied": ["write_case"],
        "child_prompt": "focused child prompt",
    }
