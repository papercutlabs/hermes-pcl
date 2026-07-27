from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cron.jobs import create_job, get_job
from cron.scheduler import run_job


FIXTURE = Path(__file__).parents[1] / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"


@pytest.fixture()
def tmp_cron_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("cron.jobs.CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr("cron.jobs.JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr("cron.jobs.OUTPUT_DIR", tmp_path / "cron" / "output")
    return tmp_path


def _write_config(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "model": "test-model",
                "pa": {
                    "enabled": True,
                    "constitution_path": str(FIXTURE),
                },
            }
        ),
        encoding="utf-8",
    )


def test_create_job_stores_pa_job_type(tmp_cron_dir):
    job = create_job(
        prompt="daily TGG status",
        schedule="every 1h",
        pa_job_type="tgg_management",
    )

    assert job["pa_job_type"] == "tgg_management"
    assert get_job(job["id"])["pa_job_type"] == "tgg_management"


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
    assert kwargs["disabled_toolsets"] == ["cronjob", "messaging", "clarify", "shell"]
    assert "TGG Management Brief" in kwargs["ephemeral_system_prompt"]
    fake_db.record_pa_behavior_event.assert_called_once()
    recorded = fake_db.record_pa_behavior_event.call_args.kwargs
    assert recorded["constitution_id"] == "bobby_tgg"
    assert recorded["job_type"] == "tgg_management"
    assert recorded["session_source"] == "cron"
