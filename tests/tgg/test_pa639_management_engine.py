"""PA-639: the Management chats run on their own engine; every other chat keeps the gateway's.

Gateway engine in these tests: openai-codex / gpt-6.1-terra / medium.
Management engine: gpt-6.2-sol / medium, set by ``pa.management_engine``.
"""

from __future__ import annotations

import sqlite3
import types
from pathlib import Path

import pytest
import yaml

import gateway.durable_jsonl_consumer as consumer
from gateway import run as gateway_run
from gateway.management_engine import (
    ManagementEngine,
    ManagementEngineConfigError,
    parse_management_engine,
)

MGMT = "120363407903158826@g.us"
TEST_MGMT = "120363426509183563@g.us"
SITE = "900000000000000001@g.us"
TERRA = ("openai-codex", "gpt-6.1-terra")
SOL62 = ("openai-codex", "gpt-6.2-sol")
BLOCK = {"model": "gpt-6.2-sol", "reasoning_effort": "medium"}


def _write(tmp_path: Path, *, management: dict | None = BLOCK) -> Path:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({"selectors": [
        *({"job_type": "tgg_management",
           "match": {"source.platform": "whatsapp", "source.chat_id": chat}}
          for chat in (TEST_MGMT, MGMT)),
        {"job_type": "tgg_nightly_whatsapp",
         "match": {"source.platform": "whatsapp", "source.chat_id": SITE}},
    ]}))
    pa = {"enabled": True, "constitution_path": str(constitution)}
    if management is not None:
        pa["management_engine"] = management
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "model": {"provider": "openai-codex", "default": "gpt-6.1-terra"},
        "agent": {"reasoning_effort": "medium"},
        "fallback_providers": [
            {"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol"},
        ],
        "pa": pa,
    }))
    return config


def _row(provider: str, model: str, status: str = "completed") -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "create table t (turn_id, turn_status, provider, model, raw_turn_envelope_json)"
    )
    conn.execute(
        "insert into t values ('t1', ?, ?, ?, '{\"final_response\": \"ok\"}')",
        (status, provider, model),
    )
    return conn.execute("select * from t").fetchone()


def _check(config: Path, chat: str, row: sqlite3.Row) -> str:
    provider, model = consumer.configured_chat_engine(config, chat)
    return consumer._assert_completed_turn(
        row, provider=provider, model=model, require_response=True,
        fallback_engines=consumer.configured_fallback_engines(config),
    )


# 1. The block ---------------------------------------------------------------


def test_block_is_parsed_and_absent_block_means_no_management_engine():
    assert parse_management_engine({"pa": {"management_engine": BLOCK}}) == ManagementEngine(
        "gpt-6.2-sol", "medium"
    )
    assert parse_management_engine({"pa": {}}) is None
    assert parse_management_engine({}) is None


@pytest.mark.parametrize("block", [
    "gpt-6.2-sol",
    {"model": "gpt-6.2-sol"},
    {"reasoning_effort": "medium"},
    {"model": "", "reasoning_effort": "medium"},
    {"model": "gpt-6.2-sol", "reasoning_effort": "turbo"},
    {"model": "gpt-6.2-sol", "reasoning_effort": "medium", "provider": "openai-direct-primary"},
])
def test_a_malformed_block_is_refused(block):
    with pytest.raises(ManagementEngineConfigError):
        parse_management_engine({"pa": {"management_engine": block}})


# 2. The consumer's engine check ---------------------------------------------


def test_management_turn_expects_6_2_sol_and_site_turn_expects_terra(tmp_path):
    config = _write(tmp_path)
    assert consumer.configured_chat_engine(config, MGMT) == SOL62
    assert consumer.configured_chat_engine(config, TEST_MGMT) == SOL62
    assert consumer.configured_chat_engine(config, SITE) == TERRA
    assert consumer.configured_chat_engine(config, "unknown@g.us") == TERRA
    assert consumer.configured_engine(config) == TERRA


def test_without_the_block_management_expects_the_gateway_engine(tmp_path):
    config = _write(tmp_path, management=None)
    assert consumer.configured_chat_engine(config, MGMT) == TERRA


def test_management_turn_on_6_2_sol_passes_and_site_turn_on_terra_passes(tmp_path):
    config = _write(tmp_path)
    assert _check(config, MGMT, _row(*SOL62)) == "t1"
    assert _check(config, SITE, _row(*TERRA)) == "t1"


def test_a_mismatch_still_fails(tmp_path):
    config = _write(tmp_path)
    with pytest.raises(consumer.ConsumerError, match="engine mismatch"):
        _check(config, MGMT, _row(*TERRA))  # management turn that ran on the gateway engine
    with pytest.raises(consumer.ConsumerError, match="engine mismatch"):
        _check(config, SITE, _row(*SOL62))  # site turn that ran on the management engine
    with pytest.raises(consumer.ConsumerError, match="engine mismatch"):
        _check(config, MGMT, _row("openai-codex", "gpt-6.2-astra"))
    with pytest.raises(consumer.ConsumerError, match="status=failed"):
        _check(config, MGMT, _row(*SOL62, status="failed"))


def test_a_listed_fallback_is_still_accepted_for_management(tmp_path):
    config = _write(tmp_path)
    assert _check(config, MGMT, _row("openai-tgg-fallback", "gpt-6.1-sol")) == "t1"


def test_a_bad_block_fails_the_check_instead_of_guessing(tmp_path):
    config = _write(tmp_path, management={"model": "gpt-6.2-sol"})
    with pytest.raises(consumer.ConsumerError, match="reasoning_effort"):
        consumer.configured_chat_engine(config, MGMT)
    # A site chat never reads the block.
    assert consumer.configured_chat_engine(config, SITE) == TERRA


class _Runner:
    """Writes the pa_turns row the real gateway would write, on the engine given."""

    def __init__(self, state_db: Path, engine: tuple[str, str]):
        self.state_db, self.engine = state_db, engine

    async def replay(self, plan):
        conn = sqlite3.connect(self.state_db)
        conn.execute(
            "create table if not exists pa_turns (turn_id, turn_status, provider, model, "
            "raw_turn_envelope_json, error_json, message_refs_json, replay_run_id, started_at)"
        )
        conn.execute(
            "insert into pa_turns values ('turn-1', 'completed', ?, ?, '{}', null, "
            "'[\"m1\"]', ?, 1.0)",
            (*self.engine, plan.run_id),
        )
        conn.commit()
        conn.close()
        return types.SimpleNamespace(processed=1, outbound=[], captured_business_mutations=[])


def _record(chat: str) -> consumer.InboxRecord:
    return consumer.InboxRecord(
        seq=1, message_id="m1", chat_id=chat, start_offset=0, end_offset=1,
        raw={"messageId": "m1", "chatId": chat, "body": "hi", "timestamp": 1},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("chat,served,ok", [
    (MGMT, SOL62, True),
    (MGMT, TERRA, False),
    (SITE, TERRA, True),
    (SITE, SOL62, False),
])
async def test_live_batch_checks_the_engine_for_its_chat(tmp_path, chat, served, ok):
    config = _write(tmp_path)
    state_db = tmp_path / "state.db"
    call = consumer.process_live_records(
        [_record(chat)], config_path=config, state_db=state_db,
        persistent_session=True, runner=_Runner(state_db, served),
    )
    if ok:
        result = await call
        assert (result["provider"], result["model"]) == served
        assert result["handled"] == [{"message_ids": ["m1"], "turn_id": "turn-1"}]
    else:
        with pytest.raises(consumer.ConsumerError, match="engine mismatch"):
            await call


# 3. The gateway's choice of engine ------------------------------------------


def _job(job_type: str, model: str = "gpt-6.1-terra"):
    brief = types.SimpleNamespace(runtime={"provider": "openai-codex", "model": model})
    return types.SimpleNamespace(job_type=job_type, job_brief=brief)


RUNTIME = {"provider": "openai-codex", "api_key": "k"}


def test_management_job_runs_on_the_management_model_and_other_jobs_do_not():
    engine = ManagementEngine("gpt-6.2-sol", "medium")
    model, runtime = gateway_run._apply_pa_job_runtime(
        "gpt-6.1-terra", RUNTIME, _job("tgg_management"), management_engine=engine
    )
    assert model == "gpt-6.2-sol" and runtime["provider"] == "openai-codex"
    for other in ("tgg_ops_ingest", "tgg_nightly_whatsapp"):
        model, _ = gateway_run._apply_pa_job_runtime(
            "gpt-6.1-terra", RUNTIME, _job(other), management_engine=engine
        )
        assert model == "gpt-6.1-terra"
    model, _ = gateway_run._apply_pa_job_runtime(
        "gpt-6.1-terra", RUNTIME, _job("tgg_management"), management_engine=None
    )
    assert model == "gpt-6.1-terra"


def test_a_fallback_engine_is_not_replaced_by_the_management_model():
    runtime = {**RUNTIME, "fallback_entry": {"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol"}}
    model, runtime = gateway_run._apply_pa_job_runtime(
        "gpt-6.1-terra", runtime, _job("tgg_management"),
        management_engine=ManagementEngine("gpt-6.2-sol", "medium"),
    )
    assert (runtime["provider"], model) == ("openai-tgg-fallback", "gpt-6.1-sol")


def _gateway(monkeypatch, tmp_path, config: dict):
    path = tmp_path / "home" / "config.yaml"
    path.parent.mkdir(exist_ok=True)
    path.write_text(yaml.safe_dump(config))
    monkeypatch.setattr(gateway_run, "_hermes_home", path.parent)
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: yaml.safe_load(path.read_text()))
    return types.SimpleNamespace(
        _session_reasoning_overrides={},
        _load_reasoning_config=gateway_run.GatewayRunner._load_reasoning_config,
    )


def _effort(runner, pa_context, session_key="agent:main:whatsapp:group:x"):
    return gateway_run.GatewayRunner._resolve_session_reasoning_config(
        runner, session_key=session_key, pa_context=pa_context
    )


def test_management_turn_uses_the_management_effort_and_site_turn_the_gateway_effort(
    monkeypatch, tmp_path
):
    config = {
        "agent": {"reasoning_effort": "medium"},
        "pa": {"management_engine": {"model": "gpt-6.2-sol", "reasoning_effort": "high"}},
    }
    runner = _gateway(monkeypatch, tmp_path, config)
    assert _effort(runner, _job("tgg_management")) == {"enabled": True, "effort": "high"}
    assert _effort(runner, _job("tgg_ops_ingest")) == {"enabled": True, "effort": "medium"}
    assert _effort(runner, None) == {"enabled": True, "effort": "medium"}
    # A session-scoped /reasoning choice still wins.
    runner._session_reasoning_overrides["k"] = {"enabled": True, "effort": "low"}
    assert _effort(runner, _job("tgg_management"), session_key="k") == {"enabled": True, "effort": "low"}


def test_without_the_block_management_uses_the_gateway_effort(monkeypatch, tmp_path):
    runner = _gateway(monkeypatch, tmp_path, {"agent": {"reasoning_effort": "medium"}, "pa": {}})
    assert _effort(runner, _job("tgg_management")) == {"enabled": True, "effort": "medium"}


def test_session_runtime_resolves_6_2_sol_for_management_and_terra_for_site(monkeypatch):
    runner = types.SimpleNamespace(_session_model_overrides={}, _session_key_for_source=lambda s: None)
    monkeypatch.setattr(gateway_run, "_resolve_runtime_agent_kwargs", lambda: dict(RUNTIME))
    config = {
        "model": {"provider": "openai-codex", "default": "gpt-6.1-terra"},
        "pa": {"management_engine": BLOCK},
    }
    resolve = gateway_run.GatewayRunner._resolve_session_agent_runtime
    model, runtime = resolve(runner, session_key="k", user_config=config, pa_context=_job("tgg_management"))
    assert (runtime["provider"], model) == SOL62
    model, runtime = resolve(runner, session_key="k", user_config=config, pa_context=_job("tgg_ops_ingest"))
    assert (runtime["provider"], model) == TERRA
