"""PA-203 round 2: Krisleen's conversation survives a model or provider change.

1. The live conversation key carries no provider or model.
2. The release carry-over moves each engine-keyed conversation to the stable
   key once, loses nothing, and is a no-op when run again.
3. The turn record and the consumer's engine check still name the engine
   that actually ran.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from gateway import conversation_key_carryover as carry
from gateway import durable_jsonl_consumer as consumer
from gateway.config import GatewayConfig, Platform, SessionResetPolicy
from gateway.replay import ReplayPlan, replay_context
from gateway.session import SessionSource, SessionStore

MGMT = "120363426509183563@g.us"
SITE = "120363000000000009@g.us"
TERRA = f"agent:live-drain:persistent-chat:openai-codex:gpt-5.6-terra:whatsapp:group:{MGMT}"
LUNA = f"agent:live-drain:persistent-chat:openai-codex:gpt-5.6-luna:whatsapp:group:{MGMT}"
SITE_TERRA = f"agent:live-drain:persistent-chat:openai-codex:gpt-5.6-terra:whatsapp:group:{SITE}"
# A pre-engine key from July 2026: must never be carried or resumed.
BARE = f"agent:live-drain:persistent-chat:whatsapp:group:{MGMT}"
NEW_MGMT = f"agent:live-drain:conversation:whatsapp:group:{MGMT}"
NEW_SITE = f"agent:live-drain:conversation:whatsapp:group:{SITE}"


def _entry(key: str, session_id: str, updated: str, **extra) -> dict:
    return {
        "session_key": key,
        "session_id": session_id,
        "created_at": "2026-07-01T00:00:00",
        "updated_at": updated,
        "display_name": None,
        "platform": "whatsapp",
        "chat_type": "group",
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "total_tokens": 0,
        "last_prompt_tokens": 123_456,
        "estimated_cost_usd": 0.0,
        "cost_status": "unknown",
        "origin": {
            "platform": "whatsapp",
            "chat_id": key.rsplit(":", 1)[-1],
            "chat_type": "group",
        },
        **extra,
    }


def _home(tmp_path: Path, workspace: str = "agent") -> Path:
    home = tmp_path / "home"
    (home / "sessions").mkdir(parents=True)
    (home / "config.yaml").write_text(yaml.safe_dump({
        "model": {"provider": "openai-codex", "default": "gpt-5.6-terra"},
        "python_sandbox": {"workspace": workspace},
    }))
    entries = {
        TERRA: _entry(TERRA, "sess-terra-mgmt", "2026-09-24T10:00:00"),
        LUNA: _entry(LUNA, "sess-luna-mgmt", "2026-08-01T10:00:00"),
        SITE_TERRA: _entry(SITE_TERRA, "sess-terra-site", "2026-09-23T10:00:00"),
        BARE: _entry(BARE, "sess-july", "2026-07-20T10:00:00"),
        "agent:main:whatsapp:dm:someone": _entry(
            "agent:main:whatsapp:dm:someone", "sess-dm", "2026-09-01T00:00:00"
        ),
    }
    (home / "sessions" / "sessions.json").write_text(json.dumps(entries, indent=2))
    return home


def _store(sessions_dir: Path) -> SessionStore:
    # Production runs session_reset mode none: one conversation per chat.
    store = SessionStore(
        sessions_dir=sessions_dir,
        config=GatewayConfig(
            group_sessions_per_user=False,
            default_reset_policy=SessionResetPolicy(mode="none"),
        ),
    )
    store._db = None
    return store


def _source(chat: str) -> SessionSource:
    return SessionSource(platform=Platform.WHATSAPP, chat_id=chat, chat_type="group")


def _session_id(store: SessionStore, chat: str) -> str:
    with replay_context(ReplayPlan(replay_namespace=consumer.persistent_chat_namespace())):
        return store.get_or_create_session(_source(chat)).session_id


# 1. The key is stable across a model change ---------------------------------


def _management_config(tmp_path: Path, provider: str, model: str) -> Path:
    constitution = tmp_path / "constitution.yaml"
    constitution.write_text(yaml.safe_dump({"selectors": [{
        "job_type": "tgg_management",
        "match": {"source.platform": "whatsapp", "source.chat_id": MGMT},
    }]}))
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump({
        "model": {"provider": provider, "default": model},
        "group_sessions_per_user": False,
        "pa": {"enabled": True, "constitution_path": str(constitution)},
    }))
    return config


def test_namespace_has_no_provider_or_model(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "gateway.config.load_gateway_config",
        lambda: GatewayConfig(group_sessions_per_user=False),
    )
    keys = set()
    for provider, model in (
        ("openai-codex", "gpt-5.6-terra"),
        ("openai-codex", "gpt-6-sol"),
        ("openai-direct-primary", "gpt-6-sol"),
    ):
        config = _management_config(tmp_path, provider, model)
        keys.add(consumer.management_conversation_session_key(
            config, platform="whatsapp", chat_id=MGMT
        ))
        assert model not in consumer.persistent_chat_namespace()
        assert provider not in consumer.persistent_chat_namespace()
    assert keys == {NEW_MGMT}


def test_a_model_change_resumes_the_same_session(tmp_path):
    sessions = tmp_path / "sessions"
    first = _session_id(_store(sessions), MGMT)
    # A restart on a new model is a new process: a fresh store reads the file.
    assert _session_id(_store(sessions), MGMT) == first


# 2. The carry-over ----------------------------------------------------------


def test_carry_over_moves_the_newest_conversation_and_keeps_every_old_entry(tmp_path):
    home = _home(tmp_path)
    sessions_file = home / "sessions" / "sessions.json"
    before = json.loads(sessions_file.read_text())

    checked = carry.carry_over(home, apply=False)
    assert sessions_file.read_text() == json.dumps(before, indent=2)
    assert checked["carried"] == {NEW_MGMT: TERRA, NEW_SITE: SITE_TERRA}

    result = carry.carry_over(home, apply=True)
    after = json.loads(sessions_file.read_text())
    assert result["carried"] == {NEW_MGMT: TERRA, NEW_SITE: SITE_TERRA}
    assert result["legacy_entries"] == 3  # the bare July key is not one
    # Every old entry is unchanged.
    for key, value in before.items():
        assert after[key] == value
    # The newest engine-keyed entry is carried whole, under the new key.
    assert after[NEW_MGMT] == {**before[TERRA], "session_key": NEW_MGMT}
    assert after[NEW_SITE] == {**before[SITE_TERRA], "session_key": NEW_SITE}
    assert set(after) == set(before) | {NEW_MGMT, NEW_SITE}

    # The live drain now resumes those sessions, never the July one.
    store = _store(home / "sessions")
    assert _session_id(store, MGMT) == "sess-terra-mgmt"
    assert _session_id(store, SITE) == "sess-terra-site"
    assert store._entries[NEW_MGMT].last_prompt_tokens == 123_456


def test_carry_over_is_idempotent_and_never_overwrites_the_new_key(tmp_path):
    home = _home(tmp_path)
    sessions_file = home / "sessions" / "sessions.json"
    carry.carry_over(home, apply=True)
    once = sessions_file.read_bytes()

    again = carry.carry_over(home, apply=True)
    assert again["carried"] == {}
    assert set(again["already_present"]) == {NEW_MGMT, NEW_SITE}
    assert sessions_file.read_bytes() == once

    # A conversation that moved on under the new key is never replaced by an
    # older engine-keyed one, even a newer-looking stale copy.
    data = json.loads(once)
    data[NEW_MGMT]["session_id"] = "sess-after-release"
    data[TERRA]["updated_at"] = "2026-12-31T00:00:00"
    sessions_file.write_text(json.dumps(data))
    carry.carry_over(home, apply=True)
    assert json.loads(sessions_file.read_text())[NEW_MGMT]["session_id"] == "sess-after-release"


def test_carry_over_without_sessions_file_is_a_no_op(tmp_path):
    home = tmp_path / "empty"
    home.mkdir()
    assert carry.carry_over(home, apply=True)["carried"] == {}
    assert not (home / "sessions" / "sessions.json").exists()


def _folder(key: str) -> str:
    return "s_" + hashlib.sha256(key.encode()).hexdigest()


def test_session_mode_workspace_is_copied_and_the_old_folder_kept(tmp_path):
    home = _home(tmp_path, workspace="session")
    old = home / "sandbox_workspaces" / _folder(TERRA) / "work"
    old.mkdir(parents=True)
    (old / "Outstanding_Cases_Update_v7.xlsx").write_bytes(b"v7")

    result = carry.carry_over(home, apply=True)

    new = home / "sandbox_workspaces" / _folder(NEW_MGMT) / "work"
    assert result["workspaces_copied"] == {_folder(NEW_MGMT): _folder(TERRA)}
    assert (new / "Outstanding_Cases_Update_v7.xlsx").read_bytes() == b"v7"
    assert (old / "Outstanding_Cases_Update_v7.xlsx").read_bytes() == b"v7"
    # Again: the new folder exists, so nothing is copied over it.
    (new / "later.xlsx").write_bytes(b"later")
    assert carry.carry_over(home, apply=True)["workspaces_copied"] == {}
    assert (new / "later.xlsx").exists()


def test_agent_mode_workspace_is_not_copied(tmp_path):
    home = _home(tmp_path, workspace="agent")
    old = home / "sandbox_workspaces" / _folder(TERRA) / "work"
    old.mkdir(parents=True)
    result = carry.carry_over(home, apply=True)
    assert result["workspaces_copied"] == {}
    assert not (home / "sandbox_workspaces" / _folder(NEW_MGMT)).exists()


def test_consumer_start_carries_over_and_never_fails(tmp_path, monkeypatch):
    home = _home(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(
        "gateway.config.load_gateway_config",
        lambda: GatewayConfig(sessions_dir=home / "sessions"),
    )
    result = consumer._carry_over_conversation_keys()
    assert result is not None and NEW_MGMT in result["carried"]
    assert NEW_MGMT in json.loads((home / "sessions" / "sessions.json").read_text())

    (home / "sessions" / "sessions.json").write_text("{not json")
    assert consumer._carry_over_conversation_keys() is None


def test_legacy_key_parser_only_accepts_engine_keys():
    assert carry.parse_legacy_key(TERRA) == (
        "openai-codex", "gpt-5.6-terra", f"whatsapp:group:{MGMT}"
    )
    assert carry.parse_legacy_key(BARE) is None
    assert carry.parse_legacy_key(NEW_MGMT) is None
    assert carry.parse_legacy_key("agent:main:whatsapp:group:x") is None


# 3. The engine check still names the engine that ran ------------------------


def test_turn_record_and_engine_check_use_the_engine_that_ran():
    from gateway.pa_observability import build_turn_record

    record = build_turn_record(
        session_id="sess-terra-mgmt", agent_id="christopher", chat_id=MGMT,
        agent_result={
            "completed": True, "model": "gpt-6-sol", "provider": "openai-codex",
            "final_response": "done",
        },
        final_response="done", started_at=1.0, completed_at=2.0,
    )
    # The resumed terra session records the model that answered.
    assert (record.provider, record.model) == ("openai-codex", "gpt-6-sol")

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT 'completed' AS turn_status, 't1' AS turn_id, "
        "'openai-codex' AS provider, 'gpt-6-sol' AS model, "
        "'{\"final_response\": \"done\"}' AS raw_turn_envelope_json"
    ).fetchone()
    assert consumer._assert_completed_turn(
        row, provider="openai-codex", model="gpt-6-sol", require_response=True
    ) == "t1"
    with pytest.raises(consumer.ConsumerError, match="engine mismatch"):
        consumer._assert_completed_turn(
            row, provider="openai-codex", model="gpt-5.6-terra", require_response=True
        )
