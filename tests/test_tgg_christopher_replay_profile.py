from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pytest

from scripts.tgg_christopher_hermes_replay import (
    _resolve_replay_profile,
    _validate_replay_args,
)


def _args(db_path: Path, **overrides):
    values = {
        "profile": "tgg-local-gpt54-mini-gemini-vision",
        "model": None,
        "vision_provider": None,
        "vision_model": None,
        "vision_concurrency": None,
        "debounce_seconds": None,
        "rotate_session_every_turns": None,
        "business_base_url": None,
        "prod_pilot_run_id": None,
        "no_local_operator_backend": False,
        "db": str(db_path),
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _sqlite_db(path: Path) -> Path:
    with sqlite3.connect(path) as conn:
        conn.execute("create table smoke (id integer primary key)")
        conn.commit()
    return path


def test_replay_profile_defaults_to_safe_tgg_local_contract(tmp_path):
    profile = _resolve_replay_profile(_args(_sqlite_db(tmp_path / "tgg.db")))

    assert profile.model == "gpt-5.4-mini"
    assert profile.vision_provider == "gemini"
    assert profile.vision_model == "gemini-3.1-flash-lite"
    assert profile.vision_concurrency == 8
    assert profile.business_mode == "copied-db-local-operator"
    assert profile.debounce_seconds == 300


def test_replay_preflight_rejects_prod_business_url(tmp_path):
    db_path = _sqlite_db(tmp_path / "tgg.db")

    with pytest.raises(SystemExit, match="localhost"):
        _validate_replay_args(
            _args(db_path, business_base_url="https://systems.papercut-labs.com")
        )


def test_replay_preflight_rejects_sqlite_sidecars(tmp_path):
    db_path = _sqlite_db(tmp_path / "tgg.db")
    Path(str(db_path) + "-wal").touch()

    with pytest.raises(SystemExit, match="sidecars"):
        _validate_replay_args(_args(db_path))


# ── deployed-config-derived profile (config-drift killer) ─────────────────


def test_eval_profile_derives_base_from_deployed_config(tmp_path):
    """tgg-eval-gpt54-mini = deployed config + NAMED deltas only: main model
    under evaluation -> gpt-5.4-mini via OpenAI direct; vision KEEPS the
    deployed fanout (gemini / deployed vision model)."""
    import yaml

    from scripts.tgg_christopher_hermes_replay import TGG_CONFIG

    profile = _resolve_replay_profile(
        _args(_sqlite_db(tmp_path / "tgg.db"), profile="tgg-eval-gpt54-mini")
    )

    # Named deltas: the model under evaluation.
    assert profile.model == "gpt-5.4-mini"
    assert profile.main_provider == "openai-direct-primary"
    assert profile.transport == "codex_responses"

    # Inherited from the DEPLOYED config: the vision fanout. provider "main"
    # in the deployed auxiliary section resolves to the deployed main
    # provider (gemini), and the model comes from auxiliary.vision.model.
    deployed = yaml.safe_load(TGG_CONFIG.read_text(encoding="utf-8"))
    deployed_main_provider = deployed["model"]["provider"]
    deployed_vision = deployed["auxiliary"]["vision"]
    expected_vision_provider = (
        deployed_main_provider
        if deployed_vision.get("provider") == "main"
        else deployed_vision.get("provider")
    )
    assert profile.vision_enabled is True
    assert profile.vision_provider == expected_vision_provider == "gemini"
    assert profile.vision_model == deployed_vision["model"] == "gemini-3.1-flash-lite"

    # Harness-level safety values unchanged from the legacy contract.
    assert profile.business_mode == "copied-db-local-operator"
    assert profile.allow_prod_url is False
    assert profile.debounce_seconds == 300


def test_legacy_profiles_still_resolve(tmp_path):
    for name in (
        "tgg-local-gpt54-mini-gemini-vision",
        "tgg-local-gpt54-mini-native-vision",
        "tgg-local-gemini-live",
    ):
        profile = _resolve_replay_profile(
            _args(_sqlite_db(tmp_path / f"{name}.db"), profile=name)
        )
        assert profile.name == name


def test_bare_reaction_records_are_skipped_at_feed():
    from scripts.tgg_christopher_hermes_replay import (
        ReplayRecord,
        _is_bare_reaction_record,
    )

    def _rec(text, kind="text", has_media=False):
        return ReplayRecord(
            source_ref="r1", chat_jid="c@g.us", chat_name="c", sender_id="s",
            ts=1, sgt="2026-06-10 10:00:00", text=text, message_kind=kind,
            has_media=has_media, media_refs=[], quoted_text="",
            reply_to_source_ref="", raw_json={},
        )

    assert _is_bare_reaction_record(_rec("[reaction: 👍]")) is True
    assert _is_bare_reaction_record(_rec("[reaction: 👍]", kind="reaction")) is True
    assert _is_bare_reaction_record(_rec("", kind="reaction")) is True
    # Real content never skipped
    assert _is_bare_reaction_record(_rec("epoxy applied, done")) is False
    assert _is_bare_reaction_record(_rec("", kind="image", has_media=True)) is False


# ── continue-session mode: session-reuse decision logic (pure part) ──────


class TestContinueSessionPlan:
    def _seed_prior_store(self, hermes_home: Path) -> None:
        (hermes_home / "sessions").mkdir(parents=True)
        (hermes_home / "sessions" / "sessions.json").write_text("{}", encoding="utf-8")
        (hermes_home / "state.db").write_bytes(b"")

    def test_fresh_run_without_flag_changes_nothing(self, tmp_path):
        from scripts.tgg_christopher_hermes_replay import _continue_session_plan

        plan = _continue_session_plan(tmp_path, continue_session=False)
        assert plan["resume"] is False
        assert plan["session_reset_mode"] is None

    def test_continue_with_prior_store_resumes(self, tmp_path):
        from scripts.tgg_christopher_hermes_replay import _continue_session_plan

        self._seed_prior_store(tmp_path)
        plan = _continue_session_plan(tmp_path, continue_session=True)
        assert plan["resume"] is True
        # Reset policy must be disabled so a wall-clock daily/idle boundary
        # between replay runs can't rotate the resumed session.
        assert plan["session_reset_mode"] == "none"

    def test_continue_without_prior_store_starts_fresh_but_protects_session(self, tmp_path):
        from scripts.tgg_christopher_hermes_replay import _continue_session_plan

        plan = _continue_session_plan(tmp_path, continue_session=True)
        assert plan["resume"] is False
        # Day-1-with-flag: still disable resets so THIS session survives to
        # the next continued run.
        assert plan["session_reset_mode"] == "none"
        assert "no prior session store" in plan["reason"]

    def test_continue_requires_both_store_files(self, tmp_path):
        from scripts.tgg_christopher_hermes_replay import _continue_session_plan

        (tmp_path / "sessions").mkdir(parents=True)
        (tmp_path / "sessions" / "sessions.json").write_text("{}", encoding="utf-8")
        # state.db missing — bridge rows / transcripts live there; without it
        # the "prior session" is not actually resumable.
        plan = _continue_session_plan(tmp_path, continue_session=True)
        assert plan["resume"] is False
        assert "state.db" in plan["reason"]


def test_prepare_hermes_home_writes_session_reset_none_in_continue_mode(tmp_path, monkeypatch):
    """Continue mode must land session_reset mode "none" in the prepared
    config (config.yaml session_reset maps to default_reset_policy); fresh
    mode must leave the key absent (live-default policy)."""
    import yaml

    from scripts.tgg_christopher_hermes_replay import (
        _prepare_hermes_home,
        _resolve_replay_profile,
    )

    profile = _resolve_replay_profile(_args(_sqlite_db(tmp_path / "tgg.db")))

    fresh_home = tmp_path / "fresh-home"
    fresh_home.mkdir()
    _prepare_hermes_home(
        fresh_home,
        chat_id="120363403845802098@g.us",
        profile=profile,
        business_base_url=None,
    )
    fresh_config = yaml.safe_load((fresh_home / "config.yaml").read_text(encoding="utf-8"))
    assert "session_reset" not in fresh_config

    cont_home = tmp_path / "cont-home"
    cont_home.mkdir()
    _prepare_hermes_home(
        cont_home,
        chat_id="120363403845802098@g.us",
        profile=profile,
        business_base_url=None,
        session_reset_mode="none",
    )
    cont_config = yaml.safe_load((cont_home / "config.yaml").read_text(encoding="utf-8"))
    assert cont_config["session_reset"]["mode"] == "none"


# ── nightly-compact: end-of-day compaction step (v6.3 item 3) ─────────────


def test_nightly_compact_flag_parses():
    from scripts.tgg_christopher_hermes_replay import _build_arg_parser

    parser = _build_arg_parser()
    args = parser.parse_args(["--nightly-compact"])
    assert args.nightly_compact is True
    assert parser.parse_args([]).nightly_compact is False


class _FakeSessionEntry:
    def __init__(self, session_id):
        self.session_id = session_id


class _FakeSessionStore:
    """Session store whose session id rotates once compress has fired —
    mirrors _compress_context ending the old session + minting a new id."""

    def __init__(self):
        self.compressed = False

    def get_or_create_session(self, source):
        return _FakeSessionEntry("sess-new" if self.compressed else "sess-old")

    def load_transcript(self, session_id):
        if session_id == "sess-new":
            return [{"role": "assistant", "content": "summary"}]
        return [
            {"role": "user", "content": "day of messages " * 50},
            {"role": "assistant", "content": "replies " * 50},
        ]


class _FakeRunner:
    def __init__(self):
        self.session_store = _FakeSessionStore()
        self.compress_events = []

    async def _handle_compress_command(self, event):
        self.compress_events.append(event)
        self.session_store.compressed = True
        return "🗜️ Compressed 2 → 1 messages\n~1,200 → ~40 tokens"


def test_run_nightly_compact_fires_gateway_compress_path():
    """The post-drain compact step goes through the SAME callable the
    gateway's manual /compress uses (no reimplemented compression), with no
    focus topic, and reports pre/post tokens + the rotated session id."""
    import asyncio
    from types import SimpleNamespace

    from scripts.tgg_christopher_hermes_replay import _run_nightly_compact

    runner = _FakeRunner()
    source = SimpleNamespace(chat_id="chat-1", platform="whatsapp")
    last_event = SimpleNamespace(
        source=source, pa_job_type="tgg_ops_ingest", pa_context={"k": "v"}
    )
    result = asyncio.run(
        _run_nightly_compact(runner, [{"event": last_event}])
    )

    assert len(runner.compress_events) == 1
    event = runner.compress_events[0]
    assert event.text == "/compress"
    assert event.get_command() == "compress"
    assert event.get_command_args() == ""  # no focus topic — standing guidance governs
    assert event.source is source
    assert event.pa_job_type == "tgg_ops_ingest"
    assert event.pa_context == {"k": "v"}

    assert result["pre_session_id"] == "sess-old"
    assert result["post_session_id"] == "sess-new"
    assert result["session_rotated"] is True
    assert result["pre_estimated_tokens"] > result["post_estimated_tokens"] > 0
    assert "Compressed" in result["gateway_reply"]


def test_run_nightly_compact_skips_when_no_turns():
    import asyncio

    from scripts.tgg_christopher_hermes_replay import _run_nightly_compact

    runner = _FakeRunner()
    assert asyncio.run(_run_nightly_compact(runner, [])) is None
    assert runner.compress_events == []
