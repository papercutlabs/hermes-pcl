"""Tests for standing PA compaction guidance (v6.3, WB f6845320).

Christopher's preserve-case-state compression POLICY short-circuited the
native compaction pipeline and treadmilled on 100%-case-traffic chats
(kept ~98% per pass). The replacement is a standing ``compaction_guidance``
string declared under ``response_policy.compression`` that keeps the NATIVE
pipeline (tool-result pruning → boundary calc → aux-model rolling summary)
and threads a retention brief into the summarizer prompt on auto-triggered
compactions, composing with any per-call ``/compress <focus>`` topic.

Covers:
  (a) compressor setter + summarizer-prompt injection (auto path);
  (b) guidance composes with — does not replace — focus_topic;
  (c) a guidance-only compression mapping does NOT short-circuit the native
      pipeline the way a strategy policy does;
  (d) gateway wiring: _apply_pa_compression_policy threads guidance to the
      compressor and only registers a POLICY when a strategy is declared;
  (e) christopher's constitution declares guidance, not a strategy policy.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agent.context_compressor import ContextCompressor
from agent.pa_constitution import load_constitution
from gateway.run import _apply_pa_compression_policy

CHRISTOPHER_CONSTITUTION = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "tgg"
    / "christopher"
    / "christopher_tgg_constitution.yaml"
)

GUIDANCE_MARKER = "STANDING COMPACTION GUIDANCE"


def _make_compressor():
    """Minimal compressor (mirrors tests/agent/test_compress_focus.py)."""
    compressor = ContextCompressor.__new__(ContextCompressor)
    compressor.protect_first_n = 2
    compressor.protect_last_n = 5
    compressor.tail_token_budget = 20000
    compressor.context_length = 200000
    compressor.threshold_percent = 0.80
    compressor.threshold_tokens = 160000
    compressor.max_summary_tokens = 10000
    compressor.quiet_mode = True
    compressor.compression_count = 0
    compressor.last_prompt_tokens = 0
    compressor._previous_summary = None
    compressor._summary_failure_cooldown_until = 0.0
    compressor.summary_model = None
    compressor.model = "test-model"
    compressor.provider = "test"
    compressor.base_url = "http://localhost"
    compressor.api_key = "test-key"
    compressor.api_mode = "chat_completions"
    return compressor


def _mock_llm(captured):
    def mock_call_llm(**kwargs):
        captured["messages"] = kwargs["messages"]
        resp = MagicMock()
        resp.choices = [MagicMock()]
        resp.choices[0].message.content = "## Goal\nSummary."
        return resp

    return mock_call_llm


def _conversation(n_pairs=4):
    messages = [{"role": "system", "content": "System prompt"}]
    for i in range(n_pairs):
        messages.append({"role": "user", "content": f"message {i}"})
        messages.append({"role": "assistant", "content": f"reply {i}"})
    return messages


# ── (a) setter + prompt injection ────────────────────────────────────────


def test_setter_normalizes_guidance():
    comp = _make_compressor()
    comp.set_pa_compaction_guidance("  keep open cases  ")
    assert comp.pa_compaction_guidance == "keep open cases"
    comp.set_pa_compaction_guidance("")
    assert comp.pa_compaction_guidance is None
    comp.set_pa_compaction_guidance(None)
    assert comp.pa_compaction_guidance is None


def test_standing_guidance_injected_into_summary_prompt():
    comp = _make_compressor()
    comp.set_pa_compaction_guidance("OPEN cases keep jobNo and last action.")
    turns = [
        {"role": "user", "content": "Block 14 update"},
        {"role": "assistant", "content": "Recorded."},
    ]
    captured = {}
    with patch("agent.context_compressor.call_llm", _mock_llm(captured)):
        result = comp._generate_summary(turns)

    assert result is not None
    prompt = captured["messages"][0]["content"]
    assert GUIDANCE_MARKER in prompt
    assert "OPEN cases keep jobNo and last action." in prompt


def test_no_guidance_no_injection():
    comp = _make_compressor()
    captured = {}
    with patch("agent.context_compressor.call_llm", _mock_llm(captured)):
        comp._generate_summary([{"role": "user", "content": "hi"}])
    assert GUIDANCE_MARKER not in captured["messages"][0]["content"]


def test_compress_auto_path_threads_guidance_to_summarizer():
    """compress() (the AUTO-compaction entry) reaches the summarizer with the
    standing guidance — no focus_topic involved, native pipeline running."""
    comp = _make_compressor()
    comp.set_pa_compaction_guidance("Closed cases collapse to one line.")
    captured = {}
    with patch("agent.context_compressor.call_llm", _mock_llm(captured)):
        comp.compress(_conversation(), current_tokens=100000)

    prompt = captured["messages"][0]["content"]
    assert GUIDANCE_MARKER in prompt
    assert "Closed cases collapse to one line." in prompt
    assert "FOCUS TOPIC" not in prompt


# ── (b) composes with per-call focus ─────────────────────────────────────


def test_standing_guidance_composes_with_focus_topic():
    comp = _make_compressor()
    comp.set_pa_compaction_guidance("Evidence detail compresses aggressively.")
    captured = {}
    with patch("agent.context_compressor.call_llm", _mock_llm(captured)):
        comp.compress(_conversation(), current_tokens=100000, focus_topic="block 130 leak")

    prompt = captured["messages"][0]["content"]
    assert GUIDANCE_MARKER in prompt
    assert "Evidence detail compresses aggressively." in prompt
    assert 'FOCUS TOPIC: "block 130 leak"' in prompt
    # Focus stays LAST so the per-call topic takes precedence.
    assert prompt.index(GUIDANCE_MARKER) < prompt.index("FOCUS TOPIC")


# ── (c) guidance-only mapping must not short-circuit the native path ─────


def test_guidance_only_policy_mapping_does_not_short_circuit():
    comp = _make_compressor()
    comp.set_pa_compression_policy({"compaction_guidance": "keep open cases"})
    assert comp._compress_with_pa_policy(_conversation()) is None


# ── (d) gateway wiring ───────────────────────────────────────────────────


class _RecordingCompressor:
    def __init__(self):
        self.policy = "UNSET"
        self.guidance = "UNSET"

    def set_pa_compression_policy(self, policy):
        self.policy = policy

    def set_pa_compaction_guidance(self, guidance):
        self.guidance = guidance


class _FakeAgent:
    def __init__(self):
        self.context_compressor = _RecordingCompressor()


class _FakeBrief:
    def __init__(self, response_policy):
        self.response_policy = response_policy


class _FakeContext:
    def __init__(self, response_policy):
        self.job_brief = _FakeBrief(response_policy)


def test_apply_wires_guidance_without_registering_policy():
    agent = _FakeAgent()
    _apply_pa_compression_policy(
        agent,
        _FakeContext({"compression": {"compaction_guidance": "keep open cases"}}),
    )
    assert agent.context_compressor.policy is None
    assert agent.context_compressor.guidance == "keep open cases"


def test_apply_still_wires_strategy_policy():
    agent = _FakeAgent()
    policy = {"strategy": "preserve-recent", "window_size": 20}
    _apply_pa_compression_policy(agent, _FakeContext({"compression": policy}))
    assert agent.context_compressor.policy == policy
    assert agent.context_compressor.guidance is None


def test_apply_clears_both_when_no_compression_block():
    agent = _FakeAgent()
    _apply_pa_compression_policy(agent, _FakeContext({}))
    assert agent.context_compressor.policy is None
    assert agent.context_compressor.guidance is None


# ── (e) christopher constitution shape ───────────────────────────────────


@pytest.mark.xfail(
    reason=(
        "Known recovered-constitution regression; restore through the authored "
        "deploy substrate in WB 7fa805be"
    ),
    strict=True,
)
def test_christopher_ops_ingest_declares_guidance_not_policy():
    constitution = load_constitution(CHRISTOPHER_CONSTITUTION)
    ops = constitution.job_briefs["tgg_ops_ingest"]
    compression = ops.response_policy.get("compression")
    assert isinstance(compression, dict)
    assert "strategy" not in compression
    guidance = compression.get("compaction_guidance")
    assert isinstance(guidance, str)
    # The three retention rules: open-keep / closed-one-line(~50 cap) / ledger-trust.
    assert "jobNo" in guidance
    assert "~50" in guidance
    assert "tgg_message_history_search" in guidance


def test_christopher_management_keeps_preserve_recent_policy():
    """Only the ops-ingest job moved to guidance; management still declares
    its preserve-recent policy (other policy consumers stay intact)."""
    constitution = load_constitution(CHRISTOPHER_CONSTITUTION)
    mgmt = constitution.job_briefs["tgg_management"]
    compression = mgmt.response_policy.get("compression")
    assert compression.get("strategy") == "preserve-recent"
