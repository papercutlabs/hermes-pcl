"""PA-648: the preserve-recent-and-conversation compression policy.

Management chats compacted with ``preserve-recent`` lost every message older
than the 20-message window, including a person's standing instruction.  The
new strategy keeps the same head and window and carries, word for word, the
recent conversation from the dropped region: people's messages and the
agent's own text replies.  Tool calls and tool output are still dropped.

All conversation text here is synthetic.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from agent import pa_carried_conversation as carry
from agent.context_compressor import ContextCompressor
from agent.model_metadata import estimate_messages_tokens_rough
from gateway.run import _add_message_recall_toolset, _apply_pa_compression_policy
from hermes_state import SessionDB
from tools.pa_read_full_message import read_full_message

NOW = datetime(2026, 11, 23, 6, 39, tzinfo=timezone.utc).timestamp()
DAY = 86_400
POLICY = {"strategy": carry.STRATEGY, "window_size": 6}


def _compressor():
    c = ContextCompressor.__new__(ContextCompressor)
    c.protect_first_n = 3
    c.protect_last_n = 20
    c.quiet_mode = True
    c.compression_count = 0
    c._last_summary_dropped_count = 0
    c._last_summary_fallback_used = False
    c._last_summary_error = None
    return c


def _tool_round(n):
    return [
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": f"call_{n}", "type": "function",
             "function": {"name": "lookup", "arguments": json.dumps({"q": n})}}]},
        {"role": "tool", "tool_call_id": f"call_{n}", "content": f"tool output {n} " + "x" * 200},
    ]


def _conversation():
    """Instruction, its reply, then enough working to push both out of the window."""
    msgs = [{"role": "system", "content": "SYSTEM"}]
    msgs.append({"role": "user", "content": "[Alex] Please list the open jobs for block 7."})
    msgs += _tool_round(1)
    msgs.append({"role": "assistant", "content": "Block 7 has 3 open jobs: J-1, J-2, J-3."})
    msgs.append({"role": "user", "content": "[system] [system] scheduled weekly report run"})
    msgs += _tool_round(2)
    msgs.append({"role": "assistant", "content": "Weekly report delivered."})
    msgs.append({"role": "user", "content": (
        '[Replying to: "jobs.xlsx"]\n\n[Alex] do not add source columns. if the job number '
        "is already in the master list just leave it, do not flag it. redo and return it"
    )})
    for n in range(3, 9):
        msgs += _tool_round(n)
    msgs.append({"role": "assistant", "content": "Redone: source columns removed, master-list rows left unflagged."})
    msgs.append({"role": "user", "content": "[Alex] thanks, now block 9"})
    return msgs


def _locator(times):
    """Every conversation message stored at the given send time."""
    def _locate():
        return {key: carry.StoredMessage(sent_at=sent_at) for key, sent_at in times.items()}
    return _locate


def _run(messages, policy=POLICY, locator=None, now=NOW, monkeypatch=None):
    c = _compressor()
    c.set_pa_compression_policy(policy)
    c.set_pa_message_locator(locator)
    monkeypatch.setattr(time, "time", lambda: now)
    return c, c.compress([m.copy() for m in messages])


INSTRUCTION = (
    '[Replying to: "jobs.xlsx"]\n\n[Alex] do not add source columns. if the job number '
    "is already in the master list just leave it, do not flag it. redo and return it"
)


def test_instruction_older_than_window_survives_word_for_word(monkeypatch):
    msgs = _conversation()
    _, today = _run(msgs, {"strategy": "preserve-recent", "window_size": 6}, monkeypatch=monkeypatch)
    assert not any(INSTRUCTION in str(m.get("content")) for m in today)

    c, out = _run(msgs, monkeypatch=monkeypatch)
    assert out[0]["content"] == "SYSTEM"
    assert out[1]["content"].startswith("[PA CONTEXT COMPACTION] preserve-recent-and-conversation:")
    block = out[2]
    assert block["role"] == "user" and carry.is_block(block["content"])
    assert INSTRUCTION in block["content"]
    # The agent's own text replies ride along; its tool working does not.
    assert "Block 7 has 3 open jobs: J-1, J-2, J-3." in block["content"]
    assert "Weekly report delivered." in block["content"]
    assert "tool output" not in block["content"]
    assert "[system]" not in block["content"]
    # Window is exactly the latest 6 messages, unchanged.
    assert out[3:] == [m for m in msgs[-6:]]
    stats = c._last_carried_conversation
    assert stats["person_messages"] == 2 and stats["agent_replies"] == 2
    assert stats["excluded_synthetic"] == 1
    assert estimate_messages_tokens_rough(out) < estimate_messages_tokens_rough(msgs)


def test_marker_states_what_was_kept(monkeypatch):
    _, out = _run(_conversation(), monkeypatch=monkeypatch)
    marker = out[1]["content"]
    assert "kept the latest 6 messages intact" in marker
    assert "2 messages people wrote and 2 replies you sent" in marker
    assert "omitted the other" in marker


def test_blocks_survive_tool_pair_sanitising(monkeypatch):
    c, out = _run(_conversation(), monkeypatch=monkeypatch)
    again = c._sanitize_tool_pairs([m.copy() for m in out])
    assert [m for m in again if carry.is_block(m.get("content"))] == [out[2]]
    entries = carry.parse_block(out[2]["content"])
    assert [e.author for e in entries] == ["person", "agent", "agent", "person"]


def test_age_window_drops_old_messages(monkeypatch):
    msgs = _conversation()
    times = {("user", "[Alex] Please list the open jobs for block 7."): NOW - 8 * DAY}
    c, out = _run(msgs, locator=_locator(times), monkeypatch=monkeypatch)
    assert "block 7." not in out[2]["content"]
    assert INSTRUCTION in out[2]["content"]
    assert c._last_carried_conversation["aged_out"] == 1


def test_token_cap_drops_oldest_first(monkeypatch):
    msgs = [{"role": "system", "content": "SYSTEM"}]
    for n in range(10):
        msgs.append({"role": "user", "content": f"[Alex] note {n} " + "y" * 600})
        msgs += _tool_round(n)
    msgs += [{"role": "user", "content": f"[Alex] recent {n}"} for n in range(6)]
    times = {("user", f"[Alex] note {n} " + "y" * 600): NOW - (10 - n) * 60 for n in range(10)}
    policy = {**POLICY, "conversation_max_tokens": 700}
    c, out = _run(msgs, policy, locator=_locator(times), monkeypatch=monkeypatch)
    entries = carry.parse_block(out[2]["content"])
    assert [e.text.split()[2] for e in entries] == ["7", "8", "9"]
    stats = c._last_carried_conversation
    assert stats["capped_out"] == 7 and stats["carried_tokens"] <= 700


def test_identical_repeat_carried_once(monkeypatch):
    msgs = [{"role": "system", "content": "SYSTEM"}]
    pasted = "[Alex] same pasted list A1 A2 A3"
    msgs += [{"role": "user", "content": pasted}, *_tool_round(1), {"role": "user", "content": pasted}]
    msgs += [{"role": "user", "content": f"[Alex] recent {n}"} for n in range(6)]
    c, out = _run(msgs, monkeypatch=monkeypatch)
    assert out[2]["content"].count(pasted) == 1
    assert c._last_carried_conversation["duplicates"] == 1


def test_later_compaction_reads_block_back_and_reapplies_age(monkeypatch):
    msgs = _conversation()
    _, first = _run(msgs, monkeypatch=monkeypatch)
    later = first + [m for n in range(20, 24) for m in _tool_round(n)]
    _, second = _run(later, monkeypatch=monkeypatch)
    assert INSTRUCTION in second[2]["content"]
    assert sum(1 for m in second if carry.is_block(m.get("content"))) == 1
    # Eight days on, everything carried has aged out (the two messages that
    # left the window in this pass are stored as sent at NOW).
    stored = _locator({
        ("assistant", "Redone: source columns removed, master-list rows left unflagged."): NOW,
        ("user", "[Alex] thanks, now block 9"): NOW,
    })
    _, third = _run(later, locator=stored, now=NOW + 8 * DAY, monkeypatch=monkeypatch)
    assert not any(carry.is_block(m.get("content")) for m in third)


def test_policy_parameters_default_and_override():
    assert carry.CarrySettings.from_policy({}) == carry.CarrySettings(7.0, 30_000, 2_000)
    assert carry.CarrySettings.from_policy({
        "conversation_days": 3, "conversation_max_tokens": 500,
        "conversation_message_max_chars": 100,
    }) == carry.CarrySettings(3.0, 500, 100)


def test_synthetic_user_messages_are_not_person_words():
    for text in (
        "[system] [system] scheduled weekly report run",
        "Continue the complete WhatsApp investigation for list 4",
        "[Christopher internal continuation] [Internal management-list continuation. x]",
        "[IMPORTANT: Background process p1 matched]",
        "[Message from Hermes session: ops]\n\nbody",
    ):
        assert carry.person_text({"role": "user", "content": text}) is None, text
    assert carry.person_text({
        "role": "user",
        "content": "[System note: Your previous turn was interrupted.]\n\n[Alex] real words",
    }) == "[Alex] real words"
    assert carry.person_text({"role": "user", "content": "[The user sent a document: 'a.xlsx']"}) is not None


# ── long messages and read_full_message ─────────────────────────────────


@pytest.fixture
def store(tmp_path):
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("s-old", "whatsapp")
    db.create_session("s-new", "whatsapp", parent_session_id="s-old")
    db.create_session("s-other", "whatsapp")
    yield db, tmp_path / "state.db"
    db.close()


def _long_message():
    rows = "\n".join(f"Blk {n} #0{n % 9}-{n:02d}" for n in range(400))
    return "[Alex] pasted addresses:\n" + rows + "\nis there any duplicates across these two lists"


def test_long_message_keeps_both_ends_and_note_names_readable_row(store, monkeypatch):
    db, path = store
    text = _long_message()
    row_id = db.append_message("s-old", "user", text)
    reply_id = db.append_message("s-old", "assistant", "No duplicates found.")
    msgs = [{"role": "system", "content": "SYSTEM"}, {"role": "user", "content": text},
            {"role": "assistant", "content": "No duplicates found."}]
    msgs += [{"role": "user", "content": f"[Alex] recent {n}"} for n in range(6)]

    def _locate():
        index = db.conversation_message_index("s-new")
        return {k: carry.StoredMessage(**v) for k, v in index.items()}

    _, out = _run(msgs, locator=_locate, monkeypatch=monkeypatch)
    entry = carry.parse_block(out[2]["content"])[0]
    assert entry.text.startswith(text[:1000])
    assert entry.text.endswith(text[-1000:])  # the question at the end survives
    note = f"full text: read_full_message session=s-old message={row_id}"
    assert note in entry.text
    assert (entry.session_id, entry.message_id) == ("s-old", row_id)

    full = json.loads(read_full_message("s-old", row_id, current_session_id="s-new", db_path=path))
    assert full["content"] == text and full["role"] == "user"
    reply = json.loads(read_full_message("s-old", reply_id, current_session_id="s-new", db_path=path))
    assert reply["content"] == "No duplicates found."


def test_read_full_message_refuses_working_unknown_and_foreign(store):
    db, path = store
    call_id = db.append_message("s-old", "assistant", "checking", tool_calls=[{"id": "c1"}])
    tool_id = db.append_message("s-old", "tool", "secret tool output", tool_call_id="c1")
    other_id = db.append_message("s-other", "user", "[Sam] another chat")
    mine = db.append_message("s-new", "user", "[Alex] here")

    def _err(sid, mid):
        return json.loads(read_full_message(sid, mid, current_session_id="s-new", db_path=path))["error"]

    assert "refused" in _err("s-old", tool_id)
    assert "refused" in _err("s-old", call_id)
    assert "no such message" in _err("s-old", 99999)
    assert "no such message" in _err("s-new", tool_id)        # id from another session
    assert "refused" in _err("s-other", other_id)             # not this chat's lineage
    assert json.loads(read_full_message("s-new", mine, current_session_id="s-new", db_path=path))["content"] == "[Alex] here"


def test_index_uses_earliest_row_across_lineage(store):
    db, _ = store
    first = db.append_message("s-old", "user", "[Alex] instruction")
    db.append_message("s-new", "user", "[Alex] instruction")  # compaction rewrite
    db.append_message("s-new", "assistant", "", tool_calls=[{"id": "c"}])
    index = db.conversation_message_index("s-new")
    assert index[("user", "[Alex] instruction")]["message_id"] == first
    assert index[("user", "[Alex] instruction")]["session_id"] == "s-old"
    assert all(role in ("user", "assistant") for role, _ in index)


# ── gateway wiring ──────────────────────────────────────────────────────


class _Recorder:
    def __init__(self):
        self.policy = self.locator = "UNSET"

    def set_pa_compression_policy(self, policy):
        self.policy = policy

    def set_pa_message_locator(self, locator):
        self.locator = locator

    def set_pa_compaction_guidance(self, guidance):
        pass


class _Agent:
    session_id = "s-new"

    def __init__(self):
        self.context_compressor = _Recorder()


class _Ctx:
    def __init__(self, compression):
        self.job_brief = type("B", (), {"response_policy": {"compression": compression}})()


def test_gateway_attaches_locator_only_for_the_carrying_strategy(store):
    db, _ = store
    agent = _Agent()
    agent._session_db = db  # the gateway's agents carry the session store
    _apply_pa_compression_policy(agent, _Ctx(dict(POLICY)))
    assert callable(agent.context_compressor.locator)
    db.append_message("s-new", "user", "[Alex] here")
    assert ("user", "[Alex] here") in agent.context_compressor.locator()
    for site_policy in ({"strategy": "preserve-recent", "window_size": 20},
                        {"strategy": "preserve-case-state", "window_size": 24}):
        agent = _Agent()
        _apply_pa_compression_policy(agent, _Ctx(site_policy), session_db=db)
        assert agent.context_compressor.locator is None
        assert agent.context_compressor.policy == site_policy


def test_recall_toolset_added_only_for_the_carrying_strategy():
    assert _add_message_recall_toolset(["memory"], _Ctx(dict(POLICY))) == ["memory", carry.RECALL_TOOLSET]
    assert _add_message_recall_toolset(["memory"], _Ctx({"strategy": "preserve-recent"})) == ["memory"]
    assert _add_message_recall_toolset(None, _Ctx(dict(POLICY))) is None


def test_recall_toolset_resolves_to_the_tool():
    from toolsets import resolve_toolset

    assert resolve_toolset(carry.RECALL_TOOLSET) == ["read_full_message"]
