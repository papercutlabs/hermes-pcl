"""Tests for the case-state echo on tgg_case_observation results plus the
state-claim gate constitution lines (v6.3 item 4, WB f6845320).

1018/1092 receipt: on day-30 christopher's read asserted "this case is
already marked completed in iLinked" while the SAME TURN's tgg_case_lookup
returned state=open. Two layers land here:

  (a) constitution: any state assertion must restate THIS turn's tool
      result; on conflict the tool result wins and the discrepancy is
      flagged (both job briefs carry the line);
  (b) deterministic code surface: the backend's observation response only
      carries {observationId}, so the tool layer remembers the most recent
      backend-returned state per jobNo (lookup/search/write harvest) and
      echoes it into the observation success result (caseState +
      caseStateNote) so fresh state is in-face at evidence-attach time.
"""

import json
from pathlib import Path

import pytest

import tools.pa_business_tools as pbt
from agent.pa_constitution import load_constitution

CHRISTOPHER_CONSTITUTION = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "tgg"
    / "christopher"
    / "christopher_tgg_constitution.yaml"
)


@pytest.fixture
def source_refs_context():
    """Bind gateway turn refs on the task-local production surface."""
    from gateway.session_context import set_session_vars

    tokens = set_session_vars(
        source_message_refs=json.dumps(["wa-current-1", "wa-current-2"])
    )
    yield
    for token in reversed(tokens):
        token.var.reset(token)


@pytest.fixture(autouse=True)
def _clean_state_cache(monkeypatch):
    monkeypatch.delenv("HERMES_PA_BUSINESS_DRY_RUN", raising=False)
    pbt._LAST_SEEN_CASE_STATE.clear()
    yield
    pbt._LAST_SEEN_CASE_STATE.clear()


def _patch_backend(monkeypatch, responses):
    """Replace the business bridge with a per-operation canned responder."""
    calls = []

    def fake_execute(config, operation, payload=None, **kwargs):
        calls.append((operation, payload))
        value = responses[operation]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(pbt, "_load_runtime_bridge_config", lambda: object())
    monkeypatch.setattr(pbt, "execute_business_operation", fake_execute)
    return calls


# ── harvest ──────────────────────────────────────────────────────────────


def test_lookup_result_harvests_case_state(monkeypatch):
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_lookup": {
                "ok": True,
                "data": {"case": {"job_no": "AM/JOB/2601/1018", "state": "open"}},
                "status_code": 200,
            }
        },
    )
    pbt._handle_tgg_case_lookup({"jobNo": "AM/JOB/2601/1018"})
    assert pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/1018"] == "open"


def test_search_candidates_harvest_case_states(monkeypatch):
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_search": {
                "ok": True,
                "data": {
                    "candidates": [
                        {"jobNo": "AM/JOB/2601/0001", "state": "completed"},
                        {"jobNo": "AM/JOB/2601/0002", "state": "hdb_confirmed"},
                    ],
                    "count": 2,
                },
                "status_code": 200,
            }
        },
    )
    pbt._handle_tgg_case_search({"search": "blk 223"})
    assert pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/0001"] == "completed"
    assert pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/0002"] == "hdb_confirmed"


def test_write_response_echoing_case_updates_state(monkeypatch):
    """A write whose response echoes the case (e.g. update_state) refreshes
    the remembered state so a later observation echoes the NEW state."""
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_update_state": {
                "ok": True,
                "data": {"case": {"job_no": "AM/JOB/2601/1018", "state": "completed"}},
                "status_code": 200,
            }
        },
    )
    pbt._handle_tgg_case_update_state(
        {"job_no": "AM/JOB/2601/1018", "state": "completed"}
    )
    assert pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/1018"] == "completed"


# ── observation echo ─────────────────────────────────────────────────────


def test_observation_result_echoes_last_known_state(monkeypatch):
    """Same-turn shape from the 1018/1092 receipt: lookup says open, the
    observation attach then carries that state in-face."""
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_lookup": {
                "ok": True,
                "data": {"case": {"job_no": "AM/JOB/2601/1018", "state": "open"}},
                "status_code": 200,
            },
            "tgg_case_observation": {
                "ok": True,
                "data": {"observationId": 11},
                "status_code": 200,
            },
        },
    )
    pbt._handle_tgg_case_lookup({"jobNo": "AM/JOB/2601/1018"})
    raw = pbt._handle_tgg_case_observation(
        {
            "jobNo": "AM/JOB/2601/1018",
            "notes": "worker photos",
            "sourceRefs": ["wa-msg-1"],
        }
    )
    data = json.loads(raw)
    assert data["ok"] is True
    assert data["caseState"] == "open"
    assert "restate THIS value" in data["caseStateNote"]


def test_observation_without_known_state_is_unchanged(monkeypatch):
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_observation": {
                "ok": True,
                "data": {"observationId": 12},
                "status_code": 200,
            }
        },
    )
    raw = pbt._handle_tgg_case_observation(
        {"jobNo": "AM/JOB/2601/9999", "sourceRefs": ["wa-msg-2"]}
    )
    data = json.loads(raw)
    assert "caseState" not in data
    assert "caseStateNote" not in data


def test_observation_error_result_not_annotated(monkeypatch):
    pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/1018"] = "open"
    _patch_backend(
        monkeypatch,
        {"tgg_case_observation": RuntimeError("backend down")},
    )
    raw = pbt._handle_tgg_case_observation(
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["wa-msg-3"]}
    )
    data = json.loads(raw)
    assert "error" in data
    assert "caseState" not in data


def test_observation_does_not_override_backend_provided_state(monkeypatch):
    """If the backend ever starts echoing caseState itself, the tool-layer
    echo must defer to it."""
    pbt._LAST_SEEN_CASE_STATE["AM/JOB/2601/1018"] = "open"
    _patch_backend(
        monkeypatch,
        {
            "tgg_case_observation": {
                "ok": True,
                "caseState": "completed",
                "data": {"observationId": 13},
                "status_code": 200,
            }
        },
    )
    raw = pbt._handle_tgg_case_observation(
        {"jobNo": "AM/JOB/2601/1018", "sourceRefs": ["wa-msg-4"]}
    )
    assert json.loads(raw)["caseState"] == "completed"


def test_observation_requires_source_refs_before_backend_write(monkeypatch):
    monkeypatch.delenv("HERMES_SESSION_SOURCE_MESSAGE_REFS", raising=False)
    calls = _patch_backend(
        monkeypatch,
        {
            "tgg_case_observation": {
                "ok": True,
                "data": {"observationId": 99},
                "status_code": 200,
            }
        },
    )
    raw = pbt._handle_tgg_case_observation({"jobNo": "AM/JOB/2601/1018"})
    data = json.loads(raw)
    assert data["error"]
    assert "sourceRefs" in data["error"]
    assert calls == []


def test_observation_injects_current_turn_source_refs(
    monkeypatch, source_refs_context
):
    calls = _patch_backend(
        monkeypatch,
        {
            "tgg_case_observation": {
                "ok": True,
                "data": {"observationId": 101},
                "status_code": 200,
            }
        },
    )

    raw = pbt._handle_tgg_case_observation({"jobNo": "AM/JOB/2601/1018"})

    assert json.loads(raw)["ok"] is True
    assert calls[0][1]["fields"]["source_refs"] == [
        "wa-current-1",
        "wa-current-2",
    ]


def test_observation_strips_model_supplied_media_and_photo_fields(monkeypatch):
    calls = _patch_backend(
        monkeypatch,
        {
            "tgg_case_observation": {
                "ok": True,
                "data": {"observationId": 100},
                "status_code": 200,
            }
        },
    )
    raw = pbt._handle_tgg_case_observation(
        {
            "jobNo": "AM/JOB/2601/1018",
            "source": "whatsapp",
            "observedAt": "2026-06-20 10:00:00",
            "notes": "worker sent completion photos",
            "confidence": "observed",
            "sourceRefs": ["wa-msg-5"],
            "mediaRefs": ["bad-model-ref"],
            "photoCount": 9,
            "fields": {
                "message_text": "done",
                "media_refs": ["bad-nested-ref"],
                "photo_count": 9,
            },
        }
    )
    assert json.loads(raw)["ok"] is True
    assert calls[0][0] == "tgg_case_observation"
    payload = calls[0][1]
    assert payload["fields"]["source_refs"] == ["wa-msg-5"]
    assert "media_refs" not in payload["fields"]
    assert "photo_count" not in payload["fields"]


def test_state_cache_is_bounded():
    for i in range(pbt._LAST_SEEN_CASE_STATE_MAX + 10):
        pbt._remember_case_state(f"AM/JOB/2601/{i:04d}", "open")
    assert len(pbt._LAST_SEEN_CASE_STATE) == pbt._LAST_SEEN_CASE_STATE_MAX


# ── constitution gate lines ──────────────────────────────────────────────


@pytest.mark.xfail(
    reason=(
        "Known recovered-constitution regression; restore through the authored "
        "deploy substrate in WB 7fa805be"
    ),
    strict=True,
)
def test_state_claim_gate_in_both_job_briefs():
    constitution = load_constitution(CHRISTOPHER_CONSTITUTION)
    for job_type in ("tgg_ops_ingest", "tgg_management"):
        instructions = "\n".join(constitution.job_briefs[job_type].instructions)
        assert "State assertions restate THIS turn's tool result" in instructions, job_type
        assert "the tool result wins" in instructions, job_type
        assert "flag the discrepancy" in instructions, job_type
