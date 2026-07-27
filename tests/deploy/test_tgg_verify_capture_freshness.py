from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
VERIFY = (
    ROOT
    / "deploy"
    / "tgg"
    / "christopher"
    / "scripts"
    / "verify_capture_freshness.sh"
)
SATURDAY = 1785020400  # 2026-07-25T23:00:00Z


def _progress(*, pending_age: int | None = None) -> dict[str, object]:
    pending = 1 if pending_age is not None else 0
    return {
        "receivedBatches": 11,
        "completedBatches": 11 - pending,
        "pendingBatches": pending,
        "receivedMessages": 20,
        "completedMessages": 20 - pending,
        "pendingMessages": pending,
        "failedBatches": 0,
        "failedMessages": 0,
        "failedBatchesSinceLastCompletion": 0,
        "failedMessagesSinceLastCompletion": 0,
        "unresolvedPersistedFailures": 0,
        "lastReceivedAt": "2026-07-25T22:59:00.000Z",
        "lastCompletedAt": "2026-07-25T22:58:59.000Z",
        "lastFailedAt": None,
        "oldestUnresolvedFailureAt": None,
        "oldestPendingAgeSeconds": pending_age,
    }


def _probe(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "probe_epoch": SATURDAY,
        "capture_mtime_epoch": SATURDAY - 13 * 3600,
        "capture_last_message_id": "M-20",
        "inbox_last_message_id": "M-20",
        "inbox_last_created_at": "2026-07-25T10:00:01+00:00",
        "capture_inbox_divergence_age_seconds": None,
        "capture_inbox_marker_found": True,
        "queue_length": 10,
        "socket_status": "connected",
        "queue_cap": 5000,
        "processing_gate_enabled": True,
        "inboundProgress": _progress(),
        "bridge_health_probe": "fixture",
    }
    value.update(overrides)
    return value


def _run(
    tmp_path: Path,
    probe: dict[str, object],
    *,
    env_overrides: dict[str, str] | None = None,
) -> tuple[subprocess.CompletedProcess[str], dict]:
    path = tmp_path / "probe.json"
    path.write_text(json.dumps(probe))
    result = subprocess.run(
        [str(VERIFY), "--probe-file", str(path)],
        text=True,
        capture_output=True,
        check=False,
        env={
            **os.environ,
            "CAPTURE_MAX_STALE_HOURS": "12",
            **(env_overrides or {}),
        },
    )
    assert result.stdout, result.stderr
    return result, json.loads(result.stdout)


def test_quiet_stale_connected_lockstep_passes(tmp_path: Path) -> None:
    result, report = _run(tmp_path, _probe())

    assert result.returncode == 0, result.stderr
    assert report["ok"] is True
    assert report["condition"] == "quiet-within-measured-pattern"
    assert report["capture_stale_hours"] == 13
    assert report["row_age_exceeds_threshold"] is True
    assert report["threshold_hours"] == 12
    assert report["freshness_sla_seconds"] == 7200


def test_induced_old_pending_inbound_fails_with_named_condition(
    tmp_path: Path,
) -> None:
    result, report = _run(
        tmp_path,
        _probe(inboundProgress=_progress(pending_age=7201)),
    )

    assert result.returncode == 1
    assert report["ok"] is False
    assert report["condition"] == "inbound-arrived-not-written"
    assert report["escalation_target"] == "edna-central"
    assert (
        "FAIL inbound-arrived-not-written: oldest inbound batch "
        "has remained pending for 7201s"
    ) in result.stderr


def test_capture_inbox_divergence_fails_only_when_consumer_expected(
    tmp_path: Path,
) -> None:
    divergent = _probe(
        inbox_last_message_id="M-19",
        capture_inbox_divergence_age_seconds=7201,
    )
    result, report = _run(tmp_path, divergent)
    assert result.returncode == 1
    assert report["condition"] == "capture-inbox-diverged"

    result, report = _run(
        tmp_path,
        {**divergent, "processing_gate_enabled": False},
    )
    assert result.returncode == 0
    assert report["condition"] == "quiet-within-measured-pattern"
    assert report["capture_inbox_lockstep"] is False


def test_ongoing_capture_does_not_hide_old_inbox_divergence(
    tmp_path: Path,
) -> None:
    result, report = _run(
        tmp_path,
        _probe(
            capture_mtime_epoch=SATURDAY - 30,
            inbox_last_message_id="M-19",
            capture_inbox_divergence_age_seconds=7201,
        ),
    )

    assert result.returncode == 1
    assert report["condition"] == "capture-inbox-diverged"
    assert report["capture_stale_seconds"] == 30
    assert report["capture_inbox_divergence_age_seconds"] == 7201
    assert "M-20" not in result.stdout
    assert "M-19" not in result.stdout


def test_missing_inbox_marker_fails_evidence_not_false_divergence(
    tmp_path: Path,
) -> None:
    result, report = _run(
        tmp_path,
        _probe(
            inbox_last_message_id="M-OLD-ROTATED",
            capture_inbox_divergence_age_seconds=9000,
            capture_inbox_marker_found=False,
        ),
    )

    assert result.returncode == 1
    assert report["condition"] == "capture-inbox-evidence-missing"
    assert "FAIL capture-inbox-evidence-missing:" in result.stderr


def test_missing_ids_are_not_lockstep(tmp_path: Path) -> None:
    result, report = _run(
        tmp_path,
        _probe(
            capture_last_message_id=None,
            inbox_last_message_id=None,
            capture_inbox_divergence_age_seconds=0,
            processing_gate_enabled=False,
        ),
    )

    assert result.returncode == 0
    assert report["capture_inbox_lockstep"] is False


def test_empty_inbox_ages_first_unmatched_capture(tmp_path: Path) -> None:
    result, report = _run(
        tmp_path,
        _probe(
            inbox_last_message_id=None,
            capture_inbox_divergence_age_seconds=7201,
            capture_inbox_marker_found=True,
        ),
    )

    assert result.returncode == 1
    assert report["condition"] == "capture-inbox-diverged"


def test_failed_inbound_since_last_completion_fails_immediately(
    tmp_path: Path,
) -> None:
    progress = _progress()
    progress.update(
        {
            "completedBatches": 10,
            "completedMessages": 19,
            "failedBatches": 1,
            "failedMessages": 1,
            "failedBatchesSinceLastCompletion": 1,
            "failedMessagesSinceLastCompletion": 1,
            "lastFailedAt": "2026-07-25T22:59:30.000Z",
        }
    )
    result, report = _run(tmp_path, _probe(inboundProgress=progress))

    assert result.returncode == 1
    assert report["condition"] == "inbound-arrived-not-written"
    assert "failed since the last successful capture completion" in result.stderr


def test_persisted_failure_survives_restart_and_fails_immediately(
    tmp_path: Path,
) -> None:
    progress = _progress()
    progress.update(
        {
            "unresolvedPersistedFailures": 1,
            "oldestUnresolvedFailureAt": "2026-07-25T21:00:00.000Z",
        }
    )
    result, report = _run(tmp_path, _probe(inboundProgress=progress))

    assert result.returncode == 1
    assert report["condition"] == "inbound-arrived-not-written"
    assert (
        "FAIL inbound-arrived-not-written: 1 unresolved persisted "
        "inbound capture failure(s) remain"
    ) in result.stderr


@pytest.mark.parametrize(
    ("overrides", "condition"),
    [
        ({"socket_status": "disconnected"}, "socket-disconnected"),
        ({"inboundProgress": None}, "inbound-telemetry-missing"),
        ({"queue_length": None}, "queue-telemetry-missing"),
        ({"queue_cap": None}, "queue-telemetry-missing"),
        ({"queue_length": 4500}, "queue-saturated"),
    ],
)
def test_failure_conditions_are_explicit(
    tmp_path: Path,
    overrides: dict[str, object],
    condition: str,
) -> None:
    result, report = _run(tmp_path, _probe(**overrides))

    assert result.returncode == 1
    assert report["condition"] == condition
    assert report["escalation_target"] == "edna-central"
    assert f"FAIL {condition}:" in result.stderr


def test_missing_required_inbound_field_fails_closed(tmp_path: Path) -> None:
    progress = _progress()
    del progress["receivedBatches"]
    result, report = _run(tmp_path, _probe(inboundProgress=progress))

    assert result.returncode == 1
    assert report["condition"] == "inbound-telemetry-missing"
    assert "receivedBatches" in report["reason"]


def test_invalid_inbound_timestamp_fails_closed(tmp_path: Path) -> None:
    progress = _progress()
    progress["lastReceivedAt"] = "not-a-timestamp"
    result, report = _run(tmp_path, _probe(inboundProgress=progress))

    assert result.returncode == 1
    assert report["condition"] == "inbound-telemetry-missing"
    assert "lastReceivedAt" in report["reason"]


@pytest.mark.parametrize(
    ("count", "timestamp"),
    [
        (1, None),
        (1, "not-a-timestamp"),
        (0, "2026-07-25T21:00:00.000Z"),
    ],
)
def test_malformed_persisted_failure_pair_fails_closed(
    tmp_path: Path,
    count: int,
    timestamp: str | None,
) -> None:
    progress = _progress()
    progress.update(
        {
            "unresolvedPersistedFailures": count,
            "oldestUnresolvedFailureAt": timestamp,
        }
    )
    result, report = _run(tmp_path, _probe(inboundProgress=progress))

    assert result.returncode == 1
    assert report["condition"] == "inbound-telemetry-missing"
    assert "oldestUnresolvedFailureAt" in report["reason"]


def test_malformed_threshold_emits_named_red_not_traceback(tmp_path: Path) -> None:
    result, report = _run(
        tmp_path,
        _probe(),
        env_overrides={"CAPTURE_MAX_STALE_HOURS": "twelve"},
    )

    assert result.returncode == 1
    assert report["condition"] == "configuration-invalid"
    assert "FAIL configuration-invalid:" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "condition",
    ["capture-evidence-missing", "capture-inbox-evidence-missing"],
)
def test_typed_subsidiary_probe_error_keeps_its_condition(
    tmp_path: Path,
    condition: str,
) -> None:
    result, report = _run(
        tmp_path,
        _probe(
            probe_error_condition=condition,
            probe_error_reason="fixture read failed",
        ),
    )

    assert result.returncode == 1
    assert report["condition"] == condition
    assert f"FAIL {condition}: fixture read failed" in result.stderr


def test_quiet_beyond_baseline_remains_informational(tmp_path: Path) -> None:
    result, report = _run(
        tmp_path,
        _probe(capture_mtime_epoch=SATURDAY - 40 * 3600),
    )

    assert result.returncode == 0
    assert report["condition"] == "quiet-outside-measured-pattern"


def test_quiet_baseline_uses_singapore_weekday(tmp_path: Path) -> None:
    sunday_2330_utc = int(
        datetime(2026, 7, 26, 23, 30, tzinfo=timezone.utc).timestamp()
    )
    result, report = _run(
        tmp_path,
        _probe(
            probe_epoch=sunday_2330_utc,
            capture_mtime_epoch=sunday_2330_utc - 20 * 3600,
        ),
    )

    assert result.returncode == 0
    assert report["quiet_baseline_limit_hours"] == 14.60
    assert report["condition"] == "quiet-outside-measured-pattern"


def test_live_health_probe_retries_before_bridge_unavailable() -> None:
    source = VERIFY.read_text()
    assert "for attempt in range(2):" in source
    assert "if attempt == 0:" in source
    assert "time.sleep(0.5)" in source


def test_unmatched_age_uses_capture_write_time_not_message_time() -> None:
    source = VERIFY.read_text()
    assert 'record.get("capturedAt")' in source
    assert 'item.get("timestamp")' not in source
