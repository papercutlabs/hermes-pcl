"""PA-203: Christopher's turn does not wait on Systems media retention.

The turn reads its attachments from capture.  Retention still runs: once,
best effort, beside the turn for a claimed row nobody has attempted, and in
the capture lane until it converges, including after the row is answered.
Neither ever delays the turn's start.
"""

import asyncio
import json
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from gateway import durable_jsonl_consumer as consumer


MANAGEMENT = "management@g.us"
SITE = "site@g.us"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _message(message_id: str, chat_id: str, *, body: str = "fixture message") -> dict:
    return {
        "messageId": message_id,
        "chatId": chat_id,
        "senderId": "fixture-user",
        "senderName": "Fixture User",
        "chatName": "Fixture Chat",
        "isGroup": True,
        "body": body,
        "hasMedia": False,
        "mediaType": None,
        "mediaUrls": [],
        "timestamp": 100,
        "fromMe": False,
    }


def _with_document(message: dict, path: Path) -> dict:
    message.update(
        {
            "hasMedia": True,
            "mediaType": "document",
            "mediaUrls": [str(path)],
            "mediaMimes": [XLSX_MIME],
        }
    )
    return message


def _with_image(message: dict, path: Path) -> dict:
    message.update({"hasMedia": True, "mediaType": "image", "mediaUrls": [str(path)]})
    return message


def _xlsx(path: Path) -> Path:
    from openpyxl import Workbook

    book = Workbook()
    book.active["A1"] = "job"
    book.active["A2"] = "AM/JOB/2608/0180"
    book.save(path)
    return path


def _png(path: Path) -> Path:
    path.write_bytes(b"\x89PNG\r\n\x1a\nfixture")
    return path


def _case_db(path: Path) -> Path:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE bridge_message_log (
          local_id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL, source_ref TEXT NOT NULL UNIQUE,
          chat_jid TEXT NOT NULL, chat_name TEXT, zone TEXT, channel_type TEXT,
          sender_id TEXT, from_me INTEGER, ts INTEGER, sgt TEXT, text TEXT,
          message_kind TEXT, has_media INTEGER, media_refs TEXT, quoted_text TEXT,
          reply_to_source_ref TEXT, raw_json TEXT
        )
        """
    )
    conn.commit()
    conn.close()
    return path


class _Env:
    def __init__(self, tmp_path: Path, *, max_attempts: int = 5):
        self.tmp = tmp_path
        self.capture = tmp_path / "capture"
        self.capture.mkdir()
        self.retained = tmp_path / "retained"
        self.config = tmp_path / "config.yaml"
        self.config.write_text(
            yaml.safe_dump(
                {
                    "model": {"provider": "test-provider", "default": "test-model"},
                    "python_sandbox": {
                        "datasets": {
                            "media": {"type": "path", "path": str(self.retained)}
                        }
                    },
                    "pa": {
                        "enabled": True,
                        "media_retention": {
                            "enabled": True,
                            "media_root": str(self.retained),
                            "media_ref_prefix": "/media/tgg/hermes",
                            "source_roots": [str(self.capture)],
                            "operation": "tgg_media_retention",
                            "min_free_percent": 0,
                            "max_attempts": max_attempts,
                            "retry_interval_seconds": 60,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        self.inbox = consumer.DurableInbox(tmp_path / "inbox.db")
        self.source = tmp_path / "events.jsonl"
        self.source.write_text("", encoding="utf-8")
        self.cursor = tmp_path / "cursor.json"
        consumer.initialize_cursor(self.source, self.cursor, position="start")
        self.case_db = _case_db(tmp_path / "case.db")
        self.plans: list = []

    def capture_events(self, *messages: dict) -> None:
        with self.source.open("a", encoding="utf-8") as handle:
            for message in messages:
                handle.write(json.dumps(message) + "\n")
        self.inbox.stage_from_source(self.source, self.cursor)

    def runner(self):
        plans = self.plans

        class Runner:
            async def replay(self, plan):
                plans.append(plan)
                return SimpleNamespace(processed=1, outbound=[])

        return Runner()

    async def process(
        self, records, *, direct_trigger_required: bool = False, drain: bool = True
    ):
        # The real turn builder runs; the fixture runner has no state.db, so
        # the turn id the gateway would record is supplied here.
        real = consumer.process_live_records

        async def live(batch, **kwargs):
            result = await real(batch, **kwargs)
            ids = [str(value) for value in result.get("submitted_message_ids") or []]
            return {**result, "handled": [{"message_ids": ids, "turn_id": "turn-fixture"}]}

        consumer.process_live_records = live
        try:
            await self._process(records, direct_trigger_required=direct_trigger_required)
        finally:
            consumer.process_live_records = real
        if drain:
            # Claim-path retention runs beside the turn; wait for it here.
            await consumer._drain_background_retention()

    async def _process(self, records, *, direct_trigger_required: bool):
        await consumer._process_claimed_chat_batch(
            self.inbox,
            records,
            config_path=self.config,
            state_db=self.tmp / "state.db",
            case_db=self.case_db,
            source_before_image_dir=self.tmp / "source-before-images",
            gate_changed_at="2026-07-21T00:00:00+00:00",
            runner=self.runner(),
            direct_trigger_required=direct_trigger_required,
        )

    def row(self, message_id: str) -> sqlite3.Row:
        with self.inbox.connect() as conn:
            return conn.execute(
                "SELECT status,retention_state,retention_attempts,retention_last_error "
                "FROM ingress_events WHERE message_id=?",
                (message_id,),
            ).fetchone()

    def make_retained_entries_old(self) -> None:
        with self.inbox.connect() as conn:
            conn.execute(
                "UPDATE ingress_events SET retention_updated_at="
                "'2026-01-01T00:00:00+00:00' WHERE retention_updated_at IS NOT NULL"
            )


class _Systems:
    """Stand-in for the Systems retention route: MEDIA_NOT_FOUND until it lands."""

    def __init__(self, monkeypatch):
        self.available = False
        self.calls = 0
        self.stall: threading.Event | None = None
        monkeypatch.setattr(consumer, "_converge_retained_media", self)

    def __call__(self, *_args, **_kwargs):
        self.calls += 1
        if self.stall is not None:
            # A Systems that does not answer: held until released (20 s cap).
            self.stall.wait(timeout=20)
        if not self.available:
            raise consumer.MediaRetentionError(
                "status_code=400 code=MEDIA_NOT_FOUND "
                "message=media[0].ref does not resolve to a retained file."
            )
        return {"ok": True, "data": {"ledgerChanged": True, "observationsChanged": False}}


def _management_batch(env: _Env) -> list[consumer.InboxRecord]:
    management, _ = env.inbox.pending_chat_batches(
        batch_size=25, priority_chats={MANAGEMENT}
    )
    assert len(management) == 1
    return management[0][1]


@pytest.mark.asyncio
async def test_batch_with_media_still_retaining_is_processed_immediately_with_its_files(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    workbook = _xlsx(env.capture / "doc_feed0b0a20c4_SPAZIO.xlsx")
    photo = _png(env.capture / "img_0123456789ab.png")
    env.capture_events(
        _with_document(_message("DOC", MANAGEMENT, body="cross-check these"), workbook),
        _with_image(_message("IMG", MANAGEMENT, body=""), photo),
    )
    # The capture lane's first attempt fails at Systems: both rows are held.
    summary = await consumer.retain_pending_media(
        env.inbox, config_path=env.config, limit=25
    )
    assert summary["held"] == 2
    assert {env.row("DOC")["retention_state"], env.row("IMG")["retention_state"]} == {
        "held"
    }

    # The batch forms at once, with both rows, while retention is unconverged.
    records = _management_batch(env)
    assert [record.message_id for record in records] == ["DOC", "IMG"]
    assert {record.retention_state for record in records} == {"held"}

    await env.process(records)

    # The turn ran once with both messages and their capture files.
    assert len(env.plans) == 1
    sent = {message["messageId"]: message for message in env.plans[0].messages}
    assert sent["DOC"]["mediaUrls"] == [str(workbook)]
    assert sent["IMG"]["mediaUrls"] == [str(photo)]
    assert Path(sent["DOC"]["mediaUrls"][0]).read_bytes() == workbook.read_bytes()
    # No retained-copy line: retention had not converged, the capture path stands.
    assert "[Attachment retained" not in sent["DOC"]["body"]
    assert "Attachment unavailable" not in sent["DOC"]["body"]
    # Answered, not requeued.  Both rows already had an attempt, so the claim
    # path made no second call; the capture lane keeps retrying them.
    assert env.row("DOC")["status"] == "completed"
    assert env.row("IMG")["status"] == "completed"
    assert env.row("DOC")["retention_attempts"] == 1
    assert systems.calls == 2


@pytest.mark.asyncio
async def test_retention_complete_at_turn_time_still_adds_the_retained_copy_line(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    systems.available = True
    workbook = _xlsx(env.capture / "doc_abcdefabcdef_jobs.xlsx")
    env.capture_events(_with_document(_message("DOC", MANAGEMENT), workbook))
    records = _management_batch(env)
    assert records[0].retention_state == "pending"
    # The capture lane converges it after selection and before the turn.
    await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)

    await env.process(records)

    body = env.plans[0].messages[0]["body"]
    assert env.plans[0].messages[0]["mediaUrls"] == [str(workbook)]
    assert "[Attachment retained for analysis in python_sandbox: /inputs/media/" in body
    assert env.row("DOC")["retention_state"] == "complete"
    assert env.row("DOC")["status"] == "completed"
    assert systems.calls == 1


@pytest.mark.asyncio
async def test_unattempted_claimed_row_gets_one_attempt_beside_the_turn(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    systems.available = True
    workbook = _xlsx(env.capture / "doc_aaaaaaaaaaaa_jobs.xlsx")
    env.capture_events(_with_document(_message("DOC", MANAGEMENT), workbook))

    await env.process(_management_batch(env))

    # The turn used capture (retention was not complete when it started); the
    # claim-path attempt then converged the row.
    assert "[Attachment retained" not in env.plans[0].messages[0]["body"]
    assert env.plans[0].messages[0]["mediaUrls"] == [str(workbook)]
    assert env.row("DOC")["retention_state"] == "complete"
    assert env.row("DOC")["retention_attempts"] == 1
    assert systems.calls == 1


@pytest.mark.asyncio
async def test_a_stalled_systems_does_not_delay_the_turn_start(tmp_path, monkeypatch):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    systems.stall = threading.Event()
    photo = _png(env.capture / "img_bbbbbbbbbbbb.png")
    workbook = _xlsx(env.capture / "doc_cccccccccccc_w.xlsx")
    env.capture_events(
        _with_image(_message("IMG", MANAGEMENT), photo),
        _with_document(_message("DOC", MANAGEMENT), workbook),
    )
    started = time.monotonic()
    try:
        await asyncio.wait_for(env.process(_management_batch(env), drain=False), timeout=5)
        elapsed = time.monotonic() - started
        # The turn ran and the rows finished while Systems was still stalled.
        assert len(env.plans) == 1
        assert elapsed < 3
        assert env.row("IMG")["status"] == "completed"
        assert env.row("DOC")["status"] == "completed"
        assert systems.calls == 1
    finally:
        systems.stall.set()
        await consumer._drain_background_retention()
    # The attempts then ran one at a time and were recorded once each.
    assert systems.calls == 2
    assert env.row("IMG")["retention_attempts"] == 1
    assert env.row("DOC")["retention_attempts"] == 1


@pytest.mark.asyncio
async def test_a_stalled_systems_does_not_delay_batch_selection_in_the_loop(
    tmp_path, monkeypatch
):
    """The capture-lane pass runs beside batch selection in run_consumer."""
    from tests.gateway.test_durable_jsonl_consumer import _enabled_consumer_args

    args = _enabled_consumer_args(tmp_path, [])
    capture = tmp_path / "capture"
    capture.mkdir()
    photo = _png(capture / "img_dddddddddddd.png")
    config = Path(args.config)
    data = yaml.safe_load(config.read_text())
    data["pa"]["media_retention"] = {
        "enabled": True, "media_root": str(tmp_path / "retained"),
        "media_ref_prefix": "/media/tgg/hermes",
        "source_roots": [str(capture)], "operation": "tgg_media_retention",
        "min_free_percent": 0,
    }
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    Path(args.source).write_text(
        json.dumps({"type": "whatsapp_capture_event",
                    "normalized": _with_image(_message("SITE-IMG", SITE), photo)}) + "\n",
        encoding="utf-8",
    )
    systems = _Systems(monkeypatch)
    systems.stall = threading.Event()
    turn_started: list[float] = []

    async def fake_process(records, **kwargs):
        turn_started.append(time.monotonic())
        return {
            "submitted_message_ids": [r.message_id for r in records],
            "handled": [{"message_ids": [r.message_id for r in records], "turn_id": "t"}],
            "captured_outbound": [],
        }

    monkeypatch.setattr(consumer, "process_live_records", fake_process)
    monkeypatch.setattr(consumer, "_new_gateway_runner", lambda *_a, **_k: object())
    started = time.monotonic()
    timer = threading.Timer(4, systems.stall.set)
    timer.start()
    try:
        assert await consumer.run_consumer(args) == 0
    finally:
        timer.cancel()
        systems.stall.set()
    assert turn_started and turn_started[0] - started < 2
    inbox = consumer.DurableInbox(Path(args.inbox))
    assert inbox.counts() == {"completed": 1}
    with inbox.connect() as conn:
        attempts = conn.execute(
            "SELECT retention_attempts FROM ingress_events WHERE message_id='SITE-IMG'"
        ).fetchone()[0]
    # The capture lane and the claim path both reached the row; one attempt.
    assert attempts == 1
    assert systems.calls == 1


def test_concurrent_attempts_on_one_row_count_once(tmp_path, monkeypatch):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    systems.stall = threading.Event()
    photo = _png(env.capture / "img_eeeeeeeeeeee.png")
    env.capture_events(_with_image(_message("IMG", SITE), photo))
    record = env.inbox.retention_candidates(limit=1)[0]
    results: list = []
    first = threading.Thread(target=lambda: results.append(_swallow(env, record)))
    first.start()
    deadline = time.monotonic() + 5
    while systems.calls == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    # A second caller (active steering, or the capture lane) skips the row.
    assert consumer._ensure_retained_unless_in_flight(
        env.inbox, record, config_path=env.config
    ) is None
    systems.stall.set()
    first.join(timeout=10)
    assert systems.calls == 1
    assert env.row("IMG")["retention_attempts"] == 1


def _swallow(env, record):
    try:
        return consumer._ensure_retained_unless_in_flight(
            env.inbox, record, config_path=env.config
        )
    except consumer.MediaRetentionError as exc:
        return exc


@pytest.mark.asyncio
async def test_turn_input_warns_when_a_held_file_is_missing(tmp_path, monkeypatch):
    env = _Env(tmp_path)
    _Systems(monkeypatch)
    photo = _png(env.capture / "img_ffffffffffff.png")
    env.capture_events(_with_image(_message("GONE", MANAGEMENT, body="see photo"), photo))
    photo.unlink()
    # The capture lane's attempt fails on the file itself: held, file level.
    await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)
    assert env.row("GONE")["retention_state"] == "held"

    await env.process(_management_batch(env))

    body = env.plans[0].messages[0]["body"]
    assert body.startswith("see photo\n\n")
    assert (
        "[Attachment unavailable: the captured file for message GONE is missing or "
        "failed its file check. Do not infer or claim the attachment's contents.]"
    ) in body
    assert env.row("GONE")["status"] == "completed"


@pytest.mark.asyncio
async def test_turn_input_warns_for_a_failed_download_before_any_attempt(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    failed = _message("NO-FILE", MANAGEMENT, body="")
    failed.update({"hasMedia": True, "mediaType": "image", "mediaUrls": []})
    env.capture_events(failed)

    await env.process(_management_batch(env), drain=False)

    body = env.plans[0].messages[0]["body"]
    assert "[Attachment unavailable: the captured file for message NO-FILE" in body
    await consumer._drain_background_retention()
    assert systems.calls == 0
    assert env.row("NO-FILE")["retention_state"] == "held"


@pytest.mark.asyncio
async def test_a_systems_failure_on_a_present_file_carries_no_warning(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    _Systems(monkeypatch)
    photo = _png(env.capture / "img_121212121212.png")
    env.capture_events(_with_image(_message("IMG", MANAGEMENT, body="photo"), photo))
    await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)
    assert env.row("IMG")["retention_state"] == "held"

    await env.process(_management_batch(env))

    assert env.plans[0].messages[0]["body"] == "photo"


def test_retention_queue_queries_use_the_open_retention_index(tmp_path):
    env = _Env(tmp_path)
    statements: list[str] = []
    real_connect = env.inbox.connect

    def tracing_connect():
        conn = real_connect()
        conn.set_trace_callback(statements.append)
        return conn

    env.inbox.connect = tracing_connect
    env.inbox.retention_candidates(limit=25, retry_interval_seconds=60)
    env.inbox.retention_last_error()
    queries = [q for q in statements if q.lstrip().upper().startswith("SELECT")]
    assert len(queries) == 2
    with real_connect() as conn:
        for query in queries:
            plan = " ".join(
                str(row[3]) for row in conn.execute("EXPLAIN QUERY PLAN " + query)
            )
            assert "ingress_events_retention_open_idx" in plan, plan


@pytest.mark.asyncio
async def test_retention_continues_after_the_turn_and_is_counted(tmp_path, monkeypatch):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    workbook = _xlsx(env.capture / "doc_111111111111_wc.xlsx")
    env.capture_events(_with_document(_message("DOC", MANAGEMENT), workbook))
    await env.process(_management_batch(env))
    assert env.row("DOC")["status"] == "completed"
    assert env.row("DOC")["retention_state"] == "held"

    # The answered row is still owed retention, and the counts show it.
    counts = env.inbox.retention_counts()
    assert counts["retention_held"] == 1
    assert counts["retention_attempts"] == 1
    assert "MEDIA_NOT_FOUND" in (env.inbox.retention_last_error() or "")
    # Spaced like any held row: not before the retry interval.
    assert env.inbox.retention_candidates(limit=10, retry_interval_seconds=60) == []
    env.make_retained_entries_old()
    assert [
        record.message_id
        for record in env.inbox.retention_candidates(limit=10, retry_interval_seconds=60)
    ] == ["DOC"]

    systems.available = True
    summary = await consumer.retain_pending_media(
        env.inbox, config_path=env.config, limit=25
    )
    assert summary["examined"] == 1
    assert summary["retained"] == 1
    row = env.row("DOC")
    assert row["retention_state"] == "complete"
    assert row["status"] == "completed"
    counts = env.inbox.retention_counts()
    assert counts["retention_held"] == 0
    assert counts["retention_complete"] == 1
    assert env.inbox.retention_last_error() is None
    assert env.inbox.retention_candidates(limit=10) == []


@pytest.mark.asyncio
async def test_claimed_rows_are_left_to_the_claim_path_attempt(tmp_path, monkeypatch):
    env = _Env(tmp_path)
    _Systems(monkeypatch)
    workbook = _xlsx(env.capture / "doc_222222222222_x.xlsx")
    env.capture_events(_with_document(_message("DOC", MANAGEMENT), workbook))
    await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)
    env.make_retained_entries_old()
    records = _management_batch(env)
    env.inbox.claim(records)
    assert env.inbox.retention_candidates(limit=10) == []
    # Still counted while it is claimed.
    assert env.inbox.retention_counts()["retention_held"] == 1


@pytest.mark.asyncio
async def test_media_that_never_retains_is_still_answered_and_shows_in_counts(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path, max_attempts=2)
    _Systems(monkeypatch)
    # Systems-side failure: retryable, never quarantined, stays held.
    workbook = _xlsx(env.capture / "doc_333333333333_wc.xlsx")
    # Item failure: the capture file is gone; quarantined at the cap.
    gone = env.capture / "img_444444444444.png"
    env.capture_events(
        _with_document(_message("DOC", MANAGEMENT), workbook),
        _with_image(_message("GONE", SITE, body="photo"), gone),
    )
    management, site = env.inbox.pending_chat_batches(
        batch_size=25, priority_chats={MANAGEMENT}
    )
    await env.process(management[0][1])
    await env.process(site[0][1])
    assert env.row("DOC")["status"] == "completed"
    assert env.row("GONE")["status"] == "completed"
    assert len(env.plans) == 2

    for _ in range(3):
        env.make_retained_entries_old()
        await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)

    counts = env.inbox.retention_counts()
    assert env.row("DOC")["retention_state"] == "held"
    assert env.row("DOC")["retention_attempts"] == 4
    assert counts["retention_held"] == 1
    assert "MEDIA_NOT_FOUND" in env.inbox.retention_last_error()
    assert counts["retention_quarantined"] == 1
    assert env.inbox.retention_quarantine_message_ids() == ["GONE"]
    assert env.row("GONE")["retention_state"] == "bypassed"
    # Answering never changed: both rows stay completed.
    assert env.inbox.counts() == {"completed": 2}


def test_legacy_unattempted_rows_are_never_swept_in_or_counted(tmp_path):
    env = _Env(tmp_path)
    photo = _png(env.capture / "img_555555555555.png")
    env.capture_events(
        _with_image(_message("LEGACY-DONE", SITE), photo),
        _with_image(_message("LEGACY-SKIP", SITE), photo),
        _with_image(_message("NEW", SITE), photo),
    )
    # The 21 July shape: answered or skipped, retention pending, no attempt.
    with env.inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET status='completed' WHERE message_id='LEGACY-DONE'"
        )
        conn.execute(
            "UPDATE ingress_events SET status='skipped' WHERE message_id='LEGACY-SKIP'"
        )
    assert [r.message_id for r in env.inbox.retention_candidates(limit=10)] == ["NEW"]
    counts = env.inbox.retention_counts()
    assert counts["retention_pending"] == 1
    assert counts["retention_held"] == 0
    assert env.inbox.retention_last_error() is None


def _batch_ids(batches) -> list[tuple[str, list[str]]]:
    return [(chat, [record.message_id for record in records]) for chat, records in batches]


def test_which_messages_are_answered_does_not_depend_on_retention(tmp_path):
    env = _Env(tmp_path)
    photo = _png(env.capture / "img_666666666666.png")
    workbook = _xlsx(env.capture / "doc_777777777777_w.xlsx")
    env.capture_events(
        _message("M-TEXT", MANAGEMENT),
        _with_document(_message("M-DOC", MANAGEMENT), workbook),
        _with_image(_message("S-IMG", SITE), photo),
        _message("S-TEXT", SITE),
        _with_image(_message("OTHER-IMG", "other@g.us"), photo),
    )
    with env.inbox.connect() as conn:
        conn.execute("UPDATE ingress_events SET status='completed' WHERE message_id='S-TEXT'")
    kwargs = {"batch_size": 25, "priority_chats": {MANAGEMENT}, "exclude_chats": {"other@g.us"}}
    unconverged = env.inbox.pending_chat_batches(**kwargs)

    # The same population once every attachment has converged: what the
    # 22 July selector would eventually have answered.
    with env.inbox.connect() as conn:
        conn.execute(
            "UPDATE ingress_events SET retention_state='complete' "
            "WHERE retention_state='pending'"
        )
    converged = env.inbox.pending_chat_batches(**kwargs)

    assert _batch_ids(unconverged[0]) == _batch_ids(converged[0]) == [
        (MANAGEMENT, ["M-TEXT", "M-DOC"])
    ]
    assert _batch_ids(unconverged[1]) == _batch_ids(converged[1]) == [(SITE, ["S-IMG"])]


def test_management_quiet_window_still_waits_for_a_sibling_file(tmp_path):
    from datetime import datetime, timedelta, timezone

    env = _Env(tmp_path)
    workbook = _xlsx(env.capture / "doc_888888888888_w.xlsx")
    env.capture_events(
        _message("ASK", MANAGEMENT, body="@Christopher cross-check the two files"),
        _with_document(_message("FILE", MANAGEMENT), workbook),
    )
    now = datetime.now(timezone.utc)
    held, _ = env.inbox.pending_chat_batches(
        batch_size=25, priority_chats={MANAGEMENT}, priority_quiet_seconds=5, now=now
    )
    assert held == []
    released, _ = env.inbox.pending_chat_batches(
        batch_size=25,
        priority_chats={MANAGEMENT},
        priority_quiet_seconds=5,
        now=now + timedelta(seconds=6),
    )
    assert _batch_ids(released) == [(MANAGEMENT, ["ASK", "FILE"])]


@pytest.mark.asyncio
async def test_mention_rule_still_skips_and_the_skipped_row_still_retains(
    tmp_path, monkeypatch
):
    env = _Env(tmp_path)
    systems = _Systems(monkeypatch)
    photo = _png(env.capture / "img_999999999999.png")
    env.capture_events(_with_image(_message("NO-MENTION", MANAGEMENT), photo))
    records = _management_batch(env)

    await env.process(records, direct_trigger_required=True)

    assert env.plans == []
    row = env.row("NO-MENTION")
    assert row["status"] == "skipped"
    # One attempt was recorded, so the capture lane keeps converging it.
    assert row["retention_attempts"] == 1
    env.make_retained_entries_old()
    systems.available = True
    await consumer.retain_pending_media(env.inbox, config_path=env.config, limit=25)
    assert env.row("NO-MENTION")["retention_state"] == "complete"
    assert env.row("NO-MENTION")["status"] == "skipped"
