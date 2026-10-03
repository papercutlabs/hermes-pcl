"""PA-333: the consumer starts the capability's photo-fill lane on its own thread and stops it on exit."""
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import durable_jsonl_consumer as consumer
from tests.gateway.test_durable_jsonl_consumer import _message, _source_projection_config, _write_jsonl

FAKE_LANE = '''
import json, threading
from pathlib import Path
def run_lane(stop_event, config_path, case_db, *, status=None):
    status["mode"] = "off"
    status["last_outcome"] = "off"
    marker = Path(config_path).parent / "lane-entered.json"
    marker.write_text(json.dumps({"thread": threading.current_thread().name, "case_db": str(case_db)}))
    stop_event.wait()
'''


def _install_lane(home: Path, source: str = FAKE_LANE) -> Path:
    path = home / consumer.PHOTO_FILL_LANE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    return path


def test_lane_thread_starts_reports_and_stops(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("pa: {}\n")
    _install_lane(tmp_path)
    stop, thread = consumer._start_photo_fill_lane(config, tmp_path / "tgg.db")
    try:
        for _ in range(100):
            if (tmp_path / "lane-entered.json").exists():
                break
            time.sleep(0.01)
        entered = json.loads((tmp_path / "lane-entered.json").read_text())
        assert entered["thread"] == "christopher-photo-fill-lane"
        assert thread.daemon and thread.is_alive()
        status = consumer._photo_fill_status()["photo_fill_lane"]
        assert status["thread_alive"] is True and status["mode"] == "off"
    finally:
        consumer._stop_photo_fill_lane(stop, thread)
    assert not thread.is_alive()
    assert consumer._photo_fill_status()["photo_fill_lane"]["thread_alive"] is False


def test_lane_exception_is_logged_and_retried(tmp_path):
    config = tmp_path / "config.yaml"
    _install_lane(tmp_path, "calls = []\ndef run_lane(stop_event, config_path, case_db, *, status=None):\n"
                            "    raise RuntimeError('lane fault')\n")
    stop = threading.Event()
    status: dict = {}
    seen = []

    def stop_after_two(timeout):
        seen.append(status.get("last_error_class"))
        if len(seen) >= 2:
            stop.set()
        return stop.is_set()

    stop.wait = stop_after_two  # type: ignore[method-assign]
    consumer._photo_fill_lane(stop, config, tmp_path / "tgg.db", status, interval_seconds=0.001)
    assert seen == ["RuntimeError", "RuntimeError"]


def test_lane_not_installed_idles(tmp_path):
    stop = threading.Event()
    status: dict = {}
    original = stop.wait

    def once(timeout):
        stop.set()
        return original(0)

    stop.wait = once  # type: ignore[method-assign]
    consumer._photo_fill_lane(stop, tmp_path / "config.yaml", tmp_path / "tgg.db", status, interval_seconds=0.001)
    assert status["last_outcome"] == "lane_not_installed"


@pytest.mark.asyncio
async def test_run_consumer_runs_the_lane_and_joins_it_on_exit(tmp_path, monkeypatch):
    config = _source_projection_config(tmp_path)
    _install_lane(config.parent)
    started: list[threading.Thread] = []
    real_start = consumer._start_photo_fill_lane

    def record(config_path, case_db):
        stop, thread = real_start(config_path, case_db)
        started.append(thread)
        return stop, thread

    monkeypatch.setattr(consumer, "_start_photo_fill_lane", record)
    monkeypatch.setattr(consumer, "_project_source_envelope", lambda *_a, **_k: {"ok": True})
    source = tmp_path / "capture.jsonl"
    _write_jsonl(source, [{"normalized": _message("lane-hook"), "capture": {"sequence": 1}}])
    cursor = tmp_path / "cursor.json"
    consumer.initialize_cursor(source, cursor, position="start")
    gate = tmp_path / "gate.json"
    gate.write_text(json.dumps({"version": 1, "enabled": False, "generation": 7}), encoding="utf-8")
    args = SimpleNamespace(
        config=str(config), source=str(source), cursor=str(cursor), inbox=str(tmp_path / "inbox.db"),
        status_file=str(tmp_path / "status.json"), processing_gate=str(gate),
        state_db=str(tmp_path / "state.db"), case_db=str(tmp_path / "case.db"),
        source_before_image_dir=str(tmp_path / "before"), lock_file=str(tmp_path / "lock"),
        activity_lock_file=None, site_concurrency=1, chat_batch_size=2,
        retention_batch_size=2, source_projection_batch_size=10,
        poll_seconds=0.01, max_records=10, once=True,
    )
    assert await consumer.run_consumer(args) == 0
    assert len(started) == 1 and not started[0].is_alive()
    status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert "photo_fill_lane" in status
