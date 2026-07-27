from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deploy.tgg.christopher.quality_eval import core


NOW = datetime(2026, 7, 27, 13, 0, tzinfo=timezone.utc)


def completed(seq: int, message_id: str, *, media: list[str] | None = None) -> dict:
    raw = {
        "type": "whatsapp_capture_event",
        "normalized": {
            "messageId": message_id,
            "senderName": "Ali",
            "text": "Kitchen tap done",
            "mediaPaths": media or [],
        },
    }
    return {
        "seq": seq,
        "message_id": message_id,
        "chat_id": "site@g.us",
        "raw_json": json.dumps(raw),
        "status": "completed",
    }


def snapshot() -> dict:
    return {
        "events": [completed(10, "wa-10"), completed(11, "wa-11")],
        "observations": [
            {
                "id": 2,
                "case_id": 7,
                "source_ref": None,
                "fields": json.dumps({"source_refs": ["wa-10", "wa-11"]}),
                "_matched_source_refs": ["wa-10", "wa-11"],
            }
        ],
        "cases": [{"id": 7, "job_no": "AM/JOB/2607/0001", "state": "completed"}],
    }


def judgment(checker: str = "judge-2") -> dict:
    registry = core.load_registry()
    values = ["pass", "fail", "unsure"]
    return {
        "checker_session_id": checker,
        "source_to_page": "pass",
        "page_to_source": "fail",
        "manager_readability": "unsure",
        "summary": "One source fact is missing.",
        "checks": [
            {"id": item.id, "result": values[index % 3], "evidence": f"evidence {index}"}
            for index, item in enumerate(registry.checks)
        ],
    }


def test_registry_has_exact_seed_checks_and_portal_scope():
    registry = core.load_registry()
    assert len(registry.checks) == 7
    assert registry.ids == (
        "sender-shows-real-name-not-site-worker",
        "no-contact-emoji-leak",
        "message-text-displayed-as-bubble-photos-underneath",
        "explicit-done-statement-moves-item-completed",
        "cant-do-statement-surfaced-not-generic-pending",
        "photos-bound-to-correct-work-item",
        "filter-covers-badge-states",
    )
    assert registry.checks[-1].scope == "portal"


@pytest.mark.parametrize(
    ("count", "last", "trigger", "expected", "reason"),
    [
        (0, None, "interval", False, "no-new-completions"),
        (25, NOW, "interval", True, "completion-threshold"),
        (1, NOW - timedelta(hours=4), "interval", True, "four-hour-nonzero"),
        (1, NOW - timedelta(hours=1), "interval", False, "below-threshold-and-window"),
        (1, NOW, "deploy", True, "deploy"),
        (1, NOW, "daily", True, "daily"),
    ],
)
def test_trigger_thresholds(count, last, trigger, expected, reason):
    state = {"last_success_at": core.iso(last) if last else None}
    assert core.trigger_due(state, count, trigger=trigger, now=NOW) == (expected, reason)


def test_collect_snapshot_uses_read_only_ssh_python():
    seen = {}

    def command(argv, **kwargs):
        seen["argv"] = argv
        seen["script"] = kwargs["input"]
        return subprocess.CompletedProcess(argv, 0, json.dumps(snapshot()), "")

    result = core.collect_snapshot(9, command=command)
    assert result["events"][0]["seq"] == 10
    target_index = seen["argv"].index("tgg-app-1")
    assert seen["argv"][target_index : target_index + 3] == ["tgg-app-1", "python3", "-"]
    assert "ConnectTimeout=15" in seen["argv"]
    assert "mode=ro" in seen["script"]
    assert "PRAGMA query_only=ON" in seen["script"]
    assert not any(word in seen["script"].upper() for word in ("UPDATE ", "INSERT ", "DELETE "))


def test_bundle_mapping_keeps_unmapped_completed_rows_visible():
    value = snapshot()
    value["events"].append(completed(12, "wa-unmapped"))
    bundles, unmapped = core.make_bundles(value)
    assert [item["message_id"] for item in bundles[0]["messages"]] == ["wa-10", "wa-11"]
    assert unmapped == ["wa-unmapped"]


def test_exact_public_per_case_url():
    url = core.case_url("AM/JOB/2607/0001")
    assert url.startswith("https://systems.papercut-labs.com/tgg?")
    assert "view=cases" in url and "mode=maintenance" in url
    assert "case=AM%2FJOB%2F2607%2F0001" in url


def test_media_pull_is_source_only_allowlisted(tmp_path):
    bundle = {
        "messages": [
            {
                "retained_media_paths": [
                    "/var/lib/tgg-capture/media/photo 1.jpg",
                    "/media/tgg/hermes/photo-2.jpg",
                ]
            }
        ]
    }
    calls = []

    def command(argv, **_kwargs):
        calls.append(argv)
        Path(argv[-1]).write_bytes(b"jpeg")
        return subprocess.CompletedProcess(argv, 0, "", "")

    pulled = core.pull_retained_media(bundle, tmp_path, command=command)
    assert calls[0][0] == "scp"
    assert "ConnectTimeout=15" in calls[0]
    assert "--" in calls[0]
    assert {
        call[call.index("--") + 1] for call in calls
    } == {
        "tgg-app-1:/var/lib/tgg-capture/media/photo 1.jpg",
        "tgg-app-1:/home/pclaw/.systems-pcl/data/media/tgg/hermes/photo-2.jpg",
    }
    assert Path(pulled[0]["local_path"]).read_bytes() == b"jpeg"
    with pytest.raises(core.EvalError, match="outside read-only roots"):
        core.pull_retained_media(
            {"messages": [{"retained_media_paths": ["/etc/passwd"]}]},
            tmp_path,
            command=command,
        )
    with pytest.raises(core.EvalError, match="traversal"):
        core.pull_retained_media(
            {"messages": [{"retained_media_paths": ["/media/tgg/hermes/../../etc/passwd"]}]},
            tmp_path,
            command=command,
        )


def test_walk_media_matches_canonical_media_object_shapes():
    raw = {
        "media": [
            {"path": "/var/lib/tgg-capture/a.jpg"},
            {"filePath": "/home/pclaw/.hermes-christopher-tgg/media/b.jpg"},
            {"localPath": "/home/pclaw/.systems-pcl/data/media/tgg/hermes/c.jpg"},
            {"url": "/media/tgg/hermes/d.jpg"},
        ],
        "unrelated": {"path": "/tmp/not-media"},
    }
    assert set(core._walk_media(raw)) == {
        "/var/lib/tgg-capture/a.jpg",
        "/home/pclaw/.hermes-christopher-tgg/media/b.jpg",
        "/home/pclaw/.systems-pcl/data/media/tgg/hermes/c.jpg",
        "/media/tgg/hermes/d.jpg",
    }


def test_browser_retries_loading_skeleton_until_case_detail(tmp_path):
    values = iter(
        [
            {},  # auth
            {},  # portal open
            {"snapshot": "Loading... skeleton"},
            {},  # wait
            {"snapshot": "TGG Cases"},
            {},  # portal screenshot
            {},  # case open
            {"url": core.case_url("AM/JOB/2607/0001")},
            {"snapshot": "Loading... skeleton"},
            {},  # wait
            {"snapshot": "Case AM/JOB/2607/0001 details"},
            {},  # case screenshot
            {},  # close
        ]
    )
    calls = []

    def command(argv, **_kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv, 0, json.dumps({"success": True, "data": next(values)}), ""
        )

    captures = core.Browser(command).capture(
        "fixture",
        [{"case": {"job_no": "AM/JOB/2607/0001"}}],
        tmp_path,
    )
    assert "AM/JOB/2607/0001" in Path(captures["AM/JOB/2607/0001"]["snapshot"]).read_text()
    assert sum(argv[-2:] == ["wait", "1000"] for argv in calls) == 2


def test_browser_rejects_false_success_envelope():
    def command(argv, **_kwargs):
        return subprocess.CompletedProcess(
            argv, 0, json.dumps({"success": False, "error": "not authenticated"}), ""
        )

    with pytest.raises(core.EvalError, match="did not succeed"):
        core.Browser(command)._run("fixture", ["open", "https://example.invalid"])


def test_strict_judgment_preserves_unsure_and_rejects_maker():
    registry = core.load_registry()
    value = core.validate_judgment(judgment(), registry, "maker-1")
    assert value["manager_readability"] == "unsure"
    assert any(item["result"] == "unsure" for item in value["checks"])
    with pytest.raises(core.EvalError, match="equals maker"):
        core.validate_judgment(judgment("maker-1"), registry, "maker-1")
    with pytest.raises(core.EvalError, match="maker_session_id is required"):
        core.validate_judgment(judgment(), registry, "")
    broken = judgment()
    broken["checks"].pop()
    with pytest.raises(core.EvalError, match="exactly match"):
        core.validate_judgment(broken, registry, "maker-1")


def test_injected_judge_command_receives_paths_and_is_strict(tmp_path):
    bundle = tmp_path / "bundle.json"
    bundle.write_text("{}")
    screen = tmp_path / "screen.png"
    screen.write_bytes(b"x")
    seen = {}

    def command(argv, **kwargs):
        seen["request"] = json.loads(kwargs["input"])
        return subprocess.CompletedProcess(argv, 0, json.dumps(judgment()), "")

    result = core.Judge(command=command, command_argv=["fixture-judge"]).judge(
        bundle,
        {
            "screenshot": str(screen),
            "portal_screenshot": str(screen),
            "snapshot": str(bundle),
            "portal_snapshot": str(bundle),
            "url": core.case_url("AM/JOB/2607/0001"),
        },
        core.load_registry(),
        "maker-1",
    )
    assert result["page_to_source"] == "fail"
    assert seen["request"]["maker_session_id"] == "maker-1"


def test_default_judge_starts_fresh_checker_thread(tmp_path, monkeypatch):
    bundle = tmp_path / "bundle.json"
    source_image = tmp_path / "source.jpg"
    source_image.write_bytes(b"jpg")
    bundle.write_text(json.dumps({"retained_media": [{"local_path": str(source_image)}]}))
    screen = tmp_path / "screen.png"
    screen.write_bytes(b"png")
    monkeypatch.setenv("CODEX_THREAD_ID", "maker-thread")
    monkeypatch.setenv("MARSHAL_SESSION_ID", "maker-thread")
    seen = {}

    def command(argv, **kwargs):
        seen["argv"] = argv
        seen["env"] = kwargs["env"]
        output = Path(argv[argv.index("--output-last-message") + 1])
        output.write_text(json.dumps(judgment("ignored-model-value")))
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"type": "thread.started", "thread_id": "fresh-checker"}) + "\n",
            "",
        )

    result = core.Judge(command=command).judge(
        bundle,
        {
            "screenshot": str(screen),
            "portal_screenshot": str(screen),
            "snapshot": str(bundle),
            "portal_snapshot": str(bundle),
            "url": core.case_url("AM/JOB/2607/0001"),
        },
        core.load_registry(),
        "maker-thread",
    )
    assert result["checker_session_id"] == "fresh-checker"
    assert "CODEX_THREAD_ID" not in seen["env"]
    assert "MARSHAL_SESSION_ID" not in seen["env"]
    image_args = [
        seen["argv"][index + 1]
        for index, value in enumerate(seen["argv"])
        if value == "-i"
    ]
    assert str(source_image) in image_args


def test_defect_filing_dedupes_locally(tmp_path):
    store = core.StateStore(tmp_path)
    calls = []

    def command(argv, **_kwargs):
        calls.append(argv)
        if argv[2] == "search":
            return subprocess.CompletedProcess(argv, 0, json.dumps({"ok": True, "data": []}), "")
        if argv[2] == "get":
            return subprocess.CompletedProcess(
                argv, 0, json.dumps({"ok": True, "data": {"lane": "ongoing"}}), ""
            )
        return subprocess.CompletedProcess(argv, 0, "wb-123\n", "")

    filer = core.DefectFiler(store, command=command)
    kwargs = {
        "job_no": "AM/JOB/2607/0001",
        "check": {"id": "no-contact-emoji-leak", "result": "fail", "evidence": "emoji"},
        "screenshot": "/tmp/case.png",
        "message_ids": ["wa-10"],
        "judgment_path": "/tmp/judgment.json",
    }
    assert filer.file(**kwargs) == ("wb-123", True)
    assert filer.file(**kwargs) == ("wb-123", False)
    assert sum(argv[2] == "create" for argv in calls) == 1


def test_defect_dedupe_read_failure_keeps_existing_row(tmp_path):
    store = core.StateStore(tmp_path)
    key = core.defect_key(
        "AM/JOB/2607/0001",
        "sender-shows-real-name-not-site-worker",
        ["wa-10"],
    )
    store.save_defects({key: "wb-existing"})

    def command(argv, **_kwargs):
        return subprocess.CompletedProcess(argv, 1, "", "read failed")

    filer = core.DefectFiler(store, command=command)
    result = filer.file(
        job_no="AM/JOB/2607/0001",
        check={
            "id": "sender-shows-real-name-not-site-worker",
            "result": "fail",
            "evidence": "wrong label",
        },
        screenshot="/tmp/case.png",
        message_ids=["wa-10"],
        judgment_path="/tmp/judgment.json",
    )
    assert result == ("wb-existing", False)


def test_defect_dedupe_ignores_fuzzy_unrelated_search_row(tmp_path):
    store = core.StateStore(tmp_path)
    calls = []

    def command(argv, **_kwargs):
        calls.append(argv)
        if argv[2] == "search":
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    {
                        "ok": True,
                        "data": [
                            {
                                "id": "wb-unrelated",
                                "lane": "ongoing",
                                "title": "Unrelated TGG output task",
                                "description": "fuzzy terms only",
                            }
                        ],
                    }
                ),
                "",
            )
        return subprocess.CompletedProcess(argv, 0, "wb-correct\n", "")

    filer = core.DefectFiler(store, command=command)
    result = filer.file(
        job_no="AM/JOB/2607/0001",
        check={"id": "no-contact-emoji-leak", "result": "fail", "evidence": "emoji"},
        screenshot="/tmp/case.png",
        message_ids=["wa-10"],
        judgment_path="/tmp/judgment.json",
    )
    assert result == ("wb-correct", True)
    assert sum(argv[2] == "create" for argv in calls) == 1


def test_state_lock_refuses_overlapping_runner(tmp_path):
    store = core.StateStore(tmp_path)
    with store.exclusive_run():
        with pytest.raises(core.RunAlreadyActive):
            with store.exclusive_run():
                pass


def test_lock_contended_cli_reports_health_instead_of_unconditional_green(tmp_path, capsys):
    store = core.StateStore(tmp_path)
    state = store.load()
    state["batches_occurred"] = 1
    store.save(state)
    with store.exclusive_run():
        rc = core.main(
            [
                "--state-dir",
                str(tmp_path),
                "run",
                "--trigger",
                "interval",
                "--maker-session-id",
                "maker",
            ]
        )
    output = json.loads(capsys.readouterr().out)
    assert rc == 1
    assert output["ok"] is False
    assert output["reason"] == "already-running"
    assert output["coverage_ratio"] == 0.0


def test_run_command_applies_default_timeout(monkeypatch):
    seen = {}

    def fake_run(argv, **kwargs):
        seen.update(kwargs)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(core.subprocess, "run", fake_run)
    core.run_command(["example"])
    assert seen["timeout"] == 900


class FakeBrowser:
    def capture(self, _run_id, bundles, run_dir):
        captures = {}
        portal = run_dir / "portal.png"
        portal.write_bytes(b"png")
        portal_snapshot = run_dir / "portal.txt"
        portal_snapshot.write_text("cases")
        for bundle in bundles:
            job = bundle["case"]["job_no"]
            screenshot = run_dir / "case.png"
            screenshot.write_bytes(b"png")
            snap = run_dir / "case.txt"
            snap.write_text("case")
            captures[job] = {
                "url": core.case_url(job),
                "screenshot": str(screenshot),
                "snapshot": str(snap),
                "portal_screenshot": str(portal),
                "portal_snapshot": str(portal_snapshot),
            }
        return captures


class FakeJudge:
    def __init__(self, fail=False):
        self.fail = fail
        self.calls = 0

    def judge(self, *args, **_kwargs):
        self.calls += 1
        if self.fail:
            raise core.EvalError("judge broke")
        if args and Path(args[0]).name == "golden-source.json":
            return json.loads((core.FIXTURE_DIR / "golden-judge.json").read_text())
        return judgment()


class FakeFiler:
    def __init__(self):
        self.calls = []

    def file(self, **kwargs):
        self.calls.append(kwargs)
        return f"wb-{len(self.calls)}", True


def test_failed_run_does_not_advance_cursor_and_retry_occurrence_is_idempotent(tmp_path):
    store = core.StateStore(tmp_path)
    judge = FakeJudge(fail=True)
    evaluator = core.Evaluator(
        store,
        collect=lambda _cursor: snapshot(),
        browser=FakeBrowser(),
        judge=judge,
        filer=FakeFiler(),
        clock=lambda: NOW,
    )
    with pytest.raises(core.EvalError, match="judge broke"):
        evaluator.run(trigger="deploy", maker_session_id="maker-1")
    assert store.load()["cursor"] == 0
    assert store.load()["batches_occurred"] == 1
    with pytest.raises(core.EvalError):
        evaluator.run(trigger="deploy", maker_session_id="maker-1")
    assert store.load()["batches_occurred"] == 1


def test_successful_run_commits_cursor_after_artifacts_and_defects(tmp_path, monkeypatch):
    monkeypatch.setattr(core, "pull_retained_media", lambda bundle, destination: [])
    store = core.StateStore(tmp_path)
    filer = FakeFiler()
    judge = FakeJudge()
    evaluator = core.Evaluator(
        store,
        collect=lambda cursor: snapshot() if cursor == 0 else {"events": [], "observations": [], "cases": []},
        browser=FakeBrowser(),
        judge=judge,
        filer=filer,
        clock=lambda: NOW,
    )
    result = evaluator.run(trigger="deploy", maker_session_id="maker-1")
    assert result["ran"] is True
    assert result["run_id"] == "cursor-1-11"
    assert result["committed_cursor"] == 11
    assert result["unmapped_message_ids"] == []
    assert store.load()["cursor"] == 11
    assert store.load()["registry_hash"] == core.load_registry().digest
    assert store.load()["batches_evaluated"] == 1
    assert store.load()["last_completed_rows"] == 2
    assert store.load()["last_unmapped_count"] == 0
    assert judge.calls == 2  # golden regression, then the live case
    assert any(call["check"]["result"] == "unsure" for call in filer.calls)
    receipt = next((tmp_path / "runs").glob("*/receipt.json"))
    assert json.loads(receipt.read_text())["completed_rows"] == 2
    second = evaluator.run(trigger="interval", maker_session_id="maker-1")
    assert second["ran"] is False


def test_successful_run_reconciles_prior_failed_occurrences(tmp_path, monkeypatch):
    monkeypatch.setattr(core, "pull_retained_media", lambda bundle, destination: [])
    store = core.StateStore(tmp_path)
    state = store.load()
    state.update(
        {
            "batches_occurred": 2,
            "batches_evaluated": 0,
            "occurred_batch_keys": ["1:9", "1:10"],
            "registry_hash": core.load_registry().digest,
        }
    )
    store.save(state)
    evaluator = core.Evaluator(
        store,
        collect=lambda _cursor: snapshot(),
        browser=FakeBrowser(),
        judge=FakeJudge(),
        filer=FakeFiler(),
        clock=lambda: NOW,
    )
    evaluator.run(trigger="deploy", maker_session_id="maker-1")
    final_state = store.load()
    assert final_state["batches_occurred"] == 3
    assert final_state["batches_evaluated"] == 3
    assert core.health(store, NOW)["coverage_ratio"] == 1.0


def test_health_reports_required_population_metrics(tmp_path):
    store = core.StateStore(tmp_path)
    state = store.load()
    state.update(
        {
            "last_success_at": core.iso(NOW - timedelta(hours=1)),
            "batches_evaluated": 3,
            "batches_occurred": 3,
            "defect_trend": [{"defects": 2}],
            "human_caught": 1,
            "loop_caught": 4,
            "cursor": 99,
            "last_completed_rows": 25,
            "last_unmapped_count": 2,
        }
    )
    store.save(state)
    result = core.health(store, NOW)
    assert result["ok"] is True
    assert result["coverage_ratio"] == 1.0
    assert result["defect_trend"] == [{"defects": 2}]
    assert (result["human_caught"], result["loop_caught"]) == (1, 4)
    assert (result["last_completed_rows"], result["last_unmapped_count"]) == (25, 2)


def test_run_cli_emits_top_level_health_metrics(tmp_path, monkeypatch, capsys):
    class StubEvaluator:
        def __init__(self, _store):
            pass

        def run(self, **_kwargs):
            return {"ok": True, "ran": False, "reason": "no-new-completions"}

    monkeypatch.setattr(core, "Evaluator", StubEvaluator)
    monkeypatch.setattr(
        core,
        "health",
        lambda _store: {
            "ok": True,
            "batches_evaluated": 4,
            "batches_occurred": 4,
            "coverage_ratio": 1.0,
            "defect_trend": [{"defects": 1}],
            "human_caught": 2,
            "loop_caught": 3,
            "last_completed_rows": 25,
            "last_unmapped_count": 0,
        },
    )
    assert core.main(
        [
            "--state-dir",
            str(tmp_path),
            "run",
            "--trigger",
            "interval",
            "--maker-session-id",
            "check-runner",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["coverage_ratio"] == 1.0
    assert output["defect_trend"] == [{"defects": 1}]
    assert (output["human_caught"], output["loop_caught"]) == (2, 3)


def test_external_finalize_strict_shape_commits_after_defect_filing(tmp_path):
    batch = snapshot()
    batch.update({"cursor_start": 9, "cursor_end": 11})
    # Root's real collector serializes fields as objects, not JSON strings.
    batch["observations"][0]["fields"] = {"source_refs": ["wa-10", "wa-11"]}
    batch_path = tmp_path / "source-batch.json"
    batch_path.write_text(json.dumps(batch))
    registry = core.load_registry()
    case_checks = []
    for item in registry.checks:
        if item.scope == "case":
            case_checks.append(
                {
                    "check_id": item.id,
                    "outcome": "unsure" if item.id == "photos-bound-to-correct-work-item" else "pass",
                    "evidence_message_ids": ["wa-10"],
                    "reason": "Grounded fixture evidence.",
                    "page_evidence": "Case screenshot.",
                }
            )
    external = {
        "evaluator_session": "independent-judge",
        "overall": "One photo association is uncertain.",
        "cases": [
            {
                "case_id": 7,
                "job_no": "AM/JOB/2607/0001",
                "checks": case_checks,
                "page_to_source": "pass",
                "source_to_page": "pass",
                "manager_sense": "pass",
            }
        ],
        "portal_checks": [
            {
                "check_id": "filter-covers-badge-states",
                "outcome": "fail",
                "evidence_message_ids": [],
                "reason": "New badge has no filter.",
                "page_evidence": "Cases screenshot.",
            }
        ],
    }
    judge_path = tmp_path / "judge-result.json"
    judge_path.write_text(json.dumps(external))
    (tmp_path / "case-7.png").write_bytes(b"png")
    (tmp_path / "portal-cases.png").write_bytes(b"portal")
    filer = FakeFiler()
    store = core.StateStore(tmp_path / "state")
    core.initialize_cursor(store, 9, NOW)
    result = core.finalize_external(
        store,
        batch_path=batch_path,
        judgment_path=judge_path,
        artifacts_dir=tmp_path,
        maker_session_id="maker-1",
        filer=filer,
        golden_judge=FakeJudge(),
        now=NOW,
    )
    assert result["committed_cursor"] == 11
    assert store.load()["cursor"] == 11
    assert len(filer.calls) == 2
    assert filer.calls[0]["message_ids"] == ["wa-10"]
    assert filer.calls[1]["screenshot"].endswith("portal-cases.png")
    evidence_dir = tmp_path / "state" / "runs" / "external-10-11"
    assert Path(filer.calls[0]["screenshot"]).parent == evidence_dir
    assert Path(filer.calls[0]["judgment_path"]) == evidence_dir / "judge-result.json"
    assert (evidence_dir / "source-batch.json").exists()
    assert (evidence_dir / "case-7.png").exists()
    assert (evidence_dir / "portal-cases.png").exists()
    assert core.finalize_external(
        store,
        batch_path=batch_path,
        judgment_path=judge_path,
        artifacts_dir=tmp_path,
        maker_session_id="maker-1",
        filer=filer,
        golden_judge=FakeJudge(),
        now=NOW,
    )["reason"] == "already-finalized"
    external["evaluator_session"] = "maker-1"
    judge_path.write_text(json.dumps(external))
    fresh_store = core.StateStore(tmp_path / "fresh-state")
    core.initialize_cursor(fresh_store, 9, NOW)
    with pytest.raises(core.EvalError, match="equals maker"):
        core.finalize_external(
            fresh_store,
            batch_path=batch_path,
            judgment_path=judge_path,
            artifacts_dir=tmp_path,
            maker_session_id="maker-1",
            filer=filer,
            golden_judge=FakeJudge(),
            now=NOW,
        )


def test_initialize_cursor_requires_pristine_state(tmp_path):
    store = core.StateStore(tmp_path / "state")
    assert core.initialize_cursor(store, 23, NOW)["cursor"] == 23
    with pytest.raises(core.EvalError, match="pristine"):
        core.initialize_cursor(store, 24, NOW)


def test_golden_fixture_covers_all_result_classes():
    fixture = json.loads((core.FIXTURE_DIR / "golden-judge.json").read_text())
    core.validate_judgment(fixture, core.load_registry(), "maker-1")
    values = {fixture[key] for key in ("source_to_page", "page_to_source", "manager_readability")}
    values.update(item["result"] for item in fixture["checks"])
    assert values == {"pass", "fail", "unsure"}
