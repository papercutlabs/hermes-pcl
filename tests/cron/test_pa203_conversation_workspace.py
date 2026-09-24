"""PA-203 Stream A: a scheduled Management job works in its conversation's folder.

The kernel jail needs Linux user namespaces, so these tests replace only the
jail process with a stand-in that keeps its file contract: seed the run's
``/work`` from the snapshot it was given, run the payload, drop
``result.json`` from the seed, and export ``/work`` with the completion marker.
Everything else is the real code: owner selection, the scheduler adapter,
snapshot, lock, merge publication, harvest and the delivery resolver.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

import tools.python_sandbox_tool as sandbox
from gateway.session_context import (
    clear_session_vars,
    get_session_env,
    set_session_vars,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pa" / "bobby_tgg_constitution.yaml"
MANAGEMENT_CHAT = "120363000000000001@g.us"
OTHER_MANAGEMENT_CHAT = "120363000000000002@g.us"
OPS_CHAT = "120363000000000003@g.us"
PROVIDER, MODEL = "openai-codex", "gpt-test"
# The live persistent-chat key shape read from the TGG host on 2026-09-24.
LIVE_KEY = (
    f"agent:live-drain:persistent-chat:{PROVIDER}:{MODEL}:whatsapp:group:{MANAGEMENT_CHAT}"
)
HOOKS: dict[str, threading.Event] = {}


class _FakeJail:
    """Stand-in for the unshare process with the jail's /work contract."""

    def __init__(self, argv, **_kwargs):
        plan = json.loads(Path(argv[-1]).read_text(encoding="utf-8"))
        run = Path(plan["run"])
        jail_work = run / "jail-work"
        jail_work.mkdir()
        if plan["seed"]:
            shutil.copytree(plan["seed"], jail_work, dirs_exist_ok=True)
        (jail_work / "result.json").unlink(missing_ok=True)
        code = (run / "script.py").read_text(encoding="utf-8")
        exec(code, {"WORK": jail_work, "Path": Path, "HOOKS": HOOKS, "json": json})
        export = run / "work"
        shutil.rmtree(export)
        shutil.copytree(jail_work, export)
        (export / sandbox._EXPORT_COMPLETE).touch()
        shutil.rmtree(jail_work)
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")
        self.returncode = 0
        self.pid = 0

    def poll(self):
        return 0

    def wait(self, timeout=None):
        return 0


@pytest.fixture()
def home(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    home.mkdir()
    constitution = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    constitution["selectors"] = [
        {
            "job_type": "tgg_management",
            "match": {"source.platform": "whatsapp", "source.chat_id": MANAGEMENT_CHAT},
        },
        {
            "job_type": "tgg_management",
            "match": {
                "source.platform": "whatsapp",
                "source.chat_id": OTHER_MANAGEMENT_CHAT,
            },
        },
        {
            "job_type": "tgg_ops_ingest",
            "match": {"source.platform": "whatsapp", "source.chat_id": OPS_CHAT},
        },
    ]
    constitution_path = home / "constitution.yaml"
    constitution_path.write_text(yaml.safe_dump(constitution), encoding="utf-8")
    (home / "config.yaml").write_text(
        yaml.safe_dump(
            {
                "model": {"provider": PROVIDER, "default": MODEL},
                "group_sessions_per_user": False,
                "pa": {"enabled": True, "constitution_path": str(constitution_path)},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr("cron.scheduler._hermes_home", home)
    monkeypatch.setattr(sandbox, "_probe", lambda force=False: (True, "ok"))
    monkeypatch.setattr(
        sandbox, "_load_config", lambda: {"enabled": True, "workspace": "session"}
    )
    monkeypatch.setattr(
        sandbox,
        "_generate_init_script",
        lambda run_dir, *args, **kwargs: json.dumps(
            {
                "run": str(run_dir),
                "seed": str(args[8]) if len(args) > 8 and args[8] else None,
            }
        ),
    )
    monkeypatch.setattr(
        sandbox,
        "subprocess",
        SimpleNamespace(
            Popen=_FakeJail,
            PIPE=subprocess.PIPE,
            DEVNULL=subprocess.DEVNULL,
            TimeoutExpired=subprocess.TimeoutExpired,
            run=subprocess.run,
        ),
    )
    HOOKS.clear()
    return home


def _write(name: str, text: str) -> str:
    return f"(WORK / {name!r}).parent.mkdir(parents=True, exist_ok=True)\n" \
        f"(WORK / {name!r}).write_text({text!r})\n"


def _read(name: str) -> str:
    return (
        "import json, os\n"
        f"p = WORK / {name!r}\n"
        "(WORK / 'result.json').write_text(json.dumps("
        "{'text': p.read_text() if p.exists() else None}))\n"
    )


def _live_turn(code: str, *, session_id: str = "live-session-1") -> dict:
    """One sandbox call from an ordinary Management conversation turn."""
    tokens = set_session_vars(
        platform="whatsapp",
        chat_id=MANAGEMENT_CHAT,
        session_key=LIVE_KEY,
        session_id=session_id,
    )
    try:
        return json.loads(
            sandbox._handle_python_sandbox({"code": code}, session_id=session_id)
        )
    finally:
        clear_session_vars(tokens)


def _job(chat_id: str = MANAGEMENT_CHAT, **extra) -> dict:
    job = {
        "id": "mgmt-morning-review",
        "name": "Morning case review",
        "prompt": "review the open cases and send the workbook",
        "pa_job_type": "tgg_management",
        "deliver": "origin",
        "origin": {"platform": "whatsapp", "chat_id": chat_id, "chat_name": "Management"},
    }
    job.update(extra)
    return job


def _run_scheduled(job: dict, code: str) -> tuple[dict, dict]:
    """Run one due job through the real tick/_process_job/run_job path.

    The model is replaced by one python_sandbox tool call; the job's delivery
    is captured instead of sent.
    """
    from cron import scheduler

    observed: dict = {}
    delivered: dict = {}

    def one_tool_call(_prompt, *args, **kwargs):
        observed["session_key"] = get_session_env("HERMES_SESSION_KEY")
        observed["tool"] = json.loads(
            sandbox._handle_python_sandbox(
                {"code": code}, session_id=get_session_env("HERMES_SESSION_ID")
            )
        )
        return {"completed": True, "final_response": "Review ready.\nMEDIA:work/report.xlsx"}

    def capture_delivery(job_arg, content, **kwargs):
        delivered.update(kwargs, content=content)
        return None

    with patch("cron.scheduler.get_due_jobs", return_value=[job]), \
         patch("cron.scheduler.advance_next_run"), \
         patch("cron.scheduler.save_job_output", return_value="out.md"), \
         patch("cron.scheduler.mark_job_run"), \
         patch("cron.scheduler._deliver_result", side_effect=capture_delivery), \
         patch("dotenv.load_dotenv"), \
         patch("hermes_state.SessionDB", return_value=MagicMock()), \
         patch("tools.mcp_tool.discover_mcp_tools", return_value=[]), \
         patch(
             "hermes_cli.runtime_provider.resolve_runtime_provider",
             return_value={
                 "api_key": "test-key",
                 "base_url": "https://provider.invalid/v1",
                 "provider": "openrouter",
                 "api_mode": "chat_completions",
             },
         ), \
         patch("run_agent.AIAgent") as agent_cls:
        agent = MagicMock()
        agent.run_conversation.side_effect = one_tool_call
        agent_cls.return_value = agent
        assert scheduler.tick(verbose=False) == 1
    return observed, delivered


# (a) ------------------------------------------------------------------------

def test_scheduled_workbook_is_in_the_later_conversation_turn(home):
    observed, delivered = _run_scheduled(
        _job(), _write("report.xlsx", "scheduled review: 47 blanks")
    )

    assert observed["tool"]["status"] == "success", observed["tool"]
    # The job keeps its own internal session identity for everything else.
    assert observed["session_key"].startswith("cron_mgmt-morning-review_")
    # Its delivery resolves the attachment from the conversation's folder.
    assert delivered["workspace_owner"] == LIVE_KEY
    sent = sandbox.resolve_sandbox_work_path("work/report.xlsx", delivered["workspace_owner"])
    assert sent.read_text() == "scheduled review: 47 blanks"

    later = _live_turn(_read("report.xlsx"))
    assert later["status"] == "success", later
    assert later["result"] == {"text": "scheduled review: 47 blanks"}


def test_conversation_key_matches_the_live_drain_namespace(home):
    from gateway.durable_jsonl_consumer import (
        management_conversation_session_key,
        persistent_chat_namespace,
    )

    assert persistent_chat_namespace(PROVIDER, MODEL) == (
        f"agent:live-drain:persistent-chat:{PROVIDER}:{MODEL}"
    )
    assert management_conversation_session_key(
        home / "config.yaml", platform="whatsapp", chat_id=MANAGEMENT_CHAT
    ) == LIVE_KEY


# (b) ------------------------------------------------------------------------

def test_scheduled_workbook_survives_conversation_compaction(home):
    before = _live_turn(_write("notes.txt", "earlier turn"), session_id="live-root")
    assert before["status"] == "success", before
    _run_scheduled(_job(), _write("report.xlsx", "scheduled review: 47 blanks"))

    # Compaction rotates the transcript session ID; the conversation key stays.
    after = _live_turn(
        "import json\n"
        "(WORK / 'result.json').write_text(json.dumps({"
        "'report': (WORK / 'report.xlsx').read_text(),"
        "'notes': (WORK / 'notes.txt').read_text()}))\n",
        session_id="live-root-compressed-child",
    )
    assert after["result"] == {
        "report": "scheduled review: 47 blanks",
        "notes": "earlier turn",
    }


# (c) ------------------------------------------------------------------------

def test_older_same_named_workbook_is_replaced_by_the_scheduled_one(home):
    old = _live_turn(_write("report.xlsx", "older review: 99 blanks"))
    assert old["status"] == "success", old

    _run_scheduled(_job(), _write("report.xlsx", "scheduled review: 47 blanks"))

    later = _live_turn(_read("report.xlsx"))
    assert later["result"] == {"text": "scheduled review: 47 blanks"}


def test_stale_concurrent_run_cannot_restore_the_older_workbook(home):
    """A turn seeded before the job published must not win with its old copy."""
    _live_turn(_write("report.xlsx", "older review: 99 blanks"))
    HOOKS["job_published"] = threading.Event()
    HOOKS["live_seeded"] = threading.Event()
    results: dict = {}

    def live():
        results["live"] = _live_turn(
            "HOOKS['live_seeded'].set()\n"
            "assert HOOKS['job_published'].wait(10)\n"
            + _write("notes.txt", "live note")
        )

    thread = threading.Thread(target=live)
    thread.start()
    assert HOOKS["live_seeded"].wait(10)
    _run_scheduled(_job(), _write("report.xlsx", "scheduled review: 47 blanks"))
    HOOKS["job_published"].set()
    thread.join(20)

    assert results["live"]["status"] == "success", results["live"]
    final = _live_turn(
        "import json\n"
        "(WORK / 'result.json').write_text(json.dumps({"
        "'report': (WORK / 'report.xlsx').read_text(),"
        "'notes': (WORK / 'notes.txt').read_text()}))\n"
    )
    assert final["result"] == {
        "report": "scheduled review: 47 blanks",
        "notes": "live note",
    }


# (d) ------------------------------------------------------------------------

def _interleaved(job_code: str, live_code: str) -> tuple[dict, dict]:
    """Job and live turn both seed before either publishes; job publishes first."""
    HOOKS["live_seeded"] = threading.Event()
    HOOKS["job_published"] = threading.Event()
    results: dict = {}

    def live():
        results["live"] = _live_turn(
            "HOOKS['live_seeded'].set()\n"
            "assert HOOKS['job_published'].wait(10)\n" + live_code
        )

    thread = threading.Thread(target=live)
    thread.start()
    assert HOOKS["live_seeded"].wait(10)
    observed, _ = _run_scheduled(_job(), job_code)
    HOOKS["job_published"].set()
    thread.join(20)
    return observed["tool"], results["live"]


def test_concurrent_job_and_live_turn_files_both_survive(home):
    job, live = _interleaved(
        _write("report.xlsx", "scheduled review"),
        _write("adhoc/summary.xlsx", "live summary"),
    )

    assert job["status"] == live["status"] == "success"
    work = home / "sandbox_workspaces" / sandbox._workspace_key(LIVE_KEY) / "work"
    assert (work / "report.xlsx").read_text() == "scheduled review"
    assert (work / "adhoc" / "summary.xlsx").read_text() == "live summary"


def test_concurrent_same_name_writes_keep_both_versions(home):
    job, live = _interleaved(
        _write("report.xlsx", "scheduled review"),
        _write("report.xlsx", "live edit"),
    )

    work = home / "sandbox_workspaces" / sandbox._workspace_key(LIVE_KEY) / "work"
    # The first publisher keeps the name; nothing the job reported moves.
    assert (work / "report.xlsx").read_text() == "scheduled review"
    [renamed] = live["workspace_renamed"]
    assert renamed["written"] == "work/report.xlsx"
    assert renamed["saved_as"] == f"work/report.{live['run_id']}.xlsx"
    assert {item["path"] for item in live["files"]} == {renamed["saved_as"]}
    saved = sandbox.resolve_sandbox_work_path(renamed["saved_as"], LIVE_KEY)
    assert saved.read_text() == "live edit"


def test_run_deletion_never_removes_a_file_another_writer_changed(tmp_path):
    work = tmp_path / "hermes" / "sandbox_workspaces" / ("s_" + "b" * 64) / "work"
    work.mkdir(parents=True)
    (work / "shared.txt").write_text("v1")
    (work / "scratch.txt").write_text("tmp")
    base = sandbox._work_manifest(work)
    output = tmp_path / "output"
    output.mkdir()  # this run deleted both files
    (work / "shared.txt").write_text("v2 from another writer")

    sandbox._publish_workspace(
        output, work, base, run_id="r_0000000d", hermes_home=tmp_path / "hermes"
    )

    assert (work / "shared.txt").read_text() == "v2 from another writer"
    assert not (work / "scratch.txt").exists()


def test_publication_lock_excludes_another_process(tmp_path):
    workspace = tmp_path / "sandbox_workspaces" / ("s_" + "c" * 64)
    workspace.mkdir(parents=True)
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import fcntl, os, sys\n"
            f"fd = os.open({str(workspace / sandbox._WORKSPACE_LOCK_FILE)!r}, os.O_RDWR | os.O_CREAT, 0o600)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\n"
            "print('held', flush=True)\n"
            "sys.stdin.read()\n",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout.readline().strip() == "held"
        acquired = threading.Event()

        def take():
            with sandbox._workspace_lock("other-process-owner", workspace):
                acquired.set()

        thread = threading.Thread(target=take)
        thread.start()
        assert not acquired.wait(0.5)
        holder.stdin.close()
        assert acquired.wait(10)
        thread.join(10)
    finally:
        holder.kill()
        holder.wait(10)


# (e) ------------------------------------------------------------------------

def test_job_from_another_conversation_cannot_see_this_folder(home):
    _run_scheduled(_job(), _write("report.xlsx", "management review"))

    observed, delivered = _run_scheduled(
        _job(OTHER_MANAGEMENT_CHAT, id="other-review"), _read("report.xlsx")
    )

    assert observed["tool"]["result"] == {"text": None}
    assert delivered["workspace_owner"] != LIVE_KEY
    with pytest.raises(ValueError):
        sandbox.resolve_sandbox_work_path("work/report.xlsx", delivered["workspace_owner"])


@pytest.mark.parametrize(
    "job",
    [
        _job(OPS_CHAT, id="ops-job"),
        _job(pa_job_type=None, id="unbound-job"),
        _job(origin=None, id="no-origin-job"),
    ],
)
def test_non_management_jobs_keep_a_per_run_folder(home, job):
    from cron.scheduler import _resolve_cron_pa_workspace_owner

    assert _resolve_cron_pa_workspace_owner(job) is None
    _live_turn(_write("report.xlsx", "management review"))

    observed, delivered = _run_scheduled(job, _read("report.xlsx"))

    assert observed["tool"]["result"] == {"text": None}
    assert delivered["workspace_owner"].startswith(f"cron_{job['id']}_")


def test_per_participant_group_sessions_have_no_conversation_folder(home):
    from cron.scheduler import _resolve_cron_pa_workspace_owner

    config = yaml.safe_load((home / "config.yaml").read_text())
    config["group_sessions_per_user"] = True
    (home / "config.yaml").write_text(yaml.safe_dump(config))

    assert _resolve_cron_pa_workspace_owner(_job()) is None


# Review fix 2 ----------------------------------------------------------------
# Publication never deletes the kept copy when the run's copy is unreadable or
# not a regular file, and a file replaced by a directory (or the reverse)
# keeps both versions.  Each case is reported in the result.


def _retained(tmp_path, files: dict[str, str]):
    work = tmp_path / "hermes" / "sandbox_workspaces" / ("s_" + "c" * 64) / "work"
    work.mkdir(parents=True)
    for name, text in files.items():
        (work / name).parent.mkdir(parents=True, exist_ok=True)
        (work / name).write_text(text)
    base = sandbox._work_manifest(work)
    output = tmp_path / "output"
    shutil.copytree(work, output)
    return work, base, output


def _publish(tmp_path, work, base, output):
    return sandbox._publish_workspace(
        output, work, base, run_id="r_0000000e", hermes_home=tmp_path / "hermes"
    )


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read a mode-0 file")
def test_unreadable_run_copy_keeps_the_retained_file(tmp_path):
    work, base, output = _retained(tmp_path, {"report.xlsx": "47 blanks"})
    (output / "report.xlsx").chmod(0)
    try:
        result = _publish(tmp_path, work, base, output)
    finally:
        (output / "report.xlsx").chmod(0o600)

    assert (work / "report.xlsx").read_text() == "47 blanks"
    assert result["kept"] == ["report.xlsx"]
    assert result["unpublished"] == ["report.xlsx"]


def test_file_replaced_by_a_directory_keeps_both_versions(tmp_path):
    work, base, output = _retained(tmp_path, {"a": "the file"})
    (output / "a").unlink()
    (output / "a").mkdir()
    (output / "a" / "b").write_text("the directory's file")

    result = _publish(tmp_path, work, base, output)

    assert (work / "a").read_text() == "the file"
    saved = result["renamed"]["a/b"]
    assert saved == "a.r_0000000e/b"
    assert (work / saved).read_text() == "the directory's file"
    assert result["kept"] == ["a"]
    assert result["unpublished"] == []


def test_directory_replaced_by_a_file_keeps_both_versions(tmp_path):
    work, base, output = _retained(tmp_path, {"a/x.txt": "inside the directory"})
    shutil.rmtree(output / "a")
    (output / "a").write_text("now a file")

    result = _publish(tmp_path, work, base, output)

    assert (work / "a" / "x.txt").read_text() == "inside the directory"
    saved = result["renamed"]["a"]
    assert (work / saved).read_text() == "now a file"
    assert result["kept"] == ["a/x.txt"]


def test_file_replaced_by_a_symlink_keeps_the_retained_file_and_reports_it(tmp_path):
    work, base, output = _retained(tmp_path, {"report.xlsx": "47 blanks"})
    (output / "report.xlsx").unlink()
    (output / "report.xlsx").symlink_to("/etc/hosts")

    result = _publish(tmp_path, work, base, output)

    assert (work / "report.xlsx").read_text() == "47 blanks"
    assert not (work / "report.xlsx").is_symlink()
    assert result["kept"] == ["report.xlsx"]


def test_a_real_deletion_is_still_published(tmp_path):
    work, base, output = _retained(tmp_path, {"old.txt": "x", "dir/y.txt": "y"})
    (output / "old.txt").unlink()
    shutil.rmtree(output / "dir")

    result = _publish(tmp_path, work, base, output)

    assert not (work / "old.txt").exists()
    assert not (work / "dir" / "y.txt").exists()
    assert result["kept"] == []


def test_type_change_is_reported_in_the_tool_result(home):
    _live_turn(_write("a", "the file"))
    code = (
        "(WORK / 'a').unlink()\n"
        "(WORK / 'a').mkdir()\n"
        "(WORK / 'a' / 'b').write_text('inside')\n"
    )
    result = _live_turn(code, session_id="live-session-2")

    [renamed] = result["workspace_renamed"]
    assert renamed["written"] == "work/a/b"
    assert renamed["saved_as"] == f"work/a.{result['run_id']}/b"
    assert result["workspace_kept"] == ["work/a"]
    assert "workspace_kept_note" in result
    work = home / "sandbox_workspaces" / sandbox._workspace_key(LIVE_KEY) / "work"
    assert (work / "a").read_text() == "the file"


# Review fix 6: scheduled delivery attachments ---------------------------------


def test_scheduled_run_records_the_files_it_published(home):
    _live_turn(_write("older.xlsx", "an older Management workbook"))

    _observed, delivered = _run_scheduled(_job(), _write("report.xlsx", "scheduled review"))

    assert delivered["published_paths"] == {"report.xlsx"}


def _bridge(job, chat_id, media, allowed, monkeypatch):
    from cron.scheduler import _deliver_whatsapp_via_tgg_reply_bridge

    sent = []

    class Response:
        status = 200

        def read(self):
            return b'{"success":true}'

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", "http://bridge.test")
    with patch(
        "urllib.request.urlopen",
        side_effect=lambda req, timeout: sent.append(json.loads(req.data)) or Response(),
    ):
        error = _deliver_whatsapp_via_tgg_reply_bridge(
            job, chat_id, "", [(media, False)],
            workspace_owner=LIVE_KEY, allowed_work_paths=allowed,
        )
    return error, sent


def test_non_origin_delivery_attaches_only_files_the_job_made(home, monkeypatch):
    _live_turn(_write("older.xlsx", "an older Management workbook"))
    _live_turn(_write("report.xlsx", "job output"), session_id="live-session-3")

    refused, sent = _bridge(_job(), OPS_CHAT, "work/older.xlsx", {"report.xlsx"}, monkeypatch)
    assert refused and "did not produce" in refused
    assert sent == []

    allowed, sent = _bridge(_job(), OPS_CHAT, "work/report.xlsx", {"report.xlsx"}, monkeypatch)
    assert allowed is None
    assert [item["fileName"] for item in sent] == ["report.xlsx"]


def test_origin_delivery_may_attach_any_file_of_the_conversation_folder(home, monkeypatch):
    _live_turn(_write("older.xlsx", "an older Management workbook"))

    error, sent = _bridge(_job(), MANAGEMENT_CHAT, "work/older.xlsx", None, monkeypatch)

    assert error is None
    assert [item["fileName"] for item in sent] == ["older.xlsx"]


@pytest.mark.parametrize(
    ("deliver", "restricted"),
    [("origin", False), (f"whatsapp:{OPS_CHAT}", True)],
)
def test_deliver_result_restricts_only_non_origin_targets(deliver, restricted):
    from cron import scheduler
    from gateway.config import Platform

    pconfig = MagicMock()
    pconfig.enabled = False
    config = MagicMock()
    config.platforms = {Platform.WHATSAPP: pconfig}
    seen = {}

    def bridge(job, chat_id, text, media, **kwargs):
        seen["allowed"] = kwargs.get("allowed_work_paths")
        return None

    job = _job(deliver=deliver)
    with patch.dict(os.environ, {"TGG_REPLY_BRIDGE_URL": "http://bridge.test"}), \
         patch("gateway.config.load_gateway_config", return_value=config), \
         patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
         patch("cron.scheduler._deliver_whatsapp_via_tgg_reply_bridge", side_effect=bridge):
        assert scheduler._deliver_result(
            job, "done\nMEDIA:work/report.xlsx",
            workspace_owner=LIVE_KEY, published_paths={"report.xlsx"},
        ) is None

    assert seen["allowed"] == ({"report.xlsx"} if restricted else None)
