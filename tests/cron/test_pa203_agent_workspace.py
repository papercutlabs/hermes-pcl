"""PA-203 round 2: one persistent Christopher workspace across everything.

``python_sandbox.workspace: agent`` gives every chat turn in every
conversation, every scheduled job and every delegated helper the same
folder.  It does not depend on the provider or model, so a model change
keeps it; files people send and files that go out are kept in it; earlier
per-conversation folders are merged in without loss; concurrent writers
keep both versions of a clashing name.

The jail stand-in and the scheduler driver come from the round 1 tests; only
the jail process is replaced.
"""

from __future__ import annotations

import contextvars
import hashlib
import io
import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

import tools.python_sandbox_tool as sandbox
from tests.cron.test_pa203_conversation_workspace import (  # noqa: F401 - fixture
    HOOKS,
    LIVE_KEY,
    MANAGEMENT_CHAT,
    MODEL,
    OPS_CHAT,
    OTHER_MANAGEMENT_CHAT,
    PROVIDER,
    _job,
    _read,
    _run_scheduled,
    _write,
    home,
)
from gateway.session_context import clear_session_vars, set_session_vars

OTHER_KEY = f"agent:live-drain:conversation:whatsapp:group:{OTHER_MANAGEMENT_CHAT}"
SANDBOX_CONFIG = {"enabled": True, "workspace": "agent"}


class _FakeMountJail:
    """Stand-in for the jail with the direct-mount contract.

    The real jail bind-mounts the agent folder at /work; this runs the
    payload with ``WORK`` set to that same folder, on a thread so the run
    loop's size watch and kill path are exercised.  Only the unshare process
    is replaced; everything around it is the real code.
    """

    def __init__(self, argv, **_kwargs):
        plan = json.loads(Path(argv[-1]).read_text(encoding="utf-8"))
        assert plan["direct_work"], "agent mode must mount the folder directly"
        assert plan["seed"] is None
        self.work = Path(plan["direct_work"])
        code = (Path(plan["run"]) / "script.py").read_text(encoding="utf-8")
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")
        self.pid = -1
        self.returncode = None
        self.killed = threading.Event()

        def body():
            try:
                exec(code, {"WORK": self.work, "Path": Path, "HOOKS": HOOKS,
                            "json": json, "KILLED": self.killed, "os": os})
                code_rc = 0
            except BaseException:
                code_rc = 1
            self.returncode = -9 if self.killed.is_set() else code_rc

        self.thread = threading.Thread(target=body, daemon=True)
        self.thread.start()

    def poll(self):
        return None if self.thread.is_alive() else self.returncode

    def wait(self, timeout=None):
        self.thread.join(timeout)
        return self.returncode


@pytest.fixture()
def agent_home(home, monkeypatch):
    monkeypatch.setattr(sandbox, "_load_config", lambda: dict(SANDBOX_CONFIG))
    monkeypatch.setattr(
        sandbox,
        "_generate_init_script",
        lambda run_dir, *args, **kwargs: json.dumps({
            "run": str(run_dir),
            "seed": str(args[8]) if len(args) > 8 and args[8] else None,
            "direct_work": str(kwargs["direct_work"]) if kwargs.get("direct_work") else None,
        }),
    )
    monkeypatch.setattr(sandbox.subprocess, "Popen", _FakeMountJail)
    monkeypatch.setattr(sandbox, "_kill_group", lambda proc, grace=5.0: proc.killed.set())
    return home


def _agent_work(home: Path) -> Path:
    return (
        home
        / "sandbox_workspaces"
        / ("s_" + hashlib.sha256(sandbox.AGENT_WORKSPACE_OWNER.encode()).hexdigest())
        / "work"
    )


def _turn(code: str, *, chat: str = MANAGEMENT_CHAT, key: str = LIVE_KEY,
          session_id: str = "live-session-1") -> dict:
    tokens = set_session_vars(
        platform="whatsapp", chat_id=chat, session_key=key, session_id=session_id
    )
    try:
        return json.loads(
            sandbox._handle_python_sandbox({"code": code}, session_id=session_id)
        )
    finally:
        clear_session_vars(tokens)


def _helper(code: str) -> dict:
    """A delegated child: its own session ID, no inherited session context."""
    result: dict = {}

    def child():
        result["tool"] = json.loads(
            sandbox._handle_python_sandbox({"code": code}, session_id="child-session-7")
        )

    thread = threading.Thread(target=lambda: contextvars.Context().run(child))
    thread.start()
    thread.join(20)
    return result["tool"]


def _files_log(home: Path) -> list[dict]:
    path = _agent_work(home) / sandbox.FILES_LOG_NAME
    return [json.loads(line) for line in path.read_text().splitlines()]


# 1. One workspace across two conversations, a scheduled job and a helper ----


def test_two_conversations_a_scheduled_job_and_a_helper_share_one_folder(agent_home):
    first = _turn(_write("from_management.txt", "management"))
    second = _turn(
        _write("from_other_chat.txt", "other chat"),
        chat=OTHER_MANAGEMENT_CHAT,
        key=OTHER_KEY,
        session_id="other-session",
    )
    observed, delivered = _run_scheduled(_job(), _write("from_job.txt", "scheduled"))
    helper = _helper(_write("from_helper.txt", "helper"))
    for result in (first, second, observed["tool"], helper):
        assert result["status"] == "success", result

    everything = (
        "import json\n"
        "names = ['from_management.txt', 'from_other_chat.txt', 'from_job.txt', "
        "'from_helper.txt']\n"
        "(WORK / 'result.json').write_text(json.dumps({n: (WORK / n).read_text() "
        "for n in names}))\n"
    )
    expected = {
        "from_management.txt": "management",
        "from_other_chat.txt": "other chat",
        "from_job.txt": "scheduled",
        "from_helper.txt": "helper",
    }
    assert _turn(everything)["result"] == expected
    assert _turn(everything, chat=OTHER_MANAGEMENT_CHAT, key=OTHER_KEY)["result"] == expected
    assert _helper(everything)["result"] == expected
    _observed, _ = _run_scheduled(_job(OPS_CHAT, id="ops-job", pa_job_type="tgg_ops_ingest"), everything)
    assert _observed["tool"]["result"] == expected

    # Delivery resolves from the same folder whatever owner it is handed.
    assert sandbox.resolve_sandbox_work_path(
        "work/from_helper.txt", delivered["workspace_owner"]
    ) == _agent_work(agent_home) / "from_helper.txt"
    assert sandbox.current_workspace_owner("anything") == sandbox.AGENT_WORKSPACE_OWNER
    workspaces = [p.name for p in (agent_home / "sandbox_workspaces").iterdir()]
    assert workspaces == [_agent_work(agent_home).parent.name]


def test_capability_folder_derivation_lands_in_the_agent_folder(agent_home):
    """tgg_file_return derives its root from ``_workspace_key(session key)``."""
    root = agent_home / "sandbox_workspaces" / sandbox._workspace_key(LIVE_KEY) / "work"
    assert root == _agent_work(agent_home)
    assert sandbox._workspace_key(OTHER_KEY) == sandbox._workspace_key("cron_x_1")


def test_session_mode_is_unchanged(home):
    assert sandbox._workspace_key(LIVE_KEY) != sandbox._workspace_key(OTHER_KEY)
    assert not sandbox.agent_workspace_enabled()


# 2. A scheduled job's attached-only file reaches a later chat turn -----------


class _Bridge:
    status = 200

    def read(self):
        return b'{"success":true}'

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _run_and_deliver(job: dict, final_response: str, monkeypatch) -> list[dict]:
    """Run a due job through tick with the real delivery to the reply bridge."""
    from cron import scheduler
    from gateway.config import Platform

    sent: list[dict] = []
    pconfig = MagicMock()
    pconfig.enabled = False
    gateway_config = MagicMock()
    gateway_config.platforms = {Platform.WHATSAPP: pconfig}
    gateway_config.group_sessions_per_user = False
    gateway_config.thread_sessions_per_user = False
    monkeypatch.setenv("TGG_REPLY_BRIDGE_URL", "http://bridge.test")
    with patch("cron.scheduler.get_due_jobs", return_value=[job]), \
         patch("cron.scheduler.advance_next_run"), \
         patch("cron.scheduler.save_job_output", return_value="out.md"), \
         patch("cron.scheduler.mark_job_run"), \
         patch("cron.scheduler.load_config", return_value={"cron": {"wrap_response": False}}), \
         patch("gateway.config.load_gateway_config", return_value=gateway_config), \
         patch("urllib.request.urlopen",
               side_effect=lambda req, timeout: sent.append(json.loads(req.data)) or _Bridge()), \
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
        agent.run_conversation.return_value = {
            "completed": True, "final_response": final_response,
        }
        agent_cls.return_value = agent
        assert scheduler.tick(verbose=False) == 1
    return sent


def test_attached_only_workbook_is_visible_to_a_later_chat_turn(agent_home, tmp_path, monkeypatch):
    """The 12 Sep incident: the job attached a finished workbook it never built."""
    _turn(_write("Outstanding_Cases_Update_v7.xlsx", "older v7: 99 blanks"))
    reviewed = tmp_path / "media" / "pa204-am186-20260912" / "TGG_AM_186_case_review_12_Sep_2026.xlsx"
    reviewed.parent.mkdir(parents=True)
    reviewed.write_bytes(b"reviewed workbook: 47 blanks")

    sent = _run_and_deliver(
        _job(), f"90 remain blank.\nMEDIA:{reviewed}", monkeypatch
    )

    assert [item.get("fileName") for item in sent if "filePath" in item] == [reviewed.name]
    later = _turn(
        "import json\n"
        "log = [json.loads(l) for l in (WORK / 'files-log.jsonl').read_text().splitlines()]\n"
        "delivered = [e for e in log if e['direction'] == 'scheduled_delivery']\n"
        "path = WORK / delivered[-1]['path'][len('work/'):]\n"
        "(WORK / 'result.json').write_text(json.dumps({'entry': delivered[-1], "
        "'text': path.read_text()}))\n",
        session_id="live-root-compressed-child",
    )
    assert later["status"] == "success", later
    assert later["result"]["text"] == "reviewed workbook: 47 blanks"
    entry = later["result"]["entry"]
    assert entry["path"] == f"work/sent/{reviewed.name}"
    assert entry["chat_id"] == MANAGEMENT_CHAT
    assert entry["scheduled_job_id"] == "mgmt-morning-review"
    assert entry["sha256"] == hashlib.sha256(reviewed.read_bytes()).hexdigest()


def test_non_management_job_may_attach_only_its_own_files(agent_home, monkeypatch):
    _turn(_write("management_only.xlsx", "Management workbook"))
    job = _job(OPS_CHAT, id="ops-job", pa_job_type="tgg_ops_ingest")

    sent = _run_and_deliver(job, "done\nMEDIA:work/management_only.xlsx", monkeypatch)

    assert not [item for item in sent if "filePath" in item]


def test_sent_and_received_files_are_kept_and_logged(agent_home, tmp_path):
    from gateway.durable_jsonl_consumer import _keep_sent_file_in_workspace

    retained = tmp_path / "retained" / "abc_spreadsheet_0.xlsx"
    retained.parent.mkdir()
    retained.write_bytes(b"sent from retained media")
    _keep_sent_file_in_workspace(retained, chat_id=MANAGEMENT_CHAT)
    _turn(_write("built.xlsx", "built in the sandbox"))
    _keep_sent_file_in_workspace(_agent_work(agent_home) / "built.xlsx", chat_id=MANAGEMENT_CHAT)

    work = _agent_work(agent_home)
    assert (work / "sent" / retained.name).read_bytes() == b"sent from retained media"
    assert not (work / "sent" / "built.xlsx").exists()
    assert [(e["direction"], e["path"]) for e in _files_log(agent_home)] == [
        ("sent", f"work/sent/{retained.name}"),
        ("sent", "work/built.xlsx"),
    ]


@pytest.mark.asyncio
async def test_a_received_document_is_saved_and_named_in_the_turn(agent_home, tmp_path, monkeypatch):
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource

    monkeypatch.setattr("hermes_cli.config.read_raw_config", lambda: {"python_sandbox": SANDBOX_CONFIG})
    document = tmp_path / "documents" / "Outstanding Cases.xlsx"
    document.parent.mkdir()
    document.write_bytes(b"Krisleen's workbook")
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(group_sessions_per_user=False)
    runner.adapters = {}
    runner._model = MODEL
    runner._base_url = None
    runner._has_setup_skill = lambda: False
    source = SessionSource(platform=Platform.WHATSAPP, chat_id=MANAGEMENT_CHAT, chat_type="group")
    event = MessageEvent(
        text="please check",
        message_type=MessageType.DOCUMENT,
        source=source,
        media_urls=[str(document)],
        media_types=["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
        message_id="3EB0DOC",
    )

    text = await runner._prepare_inbound_message_text(event=event, source=source, history=[])

    assert "Saved in your workspace as /work/received/Outstanding Cases.xlsx." in text
    later = _turn(_read("received/Outstanding Cases.xlsx"), chat=OTHER_MANAGEMENT_CHAT, key=OTHER_KEY)
    assert later["result"] == {"text": "Krisleen's workbook"}
    [entry] = _files_log(agent_home)
    assert entry["direction"] == "received" and entry["message_id"] == "3EB0DOC"


def test_a_same_named_different_file_keeps_both_versions(agent_home, tmp_path):
    first = tmp_path / "a" / "report.xlsx"
    second = tmp_path / "b" / "report.xlsx"
    for path, text in ((first, "first"), (second, "second")):
        path.parent.mkdir()
        path.write_text(text)

    one = sandbox.record_workspace_file(first, direction="received", folder="received")
    two = sandbox.record_workspace_file(second, direction="received", folder="received")
    again = sandbox.record_workspace_file(first, direction="received", folder="received")

    work = _agent_work(agent_home)
    assert one == again == "work/received/report.xlsx"
    assert two != one and two.startswith("work/received/report.i_")
    assert (work / one[5:]).read_text() == "first"
    assert (work / two[5:]).read_text() == "second"


# 3. The folder is unchanged after a model or provider change -----------------


def test_model_and_provider_change_keeps_the_folder(agent_home):
    before = _turn(_write("report.xlsx", "made under the old model"))
    assert before["status"] == "success"
    config = yaml.safe_load((agent_home / "config.yaml").read_text())
    config["model"] = {"provider": "openai-direct-primary", "default": "gpt-6-sol"}
    (agent_home / "config.yaml").write_text(yaml.safe_dump(config))
    new_key = (
        "agent:live-drain:persistent-chat:openai-direct-primary:gpt-6-sol:"
        f"whatsapp:group:{MANAGEMENT_CHAT}"
    )

    after = _turn(_read("report.xlsx"), key=new_key, session_id="fresh-session-new-model")

    assert after["result"] == {"text": "made under the old model"}
    observed, _ = _run_scheduled(_job(), _read("report.xlsx"))
    assert observed["tool"]["result"] == {"text": "made under the old model"}


# 4. Earlier folders are merged in without loss --------------------------------


def _legacy(home: Path, owner: str, files: dict[str, str], mtime: float) -> Path:
    work = home / "sandbox_workspaces" / ("s_" + hashlib.sha256(owner.encode()).hexdigest()) / "work"
    for name, text in files.items():
        (work / name).parent.mkdir(parents=True, exist_ok=True)
        (work / name).write_text(text)
        os.utime(work / name, (mtime, mtime))
    return work


def _snapshot(work: Path) -> dict[str, bytes]:
    return {
        path.relative_to(work).as_posix(): path.read_bytes()
        for path in work.rglob("*")
        if path.is_file()
    }


def test_existing_folders_merge_without_loss(agent_home):
    # The host's shape before round 2: one folder per conversation and model.
    legacy_key = (
        f"agent:live-drain:persistent-chat:{PROVIDER}:{MODEL}:whatsapp:group:{MANAGEMENT_CHAT}"
    )
    current = _legacy(agent_home, legacy_key, {
        "Outstanding_Cases_Update_v7.xlsx": "v7 current",
        "shared.txt": "same bytes",
        "nested/a.txt": "a",
    }, mtime=2_000_000_000)
    old_model = _legacy(agent_home, legacy_key.replace(MODEL, "gpt-old"), {
        "Outstanding_Cases_Update_v7.xlsx": "v7 from the old model folder",
        "shared.txt": "same bytes",
        "only_old.txt": "old",
    }, mtime=1_900_000_000)
    other = _legacy(agent_home, OTHER_KEY, {"other.txt": "other chat"}, mtime=1_950_000_000)
    before = {path: _snapshot(path) for path in (current, old_model, other)}

    listing = _turn(
        "import json, os\n"
        "files = {}\n"
        "for root, _d, names in os.walk(WORK):\n"
        "    for n in names:\n"
        "        p = os.path.join(root, n)\n"
        "        files[os.path.relpath(p, WORK)] = open(p).read()\n"
        "(WORK / 'result.json').write_text(json.dumps(files))\n"
    )["result"]

    # Nothing in any earlier folder changed or went away.
    for path, files in before.items():
        assert _snapshot(path) == files
    # Every distinct file is present; the newest copy keeps the plain name.
    old_tag = f"m_{old_model.parent.name[2:10]}"
    assert listing == {
        "Outstanding_Cases_Update_v7.xlsx": "v7 current",
        f"Outstanding_Cases_Update_v7.{old_tag}.xlsx": "v7 from the old model folder",
        "shared.txt": "same bytes",
        "nested/a.txt": "a",
        "only_old.txt": "old",
        "other.txt": "other chat",
    }
    record = json.loads(
        (_agent_work(agent_home).parent / sandbox._MERGE_RECORD_FILE).read_text()
    )
    assert set(record["merged"]) == {current.parent.name, old_model.parent.name, other.parent.name}
    assert record["merged"][old_model.parent.name]["renamed"] == {
        "Outstanding_Cases_Update_v7.xlsx": f"Outstanding_Cases_Update_v7.{old_tag}.xlsx"
    }


def test_merge_runs_once_and_earlier_folders_are_never_pruned(agent_home):
    legacy = _legacy(agent_home, LIVE_KEY, {"report.xlsx": "v1"}, mtime=1_000_000_000)
    _turn(_write("report.xlsx", "v2 edited after the move"))
    _turn(_write("note.txt", "another run"))

    assert (_agent_work(agent_home) / "report.xlsx").read_text() == "v2 edited after the move"
    assert not list(_agent_work(agent_home).glob("report.m_*"))
    assert (legacy / "report.xlsx").read_text() == "v1"


def test_delivery_before_any_sandbox_run_still_finds_a_moved_file(agent_home):
    _legacy(agent_home, LIVE_KEY, {"report.xlsx": "made before the move"}, mtime=1_000_000_000)

    resolved = sandbox.resolve_sandbox_work_path("work/report.xlsx", LIVE_KEY)

    assert resolved == _agent_work(agent_home) / "report.xlsx"
    assert resolved.read_text() == "made before the move"


# 5. Concurrent writers never lose files ---------------------------------------


def test_runs_on_the_shared_folder_are_serialised(agent_home):
    """A second run waits until the first has finished with the folder."""
    HOOKS["a_running"] = threading.Event()
    HOOKS["release_a"] = threading.Event()
    HOOKS["b_started"] = threading.Event()
    results: dict = {}

    def conversation_a():
        results["a"] = _turn(
            "HOOKS['a_running'].set()\n"
            "assert HOOKS['release_a'].wait(10)\n"
            + _write("report.xlsx", "edit from conversation A"),
        )

    first = threading.Thread(target=conversation_a)
    first.start()
    assert HOOKS["a_running"].wait(10)
    second = threading.Thread(
        target=lambda: results.__setitem__("b", _helper(
            "HOOKS['b_started'].set()\n"
            "import json\n"
            "(WORK / 'result.json').write_text(json.dumps("
            "{'seen': (WORK / 'report.xlsx').read_text()}))\n"
            + _write("report.xlsx", "edit from a helper")
        ))
    )
    second.start()
    assert not HOOKS["b_started"].wait(0.5)  # blocked while A holds the folder
    HOOKS["release_a"].set()
    first.join(20)
    second.join(20)

    assert results["a"]["status"] == results["b"]["status"] == "success"
    assert results["b"]["result"] == {"seen": "edit from conversation A"}
    assert (_agent_work(agent_home) / "report.xlsx").read_text() == "edit from a helper"
    assert "workspace_renamed" not in results["a"]


def test_concurrent_imports_and_sandbox_runs_lose_nothing(agent_home, tmp_path):
    sources = []
    for index in range(6):
        path = tmp_path / f"in{index}" / "same-name.xlsx"
        path.parent.mkdir()
        path.write_text(f"version {index}")
        sources.append(path)
    results: list = []

    def importer(path):
        results.append(
            sandbox.record_workspace_file(path, direction="received", folder="received")
        )

    threads = [threading.Thread(target=importer, args=(path,)) for path in sources]
    threads += [
        threading.Thread(target=_turn, args=(_write(f"run{index}.txt", str(index)),),
                         kwargs={"session_id": f"s{index}"})
        for index in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    work = _agent_work(agent_home)
    kept = sorted((work / path[5:]).read_text() for path in results)
    assert kept == [f"version {index}" for index in range(6)]
    assert len(set(results)) == 6
    assert sorted((work / f"run{index}.txt").read_text() for index in range(3)) == ["0", "1", "2"]
    assert len(_files_log(agent_home)) == 6


# Release helper ----------------------------------------------------------------


def test_release_helper_checks_then_proves_the_merge(home, monkeypatch, capsys):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "merge_sandbox_workspaces",
        Path(__file__).parents[2] / "deploy" / "tgg" / "christopher" / "scripts"
        / "merge_sandbox_workspaces.py",
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    _legacy(home, LIVE_KEY, {"a.xlsx": "current"}, mtime=2_000_000_000)
    _legacy(home, OTHER_KEY, {"a.xlsx": "other", "b.txt": "b"}, mtime=1_000_000_000)

    # Before the config flip: read-only check, nothing created.
    assert helper.main(["check"]) == 0
    checked = json.loads(capsys.readouterr().out)
    assert checked["workspace_mode"] == "session"
    assert checked["names_kept_twice"] == ["a.xlsx"]
    assert checked["merged_bytes"] == len("current") + len("other") + len("b")
    assert not _agent_work(home).exists()
    with pytest.raises(SystemExit):
        helper.main(["apply"])

    monkeypatch.setattr(sandbox, "_load_config", lambda: dict(SANDBOX_CONFIG))
    assert helper.main(["apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    assert applied["ok"] and applied["missing"] == []
    assert applied["files_in_agent_folder"] == 3


# Follow-up: message ids, quoted replies, deliveries in the conversation --------
# Shape of the s1 prescheduled case: a Management conversation holds v7; a
# scheduled job attaches the existing reviewed workbook; Krisleen then quotes
# that delivery.

MANAGEMENT_SESSION_ID = "20260911_153104_5c6b7d"
DELIVERY_TEXT_ID = "3EB0C508ED98E2132C2B2D"
DELIVERY_FILE_ID = "3EB0C508ED98E2132C2B2E"


def _bridge_with_ids(sent: list):
    ids = iter([DELIVERY_TEXT_ID, DELIVERY_FILE_ID, "3EB0SPARE1", "3EB0SPARE2"])

    class Response(_Bridge):
        def __init__(self):
            self.body = json.dumps({"success": True, "messageId": next(ids)}).encode()

        def read(self):
            return self.body

    return lambda req, timeout: sent.append(json.loads(req.data)) or Response()


def _sandbox_in_config(home: Path) -> None:
    config = yaml.safe_load((home / "config.yaml").read_text())
    config["python_sandbox"] = dict(SANDBOX_CONFIG)
    (home / "config.yaml").write_text(yaml.safe_dump(config))


def _management_session(home: Path) -> Path:
    sessions = home / "sessions"
    sessions.mkdir(exist_ok=True)
    (sessions / "sessions.json").write_text(json.dumps({
        LIVE_KEY: {
            "session_key": LIVE_KEY,
            "session_id": MANAGEMENT_SESSION_ID,
            "platform": "whatsapp",
            "chat_type": "group",
            "origin": {"platform": "whatsapp", "chat_id": MANAGEMENT_CHAT},
        }
    }))
    transcript = sessions / f"{MANAGEMENT_SESSION_ID}.jsonl"
    transcript.write_text(json.dumps({"role": "user", "content": "earlier turn"}) + "\n")
    return transcript


def _runner():
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(group_sessions_per_user=False)
    runner.adapters = {}
    runner._model = MODEL
    runner._base_url = None
    runner._has_setup_skill = lambda: False
    return runner


async def _krisleen(text: str, *, quoted: str | None = None, message_id: str = "3EB0KRIS",
                    document: Path | None = None) -> str:
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    source = SessionSource(platform=Platform.WHATSAPP, chat_id=MANAGEMENT_CHAT, chat_type="group")
    event = MessageEvent(
        text=text,
        message_type=MessageType.DOCUMENT if document else MessageType.TEXT,
        source=source,
        message_id=message_id,
        reply_to_message_id=quoted,
        reply_to_text="quoted text" if quoted else None,
        media_urls=[str(document)] if document else [],
        media_types=(
            ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]
            if document else []
        ),
    )
    return await _runner()._prepare_inbound_message_text(event=event, source=source, history=[])


@pytest.fixture()
def s1_delivered(agent_home, tmp_path, monkeypatch):
    """Run the s1 prescheduled shape up to the scheduled delivery."""
    from cron import scheduler

    _sandbox_in_config(agent_home)
    transcript = _management_session(agent_home)
    _turn(_write("Outstanding_Cases_Update_v7.xlsx", "older v7: 99 blanks"))
    reviewed = tmp_path / "media" / "pa204-am186-20260912" / "TGG_AM_186_case_review_12_Sep_2026.xlsx"
    reviewed.parent.mkdir(parents=True)
    reviewed.write_bytes(b"reviewed workbook: 47 blanks")
    sent: list = []
    job = _job(name="AM-186 review delivery")
    with patch("urllib.request.urlopen", side_effect=_bridge_with_ids(sent)):
        original = scheduler._deliver_result

        def deliver(*args, **kwargs):
            with patch("urllib.request.urlopen", side_effect=_bridge_with_ids(sent)):
                return original(*args, **kwargs)

        with patch("cron.scheduler._deliver_result", side_effect=deliver):
            _run_and_deliver(
                job, f"90 remain blank: 82 No update found, 8 Conflicting information.\nMEDIA:{reviewed}",
                monkeypatch,
            )
    return {"transcript": transcript, "reviewed": reviewed, "sent": sent}


def test_files_log_records_the_whatsapp_message_ids(s1_delivered, agent_home, tmp_path):
    [entry] = [e for e in _files_log(agent_home) if e["direction"] == "scheduled_delivery"]
    assert entry["message_id"] == DELIVERY_FILE_ID
    assert entry["text_message_id"] == DELIVERY_TEXT_ID
    assert entry["path"] == "work/sent/TGG_AM_186_case_review_12_Sep_2026.xlsx"

    from gateway.durable_jsonl_consumer import _keep_sent_file_in_workspace

    other = tmp_path / "live.xlsx"
    other.write_bytes(b"live send")
    _keep_sent_file_in_workspace(other, chat_id=MANAGEMENT_CHAT, message_id="3EB0LIVE")
    assert _files_log(agent_home)[-1]["message_id"] == "3EB0LIVE"


def test_scheduled_delivery_is_in_the_conversation_history(s1_delivered):
    lines = [json.loads(l) for l in s1_delivered["transcript"].read_text().splitlines()]
    delivery = lines[-1]
    assert delivery["role"] == "assistant"
    assert "Scheduled job 'AM-186 review delivery' delivered this to the chat at" in delivery["content"]
    assert "90 remain blank" in delivery["content"]
    assert "[Attached file: /work/sent/TGG_AM_186_case_review_12_Sep_2026.xlsx]" in delivery["content"]


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [DELIVERY_TEXT_ID, DELIVERY_FILE_ID])
async def test_quoting_the_delivery_names_the_delivered_file(s1_delivered, quoted):
    text = await _krisleen(
        "does this 90 blanks and 82 no update found + 8 conflicting information, the same?",
        quoted=quoted,
    )

    assert (
        "sent by the scheduled job 'AM-186 review delivery', which carried "
        "/work/sent/TGG_AM_186_case_review_12_Sep_2026.xlsx.]"
    ) in text
    assert "[The sender is replying to your " in text
    assert "v7" not in text


@pytest.mark.asyncio
async def test_quoting_a_message_without_a_file_record_adds_nothing(s1_delivered):
    text = await _krisleen("how many blanks are there now?", quoted="3EB0UNRELATED")
    assert "The sender is replying to" not in text
    plain = await _krisleen("how many blanks are there now?")
    assert "The sender is replying to" not in plain


@pytest.mark.asyncio
async def test_quoting_her_own_file_message_points_to_received(agent_home, tmp_path, monkeypatch):
    _sandbox_in_config(agent_home)
    document = tmp_path / "documents" / "Outstanding_Cases_Update_v5.xlsx"
    document.parent.mkdir()
    document.write_bytes(b"Krisleen's v5")
    await _krisleen("here is the list", message_id="3EB0KRISDOC", document=document)

    text = await _krisleen("use this one", quoted="3EB0KRISDOC", message_id="3EB0KRIS2")

    assert "that sent you a file, now saved as /work/received/Outstanding_Cases_Update_v5.xlsx.]" in text
    [entry] = _files_log(agent_home)
    assert entry["message_id"] == "3EB0KRISDOC"


def test_delivery_history_uses_the_real_session_database(agent_home, monkeypatch):
    import hermes_state
    from hermes_state import SessionDB
    from cron.scheduler import _record_delivery_in_conversation

    # The default path is fixed at import; point it at this test's home.
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", agent_home / "state.db")

    _management_session(agent_home)
    db = SessionDB()
    try:
        db.create_session(session_id=MANAGEMENT_SESSION_ID, source="whatsapp")
    finally:
        db.close()

    assert _record_delivery_in_conversation(
        {"id": "j1", "name": "Review"}, LIVE_KEY,
        {"text": "done", "files": [{"name": "r.xlsx", "workspace_path": "work/sent/r.xlsx"}]},
    )
    db = SessionDB()
    try:
        [message] = db.get_messages_as_conversation(MANAGEMENT_SESSION_ID)
    finally:
        db.close()
    assert message["role"] == "assistant"
    assert "[Attached file: /work/sent/r.xlsx]" in message["content"]
    assert not _record_delivery_in_conversation({"id": "j2"}, "no-such-key", {"text": "x"})


# Direct mount of the agent workspace -------------------------------------------


def test_init_script_mounts_the_folder_directly(tmp_path):
    work = tmp_path / "sandbox_workspaces" / "s_x" / "work"
    script = sandbox._generate_init_script(
        tmp_path / "run", {}, tmp_path / "venv", 64, tmp_path, 64, 60, 64,
        set(), None, False, direct_work=work,
    )

    assert f'mount --bind {work} "$JAIL/work"' in script
    assert 'mount -o remount,bind,nosuid,nodev,noexec "$JAIL/work"' in script
    assert 'tmpfs "$JAIL/work"' not in script
    assert "$JAIL/seed/." not in script
    assert 'remount,bind,ro "$JAIL/export"' not in script
    assert script.rstrip().endswith("direct")

    session = sandbox._generate_init_script(
        tmp_path / "run", {}, tmp_path / "venv", 64, tmp_path, 64, 60, 64,
        set(), tmp_path / "seed", False,
    )
    assert 'tmpfs "$JAIL/work"' in session
    assert 'remount,bind,ro "$JAIL/export"' in session
    assert not session.rstrip().endswith("direct")
    assert 'if len(sys.argv) < 5 or sys.argv[4] != "direct":' in sandbox._SUPERVISOR_SOURCE


def test_mount_mode_reads_and_writes_the_real_folder(agent_home):
    work = _agent_work(agent_home)
    work.mkdir(parents=True)
    (work / "existing.xlsx").write_text("already here")
    (work / "old.txt").write_text("to delete")

    result = _turn(
        "import json\n"
        "seen = (WORK / 'existing.xlsx').read_text()\n"
        "(WORK / 'new.xlsx').write_text('made in the run')\n"
        "(WORK / 'old.txt').unlink()\n"
        "(WORK / 'result.json').write_text(json.dumps({'seen': seen}))\n"
    )

    assert result["status"] == "success", result
    assert result["result"] == {"seen": "already here"}
    assert (work / "new.xlsx").read_text() == "made in the run"
    assert not (work / "old.txt").exists()
    # result.json is the run's return channel, never left in the folder.
    assert not (work / "result.json").exists()
    # files[] names what this run made or changed, not the whole folder.
    assert [item["path"] for item in result["files"]] == ["work/new.xlsx"]
    # No per-run copy of the folder was made.
    assert not list((agent_home / "sandbox_runs").glob("*/seed"))


def test_links_that_could_escape_are_removed_after_the_run(agent_home, tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_text("host file")
    code = (
        f"os.symlink({str(outside)!r}, WORK / 'escape.txt')\n"
        f"os.symlink({str(tmp_path)!r}, WORK / 'escape_dir')\n"
        f"os.link({str(outside)!r}, WORK / 'hard.txt')\n"
        "os.mkfifo(WORK / 'pipe')\n"
        "(WORK / 'real.txt').write_text('kept')\n"
    )

    result = _turn(code)

    work = _agent_work(agent_home)
    assert not (work / "escape.txt").is_symlink() and not (work / "escape.txt").exists()
    assert not (work / "escape_dir").exists()
    assert not (work / "pipe").exists()
    assert (work / "hard.txt").read_text() == "host file"
    assert os.stat(work / "hard.txt").st_nlink == 1
    outside.write_text("changed on the host")
    assert (work / "hard.txt").read_text() == "host file"
    assert (work / "real.txt").read_text() == "kept"
    assert sorted(result["workspace_removed_links"]) == [
        "work/escape.txt", "work/escape_dir", "work/pipe",
    ]


def test_size_limit_refuses_and_stops_runs(agent_home, monkeypatch):
    monkeypatch.setattr(
        sandbox, "_load_config",
        lambda: {**SANDBOX_CONFIG, "limits": {"workspace_mb": 1}},
    )
    # A run that grows the folder past the limit is stopped.
    grow = _turn(
        "chunk = b'x' * 262144\n"
        "i = 0\n"
        "while not KILLED.is_set() and i < 400:\n"
        "    (WORK / f'big{i}.bin').write_bytes(chunk)\n"
        "    i += 1\n"
        "    import time; time.sleep(0.02)\n"
    )
    assert grow["status"] == "error"
    assert "workspace size limit (1MB) exceeded" in grow["stderr"]
    assert "workspace_over_limit" in grow

    # Over the limit, the next run is refused before it starts.
    refused = _turn(_write("after.txt", "never written"))
    assert refused["status"] == "error"
    assert "over its 1MB limit" in refused["error"]
    assert not (_agent_work(agent_home) / "after.txt").exists()


@pytest.mark.asyncio
async def test_received_photos_land_in_the_folder(agent_home, tmp_path, monkeypatch):
    from gateway.config import Platform
    from gateway.platforms.base import MessageEvent, MessageType
    from gateway.session import SessionSource

    _sandbox_in_config(agent_home)
    photo = tmp_path / "images" / "3EB0PHOTO.jpg"
    photo.parent.mkdir()
    photo.write_bytes(b"\xff\xd8\xff photo bytes")
    runner = _runner()
    runner._decide_image_input_mode = lambda: "native"
    source = SessionSource(platform=Platform.WHATSAPP, chat_id=MANAGEMENT_CHAT, chat_type="group")
    event = MessageEvent(
        text="see this", message_type=MessageType.PHOTO, source=source,
        message_id="3EB0PHOTO", media_urls=[str(photo)], media_types=["image/jpeg"],
    )

    text = await runner._prepare_inbound_message_text(event=event, source=source, history=[])

    assert "[The image is also saved in your workspace as /work/received/3EB0PHOTO.jpg.]" in text
    assert (_agent_work(agent_home) / "received" / "3EB0PHOTO.jpg").read_bytes() == photo.read_bytes()
    [entry] = _files_log(agent_home)
    assert entry["message_id"] == "3EB0PHOTO" and entry["direction"] == "received"
    later = await _krisleen("what is in this photo?", quoted="3EB0PHOTO", message_id="3EB0K3")
    assert "now saved as /work/received/3EB0PHOTO.jpg" in later


def _real_jail_available() -> bool:
    from tests.test_python_sandbox_tool import _can_run_jail

    return _can_run_jail()


@pytest.mark.skipif(not _real_jail_available(), reason="kernel user namespaces unavailable")
@pytest.mark.sandbox_e2e
def test_real_jail_works_in_the_mounted_agent_folder(tmp_path, monkeypatch):
    """Linux only: the real jail with the agent folder bind-mounted at /work."""
    from tests.test_python_sandbox_tool import _config, _csv, _db

    source, csv = tmp_path / "records.db", tmp_path / "input.csv"
    _db(source)
    _csv(csv)
    config = _config(source, csv, wall_seconds=20, max_wall_seconds=20)
    config["workspace"] = "agent"
    home = tmp_path / "home"
    monkeypatch.setattr(sandbox, "_load_config", lambda: config)
    monkeypatch.setattr(sandbox, "get_hermes_home", lambda: home)
    work = (
        home / "sandbox_workspaces"
        / ("s_" + hashlib.sha256(sandbox.AGENT_WORKSPACE_OWNER.encode()).hexdigest())
        / "work"
    )
    work.mkdir(parents=True)
    (work / "existing.txt").write_text("before")
    code = r'''
import json, os, subprocess
seen = open("/work/existing.txt").read()
open("/work/made.txt", "w").write("in the jail")
os.symlink("/etc/passwd", "/work/escape")
open("/work/run.sh", "w").write("#!/bin/sh\necho hi\n")
os.chmod("/work/run.sh", 0o755)
try:
    subprocess.run(["/work/run.sh"], check=True)
    executed = True
except (PermissionError, OSError, subprocess.CalledProcessError):
    executed = False
assert os.path.exists("/work/records.db")
open(os.environ["RESULT_PATH"], "w").write(json.dumps({"seen": seen, "executed": executed}))
'''
    response = json.loads(sandbox.python_sandbox(code, ["records"], timeout_seconds=20))

    assert response["status"] == "success", response
    assert response["result"] == {"seen": "before", "executed": False}
    assert (work / "made.txt").read_text() == "in the jail"
    assert not os.path.lexists(work / "escape")
    assert not (work / "records.db").exists()
    assert not (work / "result.json").exists()
    assert "work/made.txt" in {item["path"] for item in response["files"]}
