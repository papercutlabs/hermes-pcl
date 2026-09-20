"""PA-237: optional explicit credential forwarding through the two wrappers.

The lower resolver already accepts an explicit credential and already handles
openai-codex with one, using it directly and skipping the auth store. The two
callers the PA-97 reviewer child actually uses could not pass one, so a
deployment with an isolated HERMES_HOME had no supported way to supply it.

These exercise the real wrapper chain. Nothing contacts a provider, nothing
reads a real auth store, and the credential is a synthetic sentinel. The
terminal model/transport boundary is substituted before any agent is built.

Runnable either under pytest or directly:

    python3 tests/test_pa237_explicit_credential_forwarding.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import types
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SENTINEL = "pa237-synthetic-sentinel-not-a-credential"
STUB_STORE_VALUE = "pa237-stub-store-value-not-a-credential"


@contextmanager
def isolated_home():
    """An isolated configuration home holding no auth store."""
    previous = {name: os.environ.get(name) for name in ("HERMES_HOME", "OPENAI_API_KEY")}
    with tempfile.TemporaryDirectory() as directory:
        home = Path(directory) / "hermes-home"
        home.mkdir()
        os.environ["HERMES_HOME"] = str(home)
        os.environ.pop("OPENAI_API_KEY", None)
        assert not (home / "auth.json").exists()
        try:
            yield home
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


@contextmanager
def module_installed(name, module):
    """Install a module under a name and restore whatever was there."""
    had = name in sys.modules
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        yield
    finally:
        if had:
            sys.modules[name] = previous
        else:
            sys.modules.pop(name, None)


@contextmanager
def attribute_replaced(target, name, value):
    previous = getattr(target, name)
    setattr(target, name, value)
    try:
        yield
    finally:
        setattr(target, name, previous)


def test_the_explicit_credential_reaches_the_existing_resolver():
    """Forwarded unchanged to the input the resolver already had."""
    with isolated_home():
        from hermes_cli import oneshot

        seen = {}

        def recording_resolver(**kwargs):
            seen.update(kwargs)
            return {"provider": "openai-codex", "api_mode": "codex_responses",
                    "base_url": "https://chatgpt.com/backend-api/codex",
                    "api_key": kwargs.get("explicit_api_key") or "", "source": "explicit"}

        module = types.ModuleType("hermes_cli.runtime_provider")
        module.resolve_runtime_provider = recording_resolver
        with module_installed("hermes_cli.runtime_provider", module):
            _cfg, _model, runtime = oneshot.resolve_oneshot_runtime(
                "gpt-5.6-sol", "openai-codex", SENTINEL)
        assert seen["explicit_api_key"] == SENTINEL
        assert runtime["api_key"] == SENTINEL


def test_omitting_it_takes_the_same_old_branch():
    """A caller that passes nothing behaves exactly as before."""
    with isolated_home():
        from hermes_cli import oneshot

        seen = {}

        def recording_resolver(**kwargs):
            seen.update(kwargs)
            return {"provider": "openai-codex", "api_key": "", "source": "env/config"}

        module = types.ModuleType("hermes_cli.runtime_provider")
        module.resolve_runtime_provider = recording_resolver
        with module_installed("hermes_cli.runtime_provider", module):
            oneshot.resolve_oneshot_runtime("gpt-5.6-sol", "openai-codex")
        assert seen["explicit_api_key"] is None, "omitted must stay omitted"


def test_the_real_codex_branch_uses_it_and_skips_the_auth_store():
    """The actual resolver on the provider the reviewer uses.

    resolve_codex_runtime_credentials is the auth-store read. It is replaced by
    a tripwire that fails if consulted, which is the point of supplying one.
    """
    with isolated_home():
        from hermes_cli import runtime_provider

        def tripwire():
            raise AssertionError("the auth store must not be consulted")

        with attribute_replaced(runtime_provider, "resolve_codex_runtime_credentials", tripwire):
            resolved = runtime_provider._resolve_explicit_runtime(
                provider="openai-codex", requested_provider="openai-codex",
                model_cfg={}, explicit_api_key=SENTINEL, explicit_base_url=None)
        assert resolved is not None
        assert resolved["provider"] == "openai-codex"
        assert resolved["api_key"] == SENTINEL
        assert resolved["source"] == "explicit"


def test_without_an_explicit_credential_the_old_source_is_consulted():
    """The omitted-input branch is unchanged, shown with a controlled stub."""
    with isolated_home():
        from hermes_cli import runtime_provider

        calls = []

        def controlled_source():
            calls.append("consulted")
            return {"api_key": STUB_STORE_VALUE,
                    "base_url": "https://chatgpt.com/backend-api/codex", "last_refresh": None}

        with attribute_replaced(runtime_provider, "resolve_codex_runtime_credentials",
                                controlled_source):
            resolved = runtime_provider._resolve_explicit_runtime(
                provider="openai-codex", requested_provider="openai-codex",
                model_cfg={}, explicit_api_key=None,
                explicit_base_url="https://chatgpt.com/backend-api/codex")
        assert calls == ["consulted"], "the old branch must still be taken"
        assert resolved["api_key"] == STUB_STORE_VALUE


def test_the_ephemeral_wrapper_forwards_and_leaks_nothing():
    """The whole chain, terminal boundary substituted before any agent call."""
    with isolated_home():
        import hermes_cli.ephemeral_session as ephemeral

        forwarded = {}

        def recording_oneshot(model=None, provider=None, explicit_api_key=None):
            forwarded["explicit_api_key"] = explicit_api_key
            return ({}, model or "gpt-5.6-sol",
                    {"provider": "openai-codex", "api_mode": "codex_responses",
                     "base_url": "https://chatgpt.com/backend-api/codex",
                     "api_key": explicit_api_key or "", "credential_pool": None})

        class SubstitutedAgent:
            """The terminal boundary. No request can leave it."""

            def __init__(self, **kwargs):
                forwarded["agent_api_key"] = kwargs.get("api_key")
                self.suppress_status_output = False
                self.stream_delta_callback = None
                self.tool_gen_callback = None
                # The lifecycle record reads this; an empty set is honest for a
                # substituted agent that loaded no tools.
                self.valid_tool_names = set()

            def run_conversation(self, prompt):
                return {"final_response": "done"}

            def close(self):
                return None

        oneshot_module = types.ModuleType("hermes_cli.oneshot")
        oneshot_module.resolve_oneshot_runtime = recording_oneshot
        oneshot_module._oneshot_clarify_callback = None
        run_agent = types.ModuleType("run_agent")
        run_agent.AIAgent = SubstitutedAgent

        with module_installed("hermes_cli.oneshot", oneshot_module), \
                module_installed("run_agent", run_agent):
            outcome, lifecycle = ephemeral.run_ephemeral_session(
                prompt="p", system_prompt="s", model="gpt-5.6-sol", max_iterations=1,
                allowed_tool_names=[], provider="openai-codex", explicit_api_key=SENTINEL)

        assert forwarded["explicit_api_key"] == SENTINEL
        assert forwarded["agent_api_key"] == SENTINEL
        # Nothing carries the value outward.
        assert SENTINEL not in json.dumps(outcome, default=str)
        assert SENTINEL not in json.dumps(lifecycle, default=str)


def main() -> int:
    failures = []
    for name, case in sorted(globals().items()):
        if not name.startswith("test_") or not callable(case):
            continue
        try:
            case()
            print(f"  PASS {name}")
        except Exception as error:  # noqa: BLE001 - a test report
            failures.append(name)
            print(f"  FAIL {name}: {type(error).__name__}: {error}")
    print(f"{'passed' if not failures else 'FAILED'}: "
          f"{len([n for n in globals() if n.startswith('test_')]) - len(failures)} passed, "
          f"{len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
