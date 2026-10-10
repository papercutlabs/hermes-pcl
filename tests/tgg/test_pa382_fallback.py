"""PA-382 patch tests. Run with the Hermes venv: PYTHONPATH=<runtime tree> python test_fallback_patch.py
Each check is also run against the unpatched tree (RUNTIME_ORIG) and must fail there, to show the instrument can fail."""
import os, sqlite3, sys, tempfile, types, unittest
from pathlib import Path
from unittest import mock

import yaml

CONFIG = {
    "model": {"provider": "openai-codex", "default": "gpt-6.1-sol", "credential_label": "openai-codex-oauth-2"},
    "providers": {"openai-tgg-fallback": {"api": "https://api.openai.com/v1", "key_env": "OPENAI_API_KEY_TGG_FALLBACK"}},
    "fallback_providers": [{"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol", "key_env": "OPENAI_API_KEY_TGG_FALLBACK"}],
}


def row(provider, model, status="completed"):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("create table t (turn_id, turn_status, provider, model, raw_turn_envelope_json)")
    conn.execute("insert into t values ('t1', ?, ?, ?, '{\"final_response\": \"ok\"}')", (status, provider, model))
    return conn.execute("select * from t").fetchone()


class Consumer(unittest.TestCase):
    def setUp(self):
        from gateway import durable_jsonl_consumer as c
        self.c = c
        d = tempfile.mkdtemp()
        self.cfg = Path(d) / "config.yaml"
        self.cfg.write_text(yaml.safe_dump(CONFIG))

    def check(self, r):
        fb = self.c.configured_fallback_engines(self.cfg)
        return self.c._assert_completed_turn(r, provider="openai-codex", model="gpt-6.1-sol", require_response=True, fallback_engines=fb)

    def test_configured_engine_passes(self):
        self.assertEqual(self.check(row("openai-codex", "gpt-6.1-sol")), "t1")

    def test_listed_fallback_passes(self):
        self.assertEqual(self.check(row("openai-tgg-fallback", "gpt-6.1-sol")), "t1")

    def test_unlisted_engine_fails(self):
        with self.assertRaises(self.c.ConsumerError):
            self.check(row("openai-direct-primary", "gpt-6.1-sol"))
        with self.assertRaises(self.c.ConsumerError):
            self.check(row("openai-tgg-fallback", "gpt-5.6-terra"))

    def test_failed_turn_still_fails(self):
        with self.assertRaises(self.c.ConsumerError):
            self.check(row("openai-tgg-fallback", "gpt-6.1-sol", status="failed"))


class JobRuntime(unittest.TestCase):
    def ctx(self):
        brief = types.SimpleNamespace(runtime={"provider": "openai-codex", "model": "gpt-6.1-sol", "credential_pool": "job-pool"})
        return types.SimpleNamespace(job_brief=brief)

    def test_fallback_runtime_is_not_relabelled(self):
        from gateway.run import _apply_pa_job_runtime
        rt = {"api_key": "k", "base_url": "https://api.openai.com/v1", "provider": "custom",
              "fallback_entry": {"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol"}}
        model, out = _apply_pa_job_runtime("gpt-6.1-sol", rt, self.ctx())
        self.assertEqual((model, out["provider"], out["base_url"]), ("gpt-6.1-sol", "openai-tgg-fallback", "https://api.openai.com/v1"))
        self.assertNotEqual(out.get("credential_pool"), "job-pool")

    def test_primary_runtime_still_takes_job_brief(self):
        from gateway.run import _apply_pa_job_runtime
        model, out = _apply_pa_job_runtime("x", {"api_key": "k", "provider": "openai-codex"}, self.ctx())
        self.assertEqual((model, out["provider"], out["credential_pool"]), ("gpt-6.1-sol", "openai-codex", "job-pool"))


class Ephemeral(unittest.TestCase):
    def run_resolve(self, cfg, primary_ok):
        from hermes_cli import ephemeral_session as e
        seen = {}

        def fake_rr(requested=None, target_model=None, explicit_base_url=None, explicit_api_key=None):
            seen["requested"] = requested
            return {"provider": "custom", "api_key": "k:" + str(requested), "base_url": "u"}

        def fake_oneshot(model, provider):
            if not primary_ok:
                from hermes_cli.auth import AuthError
                raise AuthError("Configured credential label 'x' is unavailable for openai-codex")
            return cfg, "gpt-6.1-sol", {"provider": "openai-codex", "api_key": "plus"}

        with mock.patch("hermes_cli.config.load_config", return_value=cfg), \
             mock.patch("hermes_cli.runtime_provider.resolve_runtime_provider", fake_rr), \
             mock.patch("hermes_cli.oneshot.resolve_oneshot_runtime", fake_oneshot):
            return e.resolve_session_runtime("gpt-6.1-sol", "openai-codex"), seen

    def test_primary_used_when_available(self):
        (model, runtime, chain, used), _ = self.run_resolve(CONFIG, True)
        self.assertEqual((runtime["api_key"], used), ("plus", None))

    def test_fallback_used_when_primary_unavailable(self):
        (model, runtime, chain, used), seen = self.run_resolve(CONFIG, False)
        self.assertEqual((runtime["api_key"], used, seen["requested"]), ("k:openai-tgg-fallback", "openai-tgg-fallback", "openai-tgg-fallback"))

    def test_no_fallback_configured_raises(self):
        from hermes_cli.auth import AuthError
        with self.assertRaises(AuthError):
            self.run_resolve({k: v for k, v in CONFIG.items() if k != "fallback_providers"}, False)

    def test_other_errors_do_not_reach_the_fallback(self):
        from hermes_cli import ephemeral_session as e
        with mock.patch("hermes_cli.config.load_config", return_value=CONFIG), \
             mock.patch("hermes_cli.oneshot.resolve_oneshot_runtime", side_effect=ValueError("config fault")), \
             mock.patch("hermes_cli.runtime_provider.resolve_runtime_provider") as rr:
            with self.assertRaises(ValueError):
                e.resolve_session_runtime("gpt-6.1-sol", "openai-codex")
            rr.assert_not_called()

    def test_follows_direct_host_profile(self):
        cfg = dict(CONFIG, model={"provider": "openai-direct-primary", "default": "gpt-6.1-sol"})
        (model, runtime, chain, used), seen = self.run_resolve(cfg, True)
        self.assertEqual((runtime["api_key"], seen["requested"]), ("k:openai-direct-primary", "openai-direct-primary"))


class Wiring(unittest.TestCase):
    def test_startup_resolver_marks_the_entry_it_used(self):
        import gateway.run as run
        d = Path(tempfile.mkdtemp())
        (d / "config.yaml").write_text(yaml.safe_dump(CONFIG))
        with mock.patch.object(run, "_hermes_home", d), \
             mock.patch("hermes_cli.runtime_provider.resolve_runtime_provider",
                        return_value={"provider": "custom", "api_key": "k", "base_url": "u", "api_mode": "chat_completions"}):
            out = run._try_resolve_fallback_provider()
        self.assertEqual(out["fallback_entry"], {"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol"})

    def test_session_resolver_relabels_nothing_and_leaks_no_marker(self):
        import gateway.run as run
        fb = {"api_key": "k", "base_url": "u", "provider": "custom", "model": "gpt-6.1-sol",
              "fallback_entry": {"provider": "openai-tgg-fallback", "model": "gpt-6.1-sol"}}
        brief = types.SimpleNamespace(runtime={"provider": "openai-codex", "model": "gpt-6.1-sol"})
        fake_self = types.SimpleNamespace(_session_model_overrides={})
        with mock.patch.object(run, "_resolve_runtime_agent_kwargs", return_value=dict(fb)), \
             mock.patch.object(run, "_resolve_gateway_model", return_value="gpt-6.1-sol"):
            model, runtime = run.GatewayRunner._resolve_session_agent_runtime(
                fake_self, session_key="s", pa_context=types.SimpleNamespace(job_brief=brief))
        self.assertEqual((model, runtime["provider"], runtime["api_key"]), ("gpt-6.1-sol", "openai-tgg-fallback", "k"))
        self.assertNotIn("fallback_entry", runtime)

    def test_call_sites_pass_the_fallback_engines(self):
        src = Path(__import__("gateway.durable_jsonl_consumer", fromlist=["x"]).__file__).read_text()
        self.assertEqual(src.count("fallback_engines=fallback_engines"), 1)
        self.assertEqual(src.count("fallback_engines=configured_fallback_engines(config_path)"), 1)

    def test_reviewer_session_uses_the_resolver_and_passes_the_chain(self):
        src = Path(__import__("hermes_cli.ephemeral_session", fromlist=["x"]).__file__).read_text()
        self.assertIn("resolve_session_runtime(model, provider)", src)
        self.assertIn("fallback_model=fallback_chain or None", src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
