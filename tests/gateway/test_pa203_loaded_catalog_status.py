"""PA-203: the consumer status file shows what the running process loaded.

A release readback reads ``loaded_catalog`` from the live service's status file
to prove the retired coordinator is absent from the loaded catalog, instead of
starting a fresh process.
"""

from __future__ import annotations

import json
import os
from types import SimpleNamespace

import gateway.durable_jsonl_consumer as consumer


def test_status_file_carries_the_process_loaded_plugins_and_tools(tmp_path, monkeypatch):
    import hermes_cli.plugins as plugins
    from tools.registry import registry

    manager = plugins.PluginManager()
    manager._discovered = True
    manager._plugins = {
        "tgg-whatsapp-evidence": SimpleNamespace(enabled=True),
        "tgg-per-case-whatsapp": SimpleNamespace(enabled=True),
        "not-enabled": SimpleNamespace(enabled=False),
    }
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    monkeypatch.setattr(registry, "_tools", {"tgg_case_search": object(), "delegate_task": object()})

    path = tmp_path / "capture-consumer-status.json"
    consumer._write_status(path, {"state": "running"})

    status = json.loads(path.read_text())
    assert status["state"] == "running"
    assert status["loaded_catalog"] == {
        "pid": os.getpid(),
        "plugins_discovered": True,
        "plugins": ["tgg-per-case-whatsapp", "tgg-whatsapp-evidence"],
        "tools": ["delegate_task", "tgg_case_search"],
    }


def test_status_write_survives_a_catalog_read_failure(tmp_path, monkeypatch):
    import hermes_cli.plugins as plugins

    def broken():
        raise RuntimeError("manager unavailable")

    monkeypatch.setattr(plugins, "get_plugin_manager", broken)
    path = tmp_path / "status.json"
    consumer._write_status(path, {"state": "running"})

    status = json.loads(path.read_text())
    assert status["loaded_catalog"]["pid"] == os.getpid()
    assert "manager unavailable" in status["loaded_catalog"]["error"]
