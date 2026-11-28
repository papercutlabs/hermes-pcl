#!/usr/bin/env python3
"""Small JSONL-logging PA business bridge for live Telegram E2E runs."""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


class Handler(BaseHTTPRequestHandler):
    log_path: Path

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            payload = json.loads(raw_body) if raw_body.strip() else {}
        except json.JSONDecodeError:
            payload = {"_raw": raw_body}
        parsed = urlparse(self.path)
        tenant = parsed.path.strip("/").split("/", 1)[0] or "unknown"
        event = {
            "ts": time.time(),
            "method": self.command,
            "path": parsed.path,
            "tenant": tenant,
            "payload": payload,
            "auth": {
                "x_tgg_token_present": bool(self.headers.get("X-TGG-Token")),
                "authorization_present": bool(self.headers.get("Authorization")),
            },
        }
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True) + "\n")
        body = json.dumps(
            {
                "ok": True,
                "tenant": tenant,
                "path": parsed.path,
                "received": payload,
            },
            sort_keys=True,
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    Handler.log_path = Path(args.log).expanduser()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(json.dumps({"ok": True, "port": args.port, "log": str(Handler.log_path)}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
