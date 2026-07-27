#!/usr/bin/env python3
"""Attention-digest wave trigger — the deletable proactive-push mechanism [WB:ee014d48].

Polls the PS operator attention-items route, tracks which item ids have already
been announced in a durable marker file, and — only when NEW open items exist —
emits ONE synthetic inbound message addressed to the management chat:

    [system] processing wave complete: N attention items now open (ids: 7, 8)

The message travels the NORMAL inbound path (a bridge-message JSON record); the
management job brief's wave-complete rule then composes the single batched
operator-facing digest from a live tgg_attention_list read, scoped to exactly
the ids this trigger names. Dedupe (nothing re-announced) is mechanical and
lives HERE, in the marker file — never in model memory.

Zero-new-items waves emit nothing: silence is correct.

Deploy shape (live): this script + a systemd timer (or cron). Rollback = stop
the timer and delete the script; no hermes core code is involved. The runtime
behavior rule alone never fires without this emitter.

Emission modes:
  --mode stdout   print the bridge-message JSON record (rig/corpus use)
  --mode append   append the record as one JSONL line to --out

State: --marker JSON file {"announced_ids": [...]}. --init seeds the marker
with every currently-open id WITHOUT emitting (so pre-existing backlog is not
re-announced when the watcher first turns on).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path


def fetch_open_items(base_url: str, token: str, tenant: str, replay_run_id: str | None):
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/operator/attention-items?state=open",
        headers={
            "Authorization": f"Bearer {token}",
            "X-PS-Tenant": tenant,
            "User-Agent": "attention-digest-watcher/1",
            **({"X-Replay-Run-Id": replay_run_id} if replay_run_id else {}),
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise SystemExit(f"attention-items route not ok: {str(payload)[:200]}")
    return payload.get("data") or []


def load_marker(path: Path) -> set[int]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    return set(int(i) for i in data.get("announced_ids", []))


def save_marker(path: Path, ids: set[int]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"announced_ids": sorted(ids), "updated_at": int(time.time())}) + "\n")
    tmp.replace(path)


def build_record(mgmt_chat: str, new_ids: list[int], now: int) -> dict:
    n = len(new_ids)
    ids_str = ", ".join(str(i) for i in new_ids)
    body = (
        f"[system] processing wave complete: {n} attention item"
        f"{'s' if n != 1 else ''} now open (ids: {ids_str})"
    )
    return {
        "messageId": f"SYSWAVE-{now}-{'-'.join(str(i) for i in new_ids)}",
        "chatId": mgmt_chat,
        "chatName": "Christopher x TGG Management",
        "senderId": "system@internal",
        "senderName": "system",
        "isGroup": True,
        "body": body,
        "hasMedia": False,
        "mediaType": None,
        "mediaUrls": [],
        "mentionedIds": [],
        "quotedMessageId": None,
        "quotedText": "",
        "timestamp": now,
        "fromMe": False,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--token-env", default="CHRISTOPHER_TGG_PS_SERVICE_TOKEN")
    ap.add_argument("--tenant", default="tgg")
    ap.add_argument("--replay-run-id", default=None,
                    help="X-Replay-Run-Id header for a replay-target PS rig; omit in prod.")
    ap.add_argument("--marker", required=True, help="durable announced-ids JSON marker file")
    ap.add_argument("--mgmt-chat", required=True, help="management chat id the trigger addresses")
    ap.add_argument("--mode", choices=("stdout", "append"), default="stdout")
    ap.add_argument("--out", help="JSONL path for --mode append")
    ap.add_argument("--timestamp", type=int, default=None,
                    help="override record timestamp (rig corpora need monotonic times)")
    ap.add_argument("--init", action="store_true",
                    help="seed marker with all currently-open ids, emit nothing")
    args = ap.parse_args()

    token = os.environ.get(args.token_env, "")
    if not token:
        raise SystemExit(f"missing token env {args.token_env}")
    if args.mode == "append" and not args.out:
        raise SystemExit("--mode append requires --out")

    items = fetch_open_items(args.base_url, token, args.tenant, args.replay_run_id)
    open_ids = [int(it["id"]) for it in items]
    marker_path = Path(args.marker)
    announced = load_marker(marker_path)

    if args.init:
        save_marker(marker_path, set(open_ids))
        print(json.dumps({"initialized": True, "open_ids": sorted(open_ids), "emitted": False}))
        return 0

    new_ids = sorted(i for i in open_ids if i not in announced)
    if not new_ids:
        print(json.dumps({"emitted": False, "new_items": 0, "open_ids": sorted(open_ids)}))
        return 0

    now = args.timestamp if args.timestamp is not None else int(time.time())
    record = build_record(args.mgmt_chat, new_ids, now)
    if args.mode == "append":
        with open(args.out, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    else:
        print(json.dumps(record))
    # marker advances only after the record is durably emitted
    save_marker(marker_path, announced | set(new_ids))
    print(json.dumps({"emitted": True, "new_items": len(new_ids), "new_ids": new_ids}),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
