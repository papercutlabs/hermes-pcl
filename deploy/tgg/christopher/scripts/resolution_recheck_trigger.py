#!/usr/bin/env python3
"""PA-689: after an applied Master reconcile, append one recheck record for Christopher.

Fired by OnSuccess= of systems-papercut-labs-tgg-master-sfc-refresh.service. It reads the newest
report-cycle manifest whose stage is applied, ok is true and updatedAt falls after the reconcile unit's last
start; reads the open human-resolution documents from tgg.db read-only (last entry initial_default or
amendment, the PA-225 held_items_check rule); takes N not already rechecked against this Master content hash (the
recheck's own entries, by idempotency key), never-rechecked documents first, then the longest since their last recheck
entry, ties by the time raised, so a document only TGG can answer does not hold the backlog behind it from day to day;
and appends one internal record on
900000000000000010@g.us under the capture writer flock. N is the recheck brief's max_documents_per_run in the
constitution the gateway reads. It sends nothing and writes no case.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import yaml

JOB_TYPE = "tgg_resolution_recheck"
CHAT = "900000000000000010@g.us"
CAPTURE = "/var/lib/tgg-capture/whatsapp/capture/events.jsonl"
LOCK = "/home/pclaw/.hermes-christopher-tgg/runtime/nightly-whatsapp-launcher/.capture-append.lock"
RECEIPTS = "/home/pclaw/.hermes-christopher-tgg/runtime/resolution-recheck"
ARTIFACTS = "/home/pclaw/.systems-pcl/data/artifacts/tgg/report-cycle"
TENANT_DB = "/home/pclaw/.systems-pcl/data/tenants/tgg.db"
CONFIG = "/home/pclaw/.hermes-christopher-tgg/config.yaml"
# The PA-225 open rule. material_evidence_unavailable is PcL's own gap and stays open (plan, out of scope 2).
OPEN_DOCUMENTS = """
WITH current AS (
  SELECT r.id, r.created_at FROM human_resolution_records r
  WHERE r.kind <> 'material_evidence_unavailable'
    AND NOT EXISTS (SELECT 1 FROM human_resolution_records s WHERE s.supersedes_id = r.id)),
latest AS (
  SELECT c.*, (SELECT e.entry_kind FROM human_resolution_document_entries e WHERE e.record_id = c.id
               ORDER BY e.created_at DESC, e.id DESC LIMIT 1) AS last_entry,
         (SELECT MAX(e.created_at) FROM human_resolution_document_entries e
          WHERE e.record_id = c.id AND e.idempotency_key LIKE 'evidence-recheck:%') AS last_recheck_at FROM current c)
SELECT id FROM latest l
WHERE last_entry IN ('initial_default', 'amendment')
  AND NOT EXISTS (SELECT 1 FROM human_resolution_document_entries e
                  WHERE e.record_id = l.id AND e.idempotency_key LIKE 'evidence-recheck:' || l.id || ':' || ? || ':%')
ORDER BY last_recheck_at IS NOT NULL, last_recheck_at, created_at, id LIMIT ?
"""


def unit_last_start(unit: str) -> datetime:
    raw = subprocess.run(["systemctl", "show", unit, "-p", "ExecMainStartTimestamp", "--timestamp=unix", "--value"],
                         check=True, capture_output=True, text=True).stdout.strip()
    if not raw.startswith("@"):
        raise SystemExit(f"RECHECK_UNIT_START_UNKNOWN:{unit}:{raw!r}")
    return datetime.fromtimestamp(int(raw[1:]), timezone.utc)


def parse_time(value: object) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def applied_manifest(root: Path, since: datetime) -> dict | None:
    found = []
    for path in root.glob("reconcile-*/manifest.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("stage") == "applied" and manifest.get("ok") is True \
                and parse_time(manifest.get("updatedAt")) > since:
            found.append(manifest)
    return max(found, key=lambda m: parse_time(m["updatedAt"])) if found else None


def max_documents(config: Path) -> int:
    pa = (yaml.safe_load(config.read_text(encoding="utf-8")) or {}).get("pa") or {}
    constitution = yaml.safe_load(Path(pa["constitution_path"]).read_text(encoding="utf-8")) or {}
    limit = ((constitution.get("job_briefs") or {}).get(JOB_TYPE) or {}).get("max_documents_per_run")
    if type(limit) is not int or limit < 1:
        raise SystemExit("RECHECK_LIMIT_NOT_CONFIGURED: the recheck brief has no max_documents_per_run")
    return limit


def open_documents(db: Path, content_hash: str, limit: int) -> list[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        conn.execute("PRAGMA query_only=ON")
        return [row[0] for row in conn.execute(OPEN_DOCUMENTS, (content_hash, limit))]
    finally:
        conn.close()


def build_record(manifest: dict, document_ids: list[str], now: datetime) -> dict:
    run_id, content_hash = manifest["runId"], manifest["sources"]["master"]["contentHash"]
    body = (f"Recheck these open human-resolution documents against Master reconcile {run_id} "
            f"(Master content hash {content_hash}):\n" + "\n".join(document_ids))
    return {
        "capturedAt": now.isoformat().replace("+00:00", "Z"),
        "source": "christopher_resolution_recheck",
        "synthetic": True,
        "external_outbound_sent": 0,
        "normalized": {
            "messageId": f"tgg-recheck:{run_id}",
            "chatId": CHAT,
            "chatName": "Christopher resolution recheck",
            "senderId": "system@internal",
            "senderName": "System",
            "timestamp": int(now.timestamp()),
            "body": body,
            "isGroup": True,
            "fromMe": False,
            "historySync": False,
            "hasMedia": False,
            "mediaType": None,
            "mediaUrls": [],
            "mentionedIds": [],
            "quotedMessageId": None,
            "quotedText": "",
            "synthetic": True,
            "metadata": {
                "source": {"platform": "whatsapp", "chat_id": CHAT},
                "job_type": JOB_TYPE,
                "recheck_run_id": run_id,
                "master_content_hash": content_hash,
                "document_ids": document_ids,
                "external_outbound_sent": 0,
            },
        },
    }


def append_once(capture: Path, lock: Path, receipts: Path, record: dict) -> bool:
    """One record per run id: the receipt is created under the capture writer flock before the append."""
    receipts.mkdir(parents=True, exist_ok=True, mode=0o750)
    receipt = receipts / (record["normalized"]["metadata"]["recheck_run_id"] + ".json")
    line = (json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with lock.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        if receipt.exists():
            return False
        fd = os.open(capture, os.O_WRONLY | os.O_APPEND)
        try:
            if os.write(fd, line) != len(line):
                raise RuntimeError("RECHECK_APPEND_INCOMPLETE")
            os.fsync(fd)
        finally:
            os.close(fd)
        receipt.write_text(json.dumps({"contract": "tgg-resolution-recheck-trigger/v1",
                                       "message_id": record["normalized"]["messageId"],
                                       "metadata": record["normalized"]["metadata"],
                                       "external_outbound_sent": 0}, indent=1) + "\n", encoding="utf-8")
        return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    when = parser.add_mutually_exclusive_group(required=True)
    when.add_argument("--since-unit", help="read the reconcile unit's last start")
    when.add_argument("--since", help="ISO time (a test against a copy)")
    parser.add_argument("--artifacts-root", type=Path, default=Path(ARTIFACTS))
    parser.add_argument("--manifest", type=Path, help="one named manifest instead of the newest (a test)")
    parser.add_argument("--tenant-db", type=Path, default=Path(TENANT_DB))
    parser.add_argument("--config", type=Path, default=Path(CONFIG))
    parser.add_argument("--capture", type=Path, default=Path(CAPTURE))
    parser.add_argument("--lock", type=Path, default=Path(LOCK))
    parser.add_argument("--receipts", type=Path, default=Path(RECEIPTS))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    since = unit_last_start(args.since_unit) if args.since_unit else parse_time(args.since)
    if args.manifest:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        if not (manifest.get("stage") == "applied" and manifest.get("ok") is True
                and parse_time(manifest.get("updatedAt")) > since):
            raise SystemExit("RECHECK_MANIFEST_NOT_APPLIED_AFTER_SINCE")
    else:
        manifest = applied_manifest(args.artifacts_root, since)
    result = {"ok": True, "since": since.isoformat(), "external_outbound_sent": 0, "records_appended": 0}
    if manifest is None:
        print(json.dumps({**result, "reason": "no applied reconcile after the unit's last start"}))
        return 0
    content_hash = manifest["sources"]["master"]["contentHash"]
    documents = open_documents(args.tenant_db, content_hash, max_documents(args.config))
    result.update(recheck_run_id=manifest["runId"], master_content_hash=content_hash, document_ids=documents)
    if documents:
        record = build_record(manifest, documents, datetime.now(timezone.utc))
        if not args.dry_run and append_once(args.capture, args.lock, args.receipts, record):
            result["records_appended"] = 1
        result["message_id"] = record["normalized"]["messageId"]
    print(json.dumps({**result, "dry_run": args.dry_run}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
