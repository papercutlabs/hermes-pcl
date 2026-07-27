#!/usr/bin/env python3
"""Studio entry point for the TGG rendered output evaluator."""

from __future__ import annotations

import sys
from pathlib import Path


DEPLOY_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DEPLOY_DIR))

from quality_eval.core import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
