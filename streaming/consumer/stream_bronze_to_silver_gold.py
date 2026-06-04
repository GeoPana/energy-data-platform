"""Wrapper for the main Phase 4 Structured Streaming job."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.streaming.stream_bronze_to_silver_gold import main


if __name__ == "__main__":
    main()
