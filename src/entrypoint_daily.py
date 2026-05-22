"""Daily entrypoint — runs positions + statinfo/voyages for a rolling 60-day window.

Designed for Cloud Scheduler. Uses a fixed RUN_ID='daily' so the checkpoint
accumulates across runs. Already-processed hours/days are skipped.

The kystdatahuset API has ~50-day ingestion lag, so a 60-day lookback ensures
we catch newly-available data without scanning a huge historical window.
"""
import os
import sys
import subprocess
from datetime import datetime, timedelta, timezone


def main():
    now = datetime.now(timezone.utc)
    window_end = (now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
    window_start = (now - timedelta(days=60)).strftime("%Y-%m-%dT00:00:00")
    window_start_date = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    window_end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    env = {
        **os.environ,
        "WINDOW_START": window_start,
        "WINDOW_END": window_end,
        "RUN_ID": "daily",
        "WORKERS": os.getenv("WORKERS", "6"),
    }
    env_sv = {
        **os.environ,
        "WINDOW_START": window_start_date,
        "WINDOW_END": window_end_date,
        "RUN_ID": "daily",
    }

    print(f"[{datetime.now():%H:%M:%S}] daily: positions {window_start}→{window_end}", flush=True)
    r1 = subprocess.run(
        [sys.executable, "/app/entrypoint.py"],
        env=env,
    )
    print(f"[{datetime.now():%H:%M:%S}] positions exit={r1.returncode}", flush=True)

    print(f"[{datetime.now():%H:%M:%S}] daily: statinfo+voyages {window_start_date}→{window_end_date}", flush=True)
    r2 = subprocess.run(
        [sys.executable, "/app/entrypoint_statinfo_voyages.py"],
        env=env_sv,
    )
    print(f"[{datetime.now():%H:%M:%S}] statinfo+voyages exit={r2.returncode}", flush=True)

    return max(r1.returncode, r2.returncode)


if __name__ == "__main__":
    sys.exit(main())
