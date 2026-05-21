"""Kystverket AIS statinfo+voyages collector — Cloud Run Job.

For each day in WINDOW_START..WINDOW_END:
  1. Read all 24 position parquets for that day from GCS
  2. Extract distinct MMSIs that transmitted that day
  3. Call /api/ais/statinfo/for-mmsis-time → one parquet per day
  4. Call /api/voyage/for-ships/by-mmsi    → one parquet per day

Outputs:
  ais/raw/statinfo/year={YYYY}/month={MM}/day={DD}.parquet
  ais/raw/voyages/year={YYYY}/month={MM}/day={DD}.parquet
  ais/raw/_checkpoint/statinfo_voyages/{RUN_ID}.json
  ais/raw/_manifest/statinfo_voyages_run={RUN_ID}.jsonl

Skips days where position parquets are incomplete (<24 files). Re-run after
the positions backfill catches up.
"""
import os
import sys
import json
import time
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq

from collector import (
    gcs_fs,
    collect_statinfo_day,
    collect_voyages_day,
    BUCKET as DEFAULT_BUCKET,
    PREFIX as DEFAULT_PREFIX,
)

BUCKET = os.getenv("GCS_BUCKET", DEFAULT_BUCKET)
PREFIX = os.getenv("GCS_PREFIX", DEFAULT_PREFIX)
WINDOW_START = datetime.fromisoformat(os.environ["WINDOW_START"]).date()
WINDOW_END = datetime.fromisoformat(os.environ["WINDOW_END"]).date()
RUN_ID = os.getenv(
    "RUN_ID",
    f"{WINDOW_START:%Y%m%d}_{WINDOW_END:%Y%m%d}",
)


def all_days():
    d = WINDOW_START
    while d < WINDOW_END:
        yield d
        d += timedelta(days=1)


def checkpoint_path():
    return f"{BUCKET}/{PREFIX}/_checkpoint/statinfo_voyages/{RUN_ID}.json"


def manifest_path():
    return f"{BUCKET}/{PREFIX}/_manifest/statinfo_voyages_run={RUN_ID}.jsonl"


def load_checkpoint(fs):
    try:
        with fs.open_input_stream(checkpoint_path()) as f:
            return json.loads(f.read().decode())
    except FileNotFoundError:
        return {"run_id": RUN_ID, "done": []}
    except Exception:
        return {"run_id": RUN_ID, "done": []}


def save_checkpoint(fs, cp):
    body = json.dumps(cp).encode()
    with fs.open_output_stream(checkpoint_path()) as f:
        f.write(body)


def append_manifest(fs, lines):
    if not lines:
        return
    p = manifest_path()
    existing = b""
    try:
        with fs.open_input_stream(p) as f:
            existing = f.read()
    except FileNotFoundError:
        pass
    body = existing + b"".join(
        (json.dumps(r, default=str) + "\n").encode() for r in lines
    )
    with fs.open_output_stream(p) as f:
        f.write(body)


def position_parquets_for_day(fs, day):
    """Return list of (path, size) parquets for the given day. Empty list if missing."""
    prefix = f"{BUCKET}/{PREFIX}/positions/year={day.year:04d}/month={day.month:02d}/day={day.day:02d}/"
    sel = fs.get_file_info(__import__('pyarrow.fs').fs.FileSelector(prefix, recursive=False))
    return [(s.path, s.size) for s in sel if s.is_file and s.path.endswith('.parquet')]


def mmsi_universe_for_day(fs, day):
    """Read all position parquets for the day, return sorted distinct MMSIs."""
    files = position_parquets_for_day(fs, day)
    if len(files) < 24:
        return None, len(files)
    seen = set()
    for path, _sz in files:
        with fs.open_input_file(path) as src:
            t = pq.read_table(src, columns=['mmsi'])
            seen.update(t['mmsi'].to_pylist())
    return sorted(seen), len(files)


def main():
    print(
        f"[{datetime.now():%H:%M:%S}] statinfo_voyages run_id={RUN_ID} "
        f"window={WINDOW_START}→{WINDOW_END}",
        flush=True,
    )

    fs = gcs_fs()
    cp = load_checkpoint(fs)
    done_set = set(cp["done"])
    all_d = list(all_days())
    todo = [d for d in all_d if d.isoformat() not in done_set]

    print(
        f"[{datetime.now():%H:%M:%S}] total={len(all_d)} done={len(done_set)} "
        f"todo={len(todo)}",
        flush=True,
    )

    if not todo:
        print(f"[{datetime.now():%H:%M:%S}] nothing to do", flush=True)
        return 0

    start = time.time()
    completed = 0
    failed = 0
    skipped = 0
    pending = []
    last_save = time.time()

    for day in todo:
        mmsis, n_files = mmsi_universe_for_day(fs, day)
        if mmsis is None:
            skipped += 1
            r = {
                "kind": "skip",
                "day": day.isoformat(),
                "reason": f"only {n_files}/24 position parquets present",
                "run_id": RUN_ID,
            }
            pending.append(r)
            print(
                f"  {day} skip (only {n_files}/24 position parquets present)",
                flush=True,
            )
            continue

        day_dt = datetime.combine(day, datetime.min.time())
        try:
            r_s = collect_statinfo_day(day_dt, mmsis, fs, RUN_ID)
            r_v = collect_voyages_day(day_dt, mmsis, fs, RUN_ID)
            pending.append(r_s)
            pending.append(r_v)
            if r_s.get("success") and r_v.get("success"):
                completed += 1
                done_set.add(day.isoformat())
            else:
                failed += 1
            print(
                f"  {day} mmsi={len(mmsis):,} "
                f"statinfo={r_s.get('rows'):,}r/{r_s.get('elapsed'):.1f}s "
                f"voyages={r_v.get('rows'):,}r/{r_v.get('elapsed'):.1f}s",
                flush=True,
            )
        except Exception as e:
            failed += 1
            err = {
                "kind": "statinfo_voyages",
                "day": day.isoformat(),
                "success": False,
                "err": f"{type(e).__name__}: {str(e)[:200]}",
                "run_id": RUN_ID,
            }
            pending.append(err)
            print(f"  {day} FAILED: {err['err']}", flush=True)

        if time.time() - last_save > 60:
            cp["done"] = sorted(done_set)
            save_checkpoint(fs, cp)
            append_manifest(fs, pending)
            pending = []
            last_save = time.time()

    cp["done"] = sorted(done_set)
    save_checkpoint(fs, cp)
    append_manifest(fs, pending)

    elapsed = time.time() - start
    print(
        f"[{datetime.now():%H:%M:%S}] DONE: completed={completed} "
        f"failed={failed} skipped={skipped} elapsed={elapsed:.0f}s",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
