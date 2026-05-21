"""Kystverket AIS positions collector — Cloud Run Job.

Pulls AIS position observations from kystdatahuset.no for the Norwegian EEZ
in 1-hour chunks and writes them as immutable observation parquets to GCS.

State on GCS:
  ais/raw/positions/year={YYYY}/month={MM}/day={DD}/hour={HH}.parquet
  ais/raw/_checkpoint/positions/{RUN_ID}.json    (list of completed hours)
  ais/raw/_manifest/run={RUN_ID}.jsonl           (per-call audit trail)

Environment variables:
  WINDOW_START       ISO datetime, e.g. 2025-04-01T00:00:00 (required)
  WINDOW_END         ISO datetime, e.g. 2025-07-01T00:00:00 (required)
  WORKERS            integer, default 8 (kystdatahuset PG limit, do not exceed)
  RUN_ID             string, default = window-derived
  GCS_BUCKET         default sondre_brreg_data
  GCS_PREFIX         default ais/raw
"""
import os
import sys
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from collector import (
    gcs_fs,
    collect_positions_hour,
    BUCKET as DEFAULT_BUCKET,
    PREFIX as DEFAULT_PREFIX,
)

BUCKET = os.getenv("GCS_BUCKET", DEFAULT_BUCKET)
PREFIX = os.getenv("GCS_PREFIX", DEFAULT_PREFIX)
WORKERS = int(os.getenv("WORKERS", "8"))
WINDOW_START = datetime.fromisoformat(os.environ["WINDOW_START"])
WINDOW_END = datetime.fromisoformat(os.environ["WINDOW_END"])
RUN_ID = os.getenv(
    "RUN_ID",
    f"{WINDOW_START:%Y%m%d}_{WINDOW_END:%Y%m%d}",
)


def all_hours():
    h = WINDOW_START
    while h < WINDOW_END:
        yield h
        h += timedelta(hours=1)


def checkpoint_path():
    return f"{BUCKET}/{PREFIX}/_checkpoint/positions/{RUN_ID}.json"


def manifest_path():
    return f"{BUCKET}/{PREFIX}/_manifest/run={RUN_ID}.jsonl"


def load_checkpoint(fs):
    p = checkpoint_path()
    try:
        with fs.open_input_stream(p) as f:
            return json.loads(f.read().decode())
    except FileNotFoundError:
        return {"run_id": RUN_ID, "done": []}
    except Exception:
        return {"run_id": RUN_ID, "done": []}


def save_checkpoint(fs, cp):
    p = checkpoint_path()
    body = json.dumps(cp).encode()
    with fs.open_output_stream(p) as f:
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


def main():
    print(
        f"[{datetime.now():%H:%M:%S}] run_id={RUN_ID} workers={WORKERS} "
        f"window={WINDOW_START}→{WINDOW_END}",
        flush=True,
    )

    fs = gcs_fs()
    cp = load_checkpoint(fs)
    done_set = set(cp["done"])
    all_h = list(all_hours())
    todo = [h for h in all_h if h.isoformat() not in done_set]

    print(
        f"[{datetime.now():%H:%M:%S}] total={len(all_h)} done={len(done_set)} "
        f"todo={len(todo)}",
        flush=True,
    )

    if not todo:
        print(f"[{datetime.now():%H:%M:%S}] nothing to do, exiting", flush=True)
        return 0

    fs_pool = [gcs_fs() for _ in range(WORKERS)]

    completed = 0
    failed = 0
    start = time.time()
    pending_manifest = []
    last_save = time.time()

    def worker(h, wid):
        return collect_positions_hour(h, fs_pool[wid % WORKERS], RUN_ID)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {}
        for i, h in enumerate(todo):
            wid = i % WORKERS
            futs[ex.submit(worker, h, wid)] = h

        for fut in as_completed(futs):
            h = futs[fut]
            try:
                r = fut.result()
                if r.get("success"):
                    completed += 1
                    done_set.add(h.isoformat())
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                r = {
                    "kind": "positions",
                    "hour": h.isoformat(),
                    "success": False,
                    "err": f"{type(e).__name__}: {str(e)[:200]}",
                    "run_id": RUN_ID,
                }
            pending_manifest.append(r)

            n = completed + failed
            if n % 20 == 0 or n == len(todo):
                elapsed = time.time() - start
                rate = n / elapsed if elapsed else 0
                eta = (len(todo) - n) / rate if rate else 0
                rows_so_far = sum(
                    x.get("rows", 0) for x in pending_manifest
                )
                print(
                    f"[{datetime.now():%H:%M:%S}] {n}/{len(todo)} "
                    f"done={completed} fail={failed} "
                    f"rate={rate * 3600:.0f}/hr eta={eta / 60:.1f}min "
                    f"rows={rows_so_far:,}",
                    flush=True,
                )

            if time.time() - last_save > 60:
                cp["done"] = sorted(done_set)
                save_checkpoint(fs, cp)
                append_manifest(fs, pending_manifest)
                pending_manifest = []
                last_save = time.time()

    cp["done"] = sorted(done_set)
    save_checkpoint(fs, cp)
    append_manifest(fs, pending_manifest)

    print(
        f"[{datetime.now():%H:%M:%S}] DONE: completed={completed} "
        f"failed={failed} total_done={len(done_set)}/{len(all_h)}",
        flush=True,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
