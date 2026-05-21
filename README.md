# kystverket-ais-collector

Cloud Run Job that pulls AIS position observations from `kystdatahuset.no` for the Norwegian EEZ in 1-hour chunks and writes them as immutable observation parquets to GCS.

## Source

- Endpoint: `POST https://kystdatahuset.no/ws/api/ais/positions/within-geom-time`
- Auth: none (public NLOD-licensed surface)
- Coverage: vessels ≥15m fishing + ≥45m pleasure in Norwegian EEZ + Svalbard/Jan Mayen zones
- Polygon: `POLYGON((-2 55,35 55,35 82,-2 82,-2 55))` (loose bbox over EEZ)
- Backend: PostgreSQL partitioned by month/day. Connection pool limit reached at ~16 concurrent clients → use `WORKERS=8` ceiling
- Recency: ~50-day trailing ingestion lag at backend (today's frontier is ~30 days behind real-time)
- History: back to ~2007

## Pattern

Collector-only. Source-shape capture, no parsing. Each 1-hour API response becomes one parquet file, with full call provenance in parquet schema metadata (source, endpoint, geom, start, end, captured_at, run_id, response_bytes, api_elapsed_s).

A later `kystverket-ais-parser` pipeline owns column decoding, MMSI→callsign→orgnr resolution, and MarU voyage segmentation.

## GCS layout

```
gs://sondre_brreg_data/ais/raw/
├── positions/year={YYYY}/month={MM}/day={DD}/hour={HH}.parquet
├── _checkpoint/positions/{RUN_ID}.json   resumable run state
└── _manifest/run={RUN_ID}.jsonl          per-call audit (timing, row counts, errors)
```

Position parquet columns (raw from API, unnamed columns 4–11 await parser decoding):

| col | type | meaning (provisional) |
|---|---|---|
| `mmsi` | int64 | Maritime Mobile Service Identity |
| `msgtime` | string | ISO datetime, Oslo local time |
| `lon` | double | longitude WGS84 |
| `lat` | double | latitude WGS84 |
| `c4` | double | likely SOG (speed over ground, knots) |
| `c5` | double | likely COG (course over ground) or heading-related |
| `c6` | int64 | likely ship_type |
| `c7` | double | likely additional speed/heading metric |
| `c8` | int64 | likely heading degrees |
| `c9` | int64 | likely navigational status |
| `c10` | int64 | likely true_heading (511 = N/A) |
| `c11` | int64 | likely rate_of_turn signed |

## Environment variables

| Var | Default | Notes |
|---|---|---|
| `WINDOW_START` | required | ISO datetime, e.g. `2025-04-01T00:00:00` |
| `WINDOW_END` | required | ISO datetime (exclusive), e.g. `2025-07-01T00:00:00` |
| `WORKERS` | 8 | Hard ceiling; >8 hits PostgreSQL `53300: too many clients` |
| `RUN_ID` | derived from window | e.g. `20250401_20250701` |
| `GCS_BUCKET` | `sondre_brreg_data` | |
| `GCS_PREFIX` | `ais/raw` | |

## Resumability

The job reads `_checkpoint/positions/{RUN_ID}.json` at start. Re-running with the same `RUN_ID` resumes from the last checkpoint. Checkpoint flushes every 60 seconds during the run. Safe to restart unattended.

## Throughput benchmarks (empirical, May 2026)

- 1-hour API call: ~35–50s wall-clock, ~280–415k position rows, ~25–37 MB JSON response → ~5–7 MB snappy parquet (~5× compression)
- 8 workers: ~13s effective per hour-chunk
- 3-month backfill (2 184 hours): ~8 hours job time
- Daily incremental (24 hours): ~5 minutes job time

## Retry policy

`collect_positions_hour` retries up to 5 times with exponential backoff (2s + attempt×3s) on:
- `53300: too many clients already` (PostgreSQL connection pool exhausted)
- `deadlock` errors
- transport exceptions (timeout, connection reset)

`success=False` responses with permanent errors (e.g. `relation does not exist` for future dates) fail fast and land in the manifest as failed.

## Build and deploy

```bash
# Build container
gcloud builds submit \
  --tag europe-north1-docker.pkg.dev/sondreskarsten-d7d14/brreg-pipelines/kystverket-ais-collector:latest

# Create job
gcloud run jobs create kystverket-ais-collector \
  --image europe-north1-docker.pkg.dev/sondreskarsten-d7d14/brreg-pipelines/kystverket-ais-collector:latest \
  --region europe-north1 \
  --service-account s1sfreracct@sondreskarsten-d7d14.iam.gserviceaccount.com \
  --memory 2Gi --cpu 2 \
  --task-timeout 24h \
  --max-retries 1 \
  --set-env-vars WORKERS=8

# Run backfill for 3-month window
gcloud run jobs execute kystverket-ais-collector \
  --region europe-north1 \
  --update-env-vars WINDOW_START=2025-04-01T00:00:00,WINDOW_END=2025-07-01T00:00:00,RUN_ID=aprjun2025
```

## Daily scheduler (post-backfill)

Cloud Scheduler in `europe-west1` triggers daily, passing yesterday's date window. To be added when production-ready.

## Not yet implemented

- `statinfo` collector (Type 5/24 vessel static data, MMSI-list scoped, per-day chunking)
- `voyages` collector (Kystverket's pre-computed voyage segments, MMSI-list scoped, per-day chunking)
- Daily incremental scheduler
- BarentsWatch real-time companion collector for the 14-day window (`barentswatch-ais-collector` repo)
- TCP NMEA receiver for full Type 5/24 message richness (`kystverket-ais-tcp-receiver` repo)
