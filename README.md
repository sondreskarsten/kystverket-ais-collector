# kystverket-ais-collector

Bulk AIS position collector for the Norwegian EEZ from kystdatahuset.no. Writes one parquet per hour for positions, one per day for vessel registry (NSR statinfo) and pre-computed voyages.

## Source

Kystdatahuset is the Norwegian Coastal Administration's public data portal. The endpoint `POST /api/ais/positions/within-geom-time` returns an unnamed 12-element array per position report for every vessel inside a bounding polygon during a time window.

No auth. No published rate limit, but the PostgreSQL backend caps at ~8 concurrent connections (exceeding returns `53300: too many clients` embedded in JSON). We use 6 parallel workers.

**Recency lag**: ~50 days. Data for today is not available until approximately 50 days later. This is a backend ingestion pipeline delay, not a policy choice. For real-time positions, use barentswatch-ais-live.

## Column mapping

The API returns unnamed arrays. Verified mapping (cross-referenced against the named `PosMsg` endpoint for MMSI 257127870 at 2025-04-01T02:00:09Z):

| Position | Name | Type | Notes |
|---|---|---|---|
| 0 | `mmsi` | int64 | MMSI identifier |
| 1 | `msgtime` | string | ISO timestamp (UTC, but displayed as Oslo local by the API) |
| 2 | `lon` | float64 | WGS84 longitude |
| 3 | `lat` | float64 | WGS84 latitude |
| 4 | `cog` | float64 | Course over ground (deg, 0.1° resolution, 360.0 = default) |
| 5 | `sog` | float64 | Speed over ground (knots, 0.1 kn res, 102.3 = N/A) |
| 6 | `msg_type` | int64 | AIS message type (1,3 = Class A; 18 = Class B) |
| 7 | `calc_speed` | float64 | Inter-point computed speed (kn), = col9/col8 × 1.944; -99 = N/A |
| 8 | `sec_prevpoint` | int64 | Seconds since previous position; -99 = N/A; median = 10 |
| 9 | `dist_prevpoint` | int64 | Meters since previous position; -99 = N/A |
| 10 | `true_heading` | int64 | Gyrocompass heading (deg, 511 = N/A) |
| 11 | `rot` | int64 | Rate of turn (signed, ±720 = max, -731 = N/A) |

**`nav_status` is absent.** The per-vessel `PosMsg` endpoint includes it (0=underway, 1=anchored, 5=moored, 7=fishing), but this bulk endpoint substitutes calc_speed/sec_prevpoint/dist_prevpoint instead. Downstream phase classification must use SOG-only heuristics. BarentsWatch live DOES include nav_status.

## Volume

~300K positions/hour, ~10M/day, ~7 MB snappy parquet per hour. 3-month backfill (Apr–Jun 2025) = 2,184 files, 15.3 GB.

## NSR statinfo (daily)

Ship registry entries for every MMSI observed that day. Fields: `mmsino, callsign, shipname, imono, shiptypegroupnor, shiptypenor, grosstonnage, length, breadth, yearofbuild, countrynameeng, etc.`

This is the bridge: `mmsi → callsign → fartøyregisteret.radio_call_sign → orgnr`. Without statinfo, AIS positions are anonymous.

**Gotcha**: recent statinfo files (within the ~50-day lag window) may have 0 rows because the backend hasn't ingested that day's NSR data yet. The parser uses the largest available statinfo file for the bridge, not the most recent.

## GCS layout

```
gs://sondre_brreg_data/ais/raw/
├── positions/year={Y}/month={M}/day={D}/hour={H}.parquet
├── statinfo/year={Y}/month={M}/day={D}.parquet
├── voyages/year={Y}/month={M}/day={D}.parquet
└── _checkpoint/positions/{RUN_ID}.json
```

## Cloud Run

| Job | Purpose | Resources | Schedule |
|---|---|---|---|
| `kystverket-ais-collector` | Backfill | 4CPU/16Gi, 6 workers, 6h timeout | manual |
| `kystverket-ais-collector-daily` | Incremental (60-day lookback) | same | 06:00 Oslo |

## Downstream

→ kystverket-ais-parser
