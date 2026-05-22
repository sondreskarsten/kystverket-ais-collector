# kystverket-ais-collector

Bulk AIS position collector for the Norwegian EEZ from kystdatahuset.no. One parquet per hour for positions, one per day for vessel registry and voyages.

## Overview

| | |
|---|---|
| **What** | Every AIS position report in Norwegian waters |
| **Schedule** | 06:00 Oslo daily (60-day rolling lookback) |
| **Runtime** | ~6 min daily; ~5h for 3-month backfill |
| **Input** | `POST kystdatahuset.no/ws/api/ais/positions/within-geom-time` (open, no auth) |
| **Output — positions** | `ais/raw/positions/year=Y/month=M/day=D/hour=H.parquet` — 24 files/day |
| **Output — statinfo** | `ais/raw/statinfo/year=Y/month=M/day=D.parquet` — 1 file/day |
| **Output — voyages** | `ais/raw/voyages/year=Y/month=M/day=D.parquet` — 1 file/day |
| **Rows/day** | ~10M positions, ~2,500 vessels (statinfo), ~2,200 voyages |
| **MB/day** | ~190 MB positions + 0.24 MB statinfo + 0.07 MB voyages |
| **Downstream** | → kystverket-ais-parser |

## Column mapping (positions)

API returns unnamed 12-element arrays. Verified against named `PosMsg` endpoint:

| Pos | Name | Type | Notes |
|---|---|---|---|
| 0 | `mmsi` | int64 | MMSI identifier |
| 1 | `msgtime` | string | ISO timestamp (UTC, displayed as Oslo local) |
| 2 | `lon` | float64 | WGS84 |
| 3 | `lat` | float64 | WGS84 |
| 4 | `cog` | float64 | Course over ground (deg, 360.0=default) |
| 5 | `sog` | float64 | Speed over ground (kn, 102.3=N/A) |
| 6 | `msg_type` | int64 | AIS message type (1,3=Class A; 18=Class B) |
| 7 | `calc_speed` | float64 | Inter-point speed (kn); = col9/col8 × 1.944; -99=N/A |
| 8 | `sec_prevpoint` | int64 | Seconds since previous position; -99=N/A; median=10 |
| 9 | `dist_prevpoint` | int64 | Meters since previous position; -99=N/A |
| 10 | `true_heading` | int64 | Gyrocompass heading (deg, 511=N/A) |
| 11 | `rot` | int64 | Rate of turn (signed, ±720=max, -731=N/A) |

## Gotchas

- **`nav_status` is absent.** Bulk endpoint substitutes calc_speed/sec_prevpoint/dist_prevpoint where per-vessel endpoint has nav_status. Phase classification must use SOG-only heuristics. BarentsWatch live DOES include nav_status.
- **~50-day ingestion lag.** Data for today not available for ~50 days. This is a backend pipeline delay at Kystverket.
- **Connection pool limit ~8.** Exceeding triggers `53300: too many clients` in JSON. Use WORKERS=6.
- **Memory: 16Gi required.** 4Gi OOMs during sustained backfill with 6+ workers.

## Statinfo (NSR vessel registry)

Ship registry entry for every MMSI observed that day: `mmsino, callsign, shipname, imono, shiptypegroupnor, shiptypenor, grosstonnage, length, breadth, yearofbuild, countrynameeng`. The `callsign` field is the bridge to fartøyregisteret.

**Gotcha**: recent statinfo files (within lag window) may have 0 rows. Use largest available file for the bridge, not most recent.

## GCS layout

```
ais/raw/
├── positions/year=Y/month=M/day=D/hour=H.parquet   (~7 MB each, 24/day)
├── statinfo/year=Y/month=M/day=D.parquet            (~240 KB each)
├── voyages/year=Y/month=M/day=D.parquet             (~70 KB each)
└── _checkpoint/positions/{RUN_ID}.json
```

## Cloud Run

| Job | Resources | Schedule |
|---|---|---|
| `kystverket-ais-collector` | 4CPU/16Gi, 6 workers | manual (backfill) |
| `kystverket-ais-collector-daily` | same | 06:00 Oslo |
