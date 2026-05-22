# kystverket-ais-collector

Collects AIS vessel position data from the Norwegian Coastal Administration's public data portal (kystdatahuset.no) for every vessel in Norwegian waters. Writes raw observation parquets to GCS — one file per hour for positions, one per day for vessel registry and voyage data.

## What is AIS data?

Every vessel above 15 meters (fishing) or 300 GT (cargo) is legally required to broadcast its position via the Automatic Identification System (AIS). The broadcast happens every 2-10 seconds and includes: vessel identity (MMSI number), position (GPS coordinates), speed over ground, course, heading, and rate of turn.

This is **real-time operational telemetry** for the entire Norwegian merchant and fishing fleet. Unlike financial statements (which arrive 6-18 months late) or catch data (which arrives with weeks of delay), AIS positions show what a vessel is doing *right now*.

## Why does a credit analyst care?

AIS data answers questions that no financial report can:

- **Is the vessel actually fishing?** A vessel that hasn't left port in 3 weeks may have mechanical issues, a crew shortage, or financial distress (can't afford fuel). This is visible from AIS before it's visible anywhere else.
- **How many days at sea?** Days-at-sea (DAS) is the fishing industry's primary activity metric. A vessel with 30 DAS this year vs 45 DAS same period last year has a 33% activity decline — computable from AIS alone.
- **Where is the vessel operating?** Fishing grounds shift with stock migration. A vessel moving from productive grounds to marginal ones may indicate access issues (quota allocation changes, territorial disputes).
- **Is the vessel idle for maintenance or for distress?** A planned maintenance stop at a known shipyard (identifiable from port stays at shipyard coordinates) is different from an unplanned stop at the home berth.

## Three data types collected

### 1. Positions (hourly)
Every AIS position report for every vessel in the Norwegian EEZ, collected in 1-hour chunks.

| Field | Description |
|---|---|
| `mmsi` | Maritime Mobile Service Identity — unique vessel radio identifier |
| `msgtime` | Timestamp of the AIS transmission |
| `lon`, `lat` | GPS coordinates (WGS84) |
| `cog` | Course over ground (degrees) |
| `sog` | Speed over ground (knots) |
| `msg_type` | AIS message type (1/3 = Class A dynamic, 18 = Class B) |
| `calc_speed` | Computed speed from consecutive positions (knots) |
| `sec_prevpoint` | Seconds since previous position report |
| `dist_prevpoint` | Meters since previous position report |
| `true_heading` | Gyrocompass heading (degrees, 511 = not available) |
| `rot` | Rate of turn (degrees/min, signed) |

**Volume**: ~300,000 positions per hour, ~10 million per day, ~7 MB parquet per hour.

### 2. Statinfo / NSR vessel registry (daily)
The Norwegian Ship Register (NSR) entry for every MMSI observed that day. This is how we know a vessel's name, call sign, IMO number, ship type, flag state, and dimensions.

The call sign from NSR is the critical bridge: `mmsi → callsign → fartøyregisteret.radio_call_sign → orgnr`. Without this bridge, AIS positions have no corporate identity.

### 3. Voyages (daily)
Kystverket's pre-computed voyage segments: origin port, destination port, ETD/ETA, cargo quantities, vessel characteristics at time of voyage. This is the Coastal Administration's own interpretation of "where did this vessel go?" — useful for validation against our MarU derivation.

## Technical details

- **API**: `POST https://kystdatahuset.no/ws/api/ais/positions/within-geom-time`
- **Auth**: none (public NLOD license)
- **Coverage**: Norwegian EEZ polygon `POLYGON((-2 55,35 55,35 82,-2 82,-2 55))`
- **Backend**: PostgreSQL partitioned by month. Connection pool limit at ~8 concurrent clients (exceeding triggers `53300: too many clients` embedded in JSON response).
- **Recency**: ~50-day ingestion lag at backend. Today's data is not available for ~50 days.
- **History**: back to ~2007

## GCS layout

```
gs://sondre_brreg_data/ais/raw/
├── positions/year={YYYY}/month={MM}/day={DD}/hour={HH}.parquet
├── statinfo/year={YYYY}/month={MM}/day={DD}.parquet
├── voyages/year={YYYY}/month={MM}/day={DD}.parquet
├── _checkpoint/positions/{RUN_ID}.json
└── _manifest/run={RUN_ID}.jsonl
```

## Important: `nav_status` is absent

The bulk geom endpoint substitutes three inter-point metrics (calc_speed, sec_prevpoint, dist_prevpoint) where the per-vessel endpoint would have `navigational_status` (0=underway, 1=anchored, 5=moored, 7=fishing). This means the downstream parser cannot use the crew's self-declared fishing status and must fall back to speed-only heuristics. The BarentsWatch live poller (barentswatch-ais-live) DOES include nav_status.

## Cloud Run

- **Backfill job**: `kystverket-ais-collector` (4CPU/16Gi, WORKERS=6)
- **Daily job**: `kystverket-ais-collector-daily` (60-day rolling lookback)
- **Schedule**: 06:00 Oslo daily
- **Throughput**: ~15s per hour-chunk with 6 workers; daily run ~6 min, 3-month backfill ~5h

## Downstream

→ **kystverket-ais-parser**: decodes positions, adds H3 spatial index + orgnr bridge
→ **barentswatch-ais-live**: uses NSR statinfo for the mmsi→callsign→orgnr bridge
