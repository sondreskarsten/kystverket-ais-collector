import os, sys, json, time, uuid, gzip, io, ssl, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pafs

from google.oauth2 import service_account
from google.auth.transport.requests import Request as GAuthRequest

KEY_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '/mnt/project/sondreskarsten-d7d14-8486be2d085b.json')
BUCKET = os.getenv('GCS_BUCKET', 'sondre_brreg_data')
PREFIX = os.getenv('GCS_PREFIX', 'ais/raw')

BASE = 'https://kystdatahuset.no/ws/api'
EEZ_POLYGON = "POLYGON((-2 55,35 55,35 82,-2 82,-2 55))"

POSITION_COLS = ['mmsi','msgtime','lon','lat','c4','c5','c6','c7','c8','c9','c10','c11']

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def gcs_fs():
    if os.path.exists(KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(
            KEY_PATH, scopes=['https://www.googleapis.com/auth/cloud-platform'])
    else:
        import google.auth
        creds, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'])
    creds.refresh(GAuthRequest())
    return pafs.GcsFileSystem(access_token=creds.token, credential_token_expiration=creds.expiry)

def post(path, body, tmout=600):
    req = urllib.request.Request(f"{BASE}{path}",
        data=json.dumps(body).encode(),
        headers={'Content-Type':'application/json', 'Accept':'application/json'},
        method='POST')
    t0 = time.time()
    raw = urllib.request.urlopen(req, context=SSL_CTX, timeout=tmout).read()
    return json.loads(raw), len(raw), time.time() - t0

def collect_positions_hour(hour_start, fs, run_id, max_retries=5):
    hour_end = hour_start + timedelta(hours=1)
    start_str = hour_start.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = hour_end.strftime('%Y-%m-%dT%H:%M:%S')
    body = {"geom": EEZ_POLYGON, "start": start_str, "end": end_str, "minSpeed": 0}
    captured_at = datetime.now(timezone.utc)
    d, sz, el = None, 0, 0
    attempts = 0
    last_err = None
    for attempt in range(max_retries):
        attempts = attempt + 1
        try:
            d, sz, el = post('/ais/positions/within-geom-time', body)
            msg = (d.get('msg') or '')
            if 'too many clients' in msg or '53300' in msg or 'deadlock' in msg.lower():
                last_err = msg[:120]
                time.sleep(2 + attempt * 3)
                continue
            if d.get('success') is False and msg:
                last_err = msg[:200]
                if attempt < max_retries - 1:
                    time.sleep(2 + attempt * 3)
                    continue
            break
        except Exception as e:
            last_err = f'{type(e).__name__}: {str(e)[:150]}'
            time.sleep(2 + attempt * 3)
    if d is None:
        return {'kind':'positions','hour':start_str,'success':False,'err':last_err,'attempts':attempts,
                'captured_at':captured_at.isoformat(),'rows':0,'bytes':0,'elapsed':0,'run_id':run_id}
    rows = d.get('data') or []
    out_path = f"{BUCKET}/{PREFIX}/positions/year={hour_start.year:04d}/month={hour_start.month:02d}/day={hour_start.day:02d}/hour={hour_start.hour:02d}.parquet"
    n = len(rows)
    if n > 0:
        cols = {POSITION_COLS[i]: [r[i] if i < len(r) else None for r in rows] for i in range(len(POSITION_COLS))}
        table = pa.table(cols)
        meta = {b'source':b'kystdatahuset', b'endpoint':b'/api/ais/positions/within-geom-time',
                b'geom':EEZ_POLYGON.encode(), b'start':start_str.encode(), b'end':end_str.encode(),
                b'captured_at':captured_at.isoformat().encode(), b'run_id':run_id.encode(),
                b'minSpeed':b'0', b'response_bytes':str(sz).encode(), b'api_elapsed_s':f'{el:.3f}'.encode()}
        table = table.replace_schema_metadata(meta)
        with fs.open_output_stream(out_path, compression=None) as f:
            pq.write_table(table, f, compression='snappy', use_dictionary=True, row_group_size=200000)
    else:
        empty = pa.table({c: pa.array([], type=pa.string() if c=='msgtime' else pa.float64() if c in ('lon','lat') else pa.int64()) for c in POSITION_COLS})
        meta = {b'source':b'kystdatahuset', b'endpoint':b'/api/ais/positions/within-geom-time',
                b'geom':EEZ_POLYGON.encode(), b'start':start_str.encode(), b'end':end_str.encode(),
                b'captured_at':captured_at.isoformat().encode(), b'run_id':run_id.encode(),
                b'minSpeed':b'0', b'response_bytes':str(sz).encode(), b'api_elapsed_s':f'{el:.3f}'.encode(),
                b'note':b'empty_response'}
        empty = empty.replace_schema_metadata(meta)
        with fs.open_output_stream(out_path, compression=None) as f:
            pq.write_table(empty, f, compression='snappy')
    return {'kind':'positions','hour':start_str,'success':d.get('success', True),
            'msg':(d.get('msg') or '')[:200],
            'captured_at':captured_at.isoformat(),'rows':n,'bytes':sz,'elapsed':el,'run_id':run_id,'path':out_path}

def collect_statinfo_day(day_start, mmsi_list, fs, run_id):
    day_end = day_start + timedelta(days=1)
    start_str = day_start.strftime('%Y%m%d%H%M')
    end_str = (day_end - timedelta(minutes=1)).strftime('%Y%m%d%H%M')
    captured_at = datetime.now(timezone.utc)
    all_rows = []
    total_bytes = 0
    total_elapsed = 0.0
    errs = []
    for chunk_i in range(0, len(mmsi_list), 500):
        chunk = mmsi_list[chunk_i:chunk_i+500]
        body = {"mmsiIds": chunk, "start": start_str, "end": end_str}
        try:
            d, sz, el = post('/ais/statinfo/for-mmsis-time', body)
            total_bytes += sz
            total_elapsed += el
            if d.get('data'):
                all_rows.extend(d['data'])
        except Exception as e:
            errs.append(f"chunk{chunk_i}: {type(e).__name__}: {e}")
    out_path = f"{BUCKET}/{PREFIX}/statinfo/year={day_start.year:04d}/month={day_start.month:02d}/day={day_start.day:02d}.parquet"
    if all_rows:
        all_keys = list(all_rows[0].keys())
        cols = {k: [r.get(k) for r in all_rows] for k in all_keys}
        table = pa.table(cols)
    else:
        table = pa.table({'mmsi': pa.array([], type=pa.int64()), 'imo_num':pa.array([], type=pa.int64()),
                          'name':pa.array([], type=pa.string()), 'callsign':pa.array([], type=pa.string())})
    meta = {b'source':b'kystdatahuset', b'endpoint':b'/api/ais/statinfo/for-mmsis-time',
            b'day':day_start.strftime('%Y-%m-%d').encode(),
            b'mmsi_count':str(len(mmsi_list)).encode(), b'chunk_size':b'500',
            b'captured_at':captured_at.isoformat().encode(), b'run_id':run_id.encode(),
            b'response_bytes':str(total_bytes).encode(), b'api_elapsed_s':f'{total_elapsed:.3f}'.encode()}
    if errs:
        meta[b'errors'] = '\n'.join(errs).encode()
    table = table.replace_schema_metadata(meta)
    with fs.open_output_stream(out_path, compression=None) as f:
        pq.write_table(table, f, compression='snappy', use_dictionary=True)
    return {'kind':'statinfo','day':day_start.strftime('%Y-%m-%d'),'success':not errs,
            'mmsi_count':len(mmsi_list),'rows':len(all_rows),'bytes':total_bytes,
            'elapsed':total_elapsed,'captured_at':captured_at.isoformat(),'run_id':run_id,'path':out_path,
            'errors':errs}

def collect_voyages_day(day_start, mmsi_list, fs, run_id):
    day_end = day_start + timedelta(days=1)
    start_str = day_start.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = (day_end - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S')
    captured_at = datetime.now(timezone.utc)
    all_rows = []
    total_bytes = 0
    total_elapsed = 0.0
    errs = []
    for chunk_i in range(0, len(mmsi_list), 500):
        chunk = mmsi_list[chunk_i:chunk_i+500]
        body = {"mmsiIds": chunk, "startTime": start_str, "endTime": end_str}
        try:
            d, sz, el = post('/voyage/for-ships/by-mmsi', body)
            total_bytes += sz
            total_elapsed += el
            if d.get('data'):
                all_rows.extend(d['data'])
        except Exception as e:
            errs.append(f"chunk{chunk_i}: {type(e).__name__}: {e}")
    out_path = f"{BUCKET}/{PREFIX}/voyages/year={day_start.year:04d}/month={day_start.month:02d}/day={day_start.day:02d}.parquet"
    if all_rows:
        all_keys = sorted({k for r in all_rows for k in r.keys()})
        cols = {k: [r.get(k) for r in all_rows] for k in all_keys}
        table = pa.table(cols)
    else:
        table = pa.table({'mmsi': pa.array([], type=pa.int64()), 'origin':pa.array([], type=pa.string()),
                          'destination':pa.array([], type=pa.string())})
    meta = {b'source':b'kystdatahuset', b'endpoint':b'/api/voyage/for-ships/by-mmsi',
            b'day':day_start.strftime('%Y-%m-%d').encode(),
            b'mmsi_count':str(len(mmsi_list)).encode(), b'chunk_size':b'500',
            b'captured_at':captured_at.isoformat().encode(), b'run_id':run_id.encode(),
            b'response_bytes':str(total_bytes).encode(), b'api_elapsed_s':f'{total_elapsed:.3f}'.encode()}
    if errs:
        meta[b'errors'] = '\n'.join(errs).encode()
    table = table.replace_schema_metadata(meta)
    with fs.open_output_stream(out_path, compression=None) as f:
        pq.write_table(table, f, compression='snappy', use_dictionary=True)
    return {'kind':'voyages','day':day_start.strftime('%Y-%m-%d'),'success':not errs,
            'mmsi_count':len(mmsi_list),'rows':len(all_rows),'bytes':total_bytes,
            'elapsed':total_elapsed,'captured_at':captured_at.isoformat(),'run_id':run_id,'path':out_path,
            'errors':errs}

def write_manifest(records, fs, run_id):
    out = io.BytesIO()
    for r in records:
        out.write((json.dumps(r) + '\n').encode())
    path = f"{BUCKET}/{PREFIX}/_manifest/run={run_id}.jsonl"
    with fs.open_output_stream(path, compression=None) as f:
        f.write(out.getvalue())
    return path
