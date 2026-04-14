"""
fetch_stooq.py
Fetches weekly and daily OHLCV data from Stooq for all watchlist symbols
and saves them as JSON files under data/.

Run by GitHub Actions on schedule, or manually:
  python fetch_stooq.py
"""

import requests
import json
import os
import time
import csv
from datetime import datetime, timezone
from io import StringIO

# ── Watchlist ────────────────────────────────────────────────────────────────
SYMBOLS = [
    'voo.us', 'vgt.us', 'qqq.us', 'vxus.us', 'vht.us',
    'vfh.us', 'vde.us', 'vnq.us', 'vox.us',  'vcr.us',
    'vdc.us', 'vis.us', 'vaw.us', 'vpu.us',
]

FREQUENCIES = ['w', 'd']   # weekly and daily

OUTPUT_DIR = 'data'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://stooq.com/',
}

STOOQ_URL = 'https://stooq.com/q/d/l/?s={symbol}&i={freq}'

# ── Helpers ──────────────────────────────────────────────────────────────────

def fetch_csv(symbol: str, freq: str, retries: int = 3) -> list[dict]:
    """Fetch and parse a Stooq CSV with retries. Returns list of {date, close} dicts."""
    url = STOOQ_URL.format(symbol=symbol, freq=freq)
    last_err = None
    resp = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=25)
            resp.raise_for_status()
            break
        except Exception as e:
            last_err = e
            if attempt < retries:
                wait = attempt * 4
                print(f'    Retry {attempt}/{retries} for {symbol} ({freq}) in {wait}s — {e}')
                time.sleep(wait)
    else:
        raise last_err

    text = resp.text.strip()
    if not text or text.lower().startswith('no data'):
        raise ValueError(f'No data returned for {symbol} ({freq})')

    reader = csv.DictReader(StringIO(text))
    rows = []
    prev_date = None

    for row in reader:
        date_str = row.get('Date', '').strip()
        close_str = row.get('Close', '').strip()

        if not date_str or not close_str:
            continue

        try:
            close = float(close_str)
        except ValueError:
            raise ValueError(f'Non-numeric close "{close_str}" for {symbol} on {date_str}')

        if close <= 0:
            continue

        date_key = date_str.replace('-', '')
        if prev_date and date_key <= prev_date:
            raise ValueError(f'Non-increasing date {date_str} for {symbol}')
        prev_date = date_key

        rows.append({'date': date_str, 'close': close})

    if len(rows) < 42:
        raise ValueError(f'Insufficient rows ({len(rows)}) for {symbol} ({freq})')

    return rows


def save(symbol: str, freq: str, rows: list[dict], error: str | None = None):
    """Write result JSON to data/<symbol>_<freq>.json"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f'{symbol.replace(".", "_")}_{freq}.json'
    path = os.path.join(OUTPUT_DIR, filename)

    payload = {
        'symbol': symbol,
        'freq': freq,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'rows': rows,
        'error': error,
    }

    with open(path, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))

    print(f'  Saved {path} ({len(rows)} rows)')


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(f'Stooq fetch started at {fetched_at}')
    print(f'Symbols: {len(SYMBOLS)}  ×  Frequencies: {FREQUENCIES}')
    print()

    ok = 0
    failed = 0

    for symbol in SYMBOLS:
        for freq in FREQUENCIES:
            label = f'{symbol} ({freq})'
            try:
                rows = fetch_csv(symbol, freq)
                save(symbol, freq, rows)
                print(f'  ✓ {label}  —  {rows[-1]["date"]} (latest)')
                ok += 1
            except Exception as e:
                print(f'  ✗ {label}  —  {e}')
                save(symbol, freq, [], error=str(e))
                failed += 1

            # Be polite — small delay between requests
            time.sleep(1.5)

        print()

    # Write a manifest so the dashboard knows when data was last updated
    manifest = {
        'fetched_at': fetched_at,
        'symbols': SYMBOLS,
        'frequencies': FREQUENCIES,
        'ok': ok,
        'failed': failed,
    }
    manifest_path = os.path.join(OUTPUT_DIR, 'manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f'Manifest written to {manifest_path}')
    print(f'Done: {ok} succeeded, {failed} failed.')

    if failed > 0:
        print(f'WARNING: {failed} symbol(s) failed — check logs above.')
        print('Successfully fetched data will still be committed.')


if __name__ == '__main__':
    main()
