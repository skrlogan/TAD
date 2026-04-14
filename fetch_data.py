"""
fetch_data.py
Fetches weekly and daily OHLCV data from Yahoo Finance via yfinance
for all watchlist symbols and saves them as JSON files under data/.

Run by GitHub Actions on schedule, or manually:
  pip install yfinance
  python fetch_data.py
"""

import json
import os
import time
from datetime import datetime, timezone

import yfinance as yf

# ── Watchlist ────────────────────────────────────────────────────────────────
# Yahoo Finance symbols (no suffix needed for US ETFs)
SYMBOLS = [
    'VOO', 'VGT', 'QQQ', 'VXUS', 'VHT',
    'VFH', 'VDE', 'VNQ', 'VOX', 'VCR',
    'VDC', 'VIS', 'VAW', 'VPU',
]

# Stooq symbol → Yahoo symbol mapping (for dashboard compatibility)
SYMBOL_MAP = {
    'voo.us':  'VOO',  'vgt.us':  'VGT',  'qqq.us':  'QQQ',
    'vxus.us': 'VXUS', 'vht.us':  'VHT',  'vfh.us':  'VFH',
    'vde.us':  'VDE',  'vnq.us':  'VNQ',  'vox.us':  'VOX',
    'vcr.us':  'VCR',  'vdc.us':  'VDC',  'vis.us':  'VIS',
    'vaw.us':  'VAW',  'vpu.us':  'VPU',
}
# Reverse map: Yahoo → Stooq-style key used in filenames
REVERSE_MAP = {v: k for k, v in SYMBOL_MAP.items()}

FREQUENCIES = ['w', 'd']   # weekly and daily

# yfinance interval strings
YF_INTERVAL = {'w': '1wk', 'd': '1d'}

# How many bars to fetch (yfinance uses period or start date)
YF_PERIOD = '10y'

OUTPUT_DIR = 'data'

# ── Helpers ──────────────────────────────────────────────────────────────────

def fetch_symbol(symbol: str, freq: str) -> list[dict]:
    """Fetch OHLCV from Yahoo Finance. Returns list of {date, close} dicts."""
    interval = YF_INTERVAL[freq]
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=YF_PERIOD, interval=interval, auto_adjust=True)

    if df is None or df.empty:
        raise ValueError(f'No data returned from Yahoo Finance for {symbol} ({freq})')

    rows = []
    prev_date = None

    for ts, row in df.iterrows():
        close = row['Close']
        if close is None or (hasattr(close, '__float__') and close != close):
            continue  # skip NaN
        close = float(close)
        if close <= 0:
            continue

        # Normalise date to YYYY-MM-DD string
        date_str = ts.strftime('%Y-%m-%d')
        date_key = date_str.replace('-', '')

        if prev_date and date_key <= prev_date:
            continue  # skip non-increasing (e.g. duplicates at week boundaries)
        prev_date = date_key

        rows.append({'date': date_str, 'close': round(close, 6)})

    if len(rows) < 42:
        raise ValueError(f'Insufficient rows ({len(rows)}) for {symbol} ({freq})')

    return rows


def stooq_key(yahoo_symbol: str) -> str:
    """Return the stooq-style symbol key for filenames, e.g. VOO -> voo_us"""
    stooq = REVERSE_MAP.get(yahoo_symbol, yahoo_symbol.lower() + '.us')
    return stooq.replace('.', '_')


def save(yahoo_symbol: str, freq: str, rows: list[dict], error: str | None = None):
    """Write result JSON to data/<stooq_key>_<freq>.json"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    key = stooq_key(yahoo_symbol)
    filename = f'{key}_{freq}.json'
    path = os.path.join(OUTPUT_DIR, filename)

    # Keep stooq-style symbol in the JSON so the dashboard recognises it
    stooq_sym = REVERSE_MAP.get(yahoo_symbol, yahoo_symbol.lower() + '.us')

    payload = {
        'symbol': stooq_sym,
        'freq': freq,
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'rows': rows,
        'error': error,
    }

    with open(path, 'w') as f:
        json.dump(payload, f, separators=(',', ':'))

    if rows:
        print(f'  Saved {path} ({len(rows)} rows, latest: {rows[-1]["date"]})')
    else:
        print(f'  Saved {path} (empty — {error})')


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(f'Yahoo Finance fetch started at {fetched_at}')
    print(f'Symbols: {SYMBOLS}')
    print(f'Frequencies: {FREQUENCIES}')
    print()

    ok = 0
    failed = 0

    for symbol in SYMBOLS:
        for freq in FREQUENCIES:
            label = f'{symbol} ({freq})'
            try:
                rows = fetch_symbol(symbol, freq)
                save(symbol, freq, rows)
                print(f'  ✓ {label}')
                ok += 1
            except Exception as e:
                print(f'  ✗ {label} — {e}')
                save(symbol, freq, [], error=str(e))
                failed += 1

            # Small delay to be polite
            time.sleep(0.5)

        print()

    # Write manifest
    manifest = {
        'fetched_at': fetched_at,
        'symbols': [REVERSE_MAP.get(s, s.lower() + '.us') for s in SYMBOLS],
        'frequencies': FREQUENCIES,
        'ok': ok,
        'failed': failed,
    }
    manifest_path = os.path.join(OUTPUT_DIR, 'manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f'Manifest written to {manifest_path}')
    print(f'Done: {ok} succeeded, {failed} failed.')

    if failed == len(SYMBOLS) * len(FREQUENCIES):
        # Only hard-fail if everything failed
        raise SystemExit('All fetches failed.')


if __name__ == '__main__':
    main()
