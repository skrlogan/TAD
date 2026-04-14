"""
fetch_data.py
Fetches weekly and daily OHLCV data from Yahoo Finance via yfinance
for all watchlist symbols and saves them as JSON files under data/.

File naming: VOO weekly -> data/voo_w.json
             VOO daily  -> data/voo_d.json

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
SYMBOLS = [
    'VOO', 'VGT', 'QQQ', 'VXUS', 'VHT',
    'VFH', 'VDE', 'VNQ', 'VOX', 'VCR',
    'VDC', 'VIS', 'VAW', 'VPU',
]

FREQUENCIES = ['w', 'd']

YF_INTERVAL = {'w': '1wk', 'd': '1d'}
YF_PERIOD = '10y'
OUTPUT_DIR = 'data'


def sym_key(symbol):
    """VOO -> voo"""
    return symbol.lower()


def fetch_symbol(symbol, freq):
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
        if close is None or (close != close):
            continue
        close = float(close)
        if close <= 0:
            continue
        date_str = ts.strftime('%Y-%m-%d')
        date_key = date_str.replace('-', '')
        if prev_date and date_key <= prev_date:
            continue
        prev_date = date_key
        rows.append({'date': date_str, 'close': round(close, 6)})

    if len(rows) < 42:
        raise ValueError(f'Insufficient rows ({len(rows)}) for {symbol} ({freq})')

    return rows


def save(symbol, freq, rows, error=None):
    """Write result to data/<sym_key>_<freq>.json"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f'{sym_key(symbol)}_{freq}.json')
    payload = {
        'symbol': symbol,
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


def main():
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(f'Yahoo Finance fetch started at {fetched_at}')
    print(f'Symbols ({len(SYMBOLS)}): {", ".join(SYMBOLS)}')
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
            time.sleep(0.5)
        print()

    manifest = {
        'fetched_at': fetched_at,
        'symbols': SYMBOLS,
        'frequencies': FREQUENCIES,
        'ok': ok,
        'failed': failed,
    }
    with open(os.path.join(OUTPUT_DIR, 'manifest.json'), 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f'Done: {ok} succeeded, {failed} failed.')
    if failed == len(SYMBOLS) * len(FREQUENCIES):
        raise SystemExit('All fetches failed.')


if __name__ == '__main__':
    main()
