"""
====================================================================================================
ALGORITHM: src/discovery_probe.py — Multi-Exchange Futures Inception Timestamp Discovery Engine
====================================================================================================
Purpose:
  Reads the dynamic `"symbols"` list from `configs/config_phase2.json` and queries both Binance
  USD-M Perpetual Futures (`klines` and `premiumIndexKlines`) and Bybit Linear USDT Perpetuals
  (`kline`) to discover the exact earliest 1-minute candle timestamp for every coin. Computes the
  synchronized panel start timestamp `T_common` (the latest coin addition across both exchanges)
  and automatically saves the inception metadata back into `configs/config_phase2.json`.

Algorithm Steps:
  Step 1: Load Dynamic Configuration from `configs/config_phase2.json`.
  Step 2: Binance USD-M Futures Earliest 1m Timestamp Query (`startTime=0, limit=1`):
          - Probe `/fapi/v1/klines` and `/fapi/v1/premiumIndexKlines` for each symbol.
  Step 3: Bybit Linear USDT Perpetual Earliest 1m Timestamp Binary Search:
          - Because Bybit V5 returns klines newest-first within bounded windows, execute a 16-step
            binary search between 2019-01-01 and Present to locate the exact first 1m bar on Bybit.
  Step 4: Compute Per-Coin Cross-Exchange Start Date & Global Synchronized `T_common`:
          - For each coin: `coin_start = max(binance_kline_start, binance_premium_start, bybit_start)`.
          - Across all coins: `T_common = max(all coin_starts)`.
  Step 5: Persist Inception Map & `T_common` to `configs/config_phase2.json` & Print Audit Table.
====================================================================================================
"""

# =============================================================================
# STEP 1: Load Dynamic Configuration from `configs/config_phase2.json`
# =============================================================================
import os
import time
import json
from datetime import datetime, timezone
import requests
import pandas as pd

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs", "config_phase2.json")

def ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"[FATAL] Configuration file not found at: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


# =============================================================================
# STEP 2: Binance USD-M Futures Earliest 1m Timestamp Query
# =============================================================================
def get_binance_earliest_ms(session: requests.Session, symbol: str, endpoint: str, sleep_sec: float = 0.35) -> int:
    url = f"https://fapi.binance.com{endpoint}"
    params = {"symbol": symbol, "interval": "1m", "startTime": 0, "limit": 1}
    for attempt in range(4):
        try:
            resp = session.get(url, params=params, timeout=15)
            time.sleep(sleep_sec)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    return int(data[0][0])
                raise RuntimeError(f"Symbol {symbol} returned empty data on Binance {endpoint}")
            elif resp.status_code in (429, 418):
                time.sleep(5.0 * (attempt + 1))
        except Exception as e:
            if attempt == 3:
                raise RuntimeError(f"[Binance Probe Error] {symbol} on {endpoint}: {e}")
            time.sleep(2.0)
    raise RuntimeError(f"[Binance Probe Failed] Could not resolve {symbol} on {endpoint}")


# =============================================================================
# STEP 3: Bybit Linear USDT Perpetual Earliest 1m Timestamp Binary Search
# =============================================================================
def get_bybit_earliest_ms(session: requests.Session, symbol: str, sleep_sec: float = 0.25) -> int:
    """
    Uses a binary search in 1,000-minute windows between 2019-01-01 and Present to locate
    the exact earliest 1-minute Linear USDT Perpetual candle on Bybit V5 API.
    """
    url = "https://api.bybit.com/v5/market/kline"
    low_ms  = int(datetime(2019, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    high_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    window_ms = 1000 * 60 * 1000  # 1,000 minutes in ms

    # Step A: Coarse binary search at 1,000-minute resolution
    earliest_found_ms = high_ms
    while (high_ms - low_ms) > window_ms:
        mid_ms = (low_ms + high_ms) // 2
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": "1",
            "start": low_ms,
            "end": mid_ms,
            "limit": 1
        }
        resp = session.get(url, params=params, timeout=15)
        time.sleep(sleep_sec)
        data = resp.json()
        rows = data.get("result", {}).get("list", [])
        if rows:
            # Data exists at or before mid_ms! Search earlier half
            high_ms = mid_ms
        else:
            # No data before mid_ms! Search later half
            low_ms = mid_ms

    # Step B: Fetch the full 1,000-bar window around `high_ms` and take the oldest bar (`rows[-1]`)
    params_final = {
        "category": "linear",
        "symbol": symbol,
        "interval": "1",
        "start": max(0, low_ms - 60000),
        "end": high_ms + window_ms,
        "limit": 1000
    }
    resp_f = session.get(url, params=params_final, timeout=15)
    time.sleep(sleep_sec)
    rows_f = resp_f.json().get("result", {}).get("list", [])
    if not rows_f:
        raise RuntimeError(f"[Bybit Probe Error] Symbol {symbol} not found on Bybit Linear Perpetuals!")

    # Bybit returns descending order, so the last element is the earliest timestamp
    earliest_found_ms = int(rows_f[-1][0])
    return earliest_found_ms


# =============================================================================
# STEP 4 & 5: Compute Synchronized `T_common` & Persist to Config
# =============================================================================
def run_discovery_probe():
    cfg = load_config()
    symbols = cfg.get("symbols", [])
    rl = cfg.get("rate_limits", {})
    b_sleep = float(rl.get("binance_sleep_sec", 0.35))
    by_sleep = float(rl.get("bybit_sleep_sec", 0.25))

    print("=========================================================================================================")
    print("  PHASE 2 INCEPTION PROBE: DISCOVERING EARLIEST 1M FUTURES TIMESTAMPS (BINANCE & BYBIT)                  ")
    print(f"  Configured Symbols ({len(symbols)}) : {symbols}")
    print("=========================================================================================================\n")

    session = requests.Session()
    inception_map = {}
    table_rows = []

    for sym in symbols:
        print(f"Probing {sym:<10} across Binance Futures & Bybit Linear Perps...")
        bn_kline_ms = get_binance_earliest_ms(session, sym, "/fapi/v1/klines", sleep_sec=b_sleep)
        bn_prem_ms  = get_binance_earliest_ms(session, sym, "/fapi/v1/premiumIndexKlines", sleep_sec=b_sleep)
        by_kline_ms = get_bybit_earliest_ms(session, sym, sleep_sec=by_sleep)

        sym_sync_ms = max(bn_kline_ms, bn_prem_ms, by_kline_ms)
        bottleneck_source = (
            "Bybit_Perp" if sym_sync_ms == by_kline_ms
            else ("Binance_Premium" if sym_sync_ms == bn_prem_ms else "Binance_Kline")
        )

        inception_map[sym] = {
            "binance_kline_start_ms": bn_kline_ms,
            "binance_kline_start_utc": ms_to_utc_str(bn_kline_ms),
            "binance_premium_start_ms": bn_prem_ms,
            "binance_premium_start_utc": ms_to_utc_str(bn_prem_ms),
            "bybit_linear_start_ms": by_kline_ms,
            "bybit_linear_start_utc": ms_to_utc_str(by_kline_ms),
            "symbol_synchronized_start_ms": sym_sync_ms,
            "symbol_synchronized_start_utc": ms_to_utc_str(sym_sync_ms),
            "limiting_source": bottleneck_source
        }

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        years_avail = (now_ms - sym_sync_ms) / (1000.0 * 60 * 60 * 24 * 365.25)

        table_rows.append({
            "Symbol": sym,
            "Binance_1m_Start": ms_to_utc_str(bn_kline_ms)[:10],
            "Binance_Prem_Start": ms_to_utc_str(bn_prem_ms)[:10],
            "Bybit_1m_Start": ms_to_utc_str(by_kline_ms)[:10],
            "Synced_Start_UTC": ms_to_utc_str(sym_sync_ms),
            "Years_Available": round(years_avail, 2),
            "Limiting_Source": bottleneck_source
        })

    df_probe = pd.DataFrame(table_rows).sort_values("Synced_Start_UTC").reset_index(drop=True)

    # Global common start timestamp = latest addition among the configured symbols
    common_start_ms = max(v["symbol_synchronized_start_ms"] for v in inception_map.values())
    common_start_utc = ms_to_utc_str(common_start_ms)
    limiting_coin = max(inception_map.items(), key=lambda kv: kv[1]["symbol_synchronized_start_ms"])[0]
    total_common_years = (int(datetime.now(timezone.utc).timestamp() * 1000) - common_start_ms) / (1000.0 * 60 * 60 * 24 * 365.25)
    est_rows_per_coin = int((int(datetime.now(timezone.utc).timestamp() * 1000) - common_start_ms) // 60000)

    cfg["common_start_ms"] = common_start_ms
    cfg["common_start_utc"] = common_start_utc
    cfg["symbol_inception_map"] = inception_map

    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

    print("\n1. Per-Symbol Cross-Exchange Inception Audit:")
    print("-" * 105)
    print(df_probe.to_string(index=False))
    print("-" * 105)
    print(f"\n2. Global Synchronized Panel Start (`T_common` — Determined by Latest Coin `{limiting_coin}`):")
    print(f"   • Common Start Timestamp : {common_start_utc} (ms: {common_start_ms})")
    print(f"   • Synchronized Span      : {total_common_years:.2f} Years across all {len(symbols)} coins")
    print(f"   • Est. 1m Bars per Coin  : {est_rows_per_coin:,} rows ({est_rows_per_coin * len(symbols):,} total rows across {len(symbols)} coins)")
    print(f"   • Saved to Config        : {CONFIG_PATH}")
    print("=========================================================================================================")


if __name__ == "__main__":
    run_discovery_probe()
