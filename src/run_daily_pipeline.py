"""
====================================================================================================
ALGORITHM: src/run_daily_pipeline.py — Unattended Daily 20-Coin Scraper, Validator & Kaggle Sync
====================================================================================================
Purpose:
  Master unattended orchestrator executed daily by `systemd` (`crypto_daily_sync.timer`).
  Automatically loads Kaggle credentials from `~/.kaggle/access_token`, probes any newly added
  symbols in `configs/config_phase2.json`, locks the 5-year anchor timestamp (`2021-10-15 07:52 UTC`),
  incrementally updates (or backfills) all 20 coins in `data_parquet/`, pushes the new dataset
  version directly to Kaggle Datasets, and executes a brief CPU matrix pulse to prevent Oracle
  Cloud Always-Free idle instance reclamation during long hiatus periods.

Algorithm Steps:
  Step 1: Resolve Paths & Auto-Inject `KAGGLE_API_TOKEN` from `~/.kaggle/access_token`.
  Step 2: Probe Any Newly Added Symbols & Lock `common_start_ms` to `2021-10-15 07:52:00 UTC`.
  Step 3: Execute Incremental / Backfill Scraper (`run_scraper`) Across All 20 Configured Coins.
  Step 4: Execute Direct Frankfurt-to-Kaggle Dataset Version Upload (`sync_to_kaggle`).
  Step 5: Execute 30-Second Anti-Idle Compute Pulse (Keeps OCI VM Metrics Active).
====================================================================================================
"""

# =============================================================================
# STEP 1: Resolve Paths & Auto-Inject Kaggle Credentials for Unattended Systemd
# =============================================================================
import os
import sys
import time
import json
from datetime import datetime, timezone
import numpy as np

SRC_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SRC_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

CONFIG_PATH = os.path.join(REPO_ROOT, "configs", "config_phase2.json")
ANCHOR_2021_MS  = 1634284320000
ANCHOR_2021_UTC = "2021-10-15 07:52:00 UTC"

def load_kaggle_auth_env():
    """Ensures KAGGLE_API_TOKEN is loaded into os.environ when running headless under systemd."""
    token_paths = [
        os.path.expanduser("~/.kaggle/access_token"),
        os.path.expanduser("~/.kaggle/kaggle.json")
    ]
    for t_path in token_paths:
        if os.path.exists(t_path):
            with open(t_path, "r") as f:
                raw = f.read().strip()
            if raw.startswith("{"):
                try:
                    d = json.loads(raw)
                    os.environ["KAGGLE_USERNAME"] = str(d.get("username", "")).strip()
                    os.environ["KAGGLE_KEY"] = str(d.get("key", "")).strip()
                except Exception:
                    pass
            elif raw:
                os.environ["KAGGLE_API_TOKEN"] = raw
                break


# =============================================================================
# STEP 2, 3, 4 & 5: Daily Orchestration Pipeline
# =============================================================================
def run_daily_orchestrator():
    t0 = time.time()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("=========================================================================================================")
    print(f"  UNATTENDED DAILY 20-COIN PIPELINE STARTED AT {now_str}")
    print("=========================================================================================================\n")

    # 1. Inject Kaggle Auth
    load_kaggle_auth_env()

    # 2. Check for unprobed symbols & preserve 2021-10-15 anchor
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    symbols = cfg.get("symbols", [])
    inc_map = cfg.get("symbol_inception_map", {})
    missing = [s for s in symbols if s not in inc_map]

    if missing:
        print(f"[Step 1] Discovering inception dates for {len(missing)} new symbol(s): {missing}...")
        from discovery_probe import run_discovery_probe
        run_discovery_probe()
        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)
        # Keep the 2021-10-15 anchor locked so existing 2021 coins are never truncated
        cfg["common_start_ms"]  = ANCHOR_2021_MS
        cfg["common_start_utc"] = ANCHOR_2021_UTC
        with open(CONFIG_PATH, "w") as f:
            json.dump(cfg, f, indent=2)

    # 3. Run Scraper (Incremental Top-Up for Existing Coins + Full Backfill for New Coins)
    print("\n[Step 2] Running Multi-Exchange 1-Minute Scraper & 7-Point Safety Validator...")
    from scrape_1m_binance_bybit import run_scraper
    run_scraper()

    # 4. Sync Updated Parquet Directory to Kaggle Datasets
    print("\n[Step 3] Publishing New Daily Dataset Version to Kaggle...")
    from kaggle_sync import sync_to_kaggle
    sync_to_kaggle()

    # 5. Brief 20-Second Anti-Idle Compute Pulse for Oracle Cloud VM Retention
    print("\n[Step 4] Running 20-second OCI anti-idle compute pulse...")
    t_pulse = time.time()
    while (time.time() - t_pulse) < 20.0:
        a = np.random.randn(600, 600)
        _ = np.dot(a, a)

    elapsed_mins = (time.time() - t0) / 60.0
    print("\n=========================================================================================================")
    print(f"  DAILY 20-COIN PIPELINE & KAGGLE SYNC COMPLETED IN {elapsed_mins:.2f} MINUTES")
    print("=========================================================================================================\n")


if __name__ == "__main__":
    run_daily_orchestrator()
