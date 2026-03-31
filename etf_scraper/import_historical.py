"""
Import historical ETF flow data from etf.com CSV exports.

CSV format (etf.com export):
    "Date","TICKER Net Flows","TICKER2 Net Flows","Aggregate Net Flows"
    "01/02/2026","522.05","-238.63","283.42"

Values are in million USD. We convert to USD.
Date format: MM/DD/YYYY

Output: data/etf_flows.csv  (date, ticker, flow_usd)
"""

import logging
import re
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

REPO_ROOT   = Path(__file__).parent.parent
SOURCE_DIR  = REPO_ROOT / "global_markets" / "data" / "etf flows"
OUTPUT_FILE = REPO_ROOT / "data" / "etf_flows.csv"


def parse_etf_csv(path: Path) -> pd.DataFrame:
    """Parse one etf.com export CSV → long-form DataFrame[date, ticker, flow_usd]."""
    df = pd.read_csv(path)

    # Identify ticker columns  (e.g. "QQQ Net Flows")
    flow_cols = [c for c in df.columns if c.strip().endswith("Net Flows")
                 and c.strip() != "Aggregate Net Flows"]

    rows = []
    for _, row in df.iterrows():
        # Parse date  MM/DD/YYYY
        try:
            dt = pd.to_datetime(row["Date"], format="%m/%d/%Y")
        except Exception:
            continue

        for col in flow_cols:
            ticker = col.replace(" Net Flows", "").strip()
            try:
                flow_m = float(str(row[col]).replace(",", ""))
            except (ValueError, TypeError):
                continue
            if flow_m == 0 and dt.day == 1 and dt.month == 1:
                continue  # skip Jan 1 zero entries (market closed)
            rows.append({
                "date":     dt.normalize(),
                "ticker":   ticker,
                "flow_usd": flow_m * 1_000_000,   # million → USD
            })

    return pd.DataFrame(rows)


def load_existing() -> pd.DataFrame:
    if OUTPUT_FILE.exists():
        df = pd.read_csv(OUTPUT_FILE, parse_dates=["date"])
        log.info(f"Existing etf_flows.csv: {len(df):,} rows")
        return df
    return pd.DataFrame(columns=["date", "ticker", "flow_usd"])


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    csv_files = sorted(SOURCE_DIR.glob("etf-fund-flow-output-*.csv"))
    if not csv_files:
        log.error(f"No CSV files found in {SOURCE_DIR}")
        return

    log.info(f"Found {len(csv_files)} source files")

    frames = []
    for f in csv_files:
        log.info(f"  Parsing {f.name} …")
        frames.append(parse_etf_csv(f))

    new_data = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    log.info(f"Parsed {len(new_data):,} rows from source files "
             f"({new_data['ticker'].nunique()} tickers)")

    existing = load_existing()
    combined = pd.concat([existing, new_data], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.normalize()
    combined = (combined
                .drop_duplicates(subset=["date", "ticker"], keep="last")
                .sort_values(["ticker", "date"])
                .reset_index(drop=True))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_FILE, index=False, date_format="%Y-%m-%d")
    log.info(f"Saved {len(combined):,} rows → {OUTPUT_FILE}")

    # Summary
    print("\nTickers imported:")
    for tk, grp in combined.groupby("ticker"):
        print(f"  {tk:8s}: {len(grp):3d} days  "
              f"{grp['date'].min().date()} → {grp['date'].max().date()}  "
              f"YTD flow: ${grp['flow_usd'].sum()/1e9:.2f}B")


if __name__ == "__main__":
    main()
