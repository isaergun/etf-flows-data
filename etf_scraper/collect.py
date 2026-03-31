"""
ETF Shares Outstanding Collector
---------------------------------
Fetches daily shares outstanding for all ETFs in the universe via yfinance,
then computes dollar flows: flow = Δshares × price.

Run manually:  python etf_scraper/collect.py
GitHub Actions: scheduled daily after market close.
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf  # used for price fetching

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ETF universe  (mirrors global_markets/config.py  ETF_UNIVERSE)
# ---------------------------------------------------------------------------
# Only ETFs with auto-scrapeable shares outstanding data (SSGA + iShares)
ETF_TICKERS: list[str] = [
    # US Equity — SSGA + iShares
    "SPY", "IWM", "IVV", "DIA",
    # US Equity — TradingView
    "QQQ", "VOO", "VTI",
    # International — iShares
    "EFA", "EEM", "IEMG", "EWJ", "FXI", "EWZ", "INDA",
    # International — TradingView
    "VEA", "VWO",
    # Fixed Income — iShares
    "TLT", "IEF", "SHY", "AGG", "HYG", "LQD", "TIP", "EMB",
    # Fixed Income — TradingView
    "BND", "BKLN", "JAAA",
    # Commodities — SSGA + iShares
    "GLD", "GLDM", "SLV", "GSG",
    # Commodities — TradingView
    "IAU",
    # Sector — SSGA
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLRE", "XLB", "XLU", "XLC",
    # Crypto — iShares
    "IBIT",
    # Private Credit — SSGA
    "SRLN",
    # Thematic — iShares
    "SOXX", "ICLN", "IBB",
    # Thematic — TradingView
    "ARKK", "GDX", "GDXJ",
]

REPO_ROOT        = Path(__file__).parent.parent
DATA_FILE        = REPO_ROOT / "data" / "etf_shares.csv"   # shares state (internal)
FLOWS_FILE       = REPO_ROOT / "data" / "etf_flows.csv"    # unified flow DB (dashboard reads this)
AUM_FILE         = REPO_ROOT / "data" / "etf_aum.csv"      # AUM snapshot (dashboard reads this)

# On first run, fetch this many calendar days of history
BOOTSTRAP_DAYS = 400   # covers full YTD + some buffer
# On incremental runs, overlap this many days to catch late-arriving data
OVERLAP_DAYS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_existing() -> pd.DataFrame:
    if DATA_FILE.exists():
        df = pd.read_csv(DATA_FILE, parse_dates=["date"])
        log.info(f"Loaded existing CSV: {len(df):,} rows, "
                 f"{df['ticker'].nunique()} tickers, "
                 f"latest {df['date'].max().date()}")
        return df
    log.info("No existing CSV — will bootstrap from scratch")
    return pd.DataFrame(columns=["date", "ticker", "shares", "price"])


def fetch_shares_today(tickers: list[str]) -> pd.DataFrame:
    """
    Scrape today's shares outstanding from SSGA, iShares, and TradingView.
    Returns DataFrame[date, ticker, shares].
    """
    from providers import ssga, ishares, tradingview

    ssga_tickers   = [tk for tk in tickers if tk in ssga.SSGA_FUNDS]
    ishares_tickers = [tk for tk in tickers if tk in ishares.ISHARES_FUNDS]
    tv_tickers      = [tk for tk in tickers if tk in tradingview.TRADINGVIEW_FUNDS]

    log.info(f"  SSGA: {len(ssga_tickers)}, iShares: {len(ishares_tickers)}, TradingView: {len(tv_tickers)} tickers")

    today = pd.Timestamp.today().normalize()
    records: list[dict] = []

    for tk, sh in ssga.fetch_all(ssga_tickers).items():
        records.append({"date": today, "ticker": tk, "shares": sh})

    for tk, sh in ishares.fetch_all(ishares_tickers).items():
        records.append({"date": today, "ticker": tk, "shares": sh})

    for tk, sh in tradingview.fetch_all(tv_tickers).items():
        records.append({"date": today, "ticker": tk, "shares": sh})

    if not records:
        return pd.DataFrame(columns=["date", "ticker", "shares"])
    return pd.DataFrame(records)


def fetch_prices(tickers: list[str], start: date) -> pd.DataFrame:
    """
    Batch-download daily close prices for tickers.
    Returns DataFrame[date, ticker, price].
    """
    if not tickers:
        return pd.DataFrame(columns=["date", "ticker", "price"])
    try:
        raw = yf.download(
            tickers, start=str(start), auto_adjust=True,
            progress=False, threads=True,
        )
        if raw.empty:
            return pd.DataFrame(columns=["date", "ticker", "price"])

        close = raw["Close"] if "Close" in raw.columns.get_level_values(0) else raw
        if isinstance(close, pd.Series):
            close = close.to_frame(name=tickers[0])

        records = []
        for dt, row in close.iterrows():
            for tk in tickers:
                if tk in row.index and not pd.isna(row[tk]):
                    records.append({"date": pd.Timestamp(dt).normalize(),
                                    "ticker": tk,
                                    "price": float(row[tk])})
        return pd.DataFrame(records)
    except Exception as exc:
        log.warning(f"Price fetch error: {exc}")
        return pd.DataFrame(columns=["date", "ticker", "price"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def compute_flows(shares_state: pd.DataFrame, today_shares: pd.DataFrame) -> pd.DataFrame:
    """
    Compute today's dollar flows: flow = Δshares × price.
    Returns DataFrame[date, ticker, flow_usd].
    """
    rows = []
    for _, row in today_shares.iterrows():
        tk    = row["ticker"]
        today = pd.Timestamp(row["date"]).normalize()
        price = row.get("price")

        # No price → store NaN (couldn't compute)
        if pd.isna(price) if price is not None else True:
            rows.append({"date": today, "ticker": tk, "flow_usd": float("nan")})
            log.debug(f"  {tk}: no price — storing NaN")
            continue

        # No previous shares → store NaN (first day, no baseline)
        prev = shares_state[shares_state["ticker"] == tk].sort_values("date")
        prev = prev[prev["date"] < today]
        if prev.empty:
            rows.append({"date": today, "ticker": tk, "flow_usd": float("nan")})
            log.debug(f"  {tk}: no previous shares — storing NaN")
            continue

        prev_shares = int(prev.iloc[-1]["shares"])
        curr_shares = int(row["shares"])
        delta       = curr_shares - prev_shares
        flow_usd    = delta * float(price)

        rows.append({"date": today, "ticker": tk, "flow_usd": flow_usd})

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["date", "ticker", "flow_usd"])


def compute_aum(today_with_price: pd.DataFrame) -> pd.DataFrame:
    """
    Compute AUM = shares_outstanding × price for each ticker.
    Uses today's scraped shares and latest available price.
    Returns DataFrame[ticker, aum_usd, updated_date].
    """
    today = pd.Timestamp.today().normalize()
    records = []
    for _, row in today_with_price.iterrows():
        shares = row.get("shares")
        price  = row.get("price")
        if pd.notna(shares) and pd.notna(price) and price > 0:
            aum = float(shares) * float(price)
            records.append({"ticker": row["ticker"], "aum_usd": aum, "updated_date": today})
            log.info(f"  AUM {row['ticker']}: ${aum/1e9:.2f}B")
    return pd.DataFrame(records) if records else pd.DataFrame(columns=["ticker", "aum_usd", "updated_date"])


def main() -> None:
    existing = load_existing()   # etf_shares.csv (state)

    log.info(f"Scraping today's shares outstanding for {len(ETF_TICKERS)} tickers …")
    shares_df = fetch_shares_today(ETF_TICKERS)

    if shares_df.empty:
        log.warning("No shares data returned — nothing to save")
        sys.exit(0)

    active_tickers = shares_df["ticker"].unique().tolist()
    log.info(f"Fetching prices for {len(active_tickers)} tickers …")
    prices_df = fetch_prices(active_tickers, date.today() - timedelta(days=5))

    # Build today's row: shares + price
    today_with_price = shares_df.merge(prices_df, on=["date", "ticker"], how="left")

    # ── 1. Update shares state ────────────────────────────────────────────────
    combined_state = pd.concat([existing, today_with_price], ignore_index=True)
    combined_state["date"] = pd.to_datetime(combined_state["date"]).dt.normalize()
    combined_state = (combined_state
                      .drop_duplicates(subset=["date", "ticker"], keep="last")
                      .sort_values(["ticker", "date"])
                      .reset_index(drop=True))
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    combined_state.to_csv(DATA_FILE, index=False, date_format="%Y-%m-%d")
    log.info(f"Shares state: {len(combined_state):,} rows → {DATA_FILE}")

    # ── 2. Compute flows and append to etf_flows.csv ─────────────────────────
    new_flows = compute_flows(existing, today_with_price)
    if new_flows.empty:
        log.info("No new flows computed (first run or no previous state)")
    else:
        flows_existing = (pd.read_csv(FLOWS_FILE, parse_dates=["date"])
                          if FLOWS_FILE.exists()
                          else pd.DataFrame(columns=["date", "ticker", "flow_usd"]))

        combined_flows = pd.concat([flows_existing, new_flows], ignore_index=True)
        combined_flows["date"] = pd.to_datetime(combined_flows["date"]).dt.normalize()
        combined_flows = (combined_flows
                          .drop_duplicates(subset=["date", "ticker"], keep="last")
                          .sort_values(["ticker", "date"])
                          .reset_index(drop=True))
        combined_flows.to_csv(FLOWS_FILE, index=False, date_format="%Y-%m-%d")

        log.info(f"Flows: +{len(new_flows)} new rows → {FLOWS_FILE}")
        for _, r in new_flows.iterrows():
            sign = "+" if r["flow_usd"] >= 0 else ""
            log.info(f"  {r['ticker']:6s}: {sign}{r['flow_usd']/1e6:.1f}M")

    # ── 3. Compute and save AUM snapshot (shares × latest price) ──────────────
    log.info("Computing AUM from shares × price …")
    # Use latest available price per ticker (independent of date match)
    latest_prices = (prices_df.sort_values("date")
                     .groupby("ticker")["price"].last()
                     .reset_index())
    aum_input = shares_df.merge(latest_prices, on="ticker", how="left")
    aum_df = compute_aum(aum_input)
    if not aum_df.empty:
        aum_df.to_csv(AUM_FILE, index=False, date_format="%Y-%m-%d")
        log.info(f"AUM: {len(aum_df)} tickers → {AUM_FILE}")


if __name__ == "__main__":
    main()
