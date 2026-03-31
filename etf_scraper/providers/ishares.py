"""
iShares (BlackRock) ETF shares outstanding scraper.
Fetches today's shares outstanding from iShares fund pages.
"""

import logging
import re
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_BASE = "https://www.ishares.com/us/products"

# Ticker → (product_id, slug)
ISHARES_FUNDS: dict[str, tuple[str, str]] = {
    # US Equity
    "IWM":  ("239710", "ishares-russell-2000-etf"),
    "IVV":  ("239726", "ishares-core-sp-500-etf"),
    # International
    "EFA":  ("239623", "ishares-msci-eafe-etf"),
    "EEM":  ("239637", "ishares-msci-emerging-markets-etf"),
    "IEMG": ("244049", "ishares-core-msci-emerging-markets-etf"),
    "EWJ":  ("239665", "ishares-msci-japan-etf"),
    "FXI":  ("239536", "ishares-china-large-cap-etf"),
    "EWZ":  ("239612", "ishares-msci-brazil-etf"),
    "INDA": ("239758", "ishares-msci-india-etf"),
    # Fixed Income
    "TLT":  ("239454", "ishares-20-year-treasury-bond-etf"),
    "IEF":  ("239456", "ishares-7-10-year-treasury-bond-etf"),
    "SHY":  ("239452", "ishares-1-3-year-treasury-bond-etf"),
    "AGG":  ("239458", "ishares-core-total-us-bond-market-etf"),
    "HYG":  ("239565", "ishares-iboxx-high-yield-corporate-bond-etf"),
    "LQD":  ("239566", "ishares-iboxx-investment-grade-corporate-bond-etf"),
    "TIP":  ("239467", "ishares-tips-bond-etf"),
    "EMB":  ("239572", "ishares-jp-morgan-usd-emerging-markets-bond-etf"),
    # Commodities
    "SLV":  ("239855", "ishares-silver-trust"),
    "GSG":  ("239757", "ishares-sp-gsci-commodity-indexed-trust"),
    # Crypto
    "IBIT": ("333011", "ishares-bitcoin-trust-etf"),
    # Thematic
    "SOXX": ("239705", "ishares-phlx-semiconductor-etf"),
    "ICLN": ("239738", "ishares-global-clean-energy-etf"),
    "IBB":  ("239699", "ishares-nasdaq-biotechnology-etf"),
}


def _parse_shares(soup: BeautifulSoup) -> int | None:
    tag = soup.find(string=lambda t: t and t.strip() == "Shares Outstanding")
    if not tag:
        return None
    texts = [
        t.strip()
        for t in tag.parent.parent.get_text(separator="|").split("|")
        if t.strip()
    ]
    for part in texts:
        cleaned = part.replace(",", "")
        if re.match(r"^\d+$", cleaned):
            return int(cleaned)
    return None


def fetch_shares(ticker: str, session: requests.Session | None = None) -> int | None:
    """
    Fetch current shares outstanding for an iShares ETF.
    Returns shares as integer, or None on failure.
    """
    fund = ISHARES_FUNDS.get(ticker)
    if not fund:
        log.debug(f"ishares: {ticker} not in ISHARES_FUNDS")
        return None

    product_id, slug = fund
    url = f"{_BASE}/{product_id}/{slug}"
    sess = session or requests.Session()
    try:
        r = sess.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as exc:
        log.warning(f"ishares: {ticker} fetch error — {exc}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    shares = _parse_shares(soup)
    if shares:
        log.info(f"ishares: {ticker} shares outstanding = {shares:,}")
    else:
        log.warning(f"ishares: {ticker} — shares outstanding not found in page")
    return shares


def fetch_all(tickers: list[str]) -> dict[str, int]:
    """Fetch shares outstanding for all iShares tickers. Returns {ticker: shares}."""
    sess = requests.Session()
    results: dict[str, int] = {}
    for tk in tickers:
        if tk not in ISHARES_FUNDS:
            continue
        sh = fetch_shares(tk, session=sess)
        if sh is not None:
            results[tk] = sh
    return results
