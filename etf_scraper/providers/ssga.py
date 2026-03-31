"""
SSGA (State Street) ETF shares outstanding scraper.
Fetches today's shares outstanding from SSGA fund pages.
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

# Ticker → SSGA fund page slug
SSGA_FUNDS: dict[str, str] = {
    "SPY":  "spdr-sp-500-etf-trust-spy",
    "GLD":  "spdr-gold-shares-gld",
    "GLDM": "spdr-gold-minishares-trust-gldm",
    "DIA":  "spdr-dow-jones-industrial-average-etf-trust-dia",
    "XLK":  "the-technology-select-sector-spdr-fund-xlk",
    "XLF":  "the-financial-select-sector-spdr-fund-xlf",
    "XLE":  "the-energy-select-sector-spdr-fund-xle",
    "XLV":  "the-health-care-select-sector-spdr-fund-xlv",
    "XLI":  "the-industrial-select-sector-spdr-fund-xli",
    "XLY":  "the-consumer-discretionary-select-sector-spdr-fund-xly",
    "XLP":  "the-consumer-staples-select-sector-spdr-fund-xlp",
    "XLRE": "the-real-estate-select-sector-spdr-fund-xlre",
    "XLB":  "the-materials-select-sector-spdr-fund-xlb",
    "XLU":  "the-utilities-select-sector-spdr-fund-xlu",
    "XLC":  "the-communication-services-select-sector-spdr-fund-xlc",
    "JETS": "us-global-jets-etf-jets",   # actually US Global, not SSGA — will fail gracefully
    "SRLN": "spdr-blackstone-senior-loan-etf-srln",
}

_BASE_URL = "https://www.ssga.com/us/en/intermediary/etfs/funds/{slug}"


def _parse_millions(text: str) -> int | None:
    """Parse '1,001.13 M' or '917.78 M' → shares as integer."""
    text = text.strip().replace(",", "")
    m = re.search(r"([\d.]+)\s*M", text, re.IGNORECASE)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    # Might be a plain number (no M suffix)
    m = re.search(r"[\d,]+", text)
    if m:
        return int(text.replace(",", "").split(".")[0])
    return None


def fetch_shares(ticker: str, session: requests.Session | None = None) -> int | None:
    """
    Fetch current shares outstanding for a SSGA ETF.
    Returns shares as integer, or None on failure.
    """
    slug = SSGA_FUNDS.get(ticker)
    if not slug:
        log.debug(f"ssga: {ticker} not in SSGA_FUNDS")
        return None

    url = _BASE_URL.format(slug=slug)
    sess = session or requests.Session()
    try:
        r = sess.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as exc:
        log.warning(f"ssga: {ticker} fetch error — {exc}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Find the <td> whose text is exactly "Shares Outstanding"
    label = soup.find(
        "td",
        string=lambda t: t and t.strip() == "Shares Outstanding",
    )
    if label is None:
        log.warning(f"ssga: {ticker} — 'Shares Outstanding' label not found in page")
        return None

    # The value is in the next <td> sibling within the same <tr>
    tr = label.parent
    tds = tr.find_all("td")
    if len(tds) < 2:
        log.warning(f"ssga: {ticker} — value TD not found")
        return None

    raw = tds[1].get_text(strip=True)
    shares = _parse_millions(raw)
    if shares:
        log.info(f"ssga: {ticker} shares outstanding = {shares:,}  (raw: {raw!r})")
    else:
        log.warning(f"ssga: {ticker} — could not parse {raw!r}")
    return shares


def fetch_all(tickers: list[str]) -> dict[str, int]:
    """Fetch shares outstanding for all SSGA tickers. Returns {ticker: shares}."""
    sess = requests.Session()
    results: dict[str, int] = {}
    for tk in tickers:
        if tk not in SSGA_FUNDS:
            continue
        sh = fetch_shares(tk, session=sess)
        if sh is not None:
            results[tk] = sh
    return results
