# ETF Flows Data

Auto-updated ETF flows, shares outstanding, and AUM data.

Updated twice daily (00:00 and 12:00 UTC) via GitHub Actions.

## Data Files

| File | Description |
|------|-------------|
| `data/etf_flows.csv` | Daily net ETF flows in USD (date, ticker, flow_usd) |
| `data/etf_shares.csv` | Daily shares outstanding (date, ticker, shares, price) |
| `data/etf_aum.csv` | Latest AUM per ticker (ticker, aum_usd, updated_date) |

## Raw URLs (for dashboard)

```
https://raw.githubusercontent.com/isaergun/etf-flows-data/main/data/etf_flows.csv
https://raw.githubusercontent.com/isaergun/etf-flows-data/main/data/etf_shares.csv
https://raw.githubusercontent.com/isaergun/etf-flows-data/main/data/etf_aum.csv
```
