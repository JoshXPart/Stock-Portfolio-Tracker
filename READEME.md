# Stock Portfolio Performance Tracker

A end-to-end data analytics project tracking historical stock performance across a 5-stock portfolio using Python, SQL, and Power BI.

## Tools & Technologies
- **Python** — yfinance, pandas, openpyxl
- **SQL** — SQLite (schema design, window functions, CTEs)
- **Power BI** — interactive 3-page dashboard with DAX measures
- **Excel** — formatted workbook output with conditional formatting

---

## Project Structure

```
Stock-Portfolio-Tracker/
├── phase1_fetch_stock_data.py   # Pull historical data via yfinance → Excel + CSV
├── phase2_load.py               # Build SQLite database + run analytics queries
├── phase2_sql.sql               # SQL schema and all analytics queries (documented)
├── output/
│   ├── portfolio.db             # SQLite database
│   ├── portfolio_phase1.xlsx    # Formatted Excel workbook
│   ├── stock_prices.csv
│   ├── stock_returns.csv
│   ├── stock_summary.csv
│   └── sql_exports/
│       ├── cumulative_returns.csv
│       ├── rolling_volatility_30d.csv
│       ├── drawdown.csv
│       ├── monthly_returns.csv
│       ├── correlation_matrix.csv
│       ├── annual_returns.csv
│       └── best_worst_days.csv
└── Stock_Portfolio_Tracker.pbix # Power BI dashboard
```

---

## Phase 1 — Data Collection (Python)

Pulls 4+ years of daily adjusted closing prices (2022–2026) for 5 tickers: **AAPL, MSFT, GOOGL, AMZN, NVDA** using the `yfinance` library.

**Outputs:**
- Long-format CSVs ready for SQL ingestion
- Formatted Excel workbook with 3 sheets: Summary Stats, Daily Prices (color-coded), Daily Returns (green/red conditional formatting)

**Key metrics computed:**
- Total return per ticker
- Annualised volatility
- Maximum drawdown

---

## Phase 2 — SQL Analytics

Builds a normalised SQLite database and runs 7 analytics queries using window functions and CTEs.

**Schema:**
```sql
stock_prices      -- trade_date, ticker, close_price
stock_returns     -- trade_date, ticker, daily_return
portfolio_summary -- one row per ticker with summary stats
```

**Analytics queries:**
| Query | Technique |
|---|---|
| Cumulative returns | `EXP(SUM(LN(...)))` window function |
| 30-day rolling volatility | `ROWS BETWEEN 29 PRECEDING` window |
| Drawdown analysis | Running max with CTE |
| Monthly aggregated returns | `GROUP BY` with compound formula |
| Pearson correlation matrix | Manual formula with self-join |
| Year-over-year annual returns | `FIRST_VALUE / LAST_VALUE` window |
| Best/worst single-day returns | `MIN/MAX` with `UNION ALL` |

---

## Phase 3 — Power BI Dashboard

Interactive 3-page report connected to `portfolio.db` via ODBC.

**Page 1 — Performance Overview**
- Cumulative returns line chart (2022–2026)
- KPI cards with DAX measures showing % return per ticker
- Ticker slicer for interactive filtering

**Page 2 — Risk Analysis**
- 30-day rolling volatility line chart
- Drawdown area chart showing peak-to-trough declines

**Page 3 — Monthly & Annual Breakdown**
- Monthly returns clustered bar chart
- Annual returns heatmap matrix (red = negative, green = positive)

---

## How to Run

**Prerequisites:**
```bash
pip install yfinance pandas openpyxl
```

**Phase 1 — Fetch data:**
```bash
python phase1_fetch_stock_data.py
```

**Phase 2 — Build database and run analytics:**
```bash
python phase2_load.py
```

**Phase 3 — Open dashboard:**
- Install [Power BI Desktop](https://powerbi.microsoft.com/desktop) (free)
- Install [SQLite ODBC Driver](http://www.ch-werner.de/sqliteodbc/)
- Open `Stock_Portfolio_Tracker.pbix`

---

## Key Findings

- **NVDA** delivered the highest total return over the period (+527%) driven by AI demand, with the highest volatility of all 5 stocks
- **2022** was a negative year across all tickers, with NVDA and AMZN both declining ~50%
- **MSFT** showed the most consistent risk-adjusted performance with lower drawdowns relative to its return
- Correlation analysis shows all 5 stocks are positively correlated, with AAPL/MSFT being the most closely correlated pair
- 
