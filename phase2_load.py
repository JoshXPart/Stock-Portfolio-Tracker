"""
Phase 2 — Python loader
Creates portfolio.db (SQLite) and loads all three Phase 1 CSVs.
Then runs each analytics query and exports results to output/sql_exports/.

Usage:
    python phase2_load.py
"""

import sqlite3
import pandas as pd
import os

DB_PATH     = "output/portfolio.db"
OUTPUT_DIR  = "output/sql_exports"
CSV_PRICES  = "output/stock_prices.csv"
CSV_RETURNS = "output/stock_returns.csv"
CSV_SUMMARY = "output/stock_summary.csv"

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date  DATE        NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    close_price DECIMAL(10,4) NOT NULL,
    UNIQUE(trade_date, ticker)
);
CREATE TABLE IF NOT EXISTS stock_returns (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date   DATE        NOT NULL,
    ticker       VARCHAR(10) NOT NULL,
    daily_return DECIMAL(12,6),
    UNIQUE(trade_date, ticker)
);
CREATE TABLE IF NOT EXISTS portfolio_summary (
    ticker        VARCHAR(10) PRIMARY KEY,
    start_date    DATE,
    end_date      DATE,
    start_price   DECIMAL(10,4),
    end_price     DECIMAL(10,4),
    total_return  DECIMAL(10,6),
    annual_vol    DECIMAL(10,6),
    max_drawdown  DECIMAL(10,6),
    trading_days  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_prices_ticker  ON stock_prices  (ticker);
CREATE INDEX IF NOT EXISTS idx_prices_date    ON stock_prices  (trade_date);
CREATE INDEX IF NOT EXISTS idx_returns_ticker ON stock_returns (ticker);
CREATE INDEX IF NOT EXISTS idx_returns_date   ON stock_returns (trade_date);
"""

ANALYTICS_QUERIES = {
    "cumulative_returns": """
        SELECT trade_date, ticker,
               ROUND(EXP(SUM(LN(1.0 + daily_return))
                     OVER (PARTITION BY ticker ORDER BY trade_date)) - 1, 6)
                     AS cumulative_return
        FROM stock_returns
        WHERE daily_return IS NOT NULL
        ORDER BY ticker, trade_date
    """,
    "rolling_volatility_30d": """
        SELECT trade_date, ticker,
               ROUND(SQRT(AVG(daily_return*daily_return) OVER w
                         - (AVG(daily_return) OVER w)*(AVG(daily_return) OVER w))
                    * SQRT(252.0), 6) AS rolling_vol_30d
        FROM stock_returns
        WHERE daily_return IS NOT NULL
        WINDOW w AS (PARTITION BY ticker ORDER BY trade_date
                     ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        ORDER BY ticker, trade_date
    """,
    "drawdown": """
        WITH rm AS (
            SELECT trade_date, ticker, close_price,
                   MAX(close_price) OVER (PARTITION BY ticker ORDER BY trade_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peak_price
            FROM stock_prices
        )
        SELECT trade_date, ticker, close_price, peak_price,
               ROUND((close_price - peak_price)/peak_price, 6) AS drawdown
        FROM rm
        ORDER BY ticker, trade_date
    """,
    "monthly_returns": """
        SELECT STRFTIME('%Y-%m', trade_date) AS year_month, ticker,
               COUNT(*) AS trading_days,
               ROUND(AVG(daily_return), 6) AS avg_daily_return,
               ROUND(EXP(SUM(LN(1.0 + daily_return))) - 1, 6) AS compound_monthly_return
        FROM stock_returns
        WHERE daily_return IS NOT NULL
        GROUP BY year_month, ticker
        ORDER BY ticker, year_month
    """,
    "correlation_matrix": """
        WITH stats AS (
            SELECT ticker,
                   AVG(daily_return) AS mean_r,
                   SQRT(AVG(daily_return*daily_return)
                        - AVG(daily_return)*AVG(daily_return)) AS std_r
            FROM stock_returns WHERE daily_return IS NOT NULL GROUP BY ticker
        ),
        joined AS (
            SELECT a.trade_date, a.ticker AS ticker_a, a.daily_return AS r_a,
                   b.ticker AS ticker_b, b.daily_return AS r_b
            FROM stock_returns a
            JOIN stock_returns b ON a.trade_date=b.trade_date
                AND a.ticker<>b.ticker AND a.ticker<b.ticker
            WHERE a.daily_return IS NOT NULL AND b.daily_return IS NOT NULL
        )
        SELECT j.ticker_a, j.ticker_b,
               ROUND((AVG(j.r_a*j.r_b) - sa.mean_r*sb.mean_r)/(sa.std_r*sb.std_r), 4)
               AS correlation
        FROM joined j
        JOIN stats sa ON j.ticker_a=sa.ticker
        JOIN stats sb ON j.ticker_b=sb.ticker
        GROUP BY j.ticker_a, j.ticker_b
        ORDER BY j.ticker_a, j.ticker_b
    """,
    "annual_returns": """
        WITH yp AS (
            SELECT STRFTIME('%Y', trade_date) AS yr, ticker, close_price,
                   FIRST_VALUE(close_price) OVER (
                       PARTITION BY ticker, STRFTIME('%Y', trade_date)
                       ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   ) AS start_price,
                   LAST_VALUE(close_price) OVER (
                       PARTITION BY ticker, STRFTIME('%Y', trade_date)
                       ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                   ) AS end_price
            FROM stock_prices
        )
        SELECT DISTINCT yr, ticker,
               ROUND((end_price - start_price)/start_price, 4) AS annual_return
        FROM yp ORDER BY ticker, yr
    """,
    "best_worst_days": """
        SELECT ticker, 'Best'  AS type, trade_date, ROUND(daily_return, 6) AS daily_return
        FROM stock_returns
        WHERE (ticker, daily_return) IN (SELECT ticker, MAX(daily_return) FROM stock_returns GROUP BY ticker)
        UNION ALL
        SELECT ticker, 'Worst' AS type, trade_date, ROUND(daily_return, 6) AS daily_return
        FROM stock_returns
        WHERE (ticker, daily_return) IN (SELECT ticker, MIN(daily_return) FROM stock_returns GROUP BY ticker)
        ORDER BY ticker, type
    """,
}


def create_db(conn):
    conn.executescript(SCHEMA)
    conn.commit()
    print("  Schema created.")


def load_csvs(conn):
    prices  = pd.read_csv(CSV_PRICES,  parse_dates=["Date"])
    returns = pd.read_csv(CSV_RETURNS, parse_dates=["Date"])
    summary = pd.read_csv(CSV_SUMMARY)

    prices.rename(columns={"Date": "trade_date", "Close_Price": "close_price",
                            "Ticker": "ticker"}, inplace=True)
    returns.rename(columns={"Date": "trade_date", "Daily_Return": "daily_return",
                             "Ticker": "ticker"}, inplace=True)

    # Summary CSV column mapping
    summary.rename(columns={
        "Ticker":            "ticker",
        "Start Date":        "start_date",
        "End Date":          "end_date",
        "Start Price ($)":   "start_price",
        "End Price ($)":     "end_price",
        "Total Return":      "total_return",
        "Annual Volatility": "annual_vol",
        "Max Drawdown":      "max_drawdown",
        "Trading Days":      "trading_days",
    }, inplace=True)

    prices["trade_date"]  = prices["trade_date"].dt.strftime("%Y-%m-%d")
    returns["trade_date"] = returns["trade_date"].dt.strftime("%Y-%m-%d")

    prices  = prices[["trade_date", "ticker", "close_price"]]
    returns = returns[["trade_date", "ticker", "daily_return"]]
    summary = summary[["ticker","start_date","end_date","start_price","end_price",
                        "total_return","annual_vol","max_drawdown","trading_days"]]

    prices.to_sql( "stock_prices",     conn, if_exists="replace", index=False)
    returns.to_sql("stock_returns",    conn, if_exists="replace", index=False)
    summary.to_sql("portfolio_summary",conn, if_exists="replace", index=False)

    print(f"  Loaded {len(prices)} price rows, {len(returns)} return rows, "
          f"{len(summary)} summary rows.")


def run_analytics(conn):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for name, sql in ANALYTICS_QUERIES.items():
        df   = pd.read_sql_query(sql, conn)
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  {name}: {len(df)} rows → {path}")


def main():
    os.makedirs("output", exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    print(f"Creating {DB_PATH}...")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        create_db(conn)
        load_csvs(conn)
        print("\nRunning analytics queries...")
        run_analytics(conn)

    print(f"\n✅ Phase 2 complete.")
    print(f"   Database : {DB_PATH}")
    print(f"   Exports  : {OUTPUT_DIR}/")
    print("\nNext → Phase 3: Connect Tableau/Power BI to portfolio.db or the SQL export CSVs")


if __name__ == "__main__":
    main()