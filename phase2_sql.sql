-- =============================================================
-- Phase 2 — Stock Portfolio Performance Tracker
-- SQL Schema + Analytics Queries
-- Compatible with: SQLite, PostgreSQL, SQL Server (notes inline)
-- =============================================================


-- =============================================================
-- SECTION 1: SCHEMA
-- =============================================================

-- Drop tables if re-running from scratch
DROP TABLE IF EXISTS stock_prices;
DROP TABLE IF EXISTS stock_returns;
DROP TABLE IF EXISTS portfolio_summary;


-- Table 1: Daily closing prices (long format from Phase 1)
CREATE TABLE stock_prices (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,  -- Use SERIAL in PostgreSQL
    trade_date  DATE        NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    close_price DECIMAL(10, 4) NOT NULL,
    UNIQUE (trade_date, ticker)
);

-- Table 2: Daily returns (long format from Phase 1)
CREATE TABLE stock_returns (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date   DATE        NOT NULL,
    ticker       VARCHAR(10) NOT NULL,
    daily_return DECIMAL(12, 6),              -- NULL allowed for first trading day
    UNIQUE (trade_date, ticker)
);

-- Table 3: Per-ticker summary statistics
CREATE TABLE portfolio_summary (
    ticker           VARCHAR(10)    PRIMARY KEY,
    start_date       DATE,
    end_date         DATE,
    start_price      DECIMAL(10, 4),
    end_price        DECIMAL(10, 4),
    total_return     DECIMAL(10, 6),
    annual_vol       DECIMAL(10, 6),
    max_drawdown     DECIMAL(10, 6),
    trading_days     INTEGER
);

-- Indexes for query performance
CREATE INDEX idx_prices_ticker     ON stock_prices  (ticker);
CREATE INDEX idx_prices_date       ON stock_prices  (trade_date);
CREATE INDEX idx_returns_ticker    ON stock_returns (ticker);
CREATE INDEX idx_returns_date      ON stock_returns (trade_date);


-- =============================================================
-- SECTION 2: LOAD DATA
-- =============================================================

-- SQLite: use the .import command in the CLI, or load via Python (see phase2_load.py)
--
--   .mode csv
--   .headers on
--   .import output/stock_prices.csv  stock_prices_staging
--   .import output/stock_returns.csv stock_returns_staging
--   .import output/stock_summary.csv portfolio_summary_staging
--
-- PostgreSQL: use COPY
--
--   COPY stock_prices  (trade_date, ticker, close_price)
--     FROM '/path/to/stock_prices.csv'  CSV HEADER;
--
--   COPY stock_returns (trade_date, ticker, daily_return)
--     FROM '/path/to/stock_returns.csv' CSV HEADER;
--
--   COPY portfolio_summary (ticker, start_date, end_date, start_price, end_price,
--                           total_return, annual_vol, max_drawdown, trading_days)
--     FROM '/path/to/stock_summary.csv' CSV HEADER;
--
-- See phase2_load.py for the fully automated Python loader (recommended).


-- =============================================================
-- SECTION 3: VALIDATION QUERIES
-- Run these after loading to confirm data integrity
-- =============================================================

-- Row counts per ticker
SELECT ticker, COUNT(*) AS trading_days
FROM stock_prices
GROUP BY ticker
ORDER BY ticker;

-- Date range check
SELECT
    ticker,
    MIN(trade_date) AS first_date,
    MAX(trade_date) AS last_date
FROM stock_prices
GROUP BY ticker;

-- Check for missing return rows (should match prices - 1 per ticker)
SELECT
    p.ticker,
    COUNT(DISTINCT p.trade_date)               AS price_rows,
    COUNT(DISTINCT r.trade_date)               AS return_rows,
    COUNT(DISTINCT p.trade_date) - 1
      - COUNT(DISTINCT r.trade_date)           AS missing
FROM stock_prices  p
LEFT JOIN stock_returns r
    ON p.ticker = r.ticker AND p.trade_date = r.trade_date
GROUP BY p.ticker;


-- =============================================================
-- SECTION 4: ANALYTICS QUERIES
-- =============================================================

-- ── Query 1: Cumulative Return ─────────────────────────────────────────────
-- Running product of (1 + daily_return) per ticker over time.
-- Window function approach: portable across SQLite ≥ 3.25, PostgreSQL, SQL Server.

SELECT
    trade_date,
    ticker,
    daily_return,
    ROUND(
        EXP(SUM(LN(1.0 + daily_return))
            OVER (PARTITION BY ticker ORDER BY trade_date)) - 1,
        6
    ) AS cumulative_return
FROM stock_returns
WHERE daily_return IS NOT NULL
ORDER BY ticker, trade_date;

-- SQLite note: LN() is available natively. If using an older version replace with:
--   EXP(SUM(LOG(1.0 + daily_return)) OVER (...))
-- SQL Server: use LOG() instead of LN().


-- ── Query 2: 30-Day Rolling Volatility ────────────────────────────────────
-- Annualised standard deviation of daily returns over a 30-day rolling window.

SELECT
    trade_date,
    ticker,
    ROUND(
        -- Annualise: daily stdev × sqrt(252)
        -- SQLite has no STDDEV; use the manual variance formula below.
        -- PostgreSQL/SQL Server: replace with STDDEV(daily_return) OVER (...)
        SQRT(
            AVG(daily_return * daily_return) OVER w
            - (AVG(daily_return) OVER w) * (AVG(daily_return) OVER w)
        ) * SQRT(252.0),
        6
    ) AS rolling_vol_30d
FROM stock_returns
WHERE daily_return IS NOT NULL
WINDOW w AS (
    PARTITION BY ticker
    ORDER BY trade_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
)
ORDER BY ticker, trade_date;


-- ── Query 3: Drawdown Analysis ─────────────────────────────────────────────
-- Rolling max close price and drawdown from peak for every trading day.

WITH running_max AS (
    SELECT
        trade_date,
        ticker,
        close_price,
        MAX(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_price
    FROM stock_prices
)
SELECT
    trade_date,
    ticker,
    close_price,
    peak_price,
    ROUND((close_price - peak_price) / peak_price, 6) AS drawdown
FROM running_max
ORDER BY ticker, trade_date;


-- ── Query 4: Worst Drawdown Periods (per ticker) ───────────────────────────
-- Finds the single largest drawdown value and the date it occurred.

WITH running_max AS (
    SELECT
        trade_date,
        ticker,
        close_price,
        MAX(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_price
    FROM stock_prices
),
drawdowns AS (
    SELECT
        trade_date,
        ticker,
        ROUND((close_price - peak_price) / peak_price, 6) AS drawdown
    FROM running_max
)
SELECT
    ticker,
    MIN(drawdown)                                  AS max_drawdown,
    MAX(trade_date) FILTER (WHERE drawdown = MIN(drawdown) OVER (PARTITION BY ticker))
                                                   AS trough_date
FROM drawdowns
GROUP BY ticker
ORDER BY max_drawdown;

-- SQL Server / older SQLite: replace FILTER (...) with a subquery or CASE expression.


-- ── Query 5: Monthly Aggregated Returns ───────────────────────────────────
-- Compound monthly return, average daily return, and monthly volatility.

SELECT
    STRFTIME('%Y-%m', trade_date)  AS year_month,   -- PostgreSQL: TO_CHAR(trade_date,'YYYY-MM')
    ticker,
    COUNT(*)                       AS trading_days,
    ROUND(AVG(daily_return), 6)    AS avg_daily_return,
    ROUND(
        EXP(SUM(LN(1.0 + daily_return))) - 1,
        6
    )                              AS compound_monthly_return,
    ROUND(
        SQRT(
            AVG(daily_return * daily_return)
            - AVG(daily_return) * AVG(daily_return)
        ) * SQRT(252.0),
        6
    )                              AS monthly_vol_annualised
FROM stock_returns
WHERE daily_return IS NOT NULL
GROUP BY year_month, ticker
ORDER BY ticker, year_month;


-- ── Query 6: Pearson Correlation Matrix ───────────────────────────────────
-- Cross-ticker correlation of daily returns. Pairs each ticker against every other.
-- Uses the manual Pearson formula (portable; no CORR() needed in SQLite).

WITH stats AS (
    SELECT
        ticker,
        AVG(daily_return)                                          AS mean_r,
        SQRT(AVG(daily_return * daily_return)
             - AVG(daily_return) * AVG(daily_return))              AS std_r
    FROM stock_returns
    WHERE daily_return IS NOT NULL
    GROUP BY ticker
),
joined AS (
    SELECT
        a.trade_date,
        a.ticker       AS ticker_a,
        a.daily_return AS r_a,
        b.ticker       AS ticker_b,
        b.daily_return AS r_b
    FROM stock_returns a
    JOIN stock_returns b
        ON  a.trade_date   = b.trade_date
        AND a.ticker      <> b.ticker
        AND a.ticker       < b.ticker        -- avoid duplicates
    WHERE a.daily_return IS NOT NULL
      AND b.daily_return IS NOT NULL
)
SELECT
    j.ticker_a,
    j.ticker_b,
    ROUND(
        (AVG(j.r_a * j.r_b) - sa.mean_r * sb.mean_r)
        / (sa.std_r * sb.std_r),
        4
    ) AS correlation
FROM joined j
JOIN stats sa ON j.ticker_a = sa.ticker
JOIN stats sb ON j.ticker_b = sb.ticker
GROUP BY j.ticker_a, j.ticker_b
ORDER BY j.ticker_a, j.ticker_b;


-- ── Query 7: Best and Worst Single-Day Returns Per Ticker ─────────────────

SELECT ticker, 'Best'  AS type, trade_date, ROUND(daily_return, 6) AS daily_return
FROM stock_returns
WHERE (ticker, daily_return) IN (
    SELECT ticker, MAX(daily_return) FROM stock_returns GROUP BY ticker
)
UNION ALL
SELECT ticker, 'Worst' AS type, trade_date, ROUND(daily_return, 6) AS daily_return
FROM stock_returns
WHERE (ticker, daily_return) IN (
    SELECT ticker, MIN(daily_return) FROM stock_returns GROUP BY ticker
)
ORDER BY ticker, type;


-- ── Query 8: Year-over-Year Annual Return Per Ticker ──────────────────────

WITH yearly_prices AS (
    SELECT
        STRFTIME('%Y', trade_date) AS yr,           -- PostgreSQL: EXTRACT(YEAR FROM trade_date)
        ticker,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker, STRFTIME('%Y', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS start_price,
        LAST_VALUE(close_price) OVER (
            PARTITION BY ticker, STRFTIME('%Y', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS end_price
    FROM stock_prices
)
SELECT DISTINCT
    yr,
    ticker,
    ROUND((end_price - start_price) / start_price, 4) AS annual_return
FROM yearly_prices
ORDER BY ticker, yr;


-- =============================================================
-- SECTION 5: EXPORT VIEWS (ready for Tableau / Power BI)
-- =============================================================

-- View: cumulative returns (Phase 3 Tableau source)
CREATE VIEW IF NOT EXISTS vw_cumulative_returns AS
SELECT
    trade_date,
    ticker,
    ROUND(
        EXP(SUM(LN(1.0 + daily_return))
            OVER (PARTITION BY ticker ORDER BY trade_date)) - 1,
        6
    ) AS cumulative_return
FROM stock_returns
WHERE daily_return IS NOT NULL;

-- View: monthly performance (Phase 3 bar chart source)
CREATE VIEW IF NOT EXISTS vw_monthly_performance AS
SELECT
    STRFTIME('%Y-%m', trade_date) AS year_month,
    ticker,
    ROUND(EXP(SUM(LN(1.0 + daily_return))) - 1, 6) AS compound_monthly_return
FROM stock_returns
WHERE daily_return IS NOT NULL
GROUP BY year_month, ticker;

-- View: drawdown over time (Phase 3 area chart source)
CREATE VIEW IF NOT EXISTS vw_drawdown AS
WITH rm AS (
    SELECT
        trade_date, ticker, close_price,
        MAX(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS peak_price
    FROM stock_prices
)
SELECT
    trade_date,
    ticker,
    ROUND((close_price - peak_price) / peak_price, 6) AS drawdown
FROM rm;