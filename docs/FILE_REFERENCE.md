# File Reference

Generated: 2026-07-11

This document focuses on significant source and operational files. The exhaustive per-file classification is in `docs/REPOSITORY_AUDIT.md`.

## Active Source Files

### `src/angel_bot_v2.py`

Purpose: Main ALPHA paper-trading bot engine.

Primary class:

- `AngelTradingBot`

Top-level functions:

- `get_db_connection()`
- `init_database()`

Key methods:

- Authentication and setup:
  - `login()`
  - `load_symbol_tokens()`
  - `load_sector_mapping()`
- Websocket:
  - `start_websocket()`
  - `on_ws_open()`
  - `on_ws_close()`
  - `on_ws_error()`
  - `on_ws_data()`
  - `subscribe_to_stocks()`
  - `check_ws_health()`
  - `_reconnect_websocket()`
- Market data:
  - `get_indicator_data()`
  - `_parse_angel_historical_data()`
  - `get_ltp()`
  - `update_bulk_market_data()`
  - `fetch_all_stocks()`
  - `_fetch_nsepython_nifty500()`
  - `_fetch_nsepython_nifty50_next()`
  - `_fetch_nse_equity_csv()`
  - `_fetch_bhavcopy()`
  - `_fetch_nse_website_api()`
  - `_fetch_hardcoded_fallback()`
  - `get_prices_bulk()`
  - `get_prices_fallback()`
- Strategy:
  - `calculate_vwap()`
  - `detect_orb()`
  - `get_market_condition()`
  - `is_market_tradeable()`
  - `score_stock()`
- Risk:
  - `check_correlation()`
  - `calculate_correlation()`
  - `check_sector_exposure()`
  - `calculate_atr()`
  - `calculate_atr_position_size()`
  - `get_dynamic_stop_loss()`
  - `get_volatility_stop()`
  - `check_daily_loss_limit()`
- Execution:
  - `place_order()`
  - `check_exits()`
  - `check_and_square_off()`
  - `scan_and_trade()`
- Reporting/publishing:
  - `save_trade()`
  - `generate_daily_summary()`
  - `sync_to_github_pages()`
  - `render_and_deploy_dashboard()`
- Calendar/lifecycle:
  - `fetch_nse_holidays()`
  - `is_trading_day()`
  - `check_market_status()`
  - `is_trading_time()`
  - `run()`
  - `shutdown()`

Dependencies:

- Internal files: `.env`, `scrip_master.json`, `trades.db`, `dashboard_backup.html`, `logs/`.
- External libraries: `SmartApi`, `pyotp`, `pandas`, `numpy`, `yfinance`, `requests`, `python-dotenv`, optional `nsepython`.
- External services: Angel SmartAPI, Angel websocket, Yahoo Finance, NSE APIs, GitHub Contents API.

Used by:

- `start_all.bat`
- Manual execution through `python src\angel_bot_v2.py`

Notes:

- `numpy as np` appears unused.
- Active configuration values are constants in this file, not loaded from `config/*.yaml`.
- The file contains embedded dashboard HTML that writes `dashboard_backup.html`, while Flask serves `web/dashboard.html`.

### `src/app.py`

Purpose: Flask web API and dashboard static-file server.

Functions/routes:

- `get_db_connection()`
- `GET /` -> `serve_dashboard()`
- `GET /api/trades` -> `get_trades()`
- `GET /api/stats` -> `get_stats()`
- `GET /api/daily_pnl` -> `get_daily_pnl()`
- `GET /api/weekly_pnl` -> `get_weekly_pnl()`
- `GET /api/monthly_pnl` -> `get_monthly_pnl()`
- `GET /api/performance` -> `get_performance()`
- `GET /api/market_status` -> `get_market_status()`

Dependencies:

- Internal files: `trades.db`, `web/dashboard.html`.
- External libraries: `flask`, `flask_cors`.
- Standard library: `sqlite3`, `os`, `datetime`.

Used by:

- `start_all.bat`
- Browser/dashboard requests through local or ngrok URL.

Notes:

- `json` import appears unused.
- Runs with `debug=True` and `host='0.0.0.0'`, which is not production-safe.
- CORS allows all origins.

### `src/__init__.py`

Purpose: Package marker.

Classes/functions: none.

Dependencies: none.

Used by: Python package/import tooling.

Recommendation: keep.

## Frontend Files

### `web/dashboard.html`

Purpose: Static dashboard for trades, PnL, positions, charts, market status, and performance.

Key JavaScript functions:

- `formatCurrency()`
- `switchPage()`
- `switchAnalyticsTab()`
- `loadDashboard()`
- `renderPositions()`
- `renderTrades()`
- `loadTopPerformers()`
- `loadDailyPNL()`
- `loadWeeklyPNL()`
- `loadMonthlyPNL()`
- `updateChart()`
- `updateStats()`
- `loadMarketStatus()`

Dependencies:

- `https://cdn.tailwindcss.com`
- `https://cdn.jsdelivr.net/npm/chart.js`
- Hardcoded API base: `https://turbine-bust-upload.ngrok-free.dev/api`

Used by:

- `src/app.py` route `/`

Notes:

- The hardcoded ngrok URL should become runtime configuration.
- The dashboard polls every 30 seconds through `loadDashboard()`.

### `web/manifest.json`

Purpose: PWA metadata for dashboard installability.

Dependencies:

- External icon URL.

Used by:

- Assumption: browser/PWA deployment if linked from dashboard. No direct active HTML link was detected in the scan.

## Operational Files

### `start_all.bat`

Purpose: Local Windows startup orchestration.

Flow:

1. `cd C:\Users\ArandaTech\trading-bot`
2. `start /B python src\app.py`
3. `start /B ngrok http 5000`
4. `python src\angel_bot_v2.py`

Used by:

- Operator.

### `trades.db`

Purpose: SQLite persistence for trade records.

Tables initialized by active bot:

- `trades`
- `positions`

Read by:

- `src/app.py`

Written by:

- `src/angel_bot_v2.py`

### `scrip_master.json`

Purpose: Local instrument master used to map NSE symbols to Angel tokens.

Read by:

- `src/angel_bot_v2.py`

### `dashboard_backup.html`

Purpose: Runtime-generated dashboard artifact.

Written by:

- `src/angel_bot_v2.py`

Notes:

- Not served by `src/app.py`.

## Configuration and Data Files

### `config/config.yaml`

Purpose: Intended trading, broker, and logging config.

Used by:

- No active source reference detected.

### `config/paper_trading_config.yaml`

Purpose: Intended paper-trading sandbox config.

Used by:

- No active source reference detected.

### `config/holidays.txt`

Purpose: Static holiday list.

Referenced by:

- `config/config.yaml`

Used by:

- No active source reference detected.

### `data/stock_config.json`

Purpose: Intended stock universe preferences.

Used by:

- No active source reference detected.

## Archived Source Files

Archived files under `archive/` are not used by `start_all.bat`, `src/app.py`, or `src/angel_bot_v2.py`. They include older bot versions, old dashboards, backtest tools, smoke tests, and empty placeholders. See `docs/ARCHIVE_REPORT.md`.
