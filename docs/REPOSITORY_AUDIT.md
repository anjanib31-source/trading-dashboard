# Repository Audit

Generated: 2026-07-11

Scope: all project files visible in the workspace, excluding `.git` internals and directories. Runtime artifacts, caches, archived files, and generated documentation are included because they affect maintenance. References are based on text search and known startup flow.

## Classification Table

| File path | Category | Purpose | Referenced by | References | Reason for classification | Recommendation |
|---|---|---|---|---|---|---|
| `.env` | Indirectly used | Stores runtime secrets and credentials. | `src/angel_bot_v2.py` via `load_dotenv()` | Environment variables: `API_KEY`, `CLIENT_CODE`, `MPIN`, `TOTP_SECRET`, `GITHUB_PAT` | Required at runtime but not imported directly. | Keep |
| `.gitignore` | Active | Git ignore policy. | Git | `.env`, `*.json`, `*.log`, `archive/`, `__pycache__/` | Controls repository hygiene. | Keep |
| `angel_session.json` | Indirectly used | Angel One session artifact. | Assumption: SmartAPI/login tooling or historical runtime | None detected | Runtime credential/session file; not currently read by active source. | Keep but do not commit |
| `dashboard_backup.html` | Indirectly used | Generated dashboard snapshot written by bot. | `src/angel_bot_v2.py` writes this path | Hardcoded ngrok API, Chart.js | Runtime-generated artifact. | Keep runtime copy or move generated files outside repo |
| `scrip_master.json` | Active | Local Angel/NSE instrument master. | `src/angel_bot_v2.py` | None | Required by `load_symbol_tokens()` before scanning. | Keep but do not commit |
| `start_all.bat` | Active | Windows launcher for full system. | Operator/manual startup | `src/app.py`, `ngrok`, `src/angel_bot_v2.py` | Primary startup entry point. | Keep |
| `trades.db` | Active | SQLite trade store. | `src/angel_bot_v2.py`, `src/app.py` | SQLite tables `trades`, `positions` | Shared persistence layer between bot and API. | Keep runtime copy; exclude from VCS |
| `src/__init__.py` | Indirectly used | Marks `src` as package. | Python package tooling | None | Empty but package-safe. | Keep |
| `src/app.py` | Active | Flask API and dashboard server. | `start_all.bat` | `trades.db`, `web/dashboard.html`, Flask, CORS | Active web/API process. | Keep |
| `src/angel_bot_v2.py` | Active | Main trading bot engine. | `start_all.bat` | `.env`, `scrip_master.json`, `trades.db`, `dashboard_backup.html`, Angel SmartAPI, Yahoo Finance, NSE, GitHub API | Active bot process and core business logic. | Keep |
| `web/dashboard.html` | Active | Static dashboard UI. | `src/app.py` serves it | Hardcoded ngrok API, Tailwind CDN, Chart.js CDN | Current served frontend. | Keep |
| `web/manifest.json` | Indirectly used | PWA manifest for dashboard. | Browser if linked/deployed | External icon URL | Static asset; not explicitly linked in current HTML scan. | Keep or wire explicitly |
| `config/config.yaml` | Indirectly used | Intended bot configuration. | No active code reference detected | `config/holidays.txt` | Useful design/config reference but active bot uses constants. | Keep; wire into code later |
| `config/holidays.txt` | Indirectly used | Static holiday list. | `config/config.yaml` | None | Config references it; active bot fetches holidays dynamically. | Keep or consolidate |
| `config/paper_trading_config.yaml` | Indirectly used | Intended paper-trading config. | No active code reference detected | Sandbox URLs | Useful but not wired into active runtime. | Keep; wire into code later |
| `data/stock_config.json` | Indirectly used | Stock universe preference config. | No active code reference detected | None | Useful data config but not wired into active bot. | Keep; wire or archive after review |
| `reports/paper_trade_report_2026-07-01.csv` | Documentation | Historical paper trade report. | Operator/manual review | None | Output/report artifact. | Keep for audit or archive by retention policy |
| `reports/paper_trade_report_2026-07-02.csv` | Documentation | Historical paper trade report. | Operator/manual review | None | Output/report artifact. | Keep for audit or archive by retention policy |
| `reports/paper_trade_report_2026-07-06.csv` | Documentation | Historical paper trade report. | Operator/manual review | None | Output/report artifact. | Keep for audit or archive by retention policy |
| `logs/angelone_trading.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/enhanced_trades.csv` | Documentation | Historical trade CSV. | Operator/manual review | None | Runtime/report artifact. | Archive or rotate |
| `logs/enhanced_trading.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/expanded_trading.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/live_bot_2026-06-30.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-06-30.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-01.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-02.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-03.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-04.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-06.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-07.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-08.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-09.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-10.log` | Documentation | Historical bot log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/paper_bot_2026-07-11.log` | Documentation | Current/historical bot log. | `src/angel_bot_v2.py` logging pattern | None | Runtime artifact for current bot. | Keep during active debugging; rotate |
| `logs/paper_trades.csv` | Documentation | Historical trade CSV. | Operator/manual review | None | Runtime/report artifact. | Archive or rotate |
| `logs/performance_optimized.log` | Safe to delete | Empty/old runtime log. | None detected | None | Empty historical artifact. | Delete if not needed |
| `logs/simulation.log` | Safe to delete | Empty/old runtime log. | None detected | None | Empty historical artifact. | Delete if not needed |
| `logs/small_capital_trades.csv` | Documentation | Historical trade CSV. | Operator/manual review | None | Runtime/report artifact. | Archive or rotate |
| `logs/small_capital_trading.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/trades.csv` | Indirectly used | Legacy trade CSV. | `archive/analyze_trades.py` | None | Referenced only by archived analyzer. | Archive with analyzer or retain for history |
| `logs/trading.log` | Documentation | Historical runtime log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-06-30/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-01/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-02/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-03/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-04/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-06/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-07/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-08/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-09/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-10/app.log` | Documentation | Historical app log. | Operator/manual review | None | Runtime artifact. | Archive or rotate |
| `logs/2026-07-11/app.log` | Documentation | Current/historical app log. | Operator/manual review | None | Runtime artifact. | Keep during active debugging; rotate |
| `archive/requirements.txt` | Legacy but safe to archive | Old dependency list. | Manual reference only | Package names | No active install file at root. | Archive; consider promoting clean root requirements |
| `archive/analyze_trades.py` | Legacy but safe to archive | CSV trade analyzer for old logs. | Manual only | `logs/trades.csv`, pandas | Not active startup path. | Archive |
| `archive/angel_trading_bot.py` | Legacy but safe to archive | Older Angel bot implementation. | Manual only | SmartAPI, Yahoo Finance, logs | Superseded by `src/angel_bot_v2.py`. | Archive |
| `archive/bot_with_finstack.py` | Legacy but safe to archive | Older scheduled bot/FinStack experiment. | Manual only | schedule, dotenv, pandas | Not active startup path. | Archive |
| `archive/dashboard.html` | Legacy but safe to archive | Older dashboard artifact. | Manual only | Browser/CDNs | Superseded by `web/dashboard.html`. | Archive |
| `archive/index.html` | Legacy but safe to archive | Older dashboard artifact. | Manual only | Browser/CDNs | Superseded by `web/dashboard.html`. | Archive |
| `archive/main.py` | Safe to delete | Empty archived placeholder. | None detected | None | Empty file. | Delete |
| `archive/run_backtest.py` | Legacy but safe to archive | Backtest experiment. | Manual only | Missing `bot_enhanced`, yfinance, pandas | Archived experiment with unresolved dependency. | Archive |
| `archive/system_check.py` | Legacy but safe to archive | Environment checker. | Manual only | psutil, importlib.metadata | Useful manually, not active runtime. | Archive |
| `archive/test_angel_login.py` | Test file | Manual Angel login smoke test. | Manual only | SmartAPI, pyotp | Test-like utility, not automated. | Archive or move to tests with safeguards |
| `archive/unused_2026-07-11/angel_bot_v2.py` | Legacy but safe to archive | Old root bot copy. | Manual only | SmartAPI, dashboard write | Superseded by active `src/angel_bot_v2.py`. | Archive |
| `archive/unused_2026-07-11/angel_trading_bot_paper.py` | Legacy but safe to archive | Older paper bot. | Manual only | SmartAPI, hardcoded credentials | Superseded and contains credential risk. | Archive; rotate credentials |
| `archive/unused_2026-07-11/dashboard.html` | Legacy but safe to archive | Old root dashboard. | Manual only | Browser/CDNs | Superseded by `web/dashboard.html`. | Archive |
| `archive/unused_2026-07-11/data_fetcher.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/index.html` | Legacy but safe to archive | Old GitHub Pages dashboard. | Manual only | Hardcoded ngrok API, Chart.js | Superseded by active dashboard/generator. | Archive |
| `archive/unused_2026-07-11/manifest.json` | Legacy but safe to archive | Old root PWA manifest. | Manual only | External icon | Superseded by `web/manifest.json`. | Archive |
| `archive/unused_2026-07-11/orchestrator.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/risk_manager.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/scheduler.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/signal_generator.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/sw.js` | Legacy but safe to archive | Old service worker. | Manual only | Browser fetch passthrough | No active manifest/HTML reference. | Archive or delete after review |
| `archive/unused_2026-07-11/test_data_fetcher.py` | Safe to delete | Empty test placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `archive/unused_2026-07-11/trade_executor.py` | Safe to delete | Empty placeholder. | None detected | None | Empty and unused. | Delete after retention window |
| `__pycache__/angel_bot_v2.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source | Regenerable cache. | Delete |
| `__pycache__/bot_enhanced.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source now absent | Stale/regenerable cache. | Delete |
| `__pycache__/bot_performance_optimized.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source now absent | Stale/regenerable cache. | Delete |
| `src/__pycache__/__init__.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source | Regenerable cache. | Delete |
| `src/__pycache__/app.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source | Regenerable cache. | Delete |
| `src/__pycache__/angel_bot_v2.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Compiled source | Regenerable cache. | Delete |
| `src/__pycache__/angel_trading_bot_paper.cpython-312.pyc` | Safe to delete | Python bytecode cache. | Python interpreter | Archived source | Stale/regenerable cache. | Delete |
| `docs/REPOSITORY_AUDIT.md` | Documentation | File classification and repository dependency audit. | Maintainers | All project files | Generated audit documentation. | Keep |
| `docs/SYSTEM_OVERVIEW.md` | Documentation | System overview for onboarding. | Maintainers | Architecture summary | Generated documentation. | Keep |
| `docs/ARCHITECTURE.md` | Documentation | Architecture details and diagrams. | Maintainers | Runtime components | Generated documentation. | Keep |
| `docs/FILE_REFERENCE.md` | Documentation | Significant file reference. | Maintainers | Source files and artifacts | Generated documentation. | Keep |
| `docs/API_REFERENCE.md` | Documentation | Flask API reference. | Maintainers | `src/app.py`, `web/dashboard.html` | Generated documentation. | Keep |
| `docs/DATA_FLOW.md` | Documentation | Data and control-flow documentation. | Maintainers | Bot/API/frontend flow | Generated documentation. | Keep |
| `docs/CONFIGURATION.md` | Documentation | Configuration and environment reference. | Maintainers | `.env`, config files, constants | Generated documentation. | Keep |
| `docs/ARCHIVE_REPORT.md` | Documentation | Archive and deletion report. | Maintainers | `archive/`, orphan files | Generated documentation. | Keep |
| `docs/IMPROVEMENT_REPORT.md` | Documentation | Prioritized technical debt and risk list. | Maintainers | Audit findings | Generated documentation. | Keep |

## Validation Summary

| Check | Result |
|---|---|
| Active syntax check | `python -m py_compile src\angel_bot_v2.py src\app.py` passed |
| Active entry points | `start_all.bat`, `src/app.py`, `src/angel_bot_v2.py` |
| Orphan files | Empty archived placeholders, stale bytecode caches, legacy dashboards |
| Circular dependencies | None detected in active Python files; active modules do not import each other |
| Dead code | Significant in archived scripts and generated dashboard template embedded in bot |
| Duplicate code | Legacy bot copies in `archive/` and old root bot copy |
| Unused imports | `src/app.py`: `json`; `src/angel_bot_v2.py`: likely `numpy as np` |
| Uncertain items | `angel_session.json`, `web/manifest.json`, config YAML/JSON files are marked as indirect/assumption where active code references were not detected |
