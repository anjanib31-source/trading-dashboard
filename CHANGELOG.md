# 📋 Changelog

All notable changes to the ALPHA Trading Bot will be documented in this file.

---

## [2.1.0] - 2026-07-16

### Added
- ✅ Smart PWA redirector with dynamic ngrok URL detection
- ✅ Auto-update ngrok URL to GitHub on bot startup
- ✅ Multiple Telegram chat support (`TELEGRAM_CHAT_IDS`)
- ✅ Trade performance alerts (3 consecutive wins, 2 losses, milestones)
- ✅ Alert frequency control (60s cooldown)
- ✅ Extended hold for high score trades (Score 9+ till market close)
- ✅ User-friendly market alerts (😴 Bot is Sleeping notifications)
- ✅ `check_performance_alerts()` method
- ✅ `send_alert_with_cooldown()` method

### Fixed
- ✅ Rate limit increased from 60 to 120 requests/min
- ✅ Telegram listener starts 24/7 (before market check)
- ✅ API Parameter Error (removed `exchange=` from `getCandleData()`)
- ✅ Circuit Breaker Sensitivity (threshold 10, timeout 30s, auto-reset)
- ✅ Leverage Position Sizing (4x applied)
- ✅ ATR Position Sizing (4x applied)
- ✅ 90-min hard exit removed for high scores
- ✅ Dynamic Stock Universe (500+ stocks)
- ✅ Ngrok URL changes now auto-synced to GitHub
- ✅ Dashboard API_BASE updated to live ngrok URL
- ✅ Duplicate `error_recovery` in `__init__` removed

### Changed
- ✅ Code cleaned and optimized (app.py 750 → 710 lines)
- ✅ start_all.bat with auto-git-push feature
- ✅ index.html with smart redirector
- ✅ Improved error messages for rate limiting
- ✅ Better logging for Telegram listener

---

## [2.0.0] - 2026-07-15

### Added
- ✅ NSE Python integration as primary data source
- ✅ 4-tier data source hierarchy
- ✅ Telegram remote control (14 commands)
- ✅ PWA dashboard with real-time data
- ✅ Health monitoring with auto-recovery
- ✅ Dynamic stock universe (500+ stocks)
- ✅ Multi-tier data sources (NSE Python, Angel One, Yahoo, Bulk Data)

### Fixed
- ✅ Initial setup and deployment issues
- ✅ Database initialization
- ✅ Position recovery on startup

---

## [1.0.0] - 2026-07-14

### Added
- ✅ Initial release
- ✅ Angel One API integration
- ✅ Paper trading mode
- ✅ Basic Telegram alerts
- ✅ SQLite database for trades
- ✅ Web dashboard
- ✅ 90-minute time exit
- ✅ Stop loss and trailing stop loss
- ✅ Partial profit taking
- ✅ Sector diversification
- ✅ Correlation filter

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ Added | New features |
| 🔧 Fixed | Bug fixes |
| 📦 Changed | Modifications to existing features |
| ⚠️ Removed | Removed features |
| 🔒 Security | Security improvements |