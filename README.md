# 🚀 ALPHA Trading Bot

[![Version](https://img.shields.io/badge/version-2.1.0-blue.svg)](https://github.com/anjanib31-source/trading-dashboard)
[![Status](https://img.shields.io/badge/status-production_ready-green.svg)](https://github.com/anjanib31-source/trading-dashboard)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

A fully automated, production-ready algorithmic trading bot for the Indian stock market, integrated with Angel One API.

---

## 📋 Table of Contents
- [Features](#-features)
- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Telegram Commands](#-telegram-commands)
- [Dashboard](#-dashboard)
- [Trading Strategy](#-trading-strategy)
- [Risk Management](#-risk-management)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Features

### Core Trading
- ✅ Angel One API integration
- ✅ 4x Leverage position sizing
- ✅ Dynamic Stop Loss based on ATR
- ✅ Trailing Stop Loss (1.5% activation, 0.7% pullback)
- ✅ Partial profit taking (3 levels: 1.5%, 2.5%, 3.5%)
- ✅ Time-based exits (90 min default, extended for high scores)
- ✅ Max daily loss limit (5%)
- ✅ Sector diversification (40% max per sector)
- ✅ Correlation filter (0.70 max)

### Data Sources
- ✅ **NSE Python** (Primary - Fast & Free)
- ✅ **Angel One API** (Secondary)
- ✅ **Yahoo Finance** (Fallback)
- ✅ **Bulk Data Store** (Emergency)
- ✅ Circuit Breaker with Auto-Reset

### Monitoring & Control
- ✅ **PWA Dashboard** with real-time data
- ✅ **14 Telegram Commands** for remote control
- ✅ **Health Monitoring** with auto-recovery
- ✅ **Performance Alerts** (3 wins, 2 losses, milestones)
- ✅ **Daily Summary** reports
- ✅ **Ngrok Auto-Update** for dynamic URLs

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Angel One API credentials
- ngrok account (for external access)

### Installation

```bash
# Clone the repository
git clone https://github.com/anjanib31-source/trading-dashboard.git
cd trading-dashboard

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env

# Edit .env with your credentials
nano .env
```

### Start the Bot

```bash
# Start everything (Flask + ngrok + Bot)
start_all.bat

# Or start components separately
python src/app.py          # Flask API server
python src/angel_bot_v2.py # Trading bot
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    ALPHA TRADING BOT                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Trading     │  │  Flask API   │  │  PWA         │         │
│  │  Engine      │  │  Server      │  │  Dashboard   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│         │                  │                  │                 │
│         ▼                  ▼                  ▼                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Angel One   │  │  SQLite      │  │  Telegram    │         │
│  │  API         │  │  Database    │  │  Bot         │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  NSE Python  │  │  Yahoo       │  │  Ngrok       │         │
│  │  (Primary)   │  │  Finance     │  │  Tunnel      │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Configuration

### Environment Variables (`.env`)

```env
# Angel One API Credentials
API_KEY=your_api_key
CLIENT_CODE=your_client_code
MPIN=your_mpin
TOTP_SECRET=your_totp_secret

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
TELEGRAM_CHAT_IDS=chat_id1,chat_id2  # Multiple chats

# Trading Parameters
CAPITAL=10000
MAX_POSITIONS=3
LEVERAGE=4
STOP_LOSS=0.02
SCAN_INTERVAL=45

# Flask API
FLASK_PORT=5000
RATE_LIMIT_PER_MINUTE=120
```

---

## 📱 Telegram Commands

| Command | Description |
|---------|-------------|
| `/status` | Show bot status (capital, P&L, positions) |
| `/health` | Detailed health check of all components |
| `/positions` | List all open positions with P&L |
| `/trades` | Show recent trades |
| `/scan` | Force a scan for trading signals |
| `/restart` | Restart the bot gracefully |
| `/stop` | Stop trading |
| `/start` | Resume trading |
| `/ws` | WebSocket connection status |
| `/reconnect` | Reconnect WebSocket |
| `/ping` | Check if bot is alive |
| `/help` | Show all commands |
| `/logs` | Show recent logs |
| `/reset` | Reset circuit breaker |

---

## 📊 Dashboard

### Local Access
```
http://localhost:5000
```

### External Access (ngrok)
```
https://your-ngrok-url.ngrok-free.dev
```

### PWA (Mobile)
```
https://anjanib31-source.github.io/trading-dashboard/
```

### Dashboard Features
- Real-time P&L
- Open positions with live prices
- Trade history
- Performance charts (Daily/Weekly/Monthly)
- System health monitoring
- Auto-refresh every 30 seconds

---

## 🎯 Trading Strategy

### Stock Selection Algorithm

1. **Price Filter:** ₹20 - ₹4000
2. **Volume Filter:** > 50% of 20-day average
3. **Market Filter:** NIFTY above SMA20 & SMA50
4. **Technical Indicators:**
   - Price > VWAP
   - Price > EMA20
   - Opening Range Breakout (ORB)
5. **Scoring System:** 7-10 (Higher = Better)

### Position Sizing

| Score | Allocation | Risk/Reward | Hold Time |
|-------|------------|-------------|-----------|
| 10 | 45% of capital | 1:2.5 | Till Market Close |
| 9 | 35% of capital | 1:2.0 | Till Market Close |
| 8 | 25% of capital | 1:1.8 | 3 hours |
| 7 | 15% of capital | 1:1.5 | 90 minutes |

---

## 🛡️ Risk Management

| Parameter | Value | Description |
|-----------|-------|-------------|
| **Stop Loss** | 2% | Fixed stop loss per trade |
| **Trailing SL** | 1.5% / 0.7% | Activation / Pullback |
| **Max Daily Loss** | 5% | Stops trading for the day |
| **Max Positions** | 3 | Concurrent positions |
| **Sector Exposure** | 40% | Max per sector |
| **Correlation** | 0.70 | Max between positions |

---

## 🔧 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Dashboard shows ₹0** | Check if Flask is running: `python src/app.py` |
| **Telegram commands not working** | Restart bot: `stop_all.bat` → `start_all.bat` |
| **Ngrok offline** | Check ngrok is running: `tasklist \| findstr ngrok` |
| **Rate limit exceeded** | Wait 60 seconds and try again |
| **WebSocket disconnected** | Send `/reconnect` or restart bot |

### Check Status

```bash
status.bat
```

### View Logs

```bash
# PowerShell
Get-Content logs/paper_bot.log -Wait

# Git Bash
tail -f logs/paper_bot.log
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- [Angel One](https://www.angelone.in/) for API access
- [NSE Python](https://github.com/swapniljariwala/nsepython) for live data
- [ngrok](https://ngrok.com/) for secure tunneling

---

## 📞 Contact

- **Developer:** Anjani
- **GitHub:** [anjanib31-source](https://github.com/anjanib31-source)
- **Telegram:** @arandatech_trading_bot

---

**Happy Trading!** 📈