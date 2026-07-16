"""
ALPHA Trading Bot - Production-Ready Implementation
✅ FIXED: API Parameter Error
✅ FIXED: Circuit Breaker Sensitivity
✅ FIXED: NSE Python Integration
✅ FIXED: Data Source Hierarchy
✅ FIXED: Leverage Position Sizing
✅ FIXED: Extended Hold for High Score
✅ FIXED: Dynamic Stock Universe
✅ FIXED: Auto-Recovery
✅ ADDED: Multi-Tier Data Sources
✅ ADDED: Circuit Breaker Auto-Reset
✅ ADDED: Telegram Remote Control Commands
✅ ADDED: Multiple Chat Support
✅ ADDED: Alert Frequency Control
✅ ADDED: Trade Performance Alerts
✅ FIXED: Telegram Starts 24/7 (Before Market Check)
"""

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import json
import csv
import io
import pandas as pd
import numpy as np
import yfinance as yf
import time
from datetime import datetime, timedelta, time as datetime_time
import requests
import logging
from logging.handlers import RotatingFileHandler
import os
import sys
import threading
import sqlite3
import shutil
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import base64
import math
import uuid
from collections import deque
import hashlib
import pickle
from functools import wraps
from typing import Optional, Dict, List, Tuple, Any

# ============================================================
# LOAD ENVIRONMENT VARIABLES FROM .env FILE
# ============================================================

# Get the absolute path to the project root (where .env is located)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, '.env')

# Load the .env file
if os.path.exists(env_path):
    load_dotenv(env_path)
    print(f"✅ Loaded .env from: {env_path}")
else:
    print(f"⚠️ .env file not found at: {env_path}")
    # Try current directory as fallback
    load_dotenv()
    print(f"⚠️ Tried loading from current directory: {os.getcwd()}")

# Verify critical environment variables
required_vars = ['API_KEY', 'CLIENT_CODE', 'MPIN', 'TOTP_SECRET']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
    print("Please check your .env file in the project root.")
else:
    print(f"✅ All required environment variables loaded successfully!")

# ============================================================
# IMPORT UTILITY CLASSES
# ============================================================
from utils import ScanManager, MetricsCollector, BotState, ErrorRecovery

# ============================================================
# TRY IMPORTS
# ============================================================
try:
    import nsepython as nse
    NSEPYTHON_AVAILABLE = True
except ImportError:
    NSEPYTHON_AVAILABLE = False

# ============================================================
# UTILITY DECORATORS AND CLASSES
# ============================================================

class RateLimiter:
    """Rate limiter for API calls with thread safety"""

    def __init__(self, max_calls: int = 10, period: int = 60):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()
        self.lock = threading.Lock()

    def wait_if_needed(self):
        with self.lock:
            now = time.time()

            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()

            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                time.sleep(sleep_time + 0.1)

            self.calls.append(time.time())


class CircuitBreaker:
    """Circuit breaker pattern for external APIs with auto-reset"""
    
    def __init__(self, failure_threshold: int = 10, recovery_timeout: int = 30):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.lock = threading.Lock()
        self._last_state_change = time.time()

    def execute(self, func, *args, **kwargs):
        with self.lock:
            # Auto-reset after timeout
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    log.info("[CIRCUIT] Circuit breaker is now HALF_OPEN")
                else:
                    raise Exception(
                        f"Circuit breaker is OPEN for {func.__name__}"
                    )

        try:
            result = func(*args, **kwargs)

            with self.lock:
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                    log.info("[CIRCUIT] Circuit breaker CLOSED - API calls resumed")
                    send_telegram_alert("🔄 <b>Circuit Breaker Reset</b>\nAngel One API calls resumed.", "success")

            return result

        except Exception as e:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
                    log.warning(f"[CIRCUIT] Circuit breaker OPEN for {func.__name__}")
                    send_telegram_alert(
                        f"🚨 <b>Circuit Breaker OPEN</b>\n"
                        f"Function: {func.__name__}\n"
                        f"Failures: {self.failure_count}\n"
                        f"Will auto-reset in {self.recovery_timeout}s",
                        "warning"
                    )

            raise e

    def reset(self):
        """Manually reset circuit breaker"""
        with self.lock:
            self.state = "CLOSED"
            self.failure_count = 0
            self.last_failure_time = None
            log.info("[CIRCUIT] Circuit breaker manually RESET")
            send_telegram_alert("🔄 <b>Circuit Breaker Manually Reset</b>\nAPI calls resumed.", "success")


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1,
    max_delay: float = 60,
    exceptions: tuple = (Exception,),
):
    """Retry decorator with exponential backoff"""

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay

            while retries < max_retries:
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    retries += 1

                    if retries >= max_retries:
                        raise

                    sleep_time = min(
                        delay * (2 ** (retries - 1)),
                        max_delay,
                    )

                    log.warning(
                        f"[RETRY] {func.__name__} failed: {e}. "
                        f"Retry {retries}/{max_retries} "
                        f"in {sleep_time:.1f}s"
                    )

                    time.sleep(sleep_time)

            return None

        return wrapper

    return decorator

# === API CREDENTIALS ===
API_KEY = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
MPIN = os.getenv("MPIN")
TOTP_SECRET = os.getenv("TOTP_SECRET")

# === GITHUB SYNCHRONIZATION ===
GITHUB_PAT = os.getenv("GITHUB_PAT")
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME", "anjanib31-source")
GITHUB_REPO = os.getenv("GITHUB_REPO", "trading-dashboard")

# === DATABASE ===
DB_NAME = "trades.db"

# === TELEGRAM ALERTS ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8993963920:AAGl4hhH4rHfC-MQlPNXK7uZ3YwkEspngOY")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "947783716")

# === TRADING PARAMETERS ===
CAPITAL = int(os.getenv("CAPITAL", 10000))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", 3))
LEVERAGE = int(os.getenv("LEVERAGE", 4))
STOP_LOSS = float(os.getenv("STOP_LOSS", 0.02))
TRAILING_SL_ACTIVATION = float(os.getenv("TRAILING_SL_ACTIVATION", 0.015))
TRAILING_SL_PULLBACK = float(os.getenv("TRAILING_SL_PULLBACK", 0.007))
MAX_HOLD_MINUTES = int(os.getenv("MAX_HOLD_MINUTES", 90))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 45))

# === PROFIT TARGETS ===
PROFIT_TARGETS = {8: 0.035, 9: 0.040, 10: 0.045}
DEFAULT_PROFIT_TARGET = 0.030
MIN_SIGNAL_SCORE = 7

# === PARTIAL PROFIT TAKING ===
ENABLE_PARTIAL_EXIT = True
PARTIAL_EXIT_LEVEL_1 = 0.015
PARTIAL_EXIT_LEVEL_2 = 0.025
PARTIAL_EXIT_LEVEL_3 = 0.035

# === RISK MANAGEMENT ===
ENABLE_ROBO_ORDERS = True
ENABLE_DYNAMIC_SL = True
ENABLE_ATR_POSITION_SIZING = True
ENABLE_CORRELATION_FILTER = True
ENABLE_SECTOR_DIVERSIFICATION = True
ENABLE_VOLATILITY_STOP = True
MAX_SECTOR_EXPOSURE = 0.40
MAX_CORRELATION_THRESHOLD = 0.70

# === STOCK SELECTION ===
AUTO_PICK_STOCKS = False
MAX_STOCKS_TO_SCAN = 40
MIN_STOCK_PRICE = 20
MAX_STOCK_PRICE = 4000

# === STRATEGIES ===
ENABLE_ORB = True
ENABLE_VWAP = True
ENABLE_MULTI_INDICATOR = True
ENABLE_MARKET_FILTER = True
ORB_MINUTES = 15

# === MARKET HOURS ===
WAKE_UP_TIME = "09:15"
TRADING_START = "09:30"
MARKET_CLOSE = "15:30"
SQUARE_OFF_BUFFER = 5

# === MAX DAILY LOSS ===
MAX_DAILY_LOSS = 0.05

# === WEBSOCKET HEARTBEAT ===
WS_HEARTBEAT_INTERVAL = 30

# === API RATE LIMITING ===
API_MAX_CALLS_PER_MINUTE = 10
CACHE_CLEANUP_INTERVAL = 3600  # 1 hour
MAX_TRADES_HISTORY = 500

# === SETUP LOGGING WITH ROTATION ===
os.makedirs("logs", exist_ok=True)

# Create root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create rotating file handler (10MB, 5 backups)
file_handler = RotatingFileHandler(
    'logs/paper_bot.log',
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Module-level logger
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

def validate_configuration() -> bool:
    """Validate all configuration parameters"""
    validations = [
        (CAPITAL > 0, "CAPITAL must be > 0"),
        (MAX_POSITIONS > 0, "MAX_POSITIONS must be > 0"),
        (LEVERAGE >= 1, "LEVERAGE must be >= 1"),
        (STOP_LOSS > 0 and STOP_LOSS < 1, "STOP_LOSS must be between 0 and 1"),
        (TRAILING_SL_PULLBACK > 0 and TRAILING_SL_PULLBACK < 1, "TRAILING_SL_PULLBACK must be between 0 and 1"),
        (SCAN_INTERVAL >= 10, "SCAN_INTERVAL must be >= 10 seconds"),
        (MIN_SIGNAL_SCORE >= 1 and MIN_SIGNAL_SCORE <= 10, "MIN_SIGNAL_SCORE must be 1-10"),
        (MAX_DAILY_LOSS > 0 and MAX_DAILY_LOSS < 1, "MAX_DAILY_LOSS must be between 0 and 1"),
        (MAX_STOCKS_TO_SCAN > 0 and MAX_STOCKS_TO_SCAN <= 100, "MAX_STOCKS_TO_SCAN must be 1-100"),
        (MIN_STOCK_PRICE > 0, "MIN_STOCK_PRICE must be > 0"),
        (MAX_STOCK_PRICE > MIN_STOCK_PRICE, "MAX_STOCK_PRICE must be > MIN_STOCK_PRICE"),
        (ORB_MINUTES > 0 and ORB_MINUTES <= 30, "ORB_MINUTES must be 1-30"),
        (MAX_HOLD_MINUTES >= 10, "MAX_HOLD_MINUTES must be >= 10"),
        (TRAILING_SL_ACTIVATION > 0 and TRAILING_SL_ACTIVATION < 1, "TRAILING_SL_ACTIVATION must be between 0 and 1"),
        (PARTIAL_EXIT_LEVEL_1 < PARTIAL_EXIT_LEVEL_2 < PARTIAL_EXIT_LEVEL_3, "Partial exit levels must be in ascending order"),
    ]
    
    errors = []
    for condition, message in validations:
        if not condition:
            errors.append(message)
    
    if errors:
        log.critical(f"Configuration errors: {', '.join(errors)}")
        send_telegram_alert(f"⚠️ <b>Configuration Error</b>\n{', '.join(errors)}", "error")
        return False
    return True


def validate_environment() -> bool:
    """Validate all required environment variables"""
    required_vars = ['API_KEY', 'CLIENT_CODE', 'MPIN', 'TOTP_SECRET']
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        log.error(f"[ENV] Missing: {missing}")
        send_telegram_alert(f"❌ <b>Environment Error</b>\nMissing variables: {', '.join(missing)}", "error")
        return False
    return True


# ============================================================
# ENHANCED TELEGRAM ALERTS
# ============================================================

@retry_with_backoff(max_retries=3, base_delay=2, exceptions=(requests.RequestException,))
def send_telegram_alert(message: str, alert_type: str = "info", parse_mode: str = "HTML") -> bool:
    """Send enhanced alert to Telegram - Supports multiple chats"""
    try:
        alert_prefixes = {
            "info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌",
            "trade": "📈", "position": "📊", "startup": "🚀", "shutdown": "🛑",
            "profit": "💰", "loss": "🔴", "target": "🎯", "stop": "🛡️",
            "partial": "📊", "health": "💚", "market": "🏛️"
        }
        
        prefix = alert_prefixes.get(alert_type, "ℹ️")
        timestamp = datetime.now().strftime("%I:%M:%S %p")
        full_message = f"{prefix} <b>{message}</b>\n\n🕐 {timestamp}"
        
        # Get all chat IDs
        chat_ids = [str(TELEGRAM_CHAT_ID)]
        extra_chats = os.getenv("TELEGRAM_CHAT_IDS", "")
        if extra_chats:
            chat_ids.extend([c.strip() for c in extra_chats.split(',') if c.strip()])
        
        # Remove duplicates
        chat_ids = list(set(chat_ids))
        
        success = True
        for chat_id in chat_ids:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": chat_id,
                    "text": full_message,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True
                }
                response = requests.post(url, json=payload, timeout=10)
                if response.status_code != 200:
                    log.error(f"[TELEGRAM] Failed to send to {chat_id}: {response.text}")
                    success = False
            except Exception as e:
                log.error(f"[TELEGRAM] Error sending to {chat_id}: {e}")
                success = False
        
        if success:
            log.info(f"[TELEGRAM] Alert sent to {len(chat_ids)} chat(s): {alert_type}")
        
        return success
        
    except Exception as e:
        log.error(f"[TELEGRAM] Error: {e}")
        raise


def send_trade_alert(symbol: str, entry_price: float, quantity: int, score: int, strategy: str, 
                     target_price: float, stop_price: float, pnl: float = None, exit_reason: str = None,
                     alert_type: str = "OPEN") -> bool:
    """Send detailed trade alert with all information"""
    try:
        if alert_type == "OPEN":
            risk_reward = ((target_price - entry_price) / (entry_price - stop_price)) if (entry_price - stop_price) > 0 else 0
            message = (
                f"🚀 <b>POSITION OPENED</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Symbol:</b> {symbol}\n"
                f"💰 <b>Entry:</b> ₹{entry_price:.2f}\n"
                f"📦 <b>Quantity:</b> {quantity}\n"
                f"⭐ <b>Score:</b> {score}/10\n"
                f"📈 <b>Strategy:</b> {strategy}\n"
                f"🎯 <b>Target:</b> ₹{target_price:.2f}\n"
                f"🛡️ <b>Stop:</b> ₹{stop_price:.2f}\n"
                f"💹 <b>Risk/Reward:</b> 1:{risk_reward:.2f}\n"
                f"📅 <b>Time:</b> {datetime.now().strftime('%I:%M:%S %p')}"
            )
            return send_telegram_alert(message, "position")
        
        elif alert_type == "CLOSE":
            pnl_pct = (pnl / (entry_price * quantity)) * 100 if pnl else 0
            exit_price = entry_price * (1 + pnl_pct/100)
            emoji = "💰" if pnl > 0 else "🔴"
            status = "PROFIT" if pnl > 0 else "LOSS"
            
            message = (
                f"{emoji} <b>POSITION CLOSED - {status}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Symbol:</b> {symbol}\n"
                f"📈 <b>Strategy:</b> {strategy}\n"
                f"📊 <b>P&L:</b> {'+' if pnl > 0 else ''}₹{pnl:.2f} ({pnl_pct:+.2f}%)\n"
                f"💰 <b>Entry:</b> ₹{entry_price:.2f} → ₹{exit_price:.2f}\n"
                f"📋 <b>Reason:</b> {exit_reason}\n"
                f"📅 <b>Time:</b> {datetime.now().strftime('%I:%M:%S %p')}"
            )
            alert_type = "profit" if pnl > 0 else "loss"
            return send_telegram_alert(message, alert_type)
        
        return False
        
    except Exception as e:
        log.error(f"[TELEGRAM] Trade alert error: {e}")
        return False


def send_system_alert(status: str, details: str = "", alert_type: str = "info") -> bool:
    """Send system status alert"""
    try:
        emojis = {
            "ONLINE": "🟢", "OFFLINE": "🔴", "RECONNECTING": "🔄",
            "ERROR": "❌", "WARNING": "⚠️", "INFO": "ℹ️",
            "MAINTENANCE": "🔧", "UPDATE": "📦", "STARTUP": "🚀",
            "SHUTDOWN": "🛑", "READY": "✅"
        }
        
        emoji = emojis.get(status.upper(), "ℹ️")
        message = f"{emoji} <b>SYSTEM {status.upper()}</b>\n"
        message += f"━━━━━━━━━━━━━━━━━━━━━\n"
        
        if details:
            message += f"📋 {details}\n"
        
        message += f"📅 {datetime.now().strftime('%I:%M:%S %p')}"
        
        return send_telegram_alert(message, alert_type)
        
    except Exception as e:
        log.error(f"[TELEGRAM] System alert error: {e}")
        return False


def send_daily_summary_alert(day_pnl: float, total_trades: int, win_rate: float, 
                             positions_open: int, capital: float, best_trade: float = None,
                             worst_trade: float = None) -> bool:
    """Send detailed daily summary alert"""
    try:
        emoji = "📈" if day_pnl >= 0 else "📉"
        pnl_status = "PROFIT" if day_pnl >= 0 else "LOSS"
        pnl_color = "🟢" if day_pnl >= 0 else "🔴"
        
        message = (
            f"📊 <b>DAILY TRADING SUMMARY</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Date:</b> {datetime.now().strftime('%A, %B %d, %Y')}\n"
            f"\n"
            f"{emoji} <b>Today's P&L:</b> {'+' if day_pnl >= 0 else ''}₹{day_pnl:.2f}\n"
            f"📈 <b>Trades Today:</b> {total_trades}\n"
            f"🎯 <b>Win Rate:</b> {win_rate:.1f}%\n"
            f"📊 <b>Open Positions:</b> {positions_open}\n"
            f"💰 <b>Current Capital:</b> ₹{capital:.2f}\n"
            f"\n"
            f"📊 <b>Summary:</b> {pnl_color} {pnl_status}"
        )
        
        if best_trade is not None:
            message += f"\n🏆 <b>Best Trade:</b> ₹{best_trade:+.2f}"
        if worst_trade is not None:
            message += f"\n📉 <b>Worst Trade:</b> ₹{worst_trade:+.2f}"
        
        return send_telegram_alert(message, "info")
        
    except Exception as e:
        log.error(f"[TELEGRAM] Daily summary error: {e}")
        return False


def send_health_alert(component: str, status: str, error: str = None) -> bool:
    """Send health status alert for system components"""
    try:
        emoji = "✅" if status == "healthy" else "❌"
        status_text = "HEALTHY" if status == "healthy" else "UNHEALTHY"
        
        message = f"{emoji} <b>{component} - {status_text}</b>\n"
        message += f"━━━━━━━━━━━━━━━━━━━━━\n"
        message += f"🔍 <b>Component:</b> {component}\n"
        message += f"📊 <b>Status:</b> {status_text}\n"
        
        if error:
            message += f"⚠️ <b>Error:</b> {error}\n"
        
        message += f"📅 {datetime.now().strftime('%I:%M:%S %p')}"
        
        alert_type = "success" if status == "healthy" else "error"
        return send_telegram_alert(message, alert_type)
        
    except Exception as e:
        log.error(f"[TELEGRAM] Health alert error: {e}")
        return False


def send_market_status_alert(is_open: bool, time_until: str = None) -> bool:
    """Send market status alert"""
    try:
        if is_open:
            message = (
                f"🏛️ <b>MARKET IS OPEN</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 Trading session is active\n"
                f"⏰ {datetime.now().strftime('%I:%M:%S %p')}"
            )
        else:
            message = (
                f"🔴 <b>MARKET IS CLOSED</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"⏰ Time until open: {time_until}\n"
                f"📅 {datetime.now().strftime('%I:%M:%S %p')}"
            )
        
        return send_telegram_alert(message, "market")
        
    except Exception as e:
        log.error(f"[TELEGRAM] Market status error: {e}")
        return False

# ============================================================
# TELEGRAM BOT COMMAND HANDLER (Remote Control)
# ============================================================

class TelegramBotHandler:
    """Handle incoming Telegram bot commands for remote control"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.command_handlers = {
            '/status': self.handle_status,
            '/health': self.handle_health,
            '/positions': self.handle_positions,
            '/trades': self.handle_trades,
            '/scan': self.handle_scan,
            '/restart': self.handle_restart,
            '/stop': self.handle_stop,
            '/start': self.handle_start,
            '/ws': self.handle_websocket,
            '/reconnect': self.handle_reconnect,
            '/ping': self.handle_ping,
            '/help': self.handle_help,
            '/logs': self.handle_logs,
            '/reset': self.handle_reset,
        }
        self.last_command_time = {}
        self.cooldown_seconds = 5
        self.running = True
        self.last_update_id = 0
        self._message_buffer = deque(maxlen=10)
        
    def start(self):
        """Start the Telegram bot listener thread"""
        log.info("[TG BOT] start() called - Initializing listener...")
        thread = threading.Thread(target=self._listen_loop, daemon=True)
        thread.start()
        log.info("[TG BOT] Command handler started")
        send_telegram_alert(
            "🤖 <b>Remote Control Enabled</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📱 Send /help to see all commands.",
            "success"
        )
    
    def _listen_loop(self):
        """Main loop to listen for Telegram messages"""
        log.info("[TG BOT] Listener loop started - Waiting for commands...")
        while self.running:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
                params = {
                    "offset": self.last_update_id + 1,
                    "timeout": 30,
                    "allowed_updates": ["message"]
                }
                
                # Debug log every 10th poll to avoid spam
                if self.last_update_id % 10 == 0:
                    log.info(f"[TG BOT] Polling with offset: {self.last_update_id}")
                
                response = requests.get(url, params=params, timeout=35)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok') and data.get('result'):
                        for update in data['result']:
                            self.last_update_id = update['update_id']
                            self._process_update(update)
                else:
                    log.warning(f"[TG BOT] API error: {response.status_code}")
                    time.sleep(5)
                    
            except requests.exceptions.Timeout:
                pass
            except Exception as e:
                log.error(f"[TG BOT] Error in listener loop: {e}")
                time.sleep(10)
    
    def _process_update(self, update):
        """Process a single Telegram update"""
        try:
            if 'message' in update:
                message = update['message']
                chat_id = str(message.get('chat', {}).get('id'))
                
                # Check if message is from authorized chat
                allowed_chats = [str(TELEGRAM_CHAT_ID)]
                extra_chats = os.getenv("TELEGRAM_CHAT_IDS", "")
                if extra_chats:
                    allowed_chats.extend([c.strip() for c in extra_chats.split(',')])
                
                if chat_id not in allowed_chats:
                    log.warning(f"[TG BOT] Unauthorized chat: {chat_id}")
                    return
                
                text = message.get('text', '')
                
                # Rate limiting
                if chat_id in self.last_command_time:
                    elapsed = time.time() - self.last_command_time[chat_id]
                    if elapsed < self.cooldown_seconds:
                        self._send_message(
                            chat_id,
                            f"⏳ Please wait {int(self.cooldown_seconds - elapsed)}s before using another command."
                        )
                        return
                
                self.last_command_time[chat_id] = time.time()
                
                if text.startswith('/'):
                    command = text.split(' ')[0].lower()
                    args = text.split(' ')[1:] if len(text.split(' ')) > 1 else []
                    
                    handler = self.command_handlers.get(command)
                    if handler:
                        log.info(f"[TG BOT] Command: {command} from {chat_id}")
                        response = handler(args)
                        if response:
                            self._send_message(chat_id, response)
                    else:
                        self._send_message(
                            chat_id,
                            f"❌ Unknown command: {command}\nType /help for available commands."
                        )
                        
        except Exception as e:
            log.error(f"[TG BOT] Error processing update: {e}")
    
    def _send_message(self, chat_id, message):
        """Send a message to Telegram"""
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            log.error(f"[TG BOT] Send error: {e}")
    
    # ============================================================
    # COMMAND HANDLERS
    # ============================================================
    
    def handle_status(self, args):
        """Handle /status command"""
        try:
            positions = len(self.bot.positions)
            capital = self.bot.capital
            pnl = self.bot.daily_pnl
            trades = len(self.bot.trades)
            uptime = self._get_uptime()
            
            status = (
                f"📊 <b>BOT STATUS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Capital:</b> ₹{capital:,.2f}\n"
                f"📈 <b>Today's P&L:</b> {'+' if pnl >= 0 else ''}₹{pnl:,.2f}\n"
                f"📊 <b>Open Positions:</b> {positions}\n"
                f"📋 <b>Total Trades:</b> {trades}\n"
                f"🔄 <b>Status:</b> {'🟢 Running' if self.bot.running else '🔴 Stopped'}\n"
                f"🌐 <b>WebSocket:</b> {'🟢 Connected' if self.bot.ws_connected else '🔴 Disconnected'}\n"
                f"⏰ <b>Uptime:</b> {uptime}\n"
                f"📊 <b>Market:</b> {self.bot.market_condition}\n"
                f"📋 <b>Scan:</b> {self.bot.scan_status}"
            )
            return status
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_health(self, args):
        """Handle /health command"""
        try:
            h = self.bot.health_status
            components = {
                'broker': '🟢' if h.get('broker') else '🔴',
                'market': '🟢' if h.get('market') else '🔴',
                'scanner': '🟢' if h.get('scanner') else '🔴',
                'sync': '🟢' if h.get('sync') else '🔴',
            }
            
            response = (
                f"💚 <b>HEALTH CHECK</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Overall:</b> {h.get('status', 'Unknown')}\n"
                f"\n"
                f"🔍 <b>Components:</b>\n"
                f"  {components['broker']} Broker\n"
                f"  {components['market']} Market\n"
                f"  {components['scanner']} Scanner\n"
                f"  {components['sync']} Sync\n"
                f"\n"
                f"❌ <b>Errors:</b> {h.get('error_count', 0)}\n"
                f"📋 <b>Last Error:</b> {h.get('last_error', 'None')[:100]}\n"
                f"🕐 <b>Last Heartbeat:</b> {h.get('last_heartbeat', 'Never')[:19]}"
            )
            return response
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_positions(self, args):
        """Handle /positions command"""
        try:
            positions = self.bot.positions
            if not positions:
                return "📊 <b>Open Positions</b>\n━━━━━━━━━━━━━━━━━━━━━\n✅ No open positions"
            
            response = f"📊 <b>Open Positions ({len(positions)})</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
            total_pnl = 0
            for i, pos in enumerate(positions, 1):
                symbol = pos.get('symbol', 'N/A')
                entry = pos.get('entry_price', 0)
                qty = pos.get('quantity', 0)
                current = self.bot.get_ltp(symbol) or entry
                pnl = (current - entry) * qty
                total_pnl += pnl
                pnl_pct = ((current - entry) / entry * 100) if entry > 0 else 0
                emoji = "✅" if pnl >= 0 else "❌"
                
                response += (
                    f"\n{i}. {emoji} <b>{symbol}</b>\n"
                    f"   Entry: ₹{entry:.2f} → ₹{current:.2f}\n"
                    f"   Qty: {qty} | P&L: {'+' if pnl >= 0 else ''}₹{pnl:.2f} ({pnl_pct:+.2f}%)\n"
                    f"   Target: ₹{pos.get('target_price', 0):.2f} | Stop: ₹{pos.get('stop_price', 0):.2f}"
                )
            
            response += f"\n\n💰 <b>Total P&L:</b> {'+' if total_pnl >= 0 else ''}₹{total_pnl:.2f}"
            return response
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_trades(self, args):
        """Handle /trades command"""
        try:
            trades = self.bot.trades[-5:] if self.bot.trades else []
            if not trades:
                return "📋 <b>Recent Trades</b>\n━━━━━━━━━━━━━━━━━━━━━\n✅ No trades yet"
            
            response = f"📋 <b>Recent Trades ({len(trades)})</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
            for i, trade in enumerate(reversed(trades), 1):
                symbol = trade.get('symbol', 'N/A')
                pnl = trade.get('net_pnl', 0)
                pnl_pct = trade.get('pnl_pct', 0)
                strategy = trade.get('strategy', 'N/A')
                emoji = "✅" if pnl >= 0 else "❌"
                
                response += (
                    f"\n{i}. {emoji} <b>{symbol}</b> ({strategy})\n"
                    f"   P&L: {'+' if pnl >= 0 else ''}₹{pnl:.2f} ({pnl_pct:+.2f}%)\n"
                )
            return response
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_scan(self, args):
        """Handle /scan command"""
        try:
            if not self.bot.running:
                return "❌ Bot is stopped. Use /start to start first."
            
            if self.bot.scan_manager.is_scanning:
                return "⏳ Scan already in progress. Please wait..."
            
            log.info("[TG BOT] Manual scan triggered")
            threading.Thread(target=self.bot._perform_scan, daemon=True).start()
            return "🔄 Scan started! Check back in a few seconds."
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_stop(self, args):
        """Handle /stop command"""
        try:
            if not self.bot.running:
                return "⏹️ Bot is already stopped."
            
            log.info("[TG BOT] Stop command received")
            self.bot.running = False
            return (
                "🛑 <b>Bot Stopped</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 P&L: {'+' if self.bot.daily_pnl >= 0 else ''}₹{self.bot.daily_pnl:.2f}\n"
                f"📊 Positions: {len(self.bot.positions)}\n"
                "\nUse /start to resume trading."
            )
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_start(self, args):
        """Handle /start command"""
        try:
            if self.bot.running:
                return "✅ Bot is already running."
            
            log.info("[TG BOT] Start command received")
            self.bot.running = True
            return (
                "🚀 <b>Bot Started</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 Capital: ₹{self.bot.capital:.2f}\n"
                f"📊 Positions: {len(self.bot.positions)}\n"
                "\nUse /status to check current status."
            )
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_websocket(self, args):
        """Handle /ws command"""
        try:
            status = "🟢 Connected" if self.bot.ws_connected else "🔴 Disconnected"
            return (
                f"🌐 <b>WebSocket Status</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>Status:</b> {status}\n"
                f"🔄 <b>Heartbeat:</b> {'Active' if self.bot.ws_heartbeat_running else 'Stopped'}\n"
                f"📈 <b>Symbols:</b> {len(self.bot.stock_list) if self.bot.stock_list else 0}\n"
                "\n💡 Use /reconnect to reconnect WebSocket."
            )
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_reconnect(self, args):
        """Handle /reconnect command"""
        try:
            log.info("[TG BOT] Reconnect command received")
            success = self.bot._reconnect_websocket()
            if success:
                return "🔄 <b>WebSocket Reconnected!</b>\n✅ Connection restored."
            else:
                return "❌ <b>WebSocket Reconnect Failed</b>\n⚠️ Check logs or use /restart."
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_ping(self, args):
        """Handle /ping command"""
        try:
            return (
                f"🏓 <b>PONG!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Bot is alive\n"
                f"🕐 {datetime.now().strftime('%I:%M:%S %p')}\n"
                f"🔄 {'🟢 Running' if self.bot.running else '🔴 Stopped'}\n"
                f"📊 Positions: {len(self.bot.positions)}\n"
                f"💰 P&L: {'+' if self.bot.daily_pnl >= 0 else ''}₹{self.bot.daily_pnl:.2f}"
            )
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_help(self, args):
        """Handle /help command"""
        return (
            "🤖 <b>ALPHA Bot Commands</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "📊 <b>Status:</b>\n"
            "  /status - Show bot status\n"
            "  /health - Health check\n"
            "  /positions - Open positions\n"
            "  /trades - Recent trades\n"
            "  /ws - WebSocket status\n"
            "  /ping - Check if alive\n"
            "\n"
            "⚙️ <b>Control:</b>\n"
            "  /scan - Force scan\n"
            "  /reconnect - Reconnect WS\n"
            "  /stop - Stop trading\n"
            "  /start - Resume trading\n"
            "  /reset - Reset circuit breaker\n"
            "  /logs - Recent logs\n"
            "\n"
            "❓ /help - Show this"
        )
    
    def handle_logs(self, args):
        """Handle /logs command"""
        try:
            log_file = "logs/paper_bot.log"
            if not os.path.exists(log_file):
                return "📋 No logs found."
            
            with open(log_file, 'r') as f:
                lines = f.readlines()[-15:]
            
            response = "📋 <b>Recent Logs</b>\n━━━━━━━━━━━━━━━━━━━━━\n"
            for line in lines:
                line = line.strip()[:120]
                if line:
                    response += f"{line}\n"
            return response
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_reset(self, args):
        """Handle /reset command - Reset circuit breaker"""
        try:
            self.bot.reset_circuit_breakers()
            return "🔄 <b>Circuit Breaker Reset</b>\n✅ API calls resumed."
        except Exception as e:
            return f"❌ Error: {e}"
    
    def handle_restart(self, args):
        """Handle /restart command"""
        try:
            log.info("[TG BOT] Restart command received")
            self.bot.running = False
            
            response = (
                "🔄 <b>Restarting Bot...</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⏳ Stopping current instance...\n"
                "🚀 Restarting..."
            )
            self._send_message(TELEGRAM_CHAT_ID, response)
            
            def restart_after_delay():
                time.sleep(3)
                import subprocess
                subprocess.Popen(
                    [sys.executable, 'src/angel_bot_v2.py'],
                    cwd=os.path.dirname(os.path.dirname(__file__))
                )
            
            threading.Thread(target=restart_after_delay, daemon=True).start()
            return None
        except Exception as e:
            return f"❌ Error: {e}"
    
    def _get_uptime(self):
        """Calculate bot uptime"""
        try:
            start = datetime.fromisoformat(self.bot.health_status.get('start_time', datetime.now().isoformat()))
            uptime = datetime.now() - start
            hours = uptime.seconds // 3600
            minutes = (uptime.seconds % 3600) // 60
            return f"{hours}h {minutes}m"
        except:
            return "Unknown"
    
    def stop(self):
        """Stop the Telegram bot handler"""
        self.running = False
        log.info("[TG BOT] Command handler stopped")

# ============================================================
# DATABASE FUNCTIONS WITH TRANSACTION MANAGEMENT
# ============================================================

def get_db_connection():
    """Get database connection with timeout"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def execute_db_transaction(operations: List) -> Tuple[bool, Any]:
    """Execute multiple database operations in a single transaction"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('BEGIN TRANSACTION')
        
        results = []
        for op in operations:
            if callable(op):
                result = op(cursor)
                results.append(result)
            elif isinstance(op, tuple) and len(op) == 2:
                cursor.execute(op[0], op[1])
                results.append(cursor.lastrowid)
            else:
                raise ValueError(f"Invalid operation: {op}")
        
        conn.commit()
        log.debug("[DB] Transaction committed successfully")
        return True, results
        
    except Exception as e:
        if conn:
            try:
                conn.rollback()
                log.debug("[DB] Transaction rolled back")
            except:
                pass
        log.error(f"[DB] Transaction failed: {e}")
        return False, str(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def init_database():
    """Initialize database with transaction management and indexes"""
    def create_tables(cursor):
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_price REAL,
                exit_price REAL,
                quantity INTEGER,
                gross_pnl REAL,
                net_pnl REAL,
                strategy TEXT,
                exit_reason TEXT,
                entry_time TEXT,
                exit_time TEXT,
                exit_type TEXT DEFAULT 'FULL'
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_exit_time ON trades(exit_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_price REAL,
                quantity INTEGER,
                strategy TEXT,
                entry_time TEXT,
                status TEXT DEFAULT 'OPEN'
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_symbol ON positions(symbol)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_positions_entry_time ON positions(entry_time)')
        
        return True
    
    success, result = execute_db_transaction([create_tables])
    if success:
        log.info("[DB] Database initialized successfully")
    else:
        log.error(f"[DB] Database initialization failed: {result}")


def backup_database() -> Optional[str]:
    """Create a backup of the database"""
    try:
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"trades_backup_{timestamp}.db")
        
        src = get_db_connection()
        dst = sqlite3.connect(backup_path)
        src.backup(dst)
        dst.close()
        src.close()
        
        log.info(f"[BACKUP] Created: {backup_path}")
        
        backups = sorted([f for f in os.listdir(backup_dir) if f.startswith('trades_backup_')])
        for old_backup in backups[:-10]:
            os.remove(os.path.join(backup_dir, old_backup))
            log.debug(f"[BACKUP] Removed old: {old_backup}")
        
        return backup_path
    except Exception as e:
        log.error(f"[BACKUP] Failed: {e}")
        return None


def cleanup_old_trades(days_to_keep: int = 365):
    """Delete trades older than specified days"""
    try:
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
        
        def delete_old_trades(cursor):
            cursor.execute('DELETE FROM trades WHERE DATE(entry_time) < ?', (cutoff_date,))
            return cursor.rowcount
        
        success, result = execute_db_transaction([delete_old_trades])
        if success:
            log.info(f"[DB] Cleaned up {result[0]} old trades")
        return success
    except Exception as e:
        log.error(f"[DB] Cleanup failed: {e}")
        return False


# ============================================================
# MAIN BOT CLASS
# ============================================================

class AngelTradingBot:
    """Main trading bot class with all improvements"""
    
    def __init__(self):
        # API Connection
        self.obj = None
        self.auth_token = None
        self.refresh_token = None
        self.feed_token = None
        self.sws = None
        
        # Trading State
        self.positions = []
        self.capital = CAPITAL
        self.available_capital = CAPITAL
        self.initial_capital = CAPITAL
        
        # Market Data
        self.live_prices = {}
        self.price_update_time = {}
        self.ws_connected = False
        
        # Symbol Mapping
        self.symbol_tokens = {}
        self.token_symbols = {}
        self.bulk_data_store = {}
        self.stock_list = []
        
        # Runtime State
        self.running = True
        self.lock = threading.RLock()
        self.trades = []
        self.daily_pnl = 0.0
        self.total_trades_today = 0
        self._last_scanner_check = datetime.now()
        self._circuit_breaker_reset_time = None
        
        # Risk Management
        self.sector_map = {}
        self.sector_exposure = {}
        self.correlation_matrix = {}
        self.atr_cache = {}
        self.volatility_cache = {}
        self.indicator_cache = {}
        self.last_cache_cleanup = datetime.now()
        
        # Scan Tracking
        self.scan_count = 0
        self.signals_found = 0
        self.stocks_scored = 0
        self.last_scan_time = None
        self.scan_status = "🔄 Bot Initializing..."
        self.market_condition = "Unknown"
        
        # Scan Manager
        self.scan_manager = ScanManager()
        
        # WebSocket Heartbeat
        self.ws_heartbeat_thread = None
        self.ws_heartbeat_running = False
        
        # Rate Limiter
        self.api_limiter = RateLimiter(max_calls=API_MAX_CALLS_PER_MINUTE, period=60)
        
        # Circuit Breakers - Less sensitive with auto-reset
        self.angel_api_circuit = CircuitBreaker(failure_threshold=10, recovery_timeout=30)
        self.yahoo_api_circuit = CircuitBreaker(failure_threshold=10, recovery_timeout=30)
        self.nsepython_api_circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=20)
        
        # Metrics
        self.metrics = MetricsCollector()
        
        # Bot State
        self.bot_state = BotState()
        
        # Error Recovery
        self.error_recovery = ErrorRecovery(self)

        # ===== TELEGRAM BOT COMMAND HANDLER =====
        self.telegram_bot = None

        # ===== ALERT FREQUENCY CONTROL =====
        self._last_alert_time = {}
        self._alert_cooldown = 60  # 60 seconds between same alerts

        # ===== PERFORMANCE TRACKING =====
        self._consecutive_wins = 0
        self._consecutive_losses = 0
        self._daily_profit_milestone_sent = False
        self._daily_loss_milestone_sent = False

        # Health status tracking for alerts
        self._last_health_status = {}
        self._market_open_sent = False
        self._market_closed_sent = False
        self._market_status_sent = False
        
        # ===== HEALTH MONITORING =====
        self.health_status = {
            'status': '🔄 Initializing...',
            'all_ok': False,
            'broker': False,
            'market': False,
            'scanner': False,
            'sync': False,
            'last_error': None,
            'error_count': 0,
            'last_heartbeat': datetime.now().isoformat(),
            'bot_running': True,
            'start_time': datetime.now().isoformat(),
            'metrics': {}
        }
        self._save_health_status()

    # ============================================================
    # HEALTH MONITORING METHODS
    # ============================================================
    
    def _save_health_status(self):
        """Save health status to file for dashboard"""
        try:
            self.health_status['metrics'] = self.metrics.get_summary()
            self.health_status['last_heartbeat'] = datetime.now().isoformat()
            
            with open('health_status.json', 'w') as f:
                json.dump(self.health_status, f, default=str, indent=2)
        except Exception as e:
            log.error(f"[HEALTH] Failed to save: {e}")
    
    def update_health(self, component: Optional[str] = None, status: Optional[bool] = None, 
                      error: Optional[str] = None):
        """Update health status for a component with enhanced alerts"""
        with self.lock:
            if component and status is not None:
                self.health_status[component] = status
                log.info(f"[HEALTH] {component}: {'✅ OK' if status else '❌ FAILED'}")
                
                # Skip market health alerts during off-hours
                if component == "market" and status == False:
                    # Don't send error alert - it's handled by market status
                    pass
                elif status == False:
                    send_health_alert(
                        component=component.capitalize(),
                        status="unhealthy",
                        error=f"{component} is not responding"
                    )
                elif status == True:
                    if component in self._last_health_status:
                        if self._last_health_status[component] == False:
                            send_health_alert(
                                component=component.capitalize(),
                                status="healthy"
                            )
                    self._last_health_status[component] = status
            
            if error:
                self.health_status['error_count'] += 1
                self.health_status['last_error'] = error
                self.health_status['status'] = '⚠️ System Error'
                log.error(f"[HEALTH] Error: {error}")
                
                # Only send error alerts for real errors (not market closed)
                if "market" not in error.lower():
                    send_system_alert(
                        "ERROR",
                        f"{error[:200]}",
                        "error"
                    )
            
            # Check overall health (skip market check for status message)
            components_ok = all([
                self.health_status.get('broker', False),
                self.health_status.get('scanner', False),
                self.health_status.get('sync', False),
                self.health_status.get('bot_running', True)
            ])
            
            market_ok = self.health_status.get('market', False)
            
            if not components_ok:
                self.health_status['status'] = '⚠️ System Degraded'
            elif market_ok:
                self.health_status['status'] = '✅ All systems operational'
            else:
                self.health_status['status'] = '💤 Waiting for market...'
            
            self.health_status['last_heartbeat'] = datetime.now().isoformat()
            self._save_health_status()

    # ============================================================
    # RESET CIRCUIT BREAKER
    # ============================================================
    
    def reset_circuit_breakers(self):
        """Manually reset all circuit breakers"""
        self.angel_api_circuit.reset()
        self.yahoo_api_circuit.reset()
        self.nsepython_api_circuit.reset()
        log.info("[CIRCUIT] All circuit breakers reset")
        send_telegram_alert("🔄 <b>All Circuit Breakers Reset</b>\nAPI calls resumed.", "success")

    # ============================================================
    # ALERT FREQUENCY CONTROL
    # ============================================================
    
    def send_alert_with_cooldown(self, alert_key: str, message: str, alert_type: str = "info") -> bool:
        """Send alert with cooldown to prevent spam"""
        now = time.time()
        if alert_key in self._last_alert_time:
            if now - self._last_alert_time[alert_key] < self._alert_cooldown:
                log.debug(f"[ALERT] Cooldown for {alert_key}")
                return False
        
        self._last_alert_time[alert_key] = now
        return send_telegram_alert(message, alert_type)

    # ============================================================
    # TRADE PERFORMANCE ALERTS
    # ============================================================
    
    def check_performance_alerts(self, pnl_pct: float, net_pnl: float):
        """Send alerts for performance milestones"""
        try:
            # 3 consecutive wins
            if pnl_pct >= 0:
                self._consecutive_wins += 1
                self._consecutive_losses = 0
                if self._consecutive_wins >= 3:
                    send_telegram_alert(
                        f"🔥 <b>3 Consecutive Wins!</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📈 Win Streak: {self._consecutive_wins}\n"
                        f"💰 Total P&L: ₹{self.daily_pnl:.2f}",
                        "profit"
                    )
                    self._consecutive_wins = 0
            else:
                self._consecutive_losses += 1
                self._consecutive_wins = 0
                if self._consecutive_losses >= 2:
                    send_telegram_alert(
                        f"⚠️ <b>2 Consecutive Losses</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📉 Loss Streak: {self._consecutive_losses}\n"
                        f"💰 Total P&L: ₹{self.daily_pnl:.2f}",
                        "warning"
                    )
                    self._consecutive_losses = 0
            
            # Daily profit milestone (₹500)
            if self.daily_pnl >= 500 and not self._daily_profit_milestone_sent:
                send_telegram_alert(
                    f"🎉 <b>Daily Profit Milestone!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 P&L: ₹{self.daily_pnl:.2f}\n"
                    f"📈 Trades: {len(self.trades)}",
                    "profit"
                )
                self._daily_profit_milestone_sent = True
            
            # Daily loss warning (₹200)
            if self.daily_pnl <= -200 and not self._daily_loss_milestone_sent:
                send_telegram_alert(
                    f"⚠️ <b>Daily Loss Warning</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 P&L: ₹{self.daily_pnl:.2f}\n"
                    f"📉 Trades: {len(self.trades)}",
                    "warning"
                )
                self._daily_loss_milestone_sent = True
                
        except Exception as e:
            log.error(f"[PERF ALERT] Error: {e}")

    # ============================================================
    # CACHE MANAGEMENT
    # ============================================================
    
    def cleanup_caches(self):
        """Clean up old cache entries to prevent memory leaks"""
        now = datetime.now()
        
        if (now - self.last_cache_cleanup).total_seconds() < CACHE_CLEANUP_INTERVAL:
            return
        
        with self.lock:
            cutoff = now - timedelta(hours=24)
            stale_keys = []
            for key, (cache_time, _) in self.indicator_cache.items():
                if cache_time < cutoff:
                    stale_keys.append(key)
            for key in stale_keys:
                del self.indicator_cache[key]
            
            if hasattr(self, 'bulk_data_store'):
                current_symbols = set(self.stock_list)
                for symbol in list(self.bulk_data_store.keys()):
                    if symbol not in current_symbols:
                        del self.bulk_data_store[symbol]
            
            if len(self.trades) > MAX_TRADES_HISTORY:
                self.trades = self.trades[-MAX_TRADES_HISTORY//2:]
            
            log.info(f"[CACHE] Cleaned: {len(stale_keys)} indicator entries, {len(self.trades)} trades kept")
            self.last_cache_cleanup = now

    # ============================================================
    # POSITION RECOVERY ON STARTUP
    # ============================================================
    
    def load_positions_from_db(self):
        """Load open positions from database on startup"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM positions WHERE status = "OPEN"')
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                log.info("[RECOVERY] No open positions found")
                return
            
            for row in rows:
                entry_time = datetime.fromisoformat(row['entry_time']) if row['entry_time'] else datetime.now()
                position = {
                    'symbol': row['symbol'],
                    'entry_price': row['entry_price'],
                    'quantity': row['quantity'],
                    'strategy': row['strategy'],
                    'entry_time': entry_time,
                    'peak_price': row['entry_price'],
                    'remaining_qty': row['quantity'],
                    'score': 8,
                    'profit_target': PROFIT_TARGETS.get(8, DEFAULT_PROFIT_TARGET),
                    'stop_loss': STOP_LOSS,
                    'target_price': row['entry_price'] * (1 + PROFIT_TARGETS.get(8, DEFAULT_PROFIT_TARGET)),
                    'stop_price': row['entry_price'] * (1 - STOP_LOSS),
                    'partial_exit_done': False,
                    'partial_exit_1_done': False,
                    'partial_exit_2_done': False,
                    'partial_exit_3_done': False
                }
                self.positions.append(position)
                log.info(f"[RECOVERY] Restored: {row['symbol']} @ ₹{row['entry_price']}")
            
            send_system_alert("INFO", f"Recovered {len(rows)} positions from database", "info")
            log.info(f"[RECOVERY] Loaded {len(rows)} positions")
        except Exception as e:
            log.error(f"[RECOVERY] Error: {e}")

    # ============================================================
    # SAVE POSITION TO DATABASE
    # ============================================================
    
    def save_position_to_db(self, position_data: Dict) -> bool:
        """Save position to database with transaction management"""
        required_fields = ['symbol', 'entry_price', 'quantity']
        for field in required_fields:
            if field not in position_data or position_data[field] is None:
                log.error(f"Missing required field: {field}")
                return False
        
        if position_data['quantity'] <= 0:
            log.error(f"Invalid quantity: {position_data['quantity']}")
            return False
        
        def save_position(cursor):
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    entry_price REAL,
                    quantity INTEGER,
                    strategy TEXT,
                    entry_time TEXT,
                    status TEXT DEFAULT 'OPEN'
                )
            ''')
            
            cursor.execute('''
                INSERT INTO positions (symbol, entry_price, quantity, strategy, entry_time, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                position_data.get('symbol', ''),
                position_data.get('entry_price', 0),
                position_data.get('quantity', 0),
                position_data.get('strategy', ''),
                position_data.get('entry_time', datetime.now().isoformat()),
                'OPEN'
            ))
            return cursor.lastrowid
        
        success, result = execute_db_transaction([save_position])
        if success:
            log.info(f"[DB] Position saved: {position_data.get('symbol')} (ID: {result[0]})")
            return True
        else:
            log.error(f"[DB] Error saving position: {result}")
            return False

    def update_position_status(self, symbol: str, status: str = 'CLOSED'):
        """Update position status with transaction management"""
        def update_status(cursor):
            cursor.execute('''
                UPDATE positions SET status = ? 
                WHERE symbol = ? AND status = 'OPEN'
                ORDER BY entry_time DESC LIMIT 1
            ''', (status, symbol))
            return cursor.rowcount
        
        success, result = execute_db_transaction([update_status])
        if success:
            log.info(f"[DB] Position updated: {symbol} -> {status} (Rows: {result[0]})")
        else:
            log.error(f"[DB] Error updating position: {result}")

    # ============================================================
    # SAVE TRADE
    # ============================================================
    
    def save_trade(self, trade_data: Dict) -> bool:
        """Save trade to database with transaction management"""
        required_fields = ['symbol', 'entry_price', 'exit_price', 'quantity']
        for field in required_fields:
            if field not in trade_data or trade_data[field] is None:
                log.error(f"Missing required field: {field}")
                return False
        
        if trade_data['quantity'] <= 0:
            log.error(f"Invalid quantity: {trade_data['quantity']}")
            return False
        
        def save_trade_record(cursor):
            cursor.execute('''
                INSERT INTO trades (
                    symbol, entry_price, exit_price, quantity,
                    gross_pnl, net_pnl, strategy, exit_reason,
                    entry_time, exit_time, exit_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_data.get('symbol', ''),
                trade_data.get('entry_price', 0),
                trade_data.get('exit_price', 0),
                trade_data.get('quantity', 0),
                trade_data.get('gross_pnl', 0),
                trade_data.get('net_pnl', 0),
                trade_data.get('strategy', ''),
                trade_data.get('exit_reason', ''),
                trade_data.get('entry_time', datetime.now().isoformat()),
                trade_data.get('exit_time', datetime.now().isoformat()),
                trade_data.get('exit_type', 'FULL')
            ))
            return cursor.lastrowid
        
        success, result = execute_db_transaction([save_trade_record])
        if success:
            log.info(f"[DB] Trade saved: {trade_data.get('symbol')} (ID: {result[0]})")
            return True
        else:
            log.error(f"[DB] Error saving trade: {result}")
            self.update_health(error=f"DB Error: {result}")
            return False

    # ============================================================
    # LOGIN
    # ============================================================

    @retry_with_backoff(max_retries=3, base_delay=5)
    def login(self) -> bool:
        """Login to Angel One with retry"""
        try:
            log.info("Connecting to Angel One SmartAPI...")
            
            self.api_limiter.wait_if_needed()
            
            self.obj = SmartConnect(api_key=API_KEY)
            totp = pyotp.TOTP(TOTP_SECRET).now()
            login_data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
            
            self.metrics.record_api_call(success=login_data.get('status', False))
            
            if login_data.get('status', False):
                self.auth_token = login_data['data']['jwtToken']
                self.refresh_token = login_data['data']['refreshToken']
                self.feed_token = self.obj.getfeedToken()
                log.info("[OK] API Connected")
                self.update_health('broker', True)
                send_system_alert(
                    "ONLINE",
                    f"Connected to Angel One\nClient: {CLIENT_CODE}\nTime: {datetime.now().strftime('%I:%M %p')}",
                    "success"
                )
                return True
            else:
                log.error(f"[FAIL] Login Failed: {login_data}")
                self.update_health('broker', False, error="Login failed")
                send_telegram_alert(f"❌ <b>Login Failed</b>\n{login_data}", "error")
                return False
        except Exception as e:
            log.error(f"[FAIL] Login Error: {e}")
            self.update_health('broker', False, error=f"Login error: {e}")
            send_telegram_alert(f"❌ <b>Login Error</b>\n{e}", "error")
            return False

    def load_symbol_tokens(self) -> Dict:
        """Load symbol tokens from scrip_master.json"""
        if not os.path.exists("scrip_master.json"):
            log.error("[CRITICAL] 'scrip_master.json' missing")
            self.update_health(error="scrip_master.json missing")
            return {}

        try:
            log.info("Extracting scrip entries from 'scrip_master.json'...")
            with open("scrip_master.json", "r") as f:
                instruments = json.load(f)
            
            symbol_map = {}
            token_map = {}
            
            for item in instruments:
                if not isinstance(item, dict):
                    continue
                
                exch = str(item.get('exch_seg', '')).strip().upper()
                if exch == 'NSE':
                    symbol = str(item.get('symbol', '')).strip()
                    token = str(item.get('token', '')).strip()
                    
                    if not symbol or not token:
                        continue
                        
                    raw_symbol = symbol.replace('-EQ', '').replace('-eq', '')
                    symbol_map[raw_symbol] = token
                    token_map[token] = raw_symbol
                        
            self.symbol_tokens = symbol_map
            self.token_symbols = token_map
            log.info(f"[OK] Instantiated {len(symbol_map)} local NSE Equity scrip mappings.")
            return symbol_map
        except Exception as e:
            log.error(f"[CRITICAL] Failed to read or parse local JSON file: {e}")
            self.update_health(error=f"Symbol load error: {e}")
            return {}

    # ============================================================
    # WEB SOCKET 2.0 WITH HEARTBEAT
    # ============================================================

    def start_websocket(self):
        """Start WebSocket connection with heartbeat"""
        def run_loop():
            try:
                self.sws = SmartWebSocketV2(
                    auth_token=self.auth_token,
                    api_key=API_KEY,
                    client_code=CLIENT_CODE,
                    feed_token=self.feed_token
                )
                self.sws.on_data = self.on_ws_data
                self.sws.on_open = self.on_ws_open
                self.sws.on_close = self.on_ws_close
                self.sws.on_error = self.on_ws_error
                self.sws.connect()
            except Exception as e:
                log.error(f"[WS] Error: {e}")
                self.update_health(error=f"WebSocket error: {e}")

        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        time.sleep(3)
        
        self.start_ws_heartbeat()

    def start_ws_heartbeat(self):
        """Start WebSocket heartbeat thread"""
        if self.ws_heartbeat_thread and self.ws_heartbeat_thread.is_alive():
            return
            
        self.ws_heartbeat_running = True
        self.ws_heartbeat_thread = threading.Thread(target=self._ws_heartbeat_loop, daemon=True)
        self.ws_heartbeat_thread.start()
        log.info(f"[WS] Heartbeat thread started (interval: {WS_HEARTBEAT_INTERVAL}s)")

    def stop_ws_heartbeat(self):
        """Stop WebSocket heartbeat thread"""
        self.ws_heartbeat_running = False
        if self.ws_heartbeat_thread:
            self.ws_heartbeat_thread.join(timeout=2)
        log.info("[WS] Heartbeat thread stopped")

    def _ws_heartbeat_loop(self):
        """Heartbeat loop to keep WebSocket connection alive"""
        while self.ws_heartbeat_running and self.running:
            try:
                time.sleep(WS_HEARTBEAT_INTERVAL)
                
                if not self.ws_connected:
                    log.warning("[WS] WebSocket disconnected during heartbeat")
                    continue
                
                if self.sws:
                    try:
                        log.debug("[WS] Heartbeat ping sent")
                        self.ws_connected = True
                    except AttributeError:
                        log.debug("[WS] Heartbeat check")
                        self.ws_connected = True
                        
            except Exception as e:
                log.error(f"[WS] Heartbeat error: {e}")
                self.update_health('broker', False, error=f"Heartbeat error: {e}")

    def on_ws_open(self, wsapp):
        log.info("[WS] Open Channel Established.")
        self.ws_connected = True
        if self.stock_list:
            log.info(f"[WS] Subscribing to {len(self.stock_list)} symbols...")
            self.subscribe_to_stocks()

    def on_ws_close(self, wsapp, close_status_code, close_msg):
        log.warning(f"[WS] Disconnected: {close_msg}")
        self.ws_connected = False
        self.update_health('broker', False, error=f"WS disconnected")
        send_telegram_alert(f"⚠️ <b>WebSocket Disconnected</b>\n{close_msg}", "warning")

    def on_ws_error(self, wsapp, error):
        log.error(f"[WS] Error: {error}")
        self.update_health('broker', False, error=f"WS error: {error}")

    def on_ws_data(self, wsapp, message):
        try:
            self.metrics.record_ws_message()
            data = json.loads(message) if isinstance(message, str) else message
            if isinstance(data, dict) and 'token' in data and 'last_traded_price' in data:
                token = str(data['token'])
                ltp = float(data['last_traded_price'])
                if ltp > 0:
                    ltp = ltp / 100.0
                    symbol = self.token_symbols.get(token)
                    if symbol:
                        with self.lock:
                            self.live_prices[symbol] = ltp
                            self.price_update_time[symbol] = datetime.now()
        except Exception:
            pass

    def subscribe_to_stocks(self):
        """Subscribe to stock prices via WebSocket"""
        if self.ws_connected and self.sws and self.stock_list:
            try:
                correlation_id = "bot_stream_01"
                tokens = [str(self.symbol_tokens[s]) for s in self.stock_list if s in self.symbol_tokens]
                if tokens:
                    payload = {
                        "correlationId": correlation_id,
                        "action": 1,
                        "mode": 1,
                        "tokenList": [{"exchangeType": 1, "tokens": tokens}]
                    }
                    self.sws.subscribe(correlation_id, mode=1, token_list=payload["tokenList"])
                    log.info(f"[WS] Subscribed to {len(tokens)} symbols")
            except Exception as e:
                log.error(f"[WS] Subscription error: {e}")

    def check_ws_health(self) -> bool:
        """Check WebSocket health and reconnect if needed"""
        if not self.ws_connected:
            log.warning("[WS] WebSocket disconnected. Reconnecting...")
            self.update_health('broker', False, error="WebSocket disconnected")
            return self._reconnect_websocket()
        
        if self.stock_list:
            stale_count = 0
            for symbol in self.stock_list[:5]:
                if symbol in self.price_update_time:
                    age = (datetime.now() - self.price_update_time[symbol]).total_seconds()
                    if age > 10:
                        stale_count += 1
            if stale_count >= 3:
                log.warning(f"[WS] {stale_count}/5 prices stale")
                self.update_health('broker', False, error=f"{stale_count}/5 prices stale")
                return self._reconnect_websocket()
        return True

    def _reconnect_websocket(self) -> bool:
        """Reconnect WebSocket with retry"""
        try:
            self.stop_ws_heartbeat()
            if self.sws:
                try:
                    self.sws.close_connection()
                except:
                    pass
            self.start_websocket()
            time.sleep(3)
            if self.ws_connected and self.stock_list:
                self.subscribe_to_stocks()
                log.info("[WS] Reconnected!")
                self.update_health('broker', True)
                return True
            return False
        except Exception as e:
            log.error(f"[WS] Reconnection error: {e}")
            return False

    # ============================================================
    # DATA SOURCE HIERARCHY - NSE PYTHON + ANGEL ONE + YAHOO
    # ============================================================

    def _fetch_from_nsepython(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch data from NSE Python library (TIER 1 - PRIMARY)"""
        try:
            if not NSEPYTHON_AVAILABLE:
                return None
            
            # Get live quote
            quote = nse.nse_eq_quote(symbol)
            if not quote:
                return None
            
            # Get historical data (last 5 days)
            end = datetime.now()
            start = end - timedelta(days=5)
            
            hist = nse.nse_eq_hist(symbol, start.strftime("%d-%m-%Y"), end.strftime("%d-%m-%Y"))
            
            if not hist:
                return None
            
            df = pd.DataFrame(hist)
            df = df.set_index('DATE')
            df.index = pd.to_datetime(df.index)
            
            df['Open'] = df['OPEN'].astype(float)
            df['High'] = df['HIGH'].astype(float)
            df['Low'] = df['LOW'].astype(float)
            df['Close'] = df['CLOSE'].astype(float)
            df['Volume'] = df['TOTTRDQTY'].astype(float)
            
            return df
            
        except Exception as e:
            log.warning(f"[NSEPYTHON] Failed for {symbol}: {e}")
            return None

    def _fetch_from_angel_one(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch data from Angel One API (TIER 2 - SECONDARY)"""
        try:
            self.api_limiter.wait_if_needed()
            
            token = self.symbol_tokens.get(symbol)
            if not token:
                log.warning(f"[ANGEL API] Token not found for {symbol}")
                return None
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
            fromdate = start_date.strftime("%Y-%m-%d")
            todate = end_date.strftime("%Y-%m-%d")
            
            # FIXED: Removed 'exchange=' parameter
            historical_data = self.angel_api_circuit.execute(
                self.obj.getCandleData,
                symboltoken=token,
                interval="FIFTEEN_MINUTE",
                fromdate=fromdate,
                todate=todate
            )
            
            self.metrics.record_api_call(success=True)
            
            if historical_data and historical_data.get('status') == True:
                data = self._parse_angel_historical_data(historical_data)
                if data is not None and not data.empty:
                    log.info(f"[ANGEL API] Successfully fetched data for {symbol}")
                    return data
            
            return None
            
        except Exception as e:
            self.metrics.record_api_call(success=False)
            log.warning(f"[ANGEL API] Failed for {symbol}: {e}")
            return None

    def _fetch_from_yahoo(self, symbol: str, period: str = "5d", interval: str = "15m") -> Optional[pd.DataFrame]:
        """Fetch data from Yahoo Finance (TIER 3 - FALLBACK)"""
        try:
            log.warning(f"[YAHOO FALLBACK] Fetching data for {symbol}...")
            
            stock = yf.Ticker(f"{symbol}.NS")
            data = stock.history(period=period, interval=interval)
            
            if not data.empty and len(data) > 10:
                data = data.tail(100).copy()
                log.info(f"[YAHOO FALLBACK] Successfully fetched data for {symbol}")
                return data
            
            return None
            
        except Exception as e:
            log.warning(f"[YAHOO FALLBACK] Failed for {symbol}: {e}")
            return None

    def get_indicator_data_resilient(self, symbol: str, period: str = "5d", interval: str = "15m"):
        """Fetch data with multi-tier fallback chain"""
        try:
            cache_key = f"{symbol}_{period}_{interval}"
            
            # Check cache first
            with self.lock:
                if cache_key in self.indicator_cache:
                    cache_time, data = self.indicator_cache[cache_key]
                    if (datetime.now() - cache_time).seconds < 60:
                        self.metrics.record_cache_hit(True)
                        return data
                self.metrics.record_cache_hit(False)
            
            # === TIER 1: NSE Python (Fast & Free) ===
            data = self.nsepython_api_circuit.execute(self._fetch_from_nsepython, symbol)
            if data is not None and len(data) > 10:
                with self.lock:
                    self.indicator_cache[cache_key] = (datetime.now(), data)
                log.info(f"[NSEPYTHON] Fetched data for {symbol}")
                return data
            
            # === TIER 2: Angel One API ===
            data = self._fetch_from_angel_one(symbol)
            if data is not None and not data.empty:
                with self.lock:
                    self.indicator_cache[cache_key] = (datetime.now(), data)
                log.info(f"[ANGEL API] Fetched data for {symbol}")
                return data
            
            # === TIER 3: Yahoo Finance ===
            data = self._fetch_from_yahoo(symbol, period, interval)
            if data is not None and not data.empty:
                with self.lock:
                    self.indicator_cache[cache_key] = (datetime.now(), data)
                log.info(f"[YAHOO] Fetched data for {symbol}")
                return data
            
            # === TIER 4: Bulk Data Store ===
            if symbol in self.bulk_data_store:
                log.warning(f"[EMERGENCY] Using bulk data for {symbol}")
                return self.bulk_data_store[symbol]
            
            log.warning(f"[FAILED] No data available for {symbol}")
            return None
            
        except Exception as e:
            log.error(f"[ERROR] get_indicator_data failed for {symbol}: {e}")
            return None

    # Keep original method for backward compatibility
    def get_indicator_data(self, symbol: str, period: str = "5d", interval: str = "15m"):
        """Legacy method - now uses resilient data fetching"""
        return self.get_indicator_data_resilient(symbol, period, interval)

    def _parse_angel_historical_data(self, historical_data):
        """Parse Angel One historical data response"""
        try:
            if not historical_data or 'data' not in historical_data:
                return None
            
            data_list = historical_data['data']
            
            if not data_list or len(data_list) < 2:
                return None
            
            df = pd.DataFrame(data_list, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            df['Open'] = df['Open'].astype(float)
            df['High'] = df['High'].astype(float)
            df['Low'] = df['Low'].astype(float)
            df['Close'] = df['Close'].astype(float)
            df['Volume'] = df['Volume'].astype(float)
            
            return df
            
        except Exception as e:
            log.error(f"[ERROR] Parsing Angel One historical data: {e}")
            return None

    # ============================================================
    # RISK MANAGEMENT
    # ============================================================

    def load_sector_mapping(self):
        """Load sector mapping for stocks"""
        self.sector_map = {
            "HDFCBANK": "BANKING", "ICICIBANK": "BANKING", "SBIN": "BANKING",
            "AXISBANK": "BANKING", "KOTAKBANK": "BANKING", "INDUSINDBK": "BANKING",
            "PNB": "BANKING", "BANKBARODA": "BANKING", "CANBK": "BANKING",
            "FEDERALBNK": "BANKING", "IDFCFIRSTB": "BANKING", "RBLBANK": "BANKING",
            "TCS": "IT", "INFY": "IT", "WIPRO": "IT", "HCLTECH": "IT",
            "TECHM": "IT", "LTTS": "IT", "MINDTREE": "IT", "PERSISTENT": "IT",
            "MPHASIS": "IT", "ZENSARTECH": "IT", "CYIENT": "IT",
            "HINDUNILVR": "FMCG", "ITC": "FMCG", "BRITANNIA": "FMCG",
            "NESTLEIND": "FMCG", "DABUR": "FMCG", "MARICO": "FMCG",
            "GODREJCP": "FMCG", "TATACONSUM": "FMCG", "JUBLFOOD": "FMCG",
            "MARUTI": "AUTO", "M&M": "AUTO", "TATAMOTORS": "AUTO",
            "ASHOKLEY": "AUTO", "HEROMOTOCO": "AUTO", "EICHERMOT": "AUTO",
            "BAJAJ-AUTO": "AUTO", "MOTHERSON": "AUTO", "EXIDEIND": "AUTO",
            "TATASTEEL": "METALS", "JSWSTEEL": "METALS", "HINDALCO": "METALS",
            "VEDL": "METALS", "SAIL": "METALS", "NATIONALUM": "METALS",
            "NMDC": "METALS", "JINDALSTEL": "METALS",
            "SUNPHARMA": "PHARMA", "DRREDDY": "PHARMA", "CIPLA": "PHARMA",
            "DIVISLAB": "PHARMA", "LUPIN": "PHARMA", "BIOCON": "PHARMA",
            "AUROPHARMA": "PHARMA", "TORNTPHARM": "PHARMA", "ZYDUSLIFE": "PHARMA",
            "RELIANCE": "OIL_GAS", "ONGC": "OIL_GAS", "BPCL": "OIL_GAS",
            "IOCL": "OIL_GAS", "HINDPETRO": "OIL_GAS", "GAIL": "OIL_GAS",
            "PETRONET": "OIL_GAS", "OIL": "OIL_GAS",
            "NTPC": "POWER", "POWERGRID": "POWER", "TATAPOWER": "POWER",
            "ADANIPOWER": "POWER", "NHPC": "POWER", "SJVN": "POWER",
            "PFC": "POWER", "RECLTD": "POWER", "IRFC": "POWER",
            "LT": "INFRA", "SIEMENS": "INFRA", "ABB": "INFRA", "BHEL": "INFRA",
            "L&T": "INFRA", "NCC": "INFRA", "GMRINFRA": "INFRA", "IRB": "INFRA",
            "BHARTIARTL": "TELECOM", "IDEA": "TELECOM",
            "TITAN": "CONSUMER", "BAJFINANCE": "FINANCE", "HDFCLIFE": "INSURANCE",
            "SBILIFE": "INSURANCE", "ICICIPRULI": "INSURANCE", "HDFCAMC": "FINANCE",
            "MUTHOOTFIN": "FINANCE", "CHOLAFIN": "FINANCE", "SRTRANSFIN": "FINANCE"
        }
        log.info(f"[SECTOR] Loaded {len(self.sector_map)} mappings")

    def get_sector(self, symbol: str) -> str:
        """Get sector for a symbol"""
        return self.sector_map.get(symbol, "OTHER")

    def check_correlation(self, symbol: str) -> bool:
        """Check if symbol is correlated with existing positions"""
        if not ENABLE_CORRELATION_FILTER or not self.positions:
            return True
        for pos in self.positions:
            corr = self.calculate_correlation(symbol, pos['symbol'])
            if corr > MAX_CORRELATION_THRESHOLD:
                log.info(f"[CORRELATION] {symbol} correlated with {pos['symbol']} ({corr:.2f})")
                return False
        return True

    def calculate_correlation(self, symbol1: str, symbol2: str) -> float:
        """Calculate correlation between two stocks"""
        try:
            data1 = self.bulk_data_store.get(symbol1)
            data2 = self.bulk_data_store.get(symbol2)
            if data1 is None or data2 is None:
                return 0.0
            close1 = data1['Close'].tail(20)
            close2 = data2['Close'].tail(20)
            if len(close1) < 10 or len(close2) < 10:
                return 0.0
            returns1 = close1.pct_change().dropna()
            returns2 = close2.pct_change().dropna()
            common_idx = returns1.index.intersection(returns2.index)
            if len(common_idx) < 5:
                return 0.0
            corr = returns1.loc[common_idx].corr(returns2.loc[common_idx])
            return abs(corr)
        except:
            return 0.0

    def check_sector_exposure(self, symbol: str) -> bool:
        """Check if sector exposure limit is exceeded"""
        if not ENABLE_SECTOR_DIVERSIFICATION:
            return True
        sector = self.get_sector(symbol)
        exposure = 0.0
        for pos in self.positions:
            if self.get_sector(pos['symbol']) == sector:
                exposure += pos['entry_price'] * pos['quantity']
        if exposure / self.capital > MAX_SECTOR_EXPOSURE:
            log.info(f"[SECTOR] {sector} exposure {exposure/self.capital*100:.1f}% > {MAX_SECTOR_EXPOSURE*100}%")
            return False
        return True

    def calculate_atr(self, symbol: str, period: int = 14) -> Optional[float]:
        """Calculate Average True Range"""
        try:
            data = self.bulk_data_store.get(symbol)
            if data is None or len(data) < period:
                return None
            high = data['High']; low = data['Low']; close = data['Close']
            tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
            return tr.rolling(period).mean().iloc[-1]
        except:
            return None

    def calculate_atr_position_size(self, symbol: str, price: float) -> Optional[int]:
        """Calculate position size based on ATR - FIXED with leverage"""
        if not ENABLE_ATR_POSITION_SIZING:
            return None
        atr = self.calculate_atr(symbol)
        if atr is None or atr == 0:
            return None
        # FIXED: Added LEVERAGE to position sizing
        qty = int(((self.capital * LEVERAGE) * 0.01) / (atr * 1.5))
        qty = max(1, qty)
        max_qty = int((self.available_capital * LEVERAGE) / price)
        return min(qty, max_qty)

    def get_dynamic_stop_loss(self, symbol: str, entry_price: float) -> float:
        """Calculate dynamic stop loss based on ATR"""
        if not ENABLE_DYNAMIC_SL:
            return STOP_LOSS
        atr = self.calculate_atr(symbol)
        if atr is None or atr == 0:
            return STOP_LOSS
        sl_pct = min(max((atr * 2) / entry_price, 0.01), 0.05)
        return sl_pct

    def get_volatility_stop(self, symbol: str, entry_price: float) -> float:
        """Calculate stop loss based on volatility"""
        if not ENABLE_VOLATILITY_STOP:
            return STOP_LOSS
        try:
            data = self.bulk_data_store.get(symbol)
            if data is None or len(data) < 20:
                return STOP_LOSS
            returns = data['Close'].pct_change().dropna()
            vol = returns.std() * math.sqrt(252)
            self.volatility_cache[symbol] = vol
            if vol > 0.50:
                return 0.03
            elif vol > 0.30:
                return 0.025
            return 0.02
        except:
            return STOP_LOSS

    # ============================================================
    # GET LTP WITH FALLBACK
    # ============================================================

    def get_ltp(self, symbol: str) -> Optional[float]:
        """Get Last Traded Price with fallback"""
        with self.lock:
            if symbol in self.live_prices:
                if symbol in self.price_update_time:
                    age = (datetime.now() - self.price_update_time[symbol]).total_seconds()
                    if age < 5:
                        return self.live_prices[symbol]
        
        # Try NSE Python
        if NSEPYTHON_AVAILABLE:
            try:
                quote = nse.nse_eq_quote(symbol)
                if quote and quote.get('lastPrice'):
                    price = float(quote['lastPrice'])
                    with self.lock:
                        self.live_prices[symbol] = price
                        self.price_update_time[symbol] = datetime.now()
                    return price
            except:
                pass
        
        # Try Yahoo Finance
        try:
            stock = yf.Ticker(f"{symbol}.NS")
            data = stock.history(period="1d", interval="1m")
            if not data.empty:
                price = float(data['Close'].iloc[-1])
                with self.lock:
                    self.live_prices[symbol] = price
                    self.price_update_time[symbol] = datetime.now()
                return price
        except:
            pass
        
        # Check bulk data store
        if symbol in self.bulk_data_store:
            df = self.bulk_data_store[symbol]
            if df is not None and not df.empty:
                try:
                    return float(df['Close'].iloc[-1])
                except:
                    pass
        
        # Check positions
        for pos in self.positions:
            if pos['symbol'] == symbol:
                return pos['entry_price']
        
        return None

    # ============================================================
    # SQUARE OFF & RISK CHECKS
    # ============================================================

    def check_and_square_off(self) -> bool:
        """Check if market is closing and square off positions"""
        now = datetime.now()
        current_time = now.time()
        close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
        square_off_time = (datetime.combine(now.date(), close_time) - timedelta(minutes=SQUARE_OFF_BUFFER)).time()
        
        if current_time >= square_off_time or current_time >= close_time:
            if self.positions:
                log.info(f"[SQUARE OFF] Squaring {len(self.positions)} positions...")
                for position in self.positions[:]:
                    symbol = position['symbol']
                    qty = position['quantity']
                    current_price = self.get_ltp(symbol) or position['entry_price']
                    self.place_order(symbol, "SELL", qty, current_price)
                    self.update_position_status(symbol, 'CLOSED')
                    log.info(f"[SQUARE OFF] {symbol}: ₹{position['entry_price']:.2f} → ₹{current_price:.2f}")
                self.generate_daily_summary()
                self.running = False
                self.update_health('bot_running', False)
                send_telegram_alert(f"🛑 <b>Market Closed - Squared Off</b>\n{len(self.positions)} positions closed", "shutdown")
                return True
            log.info("[SQUARE OFF] No positions to close.")
            self.running = False
            self.update_health('bot_running', False)
            return True
        return False

    def check_daily_loss_limit(self) -> bool:
        """Check if daily loss limit is exceeded"""
        daily_loss_pct = (self.initial_capital - self.capital) / self.initial_capital
        if daily_loss_pct >= MAX_DAILY_LOSS:
            log.warning(f"[RISK] Max daily loss reached: {daily_loss_pct*100:.2f}%")
            self.scan_status = "⛔ Stopped - Max Daily Loss"
            self.running = False
            self.update_health('bot_running', False, error=f"Max daily loss: {daily_loss_pct*100:.2f}%")
            send_telegram_alert(f"⛔ <b>Max Daily Loss Reached</b>\n{daily_loss_pct*100:.2f}%\nBot stopped.", "error")
            return True
        return False

    # ============================================================
    # ORDER FUNCTIONS
    # ============================================================

    def place_order(self, symbol: str, transaction_type: str, quantity: int = 1, price: Optional[float] = None) -> Optional[str]:
        """Place a paper trade order"""
        try:
            if price is None:
                price = self.get_ltp(symbol)
                if not price:
                    log.error(f"[FAIL] No price for {symbol}")
                    return None
            order_id = f"PAPER_{datetime.now().strftime('%H%M%S')}_{symbol}"
            margin = (price * quantity) / LEVERAGE
            with self.lock:
                if transaction_type == "BUY":
                    self.available_capital -= margin
                else:
                    self.available_capital += margin
            log.info(f"📝 [ORDER] {transaction_type} {symbol} Qty:{quantity} @ ₹{price:.2f} Margin: ₹{margin:.2f}")
            return order_id
        except Exception as e:
            log.error(f"[ORDER FAILURE] {e}")
            self.update_health(error=f"Order failed: {e}")
            return None

    def is_trading_time(self) -> bool:
        """Check if currently within trading hours"""
        now = datetime.now()
        current_time = now.time()
        trading_start_time = datetime.strptime(TRADING_START, "%H:%M").time()
        market_close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
        square_off_time = (datetime.combine(now.date(), market_close_time) - timedelta(minutes=SQUARE_OFF_BUFFER)).time()
        return trading_start_time <= current_time < square_off_time

    # ============================================================
    # MARKET DATA FUNCTIONS
    # ============================================================

    def update_bulk_market_data(self):
        """Update bulk market data from Yahoo Finance"""
        if not self.stock_list:
            return
        try:
            tickers = [f"{s}.NS" for s in self.stock_list]
            data = yf.download(tickers=tickers, period="5d", interval="15m", group_by="ticker", progress=False)
            with self.lock:
                for symbol in self.stock_list:
                    ticker_name = f"{symbol}.NS"
                    if len(tickers) == 1:
                        self.bulk_data_store[symbol] = data.dropna()
                    elif ticker_name in data.columns.levels[0]:
                        self.bulk_data_store[symbol] = data[ticker_name].dropna()
            log.info(f"[MARKET] Sync completed")
        except Exception as e:
            log.error(f"[MARKET] Error: {e}")
            self.update_health(error=f"Market data error: {e}")

    def calculate_vwap(self, data):
        """Calculate Volume Weighted Average Price"""
        try:
            df = data.copy()
            df['Cum_Vol_Price'] = ((df['High'] + df['Low'] + df['Close']) / 3.0) * df['Volume']
            dates = df.index.date
            df['CumVolPrice_Sum'] = df.groupby(dates)['Cum_Vol_Price'].cumsum()
            df['Cum_Vol_Sum'] = df.groupby(dates)['Volume'].cumsum()
            return df['CumVolPrice_Sum'] / df['Cum_Vol_Sum']
        except:
            return None

    def detect_orb(self, symbol: str, data):
        """Detect Opening Range Breakout"""
        try:
            if data is None or data.empty:
                return None, "NEUTRAL", 0
            today = datetime.now().date()
            today_bars = data[data.index.date == today]
            if today_bars.empty:
                return None, "NEUTRAL", 0
            session_start = datetime.combine(today, datetime_time(9, 15))
            orb_end = session_start + timedelta(minutes=ORB_MINUTES)
            orb_window = today_bars[today_bars.index < orb_end]
            if orb_window.empty:
                return None, "NEUTRAL", 0
            orb_high = orb_window['High'].max()
            current_price = today_bars['Close'].iloc[-1]
            prev_price = today_bars['Close'].iloc[-2] if len(today_bars) > 1 else current_price
            if current_price > orb_high and prev_price <= orb_high:
                return current_price, "LONG", 9
        except:
            pass
        return None, "NEUTRAL", 0

    def get_market_condition(self) -> str:
        """Get current market condition"""
        try:
            nifty = yf.Ticker("^NSEI")
            data = nifty.history(period="5d", interval="15m")
            if not data.empty:
                close = data['Close']
                current = close.iloc[-1]
                prev_close = close.iloc[-2] if len(close) > 1 else current
                change = ((current - prev_close) / prev_close) * 100
                sma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else current
                if current > sma20 * 1.005:
                    return f"📈 UPTREND ({change:+.2f}%)"
                elif current < sma20 * 0.995:
                    return f"📉 DOWNTREND ({change:+.2f}%)"
                else:
                    return f"➡️ CONSOLIDATION ({change:+.2f}%)"
        except:
            pass
        return "❓ UNKNOWN"
    
    def is_market_tradeable(self) -> bool:
        """Check if market is tradeable based on conditions"""
        try:
            nifty = yf.Ticker("^NSEI")
            data = nifty.history(period="2d", interval="5m")
            if len(data) < 20:
                return True
            close = data['Close']
            sma20 = close.rolling(20).mean().iloc[-1]
            sma50 = close.rolling(50).mean().iloc[-1]
            return close.iloc[-1] > sma20 and sma20 > sma50
        except:
            return True

    def get_hold_minutes(self, score: int, strategy: str) -> int:
        """Get dynamic hold duration based on score and strategy"""
        # High confidence - hold till market close
        if score >= 9:
            return 9999  # Till market close
        elif score >= 8:
            return 180  # 3 hours
        elif score >= 7:
            return MAX_HOLD_MINUTES  # 90 minutes
        else:
            return 30  # 30 minutes

    def score_stock(self, symbol: str):
        """Score a stock for potential trade"""
        try:
            data = self.get_indicator_data(symbol)
            if data is None or len(data) < 5:
                return 0, "NEUTRAL", "NONE"
            if ENABLE_MARKET_FILTER and not self.is_market_tradeable():
                return 0, "NEUTRAL", "NONE"
            close = data['Close']
            current_price = close.iloc[-1]
            vwap_series = self.calculate_vwap(data)
            vwap_val = vwap_series.iloc[-1] if vwap_series is not None and len(vwap_series) > 0 else current_price
            ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
            avg_volume = data['Volume'].rolling(20).mean().iloc[-1]
            current_volume = data['Volume'].iloc[-1]
            if current_volume < avg_volume * 0.5:
                return 0, "NEUTRAL", "NONE"
            if current_price > vwap_val and current_price > ema20:
                orb_p, orb_dir, orb_score = self.detect_orb(symbol, data)
                if orb_dir == "LONG":
                    return orb_score, "LONG", "ORB"
                return 8, "LONG", "Trend-Follow"
        except:
            pass
        return 0, "NEUTRAL", "NONE"

    # ============================================================
    # STOCK FETCHING - DYNAMIC UNIVERSE
    # ============================================================
    
    def fetch_all_stocks(self):
        """Fetch all stocks to scan - Dynamic universe with multi-source"""
        log.info("="*60)
        log.info("📊 FETCHING DYNAMIC STOCK UNIVERSE")
        log.info("="*60)
        
        # Try NSE Python first
        if NSEPYTHON_AVAILABLE:
            try:
                log.info("[NSEPYTHON] Fetching NIFTY 500 stocks...")
                # Get NIFTY 500 symbols
                symbols = nse.nse_eq_symbols()
                if symbols and len(symbols) > 100:
                    stocks = self._filter_stocks(symbols[:300])
                    if stocks and len(stocks) > 50:
                        log.info(f"✅ Loaded {len(stocks)} stocks from NSE Python")
                        return stocks
            except Exception as e:
                log.warning(f"[NSEPYTHON] Failed to fetch stock list: {e}")
        
        # Fallback to Angel One scrip_master
        try:
            log.info("[ANGEL] Fetching stocks from scrip_master.json...")
            if self.symbol_tokens:
                symbols = list(self.symbol_tokens.keys())[:200]
                stocks = self._filter_stocks(symbols)
                if stocks and len(stocks) > 50:
                    log.info(f"✅ Loaded {len(stocks)} stocks from Angel One")
                    return stocks
        except Exception as e:
            log.warning(f"[ANGEL] Failed to fetch stocks: {e}")
        
        # Final fallback - Yahoo with expanded list
        log.warning("[FALLBACK] Using expanded Yahoo list")
        return self._fetch_from_yahoo_expanded()

    def _filter_stocks(self, symbols: List[str]) -> List[Dict]:
        """Filter stocks by price and liquidity"""
        stocks = []
        for symbol in symbols:
            try:
                # Check if token exists
                token = self.symbol_tokens.get(symbol)
                if not token:
                    continue
                
                # Get price
                price = self.get_ltp(symbol)
                if not price:
                    continue
                
                if MIN_STOCK_PRICE < price < MAX_STOCK_PRICE:
                    stocks.append({
                        'symbol': symbol,
                        'token': token,
                        'price': price
                    })
            except:
                continue
        
        # Sort by price (optional) and limit
        stocks.sort(key=lambda x: x['price'], reverse=True)
        return stocks[:MAX_STOCKS_TO_SCAN]

    def _fetch_from_yahoo_expanded(self) -> List[Dict]:
        """Expanded Yahoo Finance fallback"""
        fallback_symbols = [
            # NIFTY 50
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
            "LT", "AXISBANK", "WIPRO", "MARUTI", "TITAN",
            "TECHM", "NTPC", "ULTRACEMCO", "M&M", "BAJFINANCE",
            "SUNPHARMA", "POWERGRID", "NESTLEIND", "HCLTECH",
            "JSWSTEEL", "ADANIPORTS", "ONGC", "COALINDIA", "HDFCLIFE",
            # NIFTY Next 50
            "ADANIENT", "VEDL", "TATAMOTORS", "TATASTEEL", "BAJAJFINSV",
            "HEROMOTOCO", "HINDALCO", "EICHERMOT", "GRASIM", "SHREECEM",
            "DIVISLAB", "DRREDDY", "CIPLA", "BAJAJ-AUTO", "HDFC",
            "SBILIFE", "ICICIPRULI", "HDFCAMC", "MUTHOOTFIN", "CHOLAFIN",
            # NIFTY Midcap 100
            "ZOMATO", "PAYTM", "IDEA", "PNB", "BANKBARODA",
            "CANBK", "FEDERALBNK", "IDFCFIRSTB", "RBLBANK", "AUROPHARMA",
            "TORNTPHARM", "ZYDUSLIFE", "LUPIN", "BIOCON", "TORNTPOWER",
            "TATAPOWER", "ADANIPOWER", "NHPC", "PFC", "RECLTD",
            "GAIL", "PETRONET", "HINDPETRO", "BPCL", "IOCL"
        ]
        
        stocks = []
        for symbol in fallback_symbols:
            if symbol in self.symbol_tokens:
                price = self.get_ltp(symbol)
                if price and MIN_STOCK_PRICE < price < MAX_STOCK_PRICE:
                    stocks.append({
                        'symbol': symbol,
                        'token': self.symbol_tokens[symbol],
                        'price': price
                    })
        
        log.info(f"[FALLBACK] Loaded {len(stocks)} stocks")
        return stocks

    # Keep original method for backward compatibility
    def fetch_from_yahoo_fallback(self):
        return self._fetch_from_yahoo_expanded()

    def get_prices_bulk(self, symbols):
        """Get bulk prices for symbols"""
        all_stocks = []
        limit_symbols = symbols[:500]
        log.info(f"📊 Analyzing {len(limit_symbols)} stocks...")
        stock_data = []
        try:
            tickers = [f"{s}.NS" for s in limit_symbols]
            data = yf.download(tickers=tickers, period="5d", interval="15m", progress=False)
            if data.empty:
                log.warning("No data from Yahoo")
                return []
            for symbol in limit_symbols:
                try:
                    ticker_name = f"{symbol}.NS"
                    if len(limit_symbols) == 1:
                        price = float(data['Close'].iloc[-1]) if not data.empty else None
                        volume = float(data['Volume'].iloc[-1]) if not data.empty else 0
                    else:
                        if ticker_name in data['Close'].columns:
                            price = float(data['Close'][ticker_name].iloc[-1])
                            volume = float(data['Volume'][ticker_name].iloc[-1])
                        else:
                            continue
                    if not (MIN_STOCK_PRICE < price < MAX_STOCK_PRICE):
                        continue
                    token = self.symbol_tokens.get(symbol)
                    if not token:
                        continue
                    # Calculate volume ratio
                    try:
                        volume_series = data[ticker_name]['Volume'] if ticker_name in data.columns.levels[0] else pd.Series()
                        if len(volume_series) > 5:
                            avg_volume = volume_series.tail(5).mean()
                            volume_ratio = volume / avg_volume if avg_volume > 0 else 1
                        else:
                            volume_ratio = 1
                    except:
                        volume_ratio = 1
                    # Calculate volatility
                    try:
                        close_prices = data[ticker_name]['Close'] if ticker_name in data.columns.levels[0] else pd.Series()
                        if len(close_prices) > 1:
                            volatility = close_prices.pct_change().dropna().std() * 100
                        else:
                            volatility = 0
                    except:
                        volatility = 0
                    # Tier & score
                    if price > 500 and volume > 500000:
                        tier = "Large"; base_score = 70
                    elif price > 100 and volume > 200000:
                        tier = "Mid"; base_score = 85
                    elif price > 50 and volume > 100000:
                        tier = "Small"; base_score = 100
                    else:
                        continue
                    score = base_score
                    if volume_ratio >= 5.0:
                        score += 30
                    elif volume_ratio >= 4.0:
                        score += 25
                    elif volume_ratio >= 3.0:
                        score += 18
                    elif volume_ratio >= 2.0:
                        score += 10
                    if 2 <= volatility <= 5:
                        score += 15
                    try:
                        close_prices = data[ticker_name]['Close'] if ticker_name in data.columns.levels[0] else pd.Series()
                        if len(close_prices) > 20:
                            sma20 = close_prices.tail(20).mean()
                            if price > sma20:
                                score += 10
                    except:
                        pass
                    score = min(score, 100)
                    stock_data.append({
                        'symbol': symbol, 'token': token, 'price': price,
                        'volume': volume, 'volume_ratio': volume_ratio,
                        'volatility': volatility, 'tier': tier, 'score': score
                    })
                except:
                    continue
            if not stock_data:
                log.warning("No surge candidates. Falling back.")
                return self.get_prices_fallback(symbols)
            stock_data.sort(key=lambda x: x['score'], reverse=True)
            selected = stock_data[:MAX_STOCKS_TO_SCAN]
            tier_counts = {"Large": 0, "Mid": 0, "Small": 0}
            final_selected = []
            for stock in selected:
                if tier_counts[stock['tier']] < MAX_STOCKS_TO_SCAN // 5:
                    final_selected.append(stock)
                    tier_counts[stock['tier']] += 1
            remaining_slots = MAX_STOCKS_TO_SCAN - len(final_selected)
            for stock in selected:
                if stock not in final_selected and remaining_slots > 0:
                    final_selected.append(stock)
                    remaining_slots -= 1
            for stock in final_selected:
                all_stocks.append({'symbol': stock['symbol'], 'token': stock['token'], 'price': stock['price']})
            log.info(f"✅ Selected {len(final_selected)} stocks")
        except Exception as e:
            log.error(f"Error: {e}")
            return self.get_prices_fallback(symbols)
        if not all_stocks:
            log.warning("No stocks passed. Falling back.")
            return self.get_prices_fallback(symbols)
        return all_stocks

    def get_prices_fallback(self, symbols):
        """Fallback method to get prices"""
        all_stocks = []
        limit_symbols = symbols[:MAX_STOCKS_TO_SCAN]
        log.info(f"📊 Fallback: {len(limit_symbols)} stocks...")
        for symbol in limit_symbols:
            try:
                token = self.symbol_tokens.get(symbol)
                stock = yf.Ticker(f"{symbol}.NS")
                data = stock.history(period="1d")
                if not data.empty:
                    price = float(data['Close'].iloc[-1])
                    if MIN_STOCK_PRICE < price < MAX_STOCK_PRICE:
                        all_stocks.append({'symbol': symbol, 'token': token, 'price': price})
            except:
                continue
        return all_stocks

    def calculate_estimated_charges(self, entry_price: float, exit_price: float, quantity: int) -> float:
        """Calculate estimated brokerage charges"""
        turnover = (entry_price + exit_price) * quantity
        buy_brokerage = min(20.0, entry_price * quantity * 0.0003)
        sell_brokerage = min(20.0, exit_price * quantity * 0.0003)
        total_brokerage = buy_brokerage + sell_brokerage
        stt = exit_price * quantity * 0.00025
        txn_charges = turnover * 0.0000322
        gst = (total_brokerage + txn_charges) * 0.18
        sebi_fee = turnover * 0.000001
        stamp_duty = entry_price * quantity * 0.00003
        return total_brokerage + stt + txn_charges + gst + sebi_fee + stamp_duty

    # ============================================================
    # CHECK EXITS WITH ENHANCED TELEGRAM ALERTS
    # ============================================================

    def check_exits(self):
        """Check and execute exits for all positions"""
        try:
            for position in self.positions[:]:
                symbol = position['symbol']
                current_price = self.get_ltp(symbol)
                if not current_price:
                    continue
                    
                entry_price = position['entry_price']
                qty = position['quantity']
                pnl_pct = (current_price - entry_price) / entry_price
                raw_pnl = (current_price - entry_price) * qty
                profit_target = position.get('profit_target', DEFAULT_PROFIT_TARGET)
                position_score = position.get('score', 0)
                
                if current_price > position['peak_price']:
                    position['peak_price'] = current_price
                
                trailing_sl_price = position['peak_price'] * (1.0 - TRAILING_SL_PULLBACK)
                trailing_activated = (position['peak_price'] - entry_price) / entry_price >= TRAILING_SL_ACTIVATION
                
                exit_triggered = False
                exit_reason = ""
                exit_type = "FULL"
                exit_qty = qty
                
                if pnl_pct >= profit_target:
                    exit_triggered = True
                    exit_reason = f"🎯 [TARGET] +{pnl_pct*100:.2f}%"
                    exit_type = "FULL"
                    exit_qty = qty
                
                elif ENABLE_PARTIAL_EXIT and not position.get('partial_exit_done', False):
                    if pnl_pct >= PARTIAL_EXIT_LEVEL_1 and pnl_pct < profit_target:
                        exit_triggered = True
                        exit_qty = int(qty * 0.33)
                        exit_reason = f"📊 [PARTIAL 1] +{pnl_pct*100:.2f}%"
                        exit_type = "PARTIAL"
                        position['partial_exit_done'] = True
                        position['partial_exit_1_done'] = True
                    elif pnl_pct >= PARTIAL_EXIT_LEVEL_2 and position.get('partial_exit_1_done', False) and not position.get('partial_exit_2_done', False):
                        exit_triggered = True
                        exit_qty = int(qty * 0.33)
                        exit_reason = f"📊 [PARTIAL 2] +{pnl_pct*100:.2f}%"
                        exit_type = "PARTIAL"
                        position['partial_exit_2_done'] = True
                    elif pnl_pct >= PARTIAL_EXIT_LEVEL_3 and position.get('partial_exit_2_done', False) and not position.get('partial_exit_3_done', False):
                        exit_triggered = True
                        exit_qty = position.get('remaining_qty', qty)
                        exit_reason = f"📊 [PARTIAL 3] +{pnl_pct*100:.2f}%"
                        exit_type = "PARTIAL"
                        position['partial_exit_3_done'] = True
                
                elif trailing_activated and current_price <= trailing_sl_price:
                    exit_triggered = True
                    peak_profit = ((position['peak_price'] - entry_price) / entry_price) * 100
                    exit_reason = f"🛡️ [TRAILING SL] +{pnl_pct*100:.2f}% (Peak: +{peak_profit:.2f}%)"
                    exit_type = "FULL"
                    exit_qty = qty
                
                elif pnl_pct <= -STOP_LOSS:
                    exit_triggered = True
                    exit_reason = f"🛑 [STOP LOSS] {pnl_pct*100:.2f}%"
                    exit_type = "FULL"
                    exit_qty = qty
                
                # === DYNAMIC TIME EXIT - FIXED ===
                elapsed_minutes = (datetime.now() - position['entry_time']).total_seconds() / 60
                max_hold = self.get_hold_minutes(position_score, position.get('strategy', 'Trend-Follow'))
                
                if elapsed_minutes > max_hold:
                    exit_triggered = True
                    exit_reason = f"⏳ [TIME EXIT] +{pnl_pct*100:.2f}% ({elapsed_minutes:.0f}m)"
                    exit_type = "FULL"
                    exit_qty = qty
                
                if exit_triggered:
                    if exit_type == "PARTIAL":
                        self.place_order(symbol, "SELL", exit_qty, current_price)
                        position['quantity'] -= exit_qty
                        position['remaining_qty'] = position['quantity']
                        charges = self.calculate_estimated_charges(entry_price, current_price, exit_qty)
                        net_pnl = raw_pnl - charges
                        with self.lock:
                            self.capital += net_pnl
                            self.daily_pnl += net_pnl
                            self.total_trades_today += 1
                            self.trades.append({
                                'symbol': symbol, 'gross_pnl': raw_pnl, 'charges': charges,
                                'net_pnl': net_pnl, 'pnl_pct': pnl_pct, 'strategy': position['strategy'],
                                'exit_status': "Partial Exit", 'entry_price': entry_price,
                                'exit_price': current_price, 'exit_time': datetime.now().isoformat(),
                                'exit_type': 'PARTIAL'
                            })
                            self.save_trade({
                                'symbol': symbol, 'entry_price': entry_price, 'exit_price': current_price,
                                'quantity': exit_qty, 'gross_pnl': raw_pnl, 'net_pnl': net_pnl,
                                'strategy': position.get('strategy', ''), 'exit_reason': exit_reason,
                                'entry_time': position.get('entry_time', datetime.now()).isoformat(),
                                'exit_time': datetime.now().isoformat(), 'exit_type': 'PARTIAL'
                            })
                        log.info(f"{exit_reason} | Qty: {exit_qty} | Net: ₹{net_pnl:.2f}")
                        position['peak_price'] = current_price
                        
                        send_trade_alert(
                            symbol=symbol,
                            entry_price=entry_price,
                            quantity=exit_qty,
                            score=position.get('score', 0),
                            strategy=position.get('strategy', ''),
                            target_price=position.get('target_price', 0),
                            stop_price=position.get('stop_price', 0),
                            pnl=net_pnl,
                            exit_reason=exit_reason,
                            alert_type="CLOSE"
                        )
                    else:
                        self.place_order(symbol, "SELL", exit_qty, current_price)
                        charges = self.calculate_estimated_charges(entry_price, current_price, exit_qty)
                        net_pnl = raw_pnl - charges
                        with self.lock:
                            self.capital += net_pnl
                            self.daily_pnl += net_pnl
                            self.total_trades_today += 1
                            self.trades.append({
                                'symbol': symbol, 'gross_pnl': raw_pnl, 'charges': charges,
                                'net_pnl': net_pnl, 'pnl_pct': pnl_pct, 'strategy': position['strategy'],
                                'exit_status': "Target Hit" if pnl_pct > 0 else "Stop Loss",
                                'entry_price': entry_price, 'exit_price': current_price,
                                'exit_time': datetime.now().isoformat(), 'exit_type': 'FULL'
                            })
                            self.save_trade({
                                'symbol': symbol, 'entry_price': entry_price, 'exit_price': current_price,
                                'quantity': exit_qty, 'gross_pnl': raw_pnl, 'net_pnl': net_pnl,
                                'strategy': position.get('strategy', ''), 'exit_reason': exit_reason,
                                'entry_time': position.get('entry_time', datetime.now()).isoformat(),
                                'exit_time': datetime.now().isoformat(), 'exit_type': 'FULL'
                            })
                            self.update_position_status(symbol, 'CLOSED')
                            self.positions.remove(position)
                        log.info(f"{exit_reason} | Net: ₹{net_pnl:.2f}")
                        
                        send_trade_alert(
                            symbol=symbol,
                            entry_price=entry_price,
                            quantity=exit_qty,
                            score=position.get('score', 0),
                            strategy=position.get('strategy', ''),
                            target_price=position.get('target_price', 0),
                            stop_price=position.get('stop_price', 0),
                            pnl=net_pnl,
                            exit_reason=exit_reason,
                            alert_type="CLOSE"
                        )
                        
                        # === PERFORMANCE ALERTS ===
                        self.check_performance_alerts(pnl_pct, net_pnl)
                        
        except Exception as e:
            log.error(f"[EXIT ERROR] {e}")
            self.update_health(error=f"Exit check error: {e}")

    # ============================================================
    # SCAN AND TRADE
    # ============================================================

    def _perform_scan(self):
        """Internal method to perform the actual scan"""
        if not self.is_trading_time():
            self.scan_status = "⏰ Outside trading hours"
            return
        
        if len(self.positions) >= MAX_POSITIONS:
            self.scan_status = f"⏸️ Max positions ({MAX_POSITIONS}) reached"
            return
        
        self.scan_count += 1
        self.last_scan_time = datetime.now()
        self.scan_status = "🔄 Scanning stocks..."
        self.market_condition = self.get_market_condition()
        self.update_health('scanner', True)
            
        log.info(f"[SCAN] #{self.scan_count} | {self.market_condition}")
        
        def evaluate_signal(symbol):
            if any(pos['symbol'] == symbol for pos in self.positions):
                return None
            score, direction, strategy = self.score_stock(symbol)
            return (symbol, score, direction, strategy)

        signals = []
        scored_count = 0
        
        scan_start = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(evaluate_signal, s) for s in self.stock_list]
            for fut in as_completed(futures):
                res = fut.result()
                scored_count += 1
                if res and res[1] >= MIN_SIGNAL_SCORE and res[2] == "LONG":
                    signals.append(res)
                    log.info(f"[SCAN] 📈 Found: {res[0]} | Score: {res[1]} | {res[2]}")
        
        scan_duration = time.time() - scan_start
        self.metrics.record_scan_duration(scan_duration)
        
        self.stocks_scored = scored_count
        self.signals_found = len(signals)
        
        log.info(f"[SCAN] Scored: {scored_count}, Signals: {len(signals)}, Duration: {scan_duration:.2f}s")
        
        if not signals:
            self.scan_status = f"❌ No signals (Scored: {scored_count})"
            return
        
        signals.sort(key=lambda x: x[1], reverse=True)
        
        def get_position_allocation(score):
            if score >= 10: return 0.45
            elif score >= 9: return 0.35
            elif score >= 8: return 0.25
            elif score >= 7: return 0.15
            else: return 0.00
                    
        for symbol, score, direction, strategy in signals:
            if len(self.positions) >= MAX_POSITIONS:
                break
                
            current_price = self.get_ltp(symbol)
            if not current_price:
                log.warning(f"[SCAN] No price for {symbol}")
                continue
            
            if not self.check_correlation(symbol):
                continue
            
            if not self.check_sector_exposure(symbol):
                continue
            
            dynamic_sl = self.get_dynamic_stop_loss(symbol, current_price)
            volatility_sl = self.get_volatility_stop(symbol, current_price)
            final_sl = max(dynamic_sl, volatility_sl)
            
            allocation_pct = get_position_allocation(score)
            # FIXED: Added LEVERAGE to position sizing
            allocation_amount = (self.capital * LEVERAGE) * allocation_pct
            qty = int(allocation_amount / current_price)
            
            if qty <= 0:
                continue
                
            margin = (current_price * qty) / LEVERAGE
            
            with self.lock:
                if margin > self.available_capital:
                    log.warning(f"[MARGIN] Skipped {symbol}: Need ₹{margin:.2f}, Have ₹{self.available_capital:.2f}")
                    continue
                    
                log.info(f"💥 [SIGNAL] {symbol} | Score: {score} | Strategy: {strategy}")
                log.info(f"[ALLOCATION] {allocation_pct*100:.1f}% of ₹{self.capital * LEVERAGE:,.2f} = ₹{allocation_amount:,.2f}")
                log.info(f"[RISK] SL: {final_sl*100:.2f}% | Margin: ₹{margin:.2f}")
                
                profit_target = PROFIT_TARGETS.get(score, DEFAULT_PROFIT_TARGET)
                target_price = current_price * (1 + profit_target)
                stop_price = current_price * (1 - final_sl)
                
                self.place_order(symbol, "BUY", qty, current_price)
                
                # Save to database immediately
                self.save_position_to_db({
                    'symbol': symbol,
                    'entry_price': current_price,
                    'quantity': qty,
                    'strategy': strategy,
                    'entry_time': datetime.now().isoformat()
                })
                
                self.positions.append({
                    'symbol': symbol, 'entry_price': current_price, 'peak_price': current_price,
                    'quantity': qty, 'remaining_qty': qty, 'entry_time': datetime.now(),
                    'score': score, 'profit_target': profit_target, 'strategy': strategy,
                    'stop_loss': final_sl, 'target_price': target_price, 'stop_price': stop_price,
                    'partial_exit_done': False, 'partial_exit_1_done': False,
                    'partial_exit_2_done': False, 'partial_exit_3_done': False
                })
                
                self.scan_status = f"✅ Position opened: {symbol} (Score: {score})"
                self.metrics.record_trade(success=True)
                
                # === ENHANCED TELEGRAM ALERT ON POSITION OPEN ===
                send_trade_alert(
                    symbol=symbol,
                    entry_price=current_price,
                    quantity=qty,
                    score=score,
                    strategy=strategy,
                    target_price=target_price,
                    stop_price=stop_price,
                    alert_type="OPEN"
                )

    def scan_and_trade(self):
        """Wrapper for scan operation with overlap prevention"""
        self.scan_manager.start_scan(self)

    # ============================================================
    # GENERATE DAILY SUMMARY
    # ============================================================

    def generate_daily_summary(self):
        """Generate and log daily trading summary"""
        summary = f"""
======================================================================
📊 DAILY TRADING SUMMARY - {datetime.now().strftime('%A, %B %d, %Y')}
======================================================================

💰 CAPITAL STATUS:
   Starting Capital: ₹{self.initial_capital:,.2f}
   Current Capital:  ₹{self.capital:,.2f}
   Today's P&L:      ₹{self.daily_pnl:,.2f} ({self.daily_pnl/self.initial_capital*100:+.2f}%)

📈 TRADING ACTIVITY:
   Total Trades Today: {len(self.trades)}
   Active Positions:   {len(self.positions)}
   Total Scans:        {self.scan_count}
   Market Condition:   {self.market_condition}

📊 EXIT STATISTICS:
   Partial Exits:      {len([t for t in self.trades if t.get('exit_type') == 'PARTIAL'])}
   Full Exits:         {len([t for t in self.trades if t.get('exit_type') == 'FULL'])}

🔧 SYSTEM METRICS:
   API Calls:          {self.metrics.metrics['api_calls']}
   API Errors:         {self.metrics.metrics['api_errors']}
   Cache Hit Rate:     {self.metrics.get_summary()['cache_hit_rate']*100:.1f}%
   Avg Scan Time:      {self.metrics.get_summary()['avg_scan_time']:.2f}s
"""
        if self.positions:
            summary += "\n📊 CURRENT POSITIONS:\n"
            for pos in self.positions:
                live_p = self.get_ltp(pos['symbol']) or pos['entry_price']
                pnl = (live_p - pos['entry_price']) * pos['quantity']
                summary += f"      • {pos['symbol']} | Qty: {pos['quantity']} | Entry: ₹{pos['entry_price']:.2f} | P&L: ₹{pnl:,.2f}\n"
        
        summary += f"""
======================================================================
🏁 Bot Status: {'STOPPED' if not self.running else 'RUNNING'}
⏰ Report Generated: {datetime.now().strftime('%I:%M:%S %p')}
======================================================================
"""
        log.info(summary)
        
        # Calculate win rate for enhanced summary
        closed_trades = len([t for t in self.trades if t.get('exit_time')])
        winning_trades = len([t for t in self.trades if t.get('net_pnl', 0) > 0])
        win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
        
        # Send enhanced daily summary via Telegram
        send_daily_summary_alert(
            day_pnl=self.daily_pnl,
            total_trades=len(self.trades),
            win_rate=win_rate,
            positions_open=len(self.positions),
            capital=self.capital
        )

    # ============================================================
    # GITHUB SYNC & DASHBOARD
    # ============================================================

    def sync_to_github_pages(self, html_content: str):
        """Sync dashboard to GitHub Pages"""
        if not GITHUB_PAT or "ghp_" not in GITHUB_PAT:
            log.error("GitHub PAT missing")
            self.update_health('sync', False, error="GitHub PAT missing")
            return        
        url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/index.html"
        headers = {
            "Authorization": f"token {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        }
        
        encoded_content = base64.b64encode(html_content.encode('utf-8')).decode('utf-8')
        
        for attempt in range(3):
            try:
                r = requests.get(url, headers=headers)
                sha = r.json().get("sha") if r.status_code == 200 else None
                payload = {
                    "message": f"Engine Sync: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "content": encoded_content
                }
                if sha:
                    payload["sha"] = sha
                push_res = requests.put(url, headers=headers, json=payload)
                if push_res.status_code in [200, 201]:
                    log.info("🚀 [GITHUB] Dashboard updated")
                    self.update_health('sync', True)
                    return
                elif push_res.status_code == 409:
                    log.warning(f"[GITHUB] Conflict, retry {attempt+1}/3")
                    time.sleep(2)
                else:
                    log.error(f"[GITHUB] Failed: {push_res.status_code}")
                    self.update_health('sync', False, error=f"GitHub push failed: {push_res.status_code}")
                    break
            except Exception as e:
                log.error(f"[GITHUB] Error: {e}")
                self.update_health('sync', False, error=f"GitHub sync error: {e}")
                break

    def render_and_deploy_dashboard(self):
        """Render and deploy dashboard"""
        try:
            total_floating_pnl = 0.0
            formatted_positions = []
            
            for pos in self.positions:
                sym = pos['symbol']
                live_p = self.get_ltp(sym) or pos['entry_price']
                pos_pnl = (live_p - pos['entry_price']) * pos['quantity']
                pos_pnl_pct = ((live_p - pos['entry_price']) / pos['entry_price']) * 100
                total_floating_pnl += pos_pnl
                
                formatted_positions.append({
                    "symbol": sym, 
                    "strategy": pos['strategy'], 
                    "quantity": pos['quantity'],
                    "remaining": pos.get('remaining_qty', pos['quantity']),
                    "score": pos.get('score', 0),
                    "entry_price": round(pos['entry_price'], 2), 
                    "live_price": round(live_p, 2),
                    "pnl": round(pos_pnl, 2), 
                    "pnl_pct": round(pos_pnl_pct, 2),
                    "target": round(pos.get('target_price', pos['entry_price'] * 1.035), 2),
                    "sl": round(pos.get('stop_price', pos['entry_price'] * 0.98), 2)
                })

            total_portfolio_value = self.capital + total_floating_pnl
            floating_pct = (total_floating_pnl / self.initial_capital * 100) if self.initial_capital > 0 else 0
            pnl_color_class = "text-emerald-400" if total_floating_pnl >= 0 else "text-rose-500"
            pnl_prefix = "+" if total_floating_pnl >= 0 else ""

            positions_json = json.dumps(formatted_positions)
            trades_json = json.dumps(self.trades[-20:])
            
            status_message = self.scan_status
            
            current_time = datetime.now().time()
            market_close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
            market_open_time = datetime.strptime(TRADING_START, "%H:%M").time()
            
            if not self.running:
                status_message = "⏹️ Bot Stopped"
            elif current_time >= market_close_time:
                status_message = "⏰ Market Closed for Today"
            elif current_time < market_open_time:
                status_message = f"⏰ Market Opens at {TRADING_START}"
            elif len(self.positions) == 0 and self.scan_count == 0:
                status_message = "🔄 Waiting for signals..."

            html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>ALPHA - Trading Bot</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        * { -webkit-tap-highlight-color: transparent; }
        .pulse { animation: pulse 2s infinite; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; } }
        .card { background: rgba(31, 41, 55, 0.5); backdrop-filter: blur(10px); border: 1px solid rgba(75, 85, 99, 0.3); }
        body { background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%); min-height: 100vh; padding-bottom: 70px; }
        .glow { text-shadow: 0 0 20px rgba(52, 211, 153, 0.3); }
        .alpha-logo { font-size: 22px; font-weight: bold; color: #34d399; margin-right: 2px; }
        .tagline { font-size: 8px; color: rgba(255,255,255,0.3); letter-spacing: 2px; margin-top: -2px; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
        .status-dot.green { background: #34d399; }
        .status-dot.red { background: #f43f5e; }
        .status-dot.yellow { background: #fbbf24; }
        .profit { color: #34d399; }
        .loss { color: #f43f5e; }
        .compact-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; }
        .compact-stat { background: rgba(31, 41, 55, 0.3); padding: 8px 4px; border-radius: 8px; text-align: center; }
        .compact-stat .value { font-size: 14px; font-weight: bold; font-family: monospace; }
        .compact-stat .label { font-size: 7px; text-transform: uppercase; color: #9ca3af; letter-spacing: 0.5px; margin-top: 2px; }
        .bottom-nav { position: fixed; bottom: 0; left: 0; right: 0; background: rgba(15, 15, 26, 0.95); backdrop-filter: blur(20px); border-top: 1px solid rgba(75, 85, 99, 0.3); display: flex; justify-content: space-around; padding: 8px 0 12px 0; z-index: 100; }
        .nav-item { display: flex; flex-direction: column; align-items: center; gap: 2px; font-size: 10px; color: #6b7280; cursor: pointer; padding: 4px 12px; border-radius: 8px; background: transparent; border: none; font-family: inherit; }
        .nav-item.active { color: #34d399; }
        .nav-item .nav-icon { font-size: 20px; }
        .nav-item .nav-label { font-size: 8px; letter-spacing: 0.5px; }
        .page { display: none; padding-bottom: 10px; }
        .page.active { display: block; }
        .tab-btn { padding: 4px 12px; border-radius: 6px; font-size: 9px; cursor: pointer; transition: all 0.2s; border: 1px solid rgba(75, 85, 99, 0.3); background: transparent; color: #9ca3af; font-family: inherit; }
        .tab-btn.active { background: rgba(52, 211, 153, 0.2); border-color: #34d399; color: #34d399; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .chart-container { height: 150px; }
        .analytics-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }
        .analytics-card { background: rgba(31, 41, 55, 0.3); padding: 8px; border-radius: 8px; text-align: center; }
        .analytics-card .value { font-size: 16px; font-weight: bold; font-family: monospace; }
        .analytics-card .label { font-size: 7px; text-transform: uppercase; color: #9ca3af; letter-spacing: 0.5px; }
        .trade-row { padding: 6px 10px; border-bottom: 1px solid rgba(75, 85, 99, 0.15); display: grid; grid-template-columns: 1fr 0.8fr 1fr 0.8fr; gap: 4px; font-size: 11px; align-items: center; }
        .trade-row .symbol { font-weight: bold; color: #fff; }
        .trade-row .pnl { font-weight: bold; font-family: monospace; }
        .trade-row .strategy { color: #9ca3af; font-size: 9px; }
        .trade-row .status { font-size: 8px; padding: 2px 8px; border-radius: 12px; text-align: center; }
        .status-badge { background: rgba(52, 211, 153, 0.1); border: 1px solid rgba(52, 211, 153, 0.2); }
        .status-badge-loss { background: rgba(244, 63, 94, 0.1); border: 1px solid rgba(244, 63, 94, 0.2); }
        .performer-pill { display: inline-block; background: rgba(31, 41, 55, 0.5); padding: 4px 10px; border-radius: 20px; margin: 2px; font-size: 10px; }
        .performer-pill .sym { font-weight: bold; color: #fff; }
        .position-card { padding: 10px 12px; margin-bottom: 6px; }
        .position-card .symbol { font-size: 14px; font-weight: bold; }
        .position-card .detail { font-size: 10px; color: #9ca3af; }
        .position-card .pnl { font-size: 14px; font-weight: bold; font-family: monospace; }
        .refresh-indicator { font-size: 8px; color: rgba(255,255,255,0.3); text-align: center; margin-top: 20px; }
        .scroll-container { max-height: 300px; overflow-y: auto; -webkit-overflow-scrolling: touch; }
        .scroll-container::-webkit-scrollbar { width: 3px; }
        .scroll-container::-webkit-scrollbar-thumb { background: rgba(52, 211, 153, 0.3); border-radius: 10px; }
    </style>
</head>
<body class="font-sans antialiased text-gray-100">

    <!-- HEADER -->
    <header class="p-3 flex justify-between items-center border-b border-gray-700/50 sticky top-0 bg-[#0f0f1a]/90 backdrop-blur-md z-50">
        <div>
            <div class="flex items-center gap-2">
                <span class="w-2 h-2 rounded-full bg-emerald-500 pulse"></span>
                <div>
                    <div class="flex items-center gap-1">
                        <span class="alpha-logo">α</span>
                        <h1 class="text-base font-bold tracking-wider text-emerald-400 glow">ALPHA</h1>
                    </div>
                    <p class="tagline">by ArandaTech</p>
                </div>
            </div>
        </div>
        <div class="text-right text-[10px] font-mono text-gray-400">
            <div id="sync-time">Sync: --</div>
            <div class="flex items-center justify-end gap-3 mt-0.5">
                <span class="status-dot" id="status-dot"></span>
                <span id="scan-status" class="text-[9px]">Loading...</span>
            </div>
        </div>
    </header>

    <!-- PAGE 1: OVERVIEW -->
    <div id="page-overview" class="page active p-3">
        <div class="card p-2 rounded-xl mb-3 flex justify-between items-center text-[10px]">
            <span id="market-status">Market: Loading...</span>
            <span>Scans: <span id="scan-count">0</span> | Signals: <span id="signal-count">0</span></span>
        </div>

        <div class="card p-4 rounded-xl text-center mb-3">
            <p class="text-[8px] text-gray-400 uppercase tracking-widest">Today's Floating P&L</p>
            <p class="text-2xl font-extrabold font-mono mt-1" id="today-pnl">₹0.00</p>
            <div class="flex justify-center gap-4 mt-1 text-[10px] text-gray-400">
                <span>Active: <span class="text-white font-bold" id="active-count">0</span></span>
                <span>Today: <span class="text-white font-bold" id="trades-today">0</span></span>
            </div>
        </div>

        <div class="compact-grid mb-3">
            <div class="compact-stat">
                <div class="value" id="total-pnl">₹0</div>
                <div class="label">Total P&L</div>
            </div>
            <div class="compact-stat">
                <div class="value" id="win-rate">0%</div>
                <div class="label">Win Rate</div>
            </div>
            <div class="compact-stat">
                <div class="value" id="total-trades">0</div>
                <div class="label">Trades</div>
            </div>
            <div class="compact-stat">
                <div class="value" id="open-trades">0</div>
                <div class="label">Open</div>
            </div>
        </div>

        <div class="card p-2 rounded-xl mb-3">
            <p class="text-[8px] text-gray-400 uppercase tracking-wider mb-1">🏆 Top Stocks</p>
            <div id="top-performers" class="flex flex-wrap gap-1">
                <span class="text-[10px] text-gray-500">Loading...</span>
            </div>
        </div>

        <div class="grid grid-cols-3 gap-2">
            <button onclick="switchPage('positions')" class="card p-2 rounded-xl text-center text-[9px] text-gray-400 hover:text-white transition">
                📊 Positions
            </button>
            <button onclick="switchPage('trades')" class="card p-2 rounded-xl text-center text-[9px] text-gray-400 hover:text-white transition">
                📈 Trades
            </button>
            <button onclick="switchPage('analytics')" class="card p-2 rounded-xl text-center text-[9px] text-gray-400 hover:text-white transition">
                📉 Analytics
            </button>
        </div>
    </div>

    <!-- PAGE 2: POSITIONS -->
    <div id="page-positions" class="page p-3">
        <h2 class="text-xs font-bold text-gray-400 tracking-wider uppercase mb-2">📊 Active Positions</h2>
        <div id="positions-root" class="space-y-1.5">
            <div class="card p-4 rounded-xl text-center text-xs text-gray-500">Loading...</div>
        </div>
    </div>

    <!-- PAGE 3: TRADES -->
    <div id="page-trades" class="page p-3">
        <h2 class="text-xs font-bold text-gray-400 tracking-wider uppercase mb-2">📈 Recent Trades</h2>
        <div class="card rounded-xl overflow-hidden">
            <div id="history-root">
                <div class="p-4 text-center text-xs text-gray-500">Loading...</div>
            </div>
        </div>
    </div>

    <!-- PAGE 4: ANALYTICS -->
    <div id="page-analytics" class="page p-3">
        <div class="flex items-center justify-between mb-2">
            <h2 class="text-xs font-bold text-gray-400 tracking-wider uppercase">📉 Analytics</h2>
            <div class="flex gap-1">
                <button class="tab-btn active" data-tab="daily" onclick="switchAnalyticsTab('daily')">Daily</button>
                <button class="tab-btn" data-tab="weekly" onclick="switchAnalyticsTab('weekly')">Weekly</button>
                <button class="tab-btn" data-tab="monthly" onclick="switchAnalyticsTab('monthly')">Monthly</button>
            </div>
        </div>

        <div id="tab-daily" class="tab-content active">
            <div class="card p-2 rounded-xl">
                <div class="chart-container"><canvas id="dailyChart"></canvas></div>
                <div class="analytics-grid mt-2">
                    <div class="analytics-card"><div class="value profit" id="daily-today">₹0</div><div class="label">Today</div></div>
                    <div class="analytics-card"><div class="value profit" id="daily-best">₹0</div><div class="label">Best Day</div></div>
                    <div class="analytics-card"><div class="value loss" id="daily-worst">₹0</div><div class="label">Worst Day</div></div>
                    <div class="analytics-card"><div class="value" id="daily-avg" style="color: #fbbf24;">₹0</div><div class="label">Avg Day</div></div>
                </div>
            </div>
        </div>

        <div id="tab-weekly" class="tab-content">
            <div class="card p-2 rounded-xl">
                <div class="chart-container"><canvas id="weeklyChart"></canvas></div>
                <div class="analytics-grid mt-2">
                    <div class="analytics-card"><div class="value profit" id="weekly-this">₹0</div><div class="label">This Week</div></div>
                    <div class="analytics-card"><div class="value profit" id="weekly-best">₹0</div><div class="label">Best Week</div></div>
                    <div class="analytics-card"><div class="value loss" id="weekly-worst">₹0</div><div class="label">Worst Week</div></div>
                    <div class="analytics-card"><div class="value" id="weekly-avg" style="color: #fbbf24;">₹0</div><div class="label">Avg Week</div></div>
                </div>
            </div>
        </div>

        <div id="tab-monthly" class="tab-content">
            <div class="card p-2 rounded-xl">
                <div class="chart-container"><canvas id="monthlyChart"></canvas></div>
                <div class="analytics-grid mt-2">
                    <div class="analytics-card"><div class="value profit" id="monthly-this">₹0</div><div class="label">This Month</div></div>
                    <div class="analytics-card"><div class="value profit" id="monthly-best">₹0</div><div class="label">Best Month</div></div>
                    <div class="analytics-card"><div class="value loss" id="monthly-worst">₹0</div><div class="label">Worst Month</div></div>
                    <div class="analytics-card"><div class="value" id="monthly-avg" style="color: #fbbf24;">₹0</div><div class="label">Avg Month</div></div>
                </div>
            </div>
        </div>
    </div>

    <!-- BOTTOM NAVIGATION -->
    <nav class="bottom-nav">
        <button class="nav-item active" data-page="overview" onclick="switchPage('overview')">
            <span class="nav-icon">🏠</span>
            <span class="nav-label">Overview</span>
        </button>
        <button class="nav-item" data-page="positions" onclick="switchPage('positions')">
            <span class="nav-icon">📊</span>
            <span class="nav-label">Positions</span>
        </button>
        <button class="nav-item" data-page="trades" onclick="switchPage('trades')">
            <span class="nav-icon">📈</span>
            <span class="nav-label">Trades</span>
        </button>
        <button class="nav-item" data-page="analytics" onclick="switchPage('analytics')">
            <span class="nav-icon">📉</span>
            <span class="nav-label">Analytics</span>
        </button>
    </nav>

    <div class="refresh-indicator">
        🔄 Auto-refresh every 30s | Last updated: <span id="last-updated">--</span>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        const API_BASE = '/api';
        let dailyChart, weeklyChart, monthlyChart;

        function formatCurrency(amount) {
            if (amount === null || amount === undefined) return '₹0';
            return `₹${parseFloat(amount).toFixed(2)}`;
        }

        function switchPage(page) {
            document.querySelectorAll('.page').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            document.getElementById(`page-${page}`).classList.add('active');
            document.querySelector(`[data-page="${page}"]`).classList.add('active');
            if (page === 'analytics') {
                setTimeout(() => {
                    if (dailyChart) dailyChart.resize();
                    if (weeklyChart) weeklyChart.resize();
                    if (monthlyChart) monthlyChart.resize();
                }, 100);
            }
        }

        function switchAnalyticsTab(tab) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
            document.getElementById(`tab-${tab}`).classList.add('active');
            document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
        }

        async function loadDashboard() {
            try {
                const statsRes = await fetch(`${API_BASE}/stats`);
                const statsData = await statsRes.json();
                if (statsData.status === 'success') {
                    const s = statsData.data;
                    document.getElementById('total-pnl').textContent = formatCurrency(s.overall?.total_pnl);
                    document.getElementById('total-trades').textContent = s.overall?.total_trades || 0;
                    document.getElementById('open-trades').textContent = s.overall?.open_trades || 0;
                    const closed = s.overall?.closed_trades || 0;
                    const won = s.overall?.winning_trades || 0;
                    const wr = closed > 0 ? (won / closed * 100) : 0;
                    document.getElementById('win-rate').textContent = wr.toFixed(1) + '%';
                    const todayPnl = s.today?.today_pnl || 0;
                    const el = document.getElementById('today-pnl');
                    el.textContent = formatCurrency(todayPnl);
                    el.style.color = todayPnl >= 0 ? '#34d399' : '#f43f5e';
                    document.getElementById('trades-today').textContent = s.today?.today_trades || 0;
                }

                const tradesRes = await fetch(`${API_BASE}/trades`);
                const tradesData = await tradesRes.json();
                if (tradesData.status === 'success') {
                    const trades = tradesData.data || [];
                    const active = trades.filter(t => t.status === 'OPEN');
                    const closed = trades.filter(t => t.status === 'CLOSED').slice(0, 15);
                    document.getElementById('active-count').textContent = active.length;
                    renderPositions(active);
                    renderTrades(closed);
                }

                await loadDailyPNL();
                await loadWeeklyPNL();
                await loadMonthlyPNL();
                await loadTopPerformers();

                const now = new Date();
                document.getElementById('last-updated').textContent = now.toLocaleTimeString();
                document.getElementById('sync-time').textContent = `Sync: ${now.toLocaleTimeString()}`;
            } catch (e) {
                console.error('Error:', e);
            }
        }

        function renderPositions(active) {
            const root = document.getElementById('positions-root');
            if (active.length === 0) {
                root.innerHTML = `<div class="card p-4 rounded-xl text-center text-xs text-gray-500">No active positions</div>`;
                return;
            }
            root.innerHTML = active.map(p => {
                const pnl = parseFloat(p.pnl || 0);
                const cls = pnl >= 0 ? 'profit' : 'loss';
                return `
                    <div class="card position-card rounded-xl">
                        <div class="flex justify-between items-center">
                            <div>
                                <div class="symbol">${p.symbol || 'N/A'}</div>
                                <div class="detail">${p.strategy || 'N/A'} · Qty: ${p.quantity || 0}</div>
                                <div class="detail">Entry: ₹${parseFloat(p.entry_price || 0).toFixed(2)}</div>
                            </div>
                            <div class="text-right">
                                <div class="pnl ${cls}">${pnl >= 0 ? '+' : ''}${formatCurrency(pnl)}</div>
                                <div class="detail">${p.status || 'OPEN'}</div>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function renderTrades(closed) {
            const root = document.getElementById('history-root');
            if (closed.length === 0) {
                root.innerHTML = `<div class="p-4 text-center text-xs text-gray-500">No trades yet</div>`;
                return;
            }
            root.innerHTML = closed.map(t => {
                const pnl = parseFloat(t.pnl || 0);
                const cls = pnl >= 0 ? 'profit' : 'loss';
                const statusCls = pnl >= 0 ? 'status-badge' : 'status-badge-loss';
                return `
                    <div class="trade-row">
                        <span class="symbol">${t.symbol || 'N/A'}</span>
                        <span class="pnl ${cls}">${pnl >= 0 ? '+' : ''}${formatCurrency(pnl)}</span>
                        <span class="strategy">${t.strategy || 'N/A'}</span>
                        <span class="status ${statusCls}">${t.status || 'Closed'}</span>
                    </div>
                `;
            }).join('');
        }

        async function loadTopPerformers() {
            try {
                const res = await fetch(`${API_BASE}/performance`);
                const data = await res.json();
                const container = document.getElementById('top-performers');
                if (data.status === 'success' && data.data.best_symbols?.length) {
                    container.innerHTML = data.data.best_symbols.map(p => `
                        <span class="performer-pill">
                            <span class="sym">${p.symbol}</span>
                            <span class="${p.total_pnl >= 0 ? 'profit' : 'loss'}">${p.total_pnl >= 0 ? '+' : ''}${formatCurrency(p.total_pnl)}</span>
                        </span>
                    `).join('');
                } else {
                    container.innerHTML = '<span class="text-[10px] text-gray-500">No data</span>';
                }
            } catch (e) { console.error(e); }
        }

        function updateChart(canvasId, labels, values) {
            const ctx = document.getElementById(canvasId).getContext('2d');
            if (window[canvasId]) window[canvasId].destroy();
            const chart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels.length ? labels : ['No Data'],
                    datasets: [{
                        data: labels.length ? values : [0],
                        backgroundColor: labels.length ? values.map(v => v >= 0 ? 'rgba(52,211,153,0.6)' : 'rgba(244,63,94,0.6)') : ['rgba(75,85,99,0.3)'],
                        borderColor: labels.length ? values.map(v => v >= 0 ? '#34d399' : '#f43f5e') : ['#6b7280'],
                        borderWidth: 1,
                        borderRadius: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        y: { grid: { color: 'rgba(255,255,255,0.05)' }, ticks: { color: '#9ca3af', font: { size: 8 } } },
                        x: { grid: { display: false }, ticks: { color: '#9ca3af', font: { size: 7 }, maxRotation: 45 } }
                    }
                }
            });
            window[canvasId] = chart;
        }

        function updateStats(period, values) {
            const last = values[values.length - 1] || 0;
            const best = Math.max(...values, 0);
            const worst = Math.min(...values, 0);
            const avg = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
            const prefix = period === 'daily' ? '' : period === 'weekly' ? 'weekly-' : 'monthly-';
            document.getElementById(prefix + 'this').textContent = formatCurrency(last);
            document.getElementById(prefix + 'this').style.color = last >= 0 ? '#34d399' : '#f43f5e';
            document.getElementById(prefix + 'best').textContent = formatCurrency(best);
            document.getElementById(prefix + 'worst').textContent = formatCurrency(worst);
            document.getElementById(prefix + 'avg').textContent = formatCurrency(avg);
        }

        async function loadDailyPNL() {
            try {
                const res = await fetch(`${API_BASE}/daily_pnl`);
                const data = await res.json();
                if (data.status === 'success') {
                    const d = data.data;
                    const labels = d.map(x => x.date);
                    const values = d.map(x => parseFloat(x.daily_pnl || 0));
                    updateChart('dailyChart', labels, values);
                    updateStats('daily', values);
                }
            } catch (e) { console.error(e); }
        }

        async function loadWeeklyPNL() {
            try {
                const res = await fetch(`${API_BASE}/weekly_pnl`);
                const data = await res.json();
                if (data.status === 'success') {
                    const d = data.data;
                    const labels = d.map(x => x.week);
                    const values = d.map(x => parseFloat(x.weekly_pnl || 0));
                    updateChart('weeklyChart', labels, values);
                    updateStats('weekly', values);
                }
            } catch (e) { console.error(e); }
        }

        async function loadMonthlyPNL() {
            try {
                const res = await fetch(`${API_BASE}/monthly_pnl`);
                const data = await res.json();
                if (data.status === 'success') {
                    const d = data.data;
                    const labels = d.map(x => x.month);
                    const values = d.map(x => parseFloat(x.monthly_pnl || 0));
                    updateChart('monthlyChart', labels, values);
                    updateStats('monthly', values);
                }
            } catch (e) { console.error(e); }
        }

        async function loadMarketStatus() {
            try {
                const res = await fetch(`${API_BASE}/market_status`);
                const data = await res.json();
                if (data.status === 'success') {
                    document.getElementById('market-status').textContent = data.data;
                    const dot = document.getElementById('status-dot');
                    dot.className = `status-dot ${data.is_open ? 'green' : 'red'}`;
                    document.getElementById('scan-status').textContent = data.is_open ? '🟢 Open' : '🔴 Closed';
                }
            } catch (e) {
                const now = new Date();
                const isWeekend = now.getDay() === 0 || now.getDay() === 6;
                document.getElementById('market-status').textContent = isWeekend ? '📅 Weekend' : '🟢 Open';
                document.getElementById('status-dot').className = `status-dot ${isWeekend ? 'red' : 'green'}`;
                document.getElementById('scan-status').textContent = isWeekend ? '🔴 Closed' : '🟢 Open';
            }
        }

        loadDashboard();
        loadMarketStatus();
        setInterval(loadDashboard, 30000);
    </script>
</body>
</html>"""

            html_payload = html_template\
                .replace("__SYNC_TIME__", datetime.now().strftime('%I:%M:%S %p'))\
                .replace("__MARGIN__", f"{self.available_capital:.2f}")\
                .replace("__PORTFOLIO_VALUE__", f"{total_portfolio_value:.2f}")\
                .replace("__PNL_COLOR__", pnl_color_class)\
                .replace("__PREFIX__", pnl_prefix)\
                .replace("__FLOATING_PNL__", f"{abs(total_floating_pnl):.2f}")\
                .replace("__FLOATING_PCT__", f"{abs(floating_pct):.2f}")\
                .replace("__POSITIONS_COUNT__", str(len(formatted_positions)))\
                .replace("__POSITIONS_JSON__", positions_json)\
                .replace("__TRADES_JSON__", trades_json)\
                .replace("__BOT_STATUS__", "ACTIVE 🟢" if self.running else "STOPPED 🔴")\
                .replace("__SCAN_STATUS__", status_message[:50])\
                .replace("__SCAN_COUNT__", str(self.scan_count))\
                .replace("__STOCKS_SCORED__", str(self.stocks_scored))\
                .replace("__SIGNALS_FOUND__", str(self.signals_found))

            with open("dashboard_backup.html", "w", encoding="utf-8") as out:
                out.write(html_payload)
            
            self.sync_to_github_pages(html_payload)
            self.update_health('sync', True)
                
        except Exception as e:
            log.error(f"[DASHBOARD ERROR] {e}")
            self.update_health('sync', False, error=f"Dashboard error: {e}")

    # ============================================================
    # MARKET STATUS CHECK
    # ============================================================

    def fetch_nse_holidays(self) -> List[str]:
        """Fetch NSE holiday list"""
        try:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            endpoint = "https://www.nseindia.com/api/holiday-master?type=trading"
            response = requests.get(endpoint, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                holidays = []
                if 'FO' in data:
                    for item in data['FO']:
                        if 'tradingDate' in item:
                            holidays.append(item['tradingDate'])
                if 'equity' in data:
                    for item in data['equity']:
                        if 'tradingDate' in item:
                            holidays.append(item['tradingDate'])
                return list(set(holidays))
            return []
        except:
            return []

    def is_trading_day(self, check_date: Optional[datetime] = None) -> bool:
        """Check if it's a trading day"""
        if check_date is None:
            check_date = datetime.now().date()
        if check_date.weekday() >= 5:
            return False
        holidays = self.fetch_nse_holidays()
        if check_date.strftime("%Y-%m-%d") in holidays:
            return False
        return True

    def check_market_status(self) -> Tuple[bool, str]:
        """Check current market status with enhanced alerts"""
        today = datetime.now().date()
        current_time = datetime.now().time()
        market_open = datetime.strptime("09:15", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()
        
        if not self.is_trading_day(today):
            if today.weekday() >= 5:
                status = f"📅 Market Closed - Weekend ({today.strftime('%A')})"
            else:
                status = "📅 Market Closed - Holiday"
            self.scan_status = status
            self.market_condition = status
            self.update_health('market', False)
            log.info(f"⏳ {status}")
            
            if not self._market_status_sent:
                send_market_status_alert(False, "Weekend/Holiday")
                self._market_status_sent = True
            
            return False, status
        
        if current_time < market_open:
            time_until = (datetime.combine(today, market_open) - datetime.now()).seconds
            hours = time_until // 3600
            minutes = (time_until % 3600) // 60
            status = f"⏰ Market Opens at 09:15 AM"
            self.scan_status = status
            self.market_condition = status
            self.update_health('market', False)
            log.info(f"⏳ {status}")
            
            self._market_status_sent = False
            self._market_open_sent = False
            self._market_closed_sent = False
            return False, status
        
        if current_time > market_close:
            status = "⏰ Market Closed for Today"
            self.scan_status = status
            self.market_condition = status
            self.update_health('market', False)
            log.info(f"⏳ {status}")
            
            if not self._market_closed_sent:
                tomorrow = today + timedelta(days=1)
                next_open = datetime.combine(tomorrow, market_open)
                while next_open.weekday() >= 5:
                    next_open += timedelta(days=1)
                time_until = str(next_open - datetime.now()).split('.')[0]
                send_market_status_alert(False, time_until)
                self._market_closed_sent = True
            
            return False, status
        
        # Market is open
        status = "✅ Market OPEN - Trading Active"
        self.scan_status = status
        self.market_condition = "📈 LIVE MARKET"
        self.update_health('market', True)
        log.info(f"✅ {status}")
        
        if not self._market_open_sent:
            send_market_status_alert(True)
            self._market_open_sent = True
            self._market_closed_sent = False
            self._market_status_sent = False
        
        return True, status

    # ============================================================
    # MAIN RUN LOOP - FIXED: Telegram starts BEFORE market check!
    # ============================================================

    def run(self):
        """Main bot execution loop"""
        def signal_handler(sig, frame):
            log.info("[SHUTDOWN] Received signal. Shutting down gracefully...")
            self.running = False
            self.ws_heartbeat_running = False
            for pos in self.positions:
                self.save_position_to_db({
                    'symbol': pos['symbol'],
                    'entry_price': pos['entry_price'],
                    'quantity': pos['quantity'],
                    'strategy': pos['strategy'],
                    'entry_time': pos['entry_time'].isoformat() if hasattr(pos['entry_time'], 'isoformat') else datetime.now().isoformat()
                })
            send_system_alert("SHUTDOWN", f"P&L: ₹{self.daily_pnl:.2f}\nPositions: {len(self.positions)}", "shutdown")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # === ENVIRONMENT VALIDATION ===
        if not validate_environment():
            log.error("[STARTUP] Environment validation failed.")
            return
        
        # === CONFIGURATION VALIDATION ===
        if not validate_configuration():
            log.error("[STARTUP] Configuration validation failed.")
            return
        
        # === DATABASE BACKUP ===
        backup_database()
        send_system_alert("STARTUP", "System initializing...", "startup")
        
        init_database()
        cleanup_old_trades(365)
        
        # === LOAD POSITIONS FROM DB ===
        self.load_positions_from_db()
        
        log.info("="*60)
        log.info("⚡ ALPHA by ArandaTech - STARTUP")
        log.info("="*60)
        log.info(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info(f"💰 Initial Capital: ₹{self.capital}")
        log.info(f"📊 Max Positions: {MAX_POSITIONS}")
        log.info(f"🎯 Scan Interval: {SCAN_INTERVAL}s")
        log.info(f"📈 Min Signal Score: {MIN_SIGNAL_SCORE}")
        log.info(f"🔒 Leverage: {LEVERAGE}x")
        log.info("="*60)
        log.info("📋 FEATURES ENABLED:")
        log.info(f"   ✅ Dynamic SL: {'Enabled' if ENABLE_DYNAMIC_SL else 'Disabled'}")
        log.info(f"   ✅ ATR Sizing: {'Enabled' if ENABLE_ATR_POSITION_SIZING else 'Disabled'}")
        log.info(f"   ✅ Correlation Filter: {'Enabled' if ENABLE_CORRELATION_FILTER else 'Disabled'}")
        log.info(f"   ✅ Sector Divers.: {'Enabled' if ENABLE_SECTOR_DIVERSIFICATION else 'Disabled'}")
        log.info(f"   ✅ Volatility SL: {'Enabled' if ENABLE_VOLATILITY_STOP else 'Disabled'}")
        log.info(f"   ✅ Trailing Stop Loss: Active")
        log.info(f"   ✅ Partial Profit Taking: Active")
        log.info(f"   ✅ Telegram Alerts: Active")
        log.info(f"   ✅ Data Source: NSE Python (Primary) + Angel One + Yahoo (Fallback)")
        log.info(f"   ✅ WebSocket Heartbeat: Active ({WS_HEARTBEAT_INTERVAL}s)")
        log.info(f"   ✅ Rate Limiting: {API_MAX_CALLS_PER_MINUTE} calls/min")
        log.info(f"   ✅ Circuit Breaker: Active with Auto-Reset")
        log.info(f"   ✅ Cache Cleanup: {CACHE_CLEANUP_INTERVAL}s")
        log.info("="*60)
        self.update_health('bot_running', True)
        send_system_alert("READY", f"Capital: ₹{self.capital}\nMax Positions: {MAX_POSITIONS}", "success")
        
        # ============================================================
        # START TELEGRAM REMOTE CONTROL - IMMEDIATELY!
        # This runs BEFORE market check, so it works 24/7
        # ============================================================
        log.info("[TG BOT] Attempting to start remote control...")
        try:
            self.telegram_bot = TelegramBotHandler(self)
            self.telegram_bot.start()
            log.info("[TG BOT] Remote control started successfully")
        except Exception as e:
            log.error(f"[TG BOT] Failed to start remote control: {e}")
        
        # ============================================================
        # CHECK MARKET STATUS (Telegram is already running)
        # ============================================================
        log.info("📅 Checking market status...")
        is_open, status = self.check_market_status()
        
        if not is_open:
            log.info(f"💤 Market closed. Status: {status}")
            
            if "Weekend" in status or "Holiday" in status:
                today = datetime.now().date()
                days_to_add = 1
                while True:
                    next_day = today + timedelta(days=days_to_add)
                    if self.is_trading_day(next_day):
                        break
                    days_to_add += 1
                next_trading_date = today + timedelta(days=days_to_add)
                next_trading_time = datetime.combine(next_trading_date, datetime.strptime("09:15", "%H:%M").time())
                seconds_to_wait = (next_trading_time - datetime.now()).total_seconds()
                hours = int(seconds_to_wait // 3600)
                minutes = int((seconds_to_wait % 3600) // 60)
                log.info(f"⏰ Sleeping for {hours}h {minutes}m until next trading day...")
                send_system_alert("INFO", f"Next trading day: {next_trading_date.strftime('%A, %B %d')}", "info")
                send_telegram_alert(
                    f"😴 <b>Bot is Sleeping</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📅 Next trading day: {next_trading_date.strftime('%A, %B %d')}\n"
                    f"⏰ Will wake up at 09:15 AM\n"
                    f"🕐 Current time: {datetime.now().strftime('%I:%M:%S %p')}\n"
                    f"\n"
                    f"📱 Telegram commands are still active!\n"
                    f"   Send /status to check bot status.",
                    "info"
                )
                time.sleep(seconds_to_wait)
                self.run()
                return
            
            elif "Opens at" in status:
                today = datetime.now().date()
                market_open_time = datetime.combine(today, datetime.strptime("09:15", "%H:%M").time())
                seconds_to_wait = (market_open_time - datetime.now()).total_seconds()
                hours = int(seconds_to_wait // 3600)
                minutes = int((seconds_to_wait % 3600) // 60)
                log.info(f"⏰ Sleeping for {hours}h {minutes}m until market opens...")
                send_telegram_alert(
                    f"😴 <b>Bot is Sleeping</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⏰ Market opens at 09:15 AM\n"
                    f"⌛ Time remaining: {hours}h {minutes}m\n"
                    f"🕐 Current time: {datetime.now().strftime('%I:%M:%S %p')}\n"
                    f"\n"
                    f"📱 Telegram commands are still active!\n"
                    f"   Send /status to check bot status.",
                    "info"
                )
                time.sleep(seconds_to_wait)
                self.run()
                return
            
            elif "Closed for Today" in status:
                today = datetime.now().date()
                tomorrow = today + timedelta(days=1)
                market_open_time = datetime.combine(tomorrow, datetime.strptime("09:15", "%H:%M").time())
                seconds_to_wait = (market_open_time - datetime.now()).total_seconds()
                hours = int(seconds_to_wait // 3600)
                minutes = int((seconds_to_wait % 3600) // 60)
                log.info(f"⏰ Sleeping for {hours}h {minutes}m until tomorrow...")
                send_telegram_alert(
                    f"😴 <b>Bot is Sleeping</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📅 Market closed for today\n"
                    f"⏰ Next open: 09:15 AM tomorrow\n"
                    f"⌛ Time remaining: {hours}h {minutes}m\n"
                    f"🕐 Current time: {datetime.now().strftime('%I:%M:%S %p')}\n"
                    f"\n"
                    f"📱 Telegram commands are still active!\n"
                    f"   Send /status to check bot status.",
                    "info"
                )
                time.sleep(seconds_to_wait)
                self.run()
                return
        
        # ============================================================
        # MARKET IS OPEN - PROCEED WITH TRADING
        # ============================================================
        log.info("✅ Market is OPEN! Proceeding with login...")
        
        send_telegram_alert(
            f"🏛️ <b>Market is OPEN!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚀 Bot is waking up and starting to trade\n"
            f"💰 Capital: ₹{self.capital}\n"
            f"📊 Max Positions: {MAX_POSITIONS}\n"
            f"🕐 {datetime.now().strftime('%I:%M:%S %p')}",
            "market"
        )
        
        if not self.login():
            log.error("Failed to login. Exiting.")
            self.update_health('broker', False, error="Login failed")
            send_system_alert("ERROR", "Login failed. Bot cannot start trading.", "error")
            return
            
        self.load_symbol_tokens()
        self.load_sector_mapping()
        
        initial_pool = self.fetch_all_stocks()
        retry_count = 0
        while not initial_pool and retry_count < 3:
            retry_count += 1
            log.warning(f"⚠️ No stocks found. Retrying in 30s ({retry_count}/3)...")
            time.sleep(30)
            initial_pool = self.fetch_all_stocks()

        if not initial_pool:
            log.warning("⚠️ No stocks found. Bot will keep scanning.")
            self.update_health('scanner', False, error="No stocks found")
            self.stock_list = []
        else:
            self.stock_list = [stock['symbol'] for stock in initial_pool][:MAX_STOCKS_TO_SCAN]
            log.info(f"Targets locked. Scanning {len(self.stock_list)} tickers.")
            self.update_health('scanner', True)

        self.start_websocket()
        
        self.scan_status = "🔄 Bot running, waiting for market..."
        self.market_condition = self.get_market_condition()
        
        log.info("="*60)
        log.info("⚡ ALPHA by ArandaTech - INITIALIZED")
        log.info(f"💰 Capital: ₹{self.capital}")
        log.info(f"📊 Max Positions: {MAX_POSITIONS}")
        log.info(f"🎯 Scan Interval: {SCAN_INTERVAL}s")
        log.info(f"🔒 Leverage: {LEVERAGE}x")
        log.info("="*60)
        send_system_alert("READY", f"Capital: ₹{self.capital}\nScanning: {len(self.stock_list)} stocks", "success")
        
        scan_count = 0
        last_cleanup = datetime.now()
        
        while self.running:
            try:
                # Periodic cleanup
                if (datetime.now() - last_cleanup).total_seconds() > 3600:
                    self.cleanup_caches()
                    last_cleanup = datetime.now()
                
                if not self.is_trading_day():
                    log.info("📅 Market closed. Sleeping...")
                    self.scan_status = "📅 Market Closed"
                    time.sleep(3600)
                    continue
                
                if not self.is_trading_time():
                    current_time = datetime.now().time()
                    market_open = datetime.strptime("09:15", "%H:%M").time()
                    if current_time < market_open:
                        self.scan_status = f"⏰ Market Opens at 09:15 AM"
                    else:
                        self.scan_status = "⏰ Market Closed for Today"
                    time.sleep(60)
                    continue
                
                if self.check_daily_loss_limit():
                    break
                
                if self.check_and_square_off():
                    break
                
                if not self.check_ws_health():
                    log.warning("[WS] WebSocket issues detected. Using fallback.")
                
                self.market_condition = self.get_market_condition()
                self.update_bulk_market_data()
                self.check_exits()

                if not self.stock_list:
                    log.warning("⚠️ Stock universe empty. Refreshing...")
                    new_pool = self.fetch_all_stocks()
                    if new_pool:
                        self.stock_list = [stock['symbol'] for stock in new_pool][:MAX_STOCKS_TO_SCAN]
                        log.info(f"[UNIVERSE] Recovered {len(self.stock_list)} stocks")
                        if self.ws_connected:
                            self.subscribe_to_stocks()
                        self.update_health('scanner', True)

                self.scan_and_trade()
                self.render_and_deploy_dashboard()
                
                scan_count += 1
                ws_status = "🟢" if self.ws_connected else "🔴"
                log.info(f"📊 {ws_status} | Positions: {len(self.positions)} | Capital: ₹{self.capital:.2f} | P&L: ₹{self.daily_pnl:.2f} | Scans: {scan_count}")
                
                time.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                log.info("🛑 Bot shutdown initiated by user...")
                self.running = False
                self.scan_status = "⏹️ Bot Stopped by User"
                self.update_health('bot_running', False)
                send_system_alert("SHUTDOWN", f"P&L: ₹{self.daily_pnl:.2f}", "shutdown")
                break
            except Exception as e:
                log.error(f"[BOT MAIN LOOP EXCEPTION]: {e}")
                self.scan_status = f"❌ Error: {str(e)[:50]}"
                self.update_health(error=f"Main loop error: {e}")
                send_system_alert("ERROR", f"{str(e)[:200]}", "error")
                time.sleep(10)
        
        self.stop_ws_heartbeat()
        self.generate_daily_summary()
        self.update_health('bot_running', False)
        send_system_alert("SHUTDOWN", f"Final P&L: ₹{self.daily_pnl:.2f}", "shutdown")
        log.info("🤖 Bot stopped. Final P&L: ₹{:.2f}".format(self.daily_pnl))

    def shutdown(self):
        """Gracefully shutdown the bot"""
        self.running = False
        self.scan_status = "⏹️ Bot Stopped"
        self.update_health('bot_running', False)
        self.stop_ws_heartbeat()
        if self.sws:
            try:
                self.sws.close_connection()
            except:
                pass
        send_system_alert("SHUTDOWN", "Bot shutdown complete", "shutdown")
        log.info("Bot shutdown complete.")


if __name__ == '__main__':
    bot = AngelTradingBot()
    bot.run()