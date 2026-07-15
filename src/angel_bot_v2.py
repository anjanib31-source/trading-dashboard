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

try:
    import nsepython
    NSEPYTHON_AVAILABLE = True
except ImportError:
    NSEPYTHON_AVAILABLE = False

# === LOAD ENVIRONMENT VARIABLES ===
load_dotenv()

# === API CREDENTIALS ===
API_KEY = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
MPIN = os.getenv("MPIN")
TOTP_SECRET = os.getenv("TOTP_SECRET")

# === GITHUB SYNCHRONIZATION ===
GITHUB_PAT = os.getenv("GITHUB_PAT")
GITHUB_USERNAME = "anjanib31-source"
GITHUB_REPO = "trading-dashboard"

# === DATABASE ===
DB_NAME = "trades.db"

# === TELEGRAM ALERTS ===
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8993963920:AAGl4hhH4rHfC-MQlPNXK7uZ3YwkEspngOY")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "947783716")

# === TRADING PARAMETERS ===
CAPITAL = 10000
MAX_POSITIONS = 3
LEVERAGE = 4
STOP_LOSS = 0.02
TRAILING_SL_ACTIVATION = 0.015
TRAILING_SL_PULLBACK = 0.007
MAX_HOLD_MINUTES = 90
SCAN_INTERVAL = 45

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

# === SETUP LOGGING ===
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/paper_bot_{datetime.now().strftime("%Y-%m-%d")}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# TELEGRAM ALERTS
# ============================================================

def send_telegram_alert(message):
    """Send alert to Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"[TELEGRAM] Alert sent")
            return True
        else:
            logger.error(f"[TELEGRAM] Failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"[TELEGRAM] Error: {e}")
        return False


def validate_environment():
    """Validate all required environment variables"""
    required_vars = ['API_KEY', 'CLIENT_CODE', 'MPIN', 'TOTP_SECRET']
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        logger.error(f"[ENV] Missing: {missing}")
        send_telegram_alert(f"❌ <b>Environment Error</b>\nMissing variables: {', '.join(missing)}")
        return False
    return True


# ============================================================
# DATABASE FUNCTIONS
# ============================================================

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
        
        conn.commit()
        conn.close()
        logger.info("[DB] Database initialized successfully")
    except Exception as e:
        logger.error(f"[DB] Error: {e}")


def backup_database():
    """Create a backup of the database"""
    try:
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"trades_backup_{timestamp}.db")
        shutil.copy2("trades.db", backup_path)
        logger.info(f"[BACKUP] Created: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"[BACKUP] Failed: {e}")
        return None


# ============================================================
# MAIN BOT CLASS
# ============================================================

class AngelTradingBot:
    def __init__(self):
        self.obj = None
        self.auth_token = None
        self.refresh_token = None
        self.feed_token = None
        self.sws = None
        
        self.positions = []
        self.capital = CAPITAL
        self.available_capital = CAPITAL
        self.initial_capital = CAPITAL
        
        self.live_prices = {}
        self.price_update_time = {}
        self.ws_connected = False
        
        self.symbol_tokens = {}
        self.token_symbols = {}
        self.bulk_data_store = {}  
        self.stock_list = []
        self.running = True
        self.lock = threading.RLock()
        self.trades = []
        self.daily_pnl = 0.0
        self.total_trades_today = 0
        
        self.sector_map = {}
        self.sector_exposure = {}
        self.correlation_matrix = {}
        self.atr_cache = {}
        self.volatility_cache = {}
        self.indicator_cache = {}
        
        self.scan_count = 0
        self.signals_found = 0
        self.stocks_scored = 0
        self.last_scan_time = None
        self.scan_status = "🔄 Bot Initializing..."
        self.market_condition = "Unknown"
        
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
            'start_time': datetime.now().isoformat()
        }
        self._save_health_status()

    # ============================================================
    # HEALTH MONITORING METHODS
    # ============================================================
    
    def _save_health_status(self):
        try:
            with open('health_status.json', 'w') as f:
                json.dump(self.health_status, f, default=str)
        except Exception as e:
            logger.error(f"[HEALTH] Failed: {e}")
    
    def update_health(self, component=None, status=None, error=None):
        with self.lock:
            if component and status is not None:
                self.health_status[component] = status
                logger.info(f"[HEALTH] {component}: {'✅ OK' if status else '❌ FAILED'}")
            
            if error:
                self.health_status['error_count'] += 1
                self.health_status['last_error'] = error
                self.health_status['status'] = '⚠️ System Error'
                logger.error(f"[HEALTH] Error: {error}")
                send_telegram_alert(f"⚠️ <b>Bot Error</b>\n{error[:200]}")
            
            all_ok = all([
                self.health_status.get('broker', False),
                self.health_status.get('market', False),
                self.health_status.get('scanner', False),
                self.health_status.get('sync', False),
                self.health_status.get('bot_running', True)
            ])
            
            self.health_status['all_ok'] = all_ok
            if not all_ok and not self.health_status.get('last_error'):
                self.health_status['status'] = '⚠️ System Degraded'
            elif all_ok:
                self.health_status['status'] = '✅ All systems operational'
            
            self.health_status['last_heartbeat'] = datetime.now().isoformat()
            self._save_health_status()

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
                logger.info("[RECOVERY] No open positions found")
                return
            
            for row in rows:
                position = {
                    'symbol': row['symbol'],
                    'entry_price': row['entry_price'],
                    'quantity': row['quantity'],
                    'strategy': row['strategy'],
                    'entry_time': datetime.fromisoformat(row['entry_time']) if row['entry_time'] else datetime.now(),
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
                logger.info(f"[RECOVERY] Restored: {row['symbol']} @ ₹{row['entry_price']}")
            
            send_telegram_alert(f"🔄 <b>Bot Restarted</b>\nRecovered {len(rows)} positions")
            logger.info(f"[RECOVERY] Loaded {len(rows)} positions")
        except Exception as e:
            logger.error(f"[RECOVERY] Error: {e}")

    # ============================================================
    # SAVE POSITION TO DATABASE
    # ============================================================
    
    def save_position_to_db(self, position_data):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
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
            
            conn.commit()
            conn.close()
            logger.info(f"[DB] Position saved: {position_data.get('symbol')}")
            return True
        except Exception as e:
            logger.error(f"[DB] Error saving position: {e}")
            return False

    def update_position_status(self, symbol, status='CLOSED'):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE positions SET status = ? 
                WHERE symbol = ? AND status = 'OPEN'
                ORDER BY entry_time DESC LIMIT 1
            ''', (status, symbol))
            conn.commit()
            conn.close()
            logger.info(f"[DB] Position updated: {symbol} -> {status}")
        except Exception as e:
            logger.error(f"[DB] Error updating position: {e}")

    # ============================================================
    # LOGIN
    # ============================================================

    def login(self):
        try:
            logger.info("Connecting to Angel One SmartAPI...")
            self.obj = SmartConnect(api_key=API_KEY)
            totp = pyotp.TOTP(TOTP_SECRET).now()
            login_data = self.obj.generateSession(CLIENT_CODE, MPIN, totp)
            
            if login_data.get('status', False):
                self.auth_token = login_data['data']['jwtToken']
                self.refresh_token = login_data['data']['refreshToken']
                self.feed_token = self.obj.getfeedToken()
                logger.info("[OK] API Connected")
                self.update_health('broker', True)
                send_telegram_alert(f"✅ <b>Login Successful</b>\n{datetime.now().strftime('%I:%M %p')}")
                return True
            else:
                logger.error(f"[FAIL] Login Failed: {login_data}")
                self.update_health('broker', False, error=f"Login failed")
                send_telegram_alert(f"❌ <b>Login Failed</b>\n{login_data}")
                return False
        except Exception as e:
            logger.error(f"[FAIL] Login Error: {e}")
            self.update_health('broker', False, error=f"Login error: {e}")
            send_telegram_alert(f"❌ <b>Login Error</b>\n{e}")
            return False

    def load_symbol_tokens(self):
        if not os.path.exists("scrip_master.json"):
            logger.error("[CRITICAL] 'scrip_master.json' missing")
            self.update_health(error="scrip_master.json missing")
            return {}

        try:
            logger.info("Extracting scrip entries from 'scrip_master.json'...")
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
            logger.info(f"[OK] Instantiated {len(symbol_map)} local NSE Equity scrip mappings.")
            return symbol_map
        except Exception as e:
            logger.error(f"[CRITICAL] Failed to read or parse local JSON file: {e}")
            self.update_health(error=f"Symbol load error: {e}")
            return {}

    # ============================================================
    # WEB SOCKET 2.0
    # ============================================================

    def start_websocket(self):
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
                logger.error(f"[WS] Error: {e}")
                self.update_health(error=f"WebSocket error: {e}")

        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        time.sleep(3)

    def on_ws_open(self, wsapp):
        logger.info("[WS] Open Channel Established.")
        self.ws_connected = True
        if self.stock_list:
            self.subscribe_to_stocks()

    def on_ws_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"[WS] Disconnected: {close_msg}")
        self.ws_connected = False
        self.update_health('broker', False, error=f"WS disconnected")
        send_telegram_alert(f"⚠️ <b>WebSocket Disconnected</b>\n{close_msg}")

    def on_ws_error(self, wsapp, error):
        logger.error(f"[WS] Error: {error}")
        self.update_health('broker', False, error=f"WS error: {error}")

    def on_ws_data(self, wsapp, message):
        try:
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
                    logger.info(f"[WS] Subscribed to {len(tokens)} symbols")
            except Exception as e:
                logger.error(f"[WS] Subscription error: {e}")

    def check_ws_health(self):
        if not self.ws_connected:
            logger.warning("[WS] WebSocket disconnected. Reconnecting...")
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
                logger.warning(f"[WS] {stale_count}/5 prices stale")
                self.update_health('broker', False, error=f"{stale_count}/5 prices stale")
                return self._reconnect_websocket()
        return True

    def _reconnect_websocket(self):
        try:
            if self.sws:
                try:
                    self.sws.close_connection()
                except:
                    pass
            self.start_websocket()
            time.sleep(3)
            if self.ws_connected and self.stock_list:
                self.subscribe_to_stocks()
                logger.info("[WS] Reconnected!")
                self.update_health('broker', True)
                return True
            return False
        except Exception as e:
            logger.error(f"[WS] Reconnection error: {e}")
            return False

    # ============================================================
    # DUAL DATA SOURCE: ANGEL ONE API + YAHOO FINANCE
    # ============================================================

    def get_indicator_data(self, symbol, period="5d", interval="15m"):
        """Fetch historical data - Angel One API (Primary) + Yahoo Finance (Fallback)"""
        try:
            cache_key = f"{symbol}_{period}_{interval}"
            
            # Check cache first
            with self.lock:
                if cache_key in self.indicator_cache:
                    cache_time, data = self.indicator_cache[cache_key]
                    if (datetime.now() - cache_time).seconds < 60:
                        logger.debug(f"[CACHE] Using cached data for {symbol}")
                        return data
            
            # === PRIMARY: Angel One Historical API ===
            try:
                logger.info(f"[ANGEL API] Fetching data for {symbol}...")
                
                token = self.symbol_tokens.get(symbol)
                if not token:
                    logger.warning(f"[ANGEL API] Token not found for {symbol}")
                    raise Exception("Token not found")
                
                end_date = datetime.now()
                start_date = end_date - timedelta(days=5)
                
                fromdate = start_date.strftime("%Y-%m-%d")
                todate = end_date.strftime("%Y-%m-%d")
                
                historical_data = self.obj.getCandleData(
                    exchange="NSE",
                    symboltoken=token,
                    interval="FIFTEEN_MINUTE",
                    fromdate=fromdate,
                    todate=todate
                )
                
                if historical_data and historical_data.get('status') == True:
                    data = self._parse_angel_historical_data(historical_data)
                    
                    if data is not None and not data.empty:
                        with self.lock:
                            self.indicator_cache[cache_key] = (datetime.now(), data)
                        
                        logger.info(f"[ANGEL API] Successfully fetched data for {symbol}")
                        return data
                    else:
                        logger.warning(f"[ANGEL API] Empty data for {symbol}")
                else:
                    logger.warning(f"[ANGEL API] API returned error for {symbol}")
                    
            except Exception as e:
                logger.warning(f"[ANGEL API] Failed for {symbol}: {e}")
            
            # === FALLBACK: Yahoo Finance ===
            try:
                logger.warning(f"[YAHOO FALLBACK] Fetching data for {symbol}...")
                
                stock = yf.Ticker(f"{symbol}.NS")
                data = stock.history(period=period, interval=interval)
                
                if not data.empty and len(data) > 10:
                    data = data.tail(100).copy()
                    
                    with self.lock:
                        self.indicator_cache[cache_key] = (datetime.now(), data)
                    
                    logger.info(f"[YAHOO FALLBACK] Successfully fetched data for {symbol}")
                    return data
                else:
                    logger.warning(f"[YAHOO FALLBACK] Empty data for {symbol}")
                    
            except Exception as e:
                logger.warning(f"[YAHOO FALLBACK] Failed for {symbol}: {e}")
            
            # === EMERGENCY: Bulk Data Store ===
            if symbol in self.bulk_data_store:
                logger.warning(f"[EMERGENCY] Using bulk data for {symbol}")
                return self.bulk_data_store[symbol]
            
            logger.warning(f"[FAILED] No data available for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"[ERROR] get_indicator_data failed for {symbol}: {e}")
            return None

    def _parse_angel_historical_data(self, historical_data):
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
            logger.error(f"[ERROR] Parsing Angel One historical data: {e}")
            return None

    # ============================================================
    # RISK MANAGEMENT
    # ============================================================

    def load_sector_mapping(self):
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
        logger.info(f"[SECTOR] Loaded {len(self.sector_map)} mappings")

    def get_sector(self, symbol):
        return self.sector_map.get(symbol, "OTHER")

    def check_correlation(self, symbol):
        if not ENABLE_CORRELATION_FILTER or not self.positions:
            return True
        for pos in self.positions:
            corr = self.calculate_correlation(symbol, pos['symbol'])
            if corr > MAX_CORRELATION_THRESHOLD:
                logger.info(f"[CORRELATION] {symbol} correlated with {pos['symbol']} ({corr:.2f})")
                return False
        return True

    def calculate_correlation(self, symbol1, symbol2):
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

    def check_sector_exposure(self, symbol):
        if not ENABLE_SECTOR_DIVERSIFICATION:
            return True
        sector = self.get_sector(symbol)
        exposure = 0.0
        for pos in self.positions:
            if self.get_sector(pos['symbol']) == sector:
                exposure += pos['entry_price'] * pos['quantity']
        if exposure / self.capital > MAX_SECTOR_EXPOSURE:
            logger.info(f"[SECTOR] {sector} exposure {exposure/self.capital*100:.1f}% > {MAX_SECTOR_EXPOSURE*100}%")
            return False
        return True

    def calculate_atr(self, symbol, period=14):
        try:
            data = self.bulk_data_store.get(symbol)
            if data is None or len(data) < period:
                return None
            high = data['High']; low = data['Low']; close = data['Close']
            tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
            return tr.rolling(period).mean().iloc[-1]
        except:
            return None

    def calculate_atr_position_size(self, symbol, price):
        if not ENABLE_ATR_POSITION_SIZING:
            return None
        atr = self.calculate_atr(symbol)
        if atr is None or atr == 0:
            return None
        qty = int((self.capital * 0.01) / (atr * 1.5))
        qty = max(1, qty)
        max_qty = int((self.available_capital * LEVERAGE) / price)
        return min(qty, max_qty)

    def get_dynamic_stop_loss(self, symbol, entry_price):
        if not ENABLE_DYNAMIC_SL:
            return STOP_LOSS
        atr = self.calculate_atr(symbol)
        if atr is None or atr == 0:
            return STOP_LOSS
        sl_pct = min(max((atr * 2) / entry_price, 0.01), 0.05)
        return sl_pct

    def get_volatility_stop(self, symbol, entry_price):
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

    def get_ltp(self, symbol):
        with self.lock:
            if symbol in self.live_prices:
                if symbol in self.price_update_time:
                    age = (datetime.now() - self.price_update_time[symbol]).total_seconds()
                    if age < 5:
                        return self.live_prices[symbol]
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
        if symbol in self.bulk_data_store:
            df = self.bulk_data_store[symbol]
            if df is not None and not df.empty:
                try:
                    return float(df['Close'].iloc[-1])
                except:
                    pass
        for pos in self.positions:
            if pos['symbol'] == symbol:
                return pos['entry_price']
        return None

    # ============================================================
    # SQUARE OFF & RISK CHECKS
    # ============================================================

    def check_and_square_off(self):
        now = datetime.now()
        current_time = now.time()
        close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
        square_off_time = (datetime.combine(now.date(), close_time) - timedelta(minutes=SQUARE_OFF_BUFFER)).time()
        
        if current_time >= square_off_time or current_time >= close_time:
            if self.positions:
                logger.info(f"[SQUARE OFF] Squaring {len(self.positions)} positions...")
                for position in self.positions[:]:
                    symbol = position['symbol']
                    qty = position['quantity']
                    current_price = self.get_ltp(symbol) or position['entry_price']
                    self.place_order(symbol, "SELL", qty, current_price)
                    self.update_position_status(symbol, 'CLOSED')
                    logger.info(f"[SQUARE OFF] {symbol}: ₹{position['entry_price']:.2f} → ₹{current_price:.2f}")
                self.generate_daily_summary()
                self.running = False
                self.update_health('bot_running', False)
                send_telegram_alert(f"🛑 <b>Market Closed - Squared Off</b>\n{len(self.positions)} positions closed")
                return True
            logger.info("[SQUARE OFF] No positions to close.")
            self.running = False
            self.update_health('bot_running', False)
            return True
        return False

    def check_daily_loss_limit(self):
        daily_loss_pct = (self.initial_capital - self.capital) / self.initial_capital
        if daily_loss_pct >= MAX_DAILY_LOSS:
            logger.warning(f"[RISK] Max daily loss reached: {daily_loss_pct*100:.2f}%")
            self.scan_status = "⛔ Stopped - Max Daily Loss"
            self.running = False
            self.update_health('bot_running', False, error=f"Max daily loss: {daily_loss_pct*100:.2f}%")
            send_telegram_alert(f"⛔ <b>Max Daily Loss Reached</b>\n{daily_loss_pct*100:.2f}%\nBot stopped.")
            return True
        return False

    # ============================================================
    # ORDER FUNCTIONS
    # ============================================================

    def place_order(self, symbol, transaction_type, quantity=1, price=None):
        try:
            if price is None:
                price = self.get_ltp(symbol)
                if not price:
                    logger.error(f"[FAIL] No price for {symbol}")
                    return None
            order_id = f"PAPER_{datetime.now().strftime('%H%M%S')}_{symbol}"
            margin = (price * quantity) / LEVERAGE
            with self.lock:
                if transaction_type == "BUY":
                    self.available_capital -= margin
                else:
                    self.available_capital += margin
            logger.info(f"📝 [ORDER] {transaction_type} {symbol} Qty:{quantity} @ ₹{price:.2f} Margin: ₹{margin:.2f}")
            return order_id
        except Exception as e:
            logger.error(f"[ORDER FAILURE] {e}")
            self.update_health(error=f"Order failed: {e}")
            return None

    def is_trading_time(self):
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
            logger.info(f"[MARKET] Sync completed")
        except Exception as e:
            logger.error(f"[MARKET] Error: {e}")
            self.update_health(error=f"Market data error: {e}")

    def calculate_vwap(self, data):
        try:
            df = data.copy()
            df['Cum_Vol_Price'] = ((df['High'] + df['Low'] + df['Close']) / 3.0) * df['Volume']
            dates = df.index.date
            df['CumVolPrice_Sum'] = df.groupby(dates)['Cum_Vol_Price'].cumsum()
            df['Cum_Vol_Sum'] = df.groupby(dates)['Volume'].cumsum()
            return df['CumVolPrice_Sum'] / df['Cum_Vol_Sum']
        except:
            return None

    def detect_orb(self, symbol, data):
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

    def get_market_condition(self):
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

    def is_market_tradeable(self):
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

    def score_stock(self, symbol):
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
    # STOCK FETCHING
    # ============================================================
    
    def fetch_all_stocks(self):
        logger.info("="*60)
        logger.info("📊 USING HARDCODED FALLBACK STOCKS")
        logger.info("="*60)
        return self.fetch_from_yahoo_fallback()
    
    def fetch_from_yahoo_fallback(self):
        fallback_symbols = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
            "LT", "AXISBANK", "WIPRO", "MARUTI", "TITAN",
            "TECHM", "NTPC", "ULTRACEMCO", "M&M", "BAJFINANCE",
            "SUNPHARMA", "POWERGRID", "NESTLEIND", "HCLTECH",
            "JSWSTEEL", "ADANIPORTS", "ONGC", "COALINDIA", "HDFCLIFE"
        ]
        return self.get_prices_bulk(fallback_symbols)
    
    def get_prices_bulk(self, symbols):
        all_stocks = []
        limit_symbols = symbols[:500]
        logger.info(f"📊 Analyzing {len(limit_symbols)} stocks...")
        stock_data = []
        try:
            tickers = [f"{s}.NS" for s in limit_symbols]
            data = yf.download(tickers=tickers, period="5d", interval="15m", progress=False)
            if data.empty:
                logger.warning("No data from Yahoo")
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
                logger.warning("No surge candidates. Falling back.")
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
            logger.info(f"✅ Selected {len(final_selected)} stocks")
        except Exception as e:
            logger.error(f"Error: {e}")
            return self.get_prices_fallback(symbols)
        if not all_stocks:
            logger.warning("No stocks passed. Falling back.")
            return self.get_prices_fallback(symbols)
        return all_stocks

    def get_prices_fallback(self, symbols):
        all_stocks = []
        limit_symbols = symbols[:MAX_STOCKS_TO_SCAN]
        logger.info(f"📊 Fallback: {len(limit_symbols)} stocks...")
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

    def calculate_estimated_charges(self, entry_price, exit_price, quantity):
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
    # CHECK EXITS WITH TELEGRAM ALERTS
    # ============================================================

    def check_exits(self):
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
                
                # === FIXED TIME EXIT ===
                elapsed_minutes = (datetime.now() - position['entry_time']).total_seconds() / 60
                if elapsed_minutes > MAX_HOLD_MINUTES:
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
                        logger.info(f"{exit_reason} | Qty: {exit_qty} | Net: ₹{net_pnl:.2f}")
                        position['peak_price'] = current_price
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
                        logger.info(f"{exit_reason} | Net: ₹{net_pnl:.2f}")
                        # === TELEGRAM ALERT ON EXIT ===
                        if pnl_pct >= 0:
                            send_telegram_alert(f"✅ <b>Position Closed (Profit)</b>\nSymbol: {symbol}\nP&L: +{net_pnl:.2f} ({pnl_pct*100:.2f}%)\nReason: {exit_reason}")
                        else:
                            send_telegram_alert(f"🔴 <b>Position Closed (Loss)</b>\nSymbol: {symbol}\nP&L: {net_pnl:.2f} ({pnl_pct*100:.2f}%)\nReason: {exit_reason}")
                        
        except Exception as e:
            logger.error(f"[EXIT ERROR] {e}")
            self.update_health(error=f"Exit check error: {e}")

    # ============================================================
    # SCAN AND TRADE
    # ============================================================

    def scan_and_trade(self):
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
            
        logger.info(f"[SCAN] #{self.scan_count} | {self.market_condition}")
        
        def evaluate_signal(symbol):
            if any(pos['symbol'] == symbol for pos in self.positions):
                return None
            score, direction, strategy = self.score_stock(symbol)
            return (symbol, score, direction, strategy)

        signals = []
        scored_count = 0
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(evaluate_signal, s) for s in self.stock_list]
            for fut in as_completed(futures):
                res = fut.result()
                scored_count += 1
                if res and res[1] >= MIN_SIGNAL_SCORE and res[2] == "LONG":
                    signals.append(res)
                    logger.info(f"[SCAN] 📈 Found: {res[0]} | Score: {res[1]} | {res[2]}")
        
        self.stocks_scored = scored_count
        self.signals_found = len(signals)
        
        logger.info(f"[SCAN] Scored: {scored_count}, Signals: {len(signals)}")
        
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
                logger.warning(f"[SCAN] No price for {symbol}")
                continue
            
            if not self.check_correlation(symbol):
                continue
            
            if not self.check_sector_exposure(symbol):
                continue
            
            dynamic_sl = self.get_dynamic_stop_loss(symbol, current_price)
            volatility_sl = self.get_volatility_stop(symbol, current_price)
            final_sl = max(dynamic_sl, volatility_sl)
            
            allocation_pct = get_position_allocation(score)
            allocation_amount = self.capital * allocation_pct
            qty = int(allocation_amount / current_price)
            
            if qty <= 0:
                continue
                
            margin = (current_price * qty) / LEVERAGE
            
            with self.lock:
                if margin > self.available_capital:
                    logger.warning(f"[MARGIN] Skipped {symbol}: Need ₹{margin:.2f}, Have ₹{self.available_capital:.2f}")
                    continue
                    
                logger.info(f"💥 [SIGNAL] {symbol} | Score: {score} | Qty: {qty} @ ₹{current_price:.2f}")
                logger.info(f"[ALLOCATION] {allocation_pct*100:.1f}% (₹{allocation_amount:.2f})")
                logger.info(f"[RISK] SL: {final_sl*100:.2f}% | Margin: ₹{margin:.2f}")
                
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
                
                # === TELEGRAM ALERT ON POSITION OPEN ===
                send_telegram_alert(
                    f"📈 <b>Position Opened</b>\n"
                    f"Symbol: {symbol}\n"
                    f"Entry: ₹{current_price:.2f}\n"
                    f"Qty: {qty}\n"
                    f"Score: {score}\n"
                    f"Strategy: {strategy}\n"
                    f"Target: ₹{target_price:.2f}\n"
                    f"Stop: ₹{stop_price:.2f}"
                )

    # ============================================================
    # SAVE TRADE
    # ============================================================
    
    def save_trade(self, trade_data):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
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
            conn.commit()
            conn.close()
            logger.info(f"[DB] Trade saved: {trade_data.get('symbol')}")
        except Exception as e:
            logger.error(f"[DB] Error saving trade: {e}")
            self.update_health(error=f"DB Error: {e}")

    # ============================================================
    # GENERATE DAILY SUMMARY
    # ============================================================

    def generate_daily_summary(self):
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
        logger.info(summary)
        
        # Send summary via Telegram
        if self.daily_pnl != 0 or len(self.trades) > 0:
            send_telegram_alert(f"📊 <b>Daily Summary</b>\nP&L: {self.daily_pnl:+.2f}\nTrades: {len(self.trades)}\nCapital: ₹{self.capital:.2f}")

    # ============================================================
    # GITHUB SYNC & DASHBOARD
    # ============================================================

    def sync_to_github_pages(self, html_content):
        if not GITHUB_PAT or "ghp_" not in GITHUB_PAT:
            logger.error("GitHub PAT missing")
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
                    logger.info("🚀 [GITHUB] Dashboard updated")
                    self.update_health('sync', True)
                    return
                elif push_res.status_code == 409:
                    logger.warning(f"[GITHUB] Conflict, retry {attempt+1}/3")
                    time.sleep(2)
                else:
                    logger.error(f"[GITHUB] Failed: {push_res.status_code}")
                    self.update_health('sync', False, error=f"GitHub push failed: {push_res.status_code}")
                    break
            except Exception as e:
                logger.error(f"[GITHUB] Error: {e}")
                self.update_health('sync', False, error=f"GitHub sync error: {e}")
                break

    def render_and_deploy_dashboard(self):
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
        const API_BASE = 'https://turbine-bust-upload.ngrok-free.dev/api';
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
            logger.error(f"[DASHBOARD ERROR] {e}")
            self.update_health('sync', False, error=f"Dashboard error: {e}")

    # ============================================================
    # MARKET STATUS CHECK
    # ============================================================

    def fetch_nse_holidays(self):
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

    def is_trading_day(self, check_date=None):
        if check_date is None:
            check_date = datetime.now().date()
        if check_date.weekday() >= 5:
            return False
        holidays = self.fetch_nse_holidays()
        if check_date.strftime("%Y-%m-%d") in holidays:
            return False
        return True

    def check_market_status(self):
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
            logger.info(f"⏳ {status}")
            return False, status
        
        if current_time < market_open:
            status = f"⏰ Market Opens at 09:15 AM"
            self.scan_status = status
            self.market_condition = status
            self.update_health('market', False)
            logger.info(f"⏳ {status}")
            return False, status
        
        if current_time > market_close:
            status = "⏰ Market Closed for Today"
            self.scan_status = status
            self.market_condition = status
            self.update_health('market', False)
            logger.info(f"⏳ {status}")
            return False, status
        
        status = "✅ Market OPEN - Trading Active"
        self.scan_status = status
        self.market_condition = "📈 LIVE MARKET"
        self.update_health('market', True)
        logger.info(f"✅ {status}")
        return True, status

    # ============================================================
    # MAIN RUN LOOP
    # ============================================================

    def run(self):
        # === GRACEFUL SHUTDOWN HANDLER ===
        def signal_handler(sig, frame):
            logger.info("[SHUTDOWN] Received signal. Shutting down gracefully...")
            self.running = False
            # Force save all positions to DB
            for pos in self.positions:
                self.save_position_to_db({
                    'symbol': pos['symbol'],
                    'entry_price': pos['entry_price'],
                    'quantity': pos['quantity'],
                    'strategy': pos['strategy'],
                    'entry_time': pos['entry_time'].isoformat() if hasattr(pos['entry_time'], 'isoformat') else datetime.now().isoformat()
                })
            send_telegram_alert(f"🛑 <b>Bot Shutting Down</b>\nP&L: ₹{self.daily_pnl:.2f}\nPositions: {len(self.positions)}")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # === ENVIRONMENT VALIDATION ===
        if not validate_environment():
            logger.error("[STARTUP] Environment validation failed.")
            return
        
        # === DATABASE BACKUP ===
        backup_database()
        send_telegram_alert("🔔 <b>ALPHA Bot Starting</b>\nSystem initializing...")
        
        init_database()
        
        # === LOAD POSITIONS FROM DB ===
        self.load_positions_from_db()
        
        logger.info("="*60)
        logger.info("⚡ ALPHA by ArandaTech - STARTUP")
        logger.info("="*60)
        logger.info(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"💰 Initial Capital: ₹{self.capital}")
        logger.info(f"📊 Max Positions: {MAX_POSITIONS}")
        logger.info(f"🎯 Scan Interval: {SCAN_INTERVAL}s")
        logger.info(f"📈 Min Signal Score: {MIN_SIGNAL_SCORE}")
        logger.info(f"🔒 Leverage: {LEVERAGE}x")
        logger.info("="*60)
        logger.info("📋 FEATURES ENABLED:")
        logger.info(f"   ✅ Dynamic SL: {'Enabled' if ENABLE_DYNAMIC_SL else 'Disabled'}")
        logger.info(f"   ✅ ATR Sizing: {'Enabled' if ENABLE_ATR_POSITION_SIZING else 'Disabled'}")
        logger.info(f"   ✅ Correlation Filter: {'Enabled' if ENABLE_CORRELATION_FILTER else 'Disabled'}")
        logger.info(f"   ✅ Sector Divers.: {'Enabled' if ENABLE_SECTOR_DIVERSIFICATION else 'Disabled'}")
        logger.info(f"   ✅ Volatility SL: {'Enabled' if ENABLE_VOLATILITY_STOP else 'Disabled'}")
        logger.info(f"   ✅ Trailing Stop Loss: Active")
        logger.info(f"   ✅ Partial Profit Taking: Active")
        logger.info(f"   ✅ Telegram Alerts: Active")
        logger.info(f"   ✅ Data Source: Angel One API (Primary) + Yahoo (Fallback)")
        logger.info("="*60)
        self.update_health('bot_running', True)
        send_telegram_alert(f"✅ <b>Bot Initialized</b>\nCapital: ₹{self.capital}\nMax Positions: {MAX_POSITIONS}")
        
        logger.info("📅 Checking market status...")
        is_open, status = self.check_market_status()
        
        if not is_open:
            logger.info(f"💤 Market closed. Status: {status}")
            
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
                logger.info(f"⏰ Sleeping for {hours}h {minutes}m until next trading day...")
                send_telegram_alert(f"⏰ <b>Market Closed</b>\nNext trading day: {next_trading_date.strftime('%A, %B %d')}")
                time.sleep(seconds_to_wait)
                self.run()
                return
            
            elif "Opens at" in status:
                today = datetime.now().date()
                market_open_time = datetime.combine(today, datetime.strptime("09:15", "%H:%M").time())
                seconds_to_wait = (market_open_time - datetime.now()).total_seconds()
                hours = int(seconds_to_wait // 3600)
                minutes = int((seconds_to_wait % 3600) // 60)
                logger.info(f"⏰ Sleeping for {hours}h {minutes}m until market opens...")
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
                logger.info(f"⏰ Sleeping for {hours}h {minutes}m until tomorrow...")
                time.sleep(seconds_to_wait)
                self.run()
                return
        
        logger.info("✅ Market is OPEN! Proceeding with login...")
        
        if not self.login():
            logger.error("Failed to login. Exiting.")
            self.update_health('broker', False, error="Login failed")
            send_telegram_alert("❌ <b>Login Failed</b>\nBot cannot start trading.")
            return
            
        self.load_symbol_tokens()
        self.load_sector_mapping()
        
        initial_pool = self.fetch_all_stocks()
        retry_count = 0
        while not initial_pool and retry_count < 3:
            retry_count += 1
            logger.warning(f"⚠️ No stocks found. Retrying in 30s ({retry_count}/3)...")
            time.sleep(30)
            initial_pool = self.fetch_all_stocks()

        if not initial_pool:
            logger.warning("⚠️ No stocks found. Bot will keep scanning.")
            self.update_health('scanner', False, error="No stocks found")
            self.stock_list = []
        else:
            self.stock_list = [stock['symbol'] for stock in initial_pool][:MAX_STOCKS_TO_SCAN]
            logger.info(f"Targets locked. Scanning {len(self.stock_list)} tickers.")
            self.update_health('scanner', True)

        self.start_websocket()
        
        self.scan_status = "🔄 Bot running, waiting for market..."
        self.market_condition = self.get_market_condition()
        
        logger.info("="*60)
        logger.info("⚡ ALPHA by ArandaTech - INITIALIZED")
        logger.info(f"💰 Capital: ₹{self.capital}")
        logger.info(f"📊 Max Positions: {MAX_POSITIONS}")
        logger.info(f"🎯 Scan Interval: {SCAN_INTERVAL}s")
        logger.info(f"🔒 Leverage: {LEVERAGE}x")
        logger.info("="*60)
        send_telegram_alert(f"🚀 <b>Bot Ready</b>\nCapital: ₹{self.capital}\nScanning: {len(self.stock_list)} stocks")
        
        scan_count = 0
        while self.running:
            try:
                if not self.is_trading_day():
                    logger.info("📅 Market closed. Sleeping...")
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
                    logger.warning("[WS] WebSocket issues detected. Using fallback.")
                
                self.market_condition = self.get_market_condition()
                self.update_bulk_market_data()
                self.check_exits()

                if not self.stock_list:
                    logger.warning("⚠️ Stock universe empty. Refreshing...")
                    new_pool = self.fetch_all_stocks()
                    if new_pool:
                        self.stock_list = [stock['symbol'] for stock in new_pool][:MAX_STOCKS_TO_SCAN]
                        logger.info(f"[UNIVERSE] Recovered {len(self.stock_list)} stocks")
                        if self.ws_connected:
                            self.subscribe_to_stocks()
                        self.update_health('scanner', True)

                self.scan_and_trade()
                self.render_and_deploy_dashboard()
                
                scan_count += 1
                ws_status = "🟢" if self.ws_connected else "🔴"
                logger.info(f"📊 {ws_status} | Positions: {len(self.positions)} | Capital: ₹{self.capital:.2f} | P&L: ₹{self.daily_pnl:.2f} | Scans: {scan_count}")
                
                time.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("🛑 Bot shutdown initiated by user...")
                self.running = False
                self.scan_status = "⏹️ Bot Stopped by User"
                self.update_health('bot_running', False)
                send_telegram_alert(f"🛑 <b>Bot Stopped by User</b>\nP&L: ₹{self.daily_pnl:.2f}")
                break
            except Exception as e:
                logger.error(f"[BOT MAIN LOOP EXCEPTION]: {e}")
                self.scan_status = f"❌ Error: {str(e)[:50]}"
                self.update_health(error=f"Main loop error: {e}")
                send_telegram_alert(f"⚠️ <b>Bot Error</b>\n{str(e)[:200]}")
                time.sleep(10)
        
        self.generate_daily_summary()
        self.update_health('bot_running', False)
        send_telegram_alert(f"🏁 <b>Bot Stopped</b>\nFinal P&L: ₹{self.daily_pnl:.2f}")
        logger.info("🤖 Bot stopped. Final P&L: ₹{:.2f}".format(self.daily_pnl))

    def shutdown(self):
        self.running = False
        self.scan_status = "⏹️ Bot Stopped"
        self.update_health('bot_running', False)
        if self.sws:
            try:
                self.sws.close_connection()
            except:
                pass
        send_telegram_alert("🛑 <b>Bot Shutdown Complete</b>")
        logger.info("Bot shutdown complete.")


if __name__ == '__main__':
    bot = AngelTradingBot()
    bot.run()