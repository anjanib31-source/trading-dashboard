from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp
import json
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
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import nsepython
    NSEPYTHON_AVAILABLE = True
except ImportError:
    NSEPYTHON_AVAILABLE = False

# === CONFIGURATION & CREDENTIALS ===
import os
from dotenv import load_dotenv

# Load variables from the .env file located in the same directory
load_dotenv()

API_KEY = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
MPIN = os.getenv("MPIN")
TOTP_SECRET = os.getenv("TOTP_SECRET")

# === GITHUB SYNCHRONIZATION CONSTANTS ===
GITHUB_PAT = os.getenv("GITHUB_PAT")
GITHUB_USERNAME = "anjanib31-source" # This is safe to keep here
GITHUB_REPO = "trading-dashboard"   # This is safe to keep here

# === TRADING PARAMETERS ===
CAPITAL = 10000
MAX_POSITIONS = 3
LEVERAGE = 4
STOP_LOSS = 0.02
TRAILING_SL_ACTIVATION = 0.015  
TRAILING_SL_PULLBACK = 0.007    
MAX_HOLD_MINUTES = 90
SCAN_INTERVAL = 45

PROFIT_TARGETS = {8: 0.035, 9: 0.040, 10: 0.045}
DEFAULT_PROFIT_TARGET = 0.030
MIN_SIGNAL_SCORE = 7

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
        self.lock = threading.Lock()
        self.trades = []

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
                logger.info("[OK] API Connected via SmartAPI Session.")
                return True
            else:
                logger.error(f"[FAIL] Login Failed: {login_data}")
                return False
        except Exception as e:
            logger.error(f"[FAIL] Login Error: {e}")
            return False

    def load_symbol_tokens(self):
        if not os.path.exists("scrip_master.json"):
            logger.error("[CRITICAL] 'scrip_master.json' missing from directory workspace.")
            return {}

        try:
            logger.info("Extracting scrip entries from local 'scrip_master.json'...")
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
            return {}

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
                logger.error(f"[WS] Execution lifecycle dropped: {e}")

        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
        time.sleep(3)

    def on_ws_open(self, wsapp):
        logger.info("[WS] Open Channel Established.")
        self.ws_connected = True
        if self.stock_list:
            self.subscribe_to_stocks()

    def on_ws_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"[WS] Disconnected: {close_msg} ({close_status_code})")
        self.ws_connected = False

    def on_ws_error(self, wsapp, error):
        logger.error(f"[WS] Channel Runtime Error: {error}")

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
                    logger.info(f"[WS] Active Subscriptions bound to {len(tokens)} runtime symbols.")
            except Exception as e:
                logger.error(f"[WS] Subscription Error Payload: {e}")

    def get_ltp(self, symbol):
        with self.lock:
            if symbol in self.live_prices:
                return self.live_prices[symbol]
                
        if symbol in self.bulk_data_store:
            df = self.bulk_data_store[symbol]
            if df is not None and not df.empty:
                try:
                    return float(df['Close'].iloc[-1])
                except Exception:
                    pass
        return None

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
            logger.info(f"[MARKET ENGINE] Sync completed for {len(self.stock_list)} matrix trackers.")
        except Exception as e:
            logger.error(f"[MARKET ENGINE] Bulk synchronized download failed: {e}")

    def calculate_vwap(self, data):
        try:
            df = data.copy()
            df['Cum_Vol_Price'] = (df['High'] + df['Low'] + df['Close']) / 3.0 * df['Volume']
            dates = df.index.date
            df['CumVolPrice_Sum'] = df.groupby(dates)['Cum_Vol_Price'].cumsum()
            df['Cum_Vol_Sum'] = df.groupby(dates)['Volume'].cumsum()
            return df['CumVolPrice_Sum'] / df['Cum_Vol_Sum']
        except Exception:
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
        except Exception:
            pass
        return None, "NEUTRAL", 0

    def is_market_tradeable(self):
        try:
            nifty = yf.Ticker("^NSEI")
            data = nifty.history(period="2d", interval="5m")
            if len(data) < 20: 
                return True
            close = data['Close']
            sma20 = close.rolling(20).mean().iloc[-1]
            sma50 = close.rolling(50).mean().iloc[-1]
            return (close.iloc[-1] > sma20 and sma20 > sma50)
        except Exception:
            return True

    def score_stock(self, symbol):
        try:
            if symbol not in self.bulk_data_store:
                return 0, "NEUTRAL", "NONE"
            data = self.bulk_data_store[symbol]
            if data is None or len(data) < 5:
                return 0, "NEUTRAL", "NONE"
            if ENABLE_MARKET_FILTER and not self.is_market_tradeable():
                return 0, "NEUTRAL", "NONE"

            close = data['Close']
            current_price = close.iloc[-1]
            vwap_series = self.calculate_vwap(data)
            vwap_val = vwap_series.iloc[-1] if vwap_series is not None and len(vwap_series) > 0 else current_price
            ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
            
            if current_price > vwap_val and current_price > ema20:
                orb_p, orb_dir, orb_score = self.detect_orb(symbol, data)
                if orb_dir == "LONG":
                    return orb_score, "LONG", "ORB"
                return 8, "LONG", "Trend-Follow"
        except Exception:
            pass
        return 0, "NEUTRAL", "NONE"

    def get_prices_bulk(self, symbols):
        all_stocks = []
        limit_symbols = symbols[:120] 
        try:
            tickers = [f"{s}.NS" for s in limit_symbols]
            data = yf.download(tickers=tickers, period="1d", progress=False)
            for symbol in limit_symbols:
                ticker_name = f"{symbol}.NS"
                if len(limit_symbols) == 1:
                    price = float(data['Close'].iloc[-1]) if not data.empty else None
                else:
                    price = float(data['Close'][ticker_name].iloc[-1]) if ticker_name in data['Close'].columns else None
                
                if price and MIN_STOCK_PRICE < price < MAX_STOCK_PRICE:
                    token = self.symbol_tokens.get(symbol)
                    if token:
                        all_stocks.append({'symbol': symbol, 'token': token, 'price': price})
        except Exception as e:
            logger.error(f"Error filtering stock pricing boundaries: {e}")
        return all_stocks

    def fetch_all_stocks(self):
        if AUTO_PICK_STOCKS and NSEPYTHON_AVAILABLE:
            try:
                logger.info("[AUTO] Extracting Nifty 500 pool dynamically...")
                symbols = nsepython.nse_nifty500()
                if symbols and len(symbols) > 10:
                    return self.get_prices_bulk(symbols)
            except Exception as e:
                logger.warning(f"Nsepython failure ({e}). Falling back to layout array.")
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

    def calculate_estimated_charges(self, entry_price, exit_price, quantity):
        turnover = (entry_price + exit_price) * quantity
        buy_brokerage = min(20.0, entry_price * quantity * 0.0003)
        sell_brokerage = min(20.0, exit_price * quantity * 0.0003)
        total_brokerage = buy_brokerage + sell_brokerage
        stt = (exit_price * quantity) * 0.00025
        txn_charges = turnover * 0.0000322
        gst = (total_brokerage + txn_charges) * 0.18
        sebi_fee = turnover * 0.000001
        stamp_duty = (entry_price * quantity) * 0.00003
        return total_brokerage + stt + txn_charges + gst + sebi_fee + stamp_duty

    def place_order(self, symbol, transaction_type, quantity=1, price=None):
        try:
            if price is None:
                price = self.get_ltp(symbol)
                if not price:
                    logger.error(f"[FAIL] Could not get price token execution context for {symbol}")
                    return None
            
            order_id = f"PAPER_{datetime.now().strftime('%H%M%S')}_{symbol}"
            margin = (price * quantity) / LEVERAGE
            
            with self.lock:
                if transaction_type == "BUY":
                    self.available_capital -= margin
                else:
                    self.available_capital += margin
                    
            logger.info(f"📝 [PAPER ORDER] {transaction_type} | {symbol} | Qty: {quantity} @ Rs.{price:.2f} | Margin: Rs.{margin:.2f}")
            return order_id
        except Exception as e:
            logger.error(f"[ORDER FAILURE] System exception occurred inside place_order processing layout: {e}")
            return None

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
                
                if current_price > position['peak_price']:
                    position['peak_price'] = current_price
                
                trailing_sl_price = position['peak_price'] * (1.0 - TRAILING_SL_PULLBACK)
                exit_triggered = False
                exit_reason = ""
                
                if pnl_pct >= position.get('profit_target', DEFAULT_PROFIT_TARGET):
                    exit_triggered = True
                    exit_reason = f"🎯 [PROFIT TARGET] Exited {symbol} at +{pnl_pct*100:.2f}%"
                elif (position['peak_price'] - entry_price) / entry_price >= TRAILING_SL_ACTIVATION and current_price <= trailing_sl_price:
                    exit_triggered = True
                    exit_reason = f"🛡️ [TRAILING SL] Exited {symbol} at +{pnl_pct*100:.2f}%"
                elif pnl_pct <= -STOP_LOSS:
                    exit_triggered = True
                    exit_reason = f"🛑 [STOP LOSS] Exited {symbol} at {pnl_pct*100:.2f}%"
                elif ((datetime.now() - position['entry_time']).total_seconds() / 60) > MAX_HOLD_MINUTES:
                    exit_triggered = True
                    exit_reason = f"⏳ [TIME EXPIRED] Exited {symbol} at {pnl_pct*100:.2f}%"
                
                if exit_triggered:
                    self.place_order(symbol, "SELL", qty, current_price)
                    charges = self.calculate_estimated_charges(entry_price, current_price, qty)
                    net_pnl = raw_pnl - charges
                    
                    with self.lock:
                        self.capital += net_pnl
                        self.trades.append({
                            'symbol': symbol, 'gross_pnl': raw_pnl, 'charges': charges,
                            'net_pnl': net_pnl, 'pnl_pct': pnl_pct, 'strategy': position['strategy'],
                            'exit_status': "Target Hit" if pnl_pct > 0 else "Stop Loss", 'direction': 'BUY/SELL',
                            'entry_price': entry_price, 'exit_price': current_price
                        })
                        self.positions.remove(position)
                    logger.info(f"{exit_reason} | Fees: ₹{charges:.2f} | Net Yield: ₹{net_pnl:.2f}")
        except Exception as e:
            logger.error(f"[EXIT ENGINE ERROR] Exception tracking runtime matrix state: {e}")

    def scan_and_trade(self):
        if len(self.positions) >= MAX_POSITIONS:
            logger.info(f"[SCAN] Max position cap hit ({len(self.positions)}/{MAX_POSITIONS}). Skipping setups.")
            return
            
        logger.info("[SCAN] Sweeping downloaded metrics to evaluate trade setups...")
        def evaluate_signal(symbol):
            if any(pos['symbol'] == symbol for pos in self.positions):
                return None
            score, direction, strategy = self.score_stock(symbol)
            return (symbol, score, direction, strategy)

        signals = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(evaluate_signal, s) for s in self.stock_list]
            for fut in as_completed(futures):
                res = fut.result()
                if res and res[1] >= MIN_SIGNAL_SCORE and res[2] == "LONG":
                    signals.append(res)
                    
        for symbol, score, direction, strategy in signals:
            if len(self.positions) >= MAX_POSITIONS:
                break
                
            current_price = self.get_ltp(symbol)
            if not current_price:
                continue
                
            qty = int((self.capital / MAX_POSITIONS) / current_price)
            if qty <= 0:
                continue
                
            margin = (current_price * qty) / LEVERAGE
            
            with self.lock:
                if margin > self.available_capital:
                    logger.warning(f"[MARGIN INSURGENCY] Skipped {symbol}: Need ₹{margin:.2f}, Have ₹{self.available_capital:.2f}")
                    continue
                    
                logger.info(f"💥 [SIGNAL DETECTED] {symbol} -> Score: {score} Qty: {qty} @ ₹{current_price}")
                
                order_id = f"PAPER_{datetime.now().strftime('%H%M%S')}_{symbol}"
                self.available_capital -= margin
                
                logger.info(f"📝 [PAPER ORDER] BUY | {symbol} | Qty: {qty} @ Rs.{current_price:.2f} | Margin: Rs.{margin:.2f}")
                
                self.positions.append({
                    'symbol': symbol, 'entry_price': current_price, 'peak_price': current_price,  
                    'quantity': qty, 'entry_time': datetime.now(), 'score': score,
                    'profit_target': PROFIT_TARGETS.get(score, DEFAULT_PROFIT_TARGET), 'strategy': strategy
                })

    def sync_to_github_pages(self, html_content):
        """Pushes compiled dashboard source cleanly to your GitHub Pages repository."""
        if not GITHUB_PAT or "ghp_" not in GITHUB_PAT:
            return
        
        url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/index.html"
        headers = {
            "Authorization": f"token {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        import base64
        encoded_content = base64.b64encode(html_content.encode('utf-8')).decode('utf-8')
        
        try:
            # Query if dashboard code already exists to grab file blob sha key match
            r = requests.get(url, headers=headers)
            sha = r.json().get("sha") if r.status_code == 200 else None
            
            payload = {
                "message": f"Engine Sync Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "content": encoded_content
            }
            if sha:
                payload["sha"] = sha
                
            push_res = requests.put(url, headers=headers, json=payload)
            if push_res.status_code in [200, 201]:
                logger.info("🚀 [GITHUB PUSH] Live dashboard updated successfully on GitHub Pages.")
            else:
                logger.warning(f"[GITHUB PUSH FAILED] Response code: {push_res.status_code} - {push_res.text}")
        except Exception as e:
            logger.error(f"[GITHUB SYNC EXCEPTION] Internal connectivity error: {e}")

    def render_and_deploy_dashboard(self):
        """Compiles the entire runtime status into a beautifully structured single-page HTML PWA dashboard."""
        try:
            total_floating_pnl = 0.0
            formatted_positions = []
            
            for pos in self.positions:
                sym = pos['symbol']
                live_p = self.get_ltp(sym) or pos['entry_price']
                pos_pnl = (live_p - pos['entry_price']) * pos['quantity']
                pos_pnl_pct = ((live_p - pos['entry_price']) / pos['entry_price']) * 100
                total_floating_pnl += pos_pnl
                
                target_price = pos['entry_price'] * (1.0 + pos['profit_target'])
                sl_price = pos['entry_price'] * (1.0 - STOP_LOSS)
                
                formatted_positions.append({
                    "symbol": sym, "strategy": pos['strategy'], "quantity": pos['quantity'],
                    "entry_price": round(pos['entry_price'], 2), "live_price": round(live_p, 2),
                    "pnl": round(pos_pnl, 2), "pnl_pct": round(pos_pnl_pct, 2),
                    "sl_price": round(sl_price, 2), "target_price": round(target_price, 2)
                })

            total_portfolio_value = self.available_capital + (self.capital - self.available_capital) + total_floating_pnl
            floating_pct = (total_floating_pnl / self.initial_capital) * 100
            pnl_color_class = "text-emerald-400" if total_floating_pnl >= 0 else "text-rose-500"
            pnl_prefix = "+" if total_floating_pnl >= 0 else ""

            ohlc_js_store = {}
            for pos in self.positions:
                sym = pos['symbol']
                if sym in self.bulk_data_store:
                    df = self.bulk_data_store[sym]
                    if df is not None and not df.empty:
                        candles = []
                        vwap_series = self.calculate_vwap(df)
                        for idx, row in df.iterrows():
                            candles.append({
                                "time": int(idx.timestamp()), "open": float(row['Open']), "high": float(row['High']),
                                "low": float(row['Low']), "close": float(row['Close']),
                                "vwap": float(vwap_series.loc[idx]) if vwap_series is not None else float(row['Close'])
                            })
                        ohlc_js_store[sym] = candles

            if not ohlc_js_store and self.stock_list:
                for sym in self.stock_list[:2]:
                    if sym in self.bulk_data_store:
                        df = self.bulk_data_store[sym]
                        if df is not None and not df.empty:
                            candles = []
                            for idx, row in df.iterrows():
                                candles.append({
                                    "time": int(idx.timestamp()), "open": float(row['Open']), "high": float(row['High']),
                                    "low": float(row['Low']), "close": float(row['Close']), "vwap": float(row['Close'])
                                })
                            ohlc_js_store[sym] = candles

            # Clean replacement rendering to completely dodge python template interpreter bugs
            html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Matrix Engine Live Tracker</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
    <style>
        body { background-color: #0b0e11; color: #e9ecef; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
        .card { background-color: #161a1e; border: 1px solid #2b3139; }
        .pulse { animation: pulse-animation 2s infinite; }
        @keyframes pulse-animation { 0% { opacity: 0.4; } 50% { opacity: 1; } 100% { opacity: 0.4; } }
    </style>
</head>
<body class="p-3 md:p-6 max-w-lg mx-auto font-sans antialiased">
    <header class="flex justify-between items-center mb-5 border-b border-gray-800 pb-3">
        <div>
            <div class="flex items-center gap-1.5">
                <span class="w-2 h-2 rounded-full bg-emerald-500 pulse"></span>
                <h1 class="text-sm font-bold tracking-wider text-emerald-400">MATRIX BOT ALIVE</h1>
            </div>
            <p class="text-[10px] text-gray-500">Sync: __SYNC_TIME__</p>
        </div>
        <div class="text-right text-[11px] font-mono text-gray-400">
            <div>Margin: <span class="text-white font-bold">₹__MARGIN__</span></div>
            <div>Value: <span class="text-white font-bold">₹__PORTFOLIO_VALUE__</span></div>
        </div>
    </header>

    <div class="card p-4 rounded-xl text-center mb-5 shadow-lg">
        <p class="text-[10px] text-gray-400 uppercase tracking-widest font-semibold">Today's Floating P&L</p>
        <p class="text-3xl font-extrabold font-mono mt-1 __PNL_COLOR__">
            __PREFIX__₹__FLOATING_PNL__ <span class="text-lg font-medium">(__PREFIX____FLOATING_PCT__%)</span>
        </p>
    </div>

    <section class="mb-6">
        <h2 class="text-xs font-bold text-gray-400 tracking-wider uppercase mb-3">Active Tactical Holdings (__POSITIONS_COUNT__)</h2>
        <div class="space-y-3" id="positions-root"></div>
    </section>

    <section class="mb-10">
        <h2 class="text-xs font-bold text-gray-400 tracking-wider uppercase mb-2">Today's Closed Battle Log</h2>
        <div class="card rounded-xl overflow-hidden text-xs">
            <table class="w-full text-left font-mono">
                <thead class="bg-gray-800 text-[10px] text-gray-400 border-b border-gray-700">
                    <tr>
                        <th class="p-2.5">Symbol</th>
                        <th class="p-2.5">P&L</th>
                        <th class="p-2.5 text-right">Status</th>
                    </tr>
                </thead>
                <tbody id="history-table-root"></tbody>
            </table>
        </div>
    </section>

    <script>
        const positionsData = __POSITIONS_JSON__;
        const historicalTrades = __TRADES_JSON__;
        
        const posRoot = document.getElementById('positions-root');
        if (positionsData.length === 0) {
            posRoot.innerHTML = `<div class="card p-6 rounded-xl text-center text-xs text-gray-500 font-mono">No active tactical positions held. Engine running indicators sweep...</div>`;
        } else {
            positionsData.forEach(pos => {
                const isProfit = pos.pnl >= 0;
                const pnlColor = isProfit ? 'text-emerald-400' : 'text-rose-500';
                const totalRange = pos.target_price - pos.sl_price;
                const progressPct = totalRange > 0 ? Math.min(100, Math.max(0, ((pos.live_price - pos.sl_price) / totalRange) * 100)) : 50;

                posRoot.innerHTML += `
                    <div class="card p-3.5 rounded-xl flex flex-col justify-between">
                        <div class="flex justify-between items-start">
                            <div>
                                <div class="flex items-center gap-1.5">
                                    <h3 class="text-base font-bold tracking-tight text-white">${pos.symbol}</h3>
                                    <span class="bg-gray-800 text-gray-400 font-mono text-[9px] px-1.5 py-0.5 rounded font-bold">${pos.strategy}</span>
                                </div>
                                <p class="text-[11px] text-gray-400 mt-0.5 font-mono">Qty: ${pos.quantity} | Entry: ₹${pos.entry_price} | Live: ₹${pos.live_price}</p>
                            </div>
                            <div class="text-right font-mono">
                                <span class="text-sm font-bold ${pnlColor}">${pos.pnl >= 0 ? '+' : ''}₹${pos.pnl}</span>
                                <div class="text-[10px] ${pnlColor}">(${pos.pnl_pct >= 0 ? '+' : ''}${pos.pnl_pct}%)</div>
                            </div>
                        </div>
                    </div>`;
            });
        }

        const tableRoot = document.getElementById('history-table-root');
        if (historicalTrades.length === 0) {
            tableRoot.innerHTML = `<tr><td colspan="3" class="p-4 text-center text-gray-500 italic">No closed trades recorded during this session.</td></tr>`;
        } else {
            historicalTrades.forEach(t => {
                const pnlColor = t.net_pnl >= 0 ? 'text-emerald-400' : 'text-rose-500';
                tableRoot.innerHTML += `
                    <tr class="border-b border-gray-800 hover:bg-gray-800/40 transition">
                        <td class="p-2.5 font-bold text-white">${t.symbol}<div class="text-[9px] text-gray-500 font-normal">${t.strategy}</div></td>
                        <td class="p-2.5 ${pnlColor} font-bold">${t.net_pnl >= 0 ? '+' : ''}₹${t.net_pnl.toFixed(2)}</td>
                        <td class="p-2.5 text-right font-semibold">${t.exit_status}</td>
                    </tr>`;
            });
        }
    </script>
</body>
</html>
"""
            # Safe token substitution block
            html_payload = html_template\
                .replace("__SYNC_TIME__", datetime.now().strftime('%I:%M:%S %p'))\
                .replace("__MARGIN__", f"{self.available_capital:.2f}")\
                .replace("__PORTFOLIO_VALUE__", f"{total_portfolio_value:.2f}")\
                .replace("__PNL_COLOR__", pnl_color_class)\
                .replace("__PREFIX__", pnl_prefix)\
                .replace("__FLOATING_PNL__", f"{total_floating_pnl:.2f}")\
                .replace("__FLOATING_PCT__", f"{floating_pct:.2f}")\
                .replace("__POSITIONS_COUNT__", str(len(formatted_positions)))\
                .replace("__POSITIONS_JSON__", json.dumps(formatted_positions))\
                .replace("__TRADES_JSON__", json.dumps(self.trades))

            with open("dashboard.html", "w", encoding="utf-8") as out:
                out.write(html_payload)
            
            # Send live update to GitHub Pages deployment pipeline
            self.sync_to_github_pages(html_payload)
                
        except Exception as e:
            logger.error(f"[DASHBOARD LIFE-CYCLE ERROR] Compiler layout drop: {e}")

    def run(self):
        """Main orchestrator running loops on set intervals."""
        if not self.login():
            return
            
        self.load_symbol_tokens()
        
        # Populate initial tracking stock tickers
        initial_pool = self.fetch_all_stocks()
        self.stock_list = [stock['symbol'] for stock in initial_pool][:MAX_STOCKS_TO_SCAN]
        logger.info(f"Targets locked. Scanning {len(self.stock_list)} tickers.")
        
        self.start_websocket()
        
        while self.running:
            try:
                # Sync fresh historical technical charts
                self.update_bulk_market_data()
                
                # Check active open trail boundaries
                self.check_exits()
                
                # Scan for new high probability setups
                self.scan_and_trade()
                
                # Regenerate and deploy web dashboard source
                self.render_and_deploy_dashboard()
                
                time.sleep(SCAN_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Deactivating bot session manually...")
                self.running = False
            except Exception as e:
                logger.error(f"[BOT MAIN LOOP EXCEPTION]: {e}")
                time.sleep(10)


if __name__ == '__main__':
    bot = AngelTradingBot()
    bot.run()