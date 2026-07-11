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

# === CONFIGURATION ===
API_KEY = "llSx3AHl"
CLIENT_CODE = "AACI948453"
MPIN = "1221"
TOTP_SECRET = "JZXSY4HGILRACPI7S4K734BDDM"

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
                            'net_pnl': net_pnl, 'pnl_pct': pnl_pct, 'strategy': position['strategy']
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
                
                # Inline safe execution layer to guarantee state persistence
                order_id = f"PAPER_{datetime.now().strftime('%H%M%S')}_{symbol}"
                self.available_capital -= margin
                
                logger.info(f"📝 [PAPER ORDER] BUY | {symbol} | Qty: {qty} @ Rs.{current_price:.2f} | Margin: Rs.{margin:.2f}")
                
                self.positions.append({
                    'symbol': symbol, 'entry_price': current_price, 'peak_price': current_price,  
                    'quantity': qty, 'entry_time': datetime.now(), 'score': score,
                    'profit_target': PROFIT_TARGETS.get(score, DEFAULT_PROFIT_TARGET), 'strategy': strategy
                })

    def wait_for_market_open(self):
        while True:
            now = datetime.now()
            current_time = now.time()
            wake_up_time = datetime.strptime(WAKE_UP_TIME, "%H:%M").time()
            trading_start_time = datetime.strptime(TRADING_START, "%H:%M").time()
            market_close_time = datetime.strptime(MARKET_CLOSE, "%H:%M").time()
            
            if current_time > market_close_time:
                logger.info("Market Closed for today.")
                return False
            if current_time < wake_up_time:
                seconds = (datetime.combine(now.date(), wake_up_time) - now).total_seconds()
                logger.info(f"[WAIT] Sleeping until {WAKE_UP_TIME}...")
                time.sleep(min(60, seconds))
                continue
            return True

    def generate_report(self):
        logger.info("\n" + "="*60 + "\n[REPORT] DAILY NET TRADING REPORT\n" + "="*60)
        logger.info(f"[INITIAL CAPITAL]  Rs.{self.initial_capital:.2f}")
        logger.info(f"[FINAL CAPITAL]    Rs.{self.capital:.2f}")
        
        total_gross = sum(t['gross_pnl'] for t in self.trades)
        total_charges = sum(t['charges'] for t in self.trades)
        total_net = sum(t['net_pnl'] for t in self.trades)
        
        logger.info(f"[TOTAL GROSS P&L]  Rs.{total_gross:.2f}")
        logger.info(f"[TOTAL TAXES/FEES] Rs.{total_charges:.2f}")
        logger.info(f"[NET TAKE HOME]    Rs.{total_net:.2f}")
        logger.info(f"[TOTAL TRADES]      {len(self.trades)}")
        
        if self.trades:
            wins = sum(1 for t in self.trades if t['net_pnl'] > 0)
            logger.info(f"[TRUE WIN RATE]                    {(wins / len(self.trades)) * 100:.1f}%")
        logger.info("="*60)

    def run(self):
        logger.info("Starting Bot Core Engine Lifecycle...")
        if not self.login():
            return
            
        self.load_symbol_tokens()
        if not self.symbol_tokens:
            logger.error("[CRITICAL] Token dictionary empty. Execution blocked.")
            return

        if not self.wait_for_market_open():
            return
            
        raw_stocks = self.fetch_all_stocks()
        self.stock_list = [s['symbol'] for s in raw_stocks if s['symbol'] in self.symbol_tokens][:MAX_STOCKS_TO_SCAN]
        
        if not self.stock_list:
            logger.error("[CRITICAL] Watchlist calculations returned 0 active symbols.")
            return
            
        logger.info(f"[READY] Watchlist calculated with {len(self.stock_list)} valid symbols.")
        self.start_websocket()
        
        while self.running:
            try:
                current_time = datetime.now().time()
                if current_time >= datetime.strptime(MARKET_CLOSE, "%H:%M").time():
                    logger.info("[EOD] Market close. Squaring off active positions...")
                    for position in self.positions[:]:
                        current_p = self.get_ltp(position['symbol']) or position['entry_price']
                        self.place_order(position['symbol'], "SELL", position['quantity'], current_p)
                    self.generate_report()
                    break
                
                if current_time < datetime.strptime(TRADING_START, "%H:%M").time():
                    time.sleep(10)
                    continue

                self.update_bulk_market_data()
                self.scan_and_trade()
                self.check_exits()
                time.sleep(SCAN_INTERVAL)
            except KeyboardInterrupt:
                self.running = False
                self.generate_report()


if __name__ == "__main__":
    bot = AngelTradingBot()
    bot.run()