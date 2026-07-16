"""
ALPHA Bot - Flask API Server
Production-ready with logging, rate limiting, health checks, and CORS
✅ FIXED: Increased rate limit to 120 requests/min
✅ FIXED: Better error handling
✅ ADDED: /api/data endpoint for consolidated dashboard data
"""

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import sqlite3
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from functools import wraps
import time
from dotenv import load_dotenv
import sys
import threading

# Load environment variables
load_dotenv()

# === CONFIGURATION ===
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), 'trades.db'))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))  # Increased from 60 to 120
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
PORT = int(os.getenv("PORT", 5000))
HOST = os.getenv("HOST", "0.0.0.0")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# === SETUP LOGGING WITH ROTATION ===
os.makedirs("logs", exist_ok=True)

# Create root logger
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL))

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Create rotating file handler (10MB, 5 backups)
file_handler = RotatingFileHandler(
    'logs/app.log',
    maxBytes=10*1024*1024,
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(getattr(logging, LOG_LEVEL))

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(getattr(logging, LOG_LEVEL))

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

log = logging.getLogger(__name__)

# === FLASK APP ===
app = Flask(__name__, static_folder='../web', static_url_path='')

# === CORS CONFIGURATION ===
if CORS_ORIGINS == ["*"]:
    CORS(app, origins='*')
else:
    CORS(app, origins=CORS_ORIGINS)

# === RATE LIMITING ===
class RateLimiter:
    """Simple in-memory rate limiter with thread safety"""
    def __init__(self, max_calls_per_minute):
        self.max_calls = max_calls_per_minute
        self.calls = {}
        self.lock = threading.Lock()
    
    def is_allowed(self, client_id):
        with self.lock:
            now = time.time()
            window = now - 60  # Last 60 seconds
            
            if client_id not in self.calls:
                self.calls[client_id] = []
            
            # Clean old calls
            self.calls[client_id] = [t for t in self.calls[client_id] if t > window]
            
            if len(self.calls[client_id]) >= self.max_calls:
                return False
            
            self.calls[client_id].append(now)
            return True

rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

def rate_limit(f):
    """Decorator to apply rate limiting"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_id = request.remote_addr or 'unknown'
        if not rate_limiter.is_allowed(client_id):
            log.warning(f"Rate limit exceeded for {client_id}")
            return jsonify({
                'status': 'error',
                'message': 'Rate limit exceeded. Please wait a moment and try again.'
            }), 429
        return f(*args, **kwargs)
    return decorated_function

# === DATABASE FUNCTIONS ===
def get_db_connection():
    """Get database connection with timeout"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        raise

# === ROUTES ===

@app.route('/')
@rate_limit
def serve_dashboard():
    """Serve the main dashboard"""
    try:
        return send_from_directory('../web', 'dashboard.html')
    except Exception as e:
        log.error(f"Failed to serve dashboard: {e}")
        return jsonify({'status': 'error', 'message': 'Dashboard not found'}), 404

@app.route('/api/trades')
@rate_limit
def get_trades():
    """Get recent trades"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        limit = request.args.get('limit', 500, type=int)
        limit = min(limit, 1000)
        
        cursor.execute('''
            SELECT 
                id, symbol, entry_price, exit_price, quantity,
                net_pnl as pnl,
                gross_pnl,
                strategy, exit_reason, entry_time, exit_time,
                CASE WHEN exit_time IS NULL THEN 'OPEN' ELSE 'CLOSED' END as status,
                exit_type
            FROM trades 
            ORDER BY entry_time DESC
            LIMIT ?
        ''', (limit,))
        trades = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        log.info(f"Returned {len(trades)} trades")
        return jsonify({'status': 'success', 'data': trades, 'count': len(trades)})
    except Exception as e:
        log.error(f"Error in get_trades: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/positions')
@rate_limit
def get_positions():
    """Get currently open positions with live P&L"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                id,
                symbol,
                entry_price,
                quantity,
                strategy,
                entry_time,
                status
            FROM positions 
            WHERE status = 'OPEN'
            ORDER BY entry_time DESC
        ''')
        positions = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        # Try to get live prices from health_status.json for P&L calculation
        health_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'health_status.json')
        live_prices = {}
        if os.path.exists(health_file):
            try:
                with open(health_file, 'r') as f:
                    health_data = json.load(f)
                    if 'positions' in health_data:
                        for pos in health_data['positions']:
                            live_prices[pos.get('symbol')] = pos.get('live_price', pos.get('entry_price', 0))
            except:
                pass
        
        # Add P&L to positions if we have live prices
        for pos in positions:
            symbol = pos['symbol']
            if symbol in live_prices:
                entry = pos['entry_price'] or 0
                live = live_prices[symbol]
                qty = pos['quantity'] or 0
                pos['pnl'] = round((live - entry) * qty, 2)
                pos['live_price'] = live
            else:
                pos['pnl'] = 0
                pos['live_price'] = pos['entry_price'] or 0
        
        log.info(f"Returned {len(positions)} open positions")
        return jsonify({
            'status': 'success', 
            'data': positions, 
            'count': len(positions)
        })
    except Exception as e:
        log.error(f"Error in get_positions: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/stats')
@rate_limit
def get_stats():
    """Get trading statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        stats = {}
        
        # Overall stats
        cursor.execute('''
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN 1 ELSE 0 END) as closed_trades,
                SUM(CASE WHEN exit_time IS NULL THEN 1 ELSE 0 END) as open_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as total_pnl,
                SUM(CASE WHEN exit_time IS NOT NULL AND net_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN exit_time IS NOT NULL AND net_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                AVG(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE NULL END) as avg_pnl,
                MAX(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as max_profit,
                MIN(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as max_loss
            FROM trades
        ''')
        stats['overall'] = dict(cursor.fetchone())
        
        # Today's stats
        cursor.execute('''
            SELECT 
                COUNT(*) as today_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as today_pnl
            FROM trades WHERE DATE(entry_time) = ?
        ''', (today,))
        stats['today'] = dict(cursor.fetchone())
        
        # This week's stats
        cursor.execute('''
            SELECT 
                COUNT(*) as week_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as week_pnl
            FROM trades WHERE DATE(entry_time) >= ?
        ''', (week_ago,))
        stats['week'] = dict(cursor.fetchone())
        
        # This month's stats
        cursor.execute('''
            SELECT 
                COUNT(*) as month_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as month_pnl
            FROM trades WHERE DATE(entry_time) >= ?
        ''', (month_ago,))
        stats['month'] = dict(cursor.fetchone())
        
        # Get open positions count
        cursor.execute('''
            SELECT COUNT(*) as open_positions_count
            FROM positions 
            WHERE status = 'OPEN'
        ''')
        open_count = cursor.fetchone()[0]
        stats['open_positions_count'] = open_count
        
        conn.close()
        log.info(f"Stats returned: {stats['overall']['total_trades']} total trades")
        return jsonify({'status': 'success', 'data': stats})
    except Exception as e:
        log.error(f"Error in get_stats: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/daily_pnl')
@rate_limit
def get_daily_pnl():
    """Get daily P&L for the last 30 days"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                DATE(entry_time) as date,
                COUNT(*) as trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as daily_pnl
            FROM trades 
            WHERE DATE(entry_time) >= DATE('now', '-30 days')
            GROUP BY DATE(entry_time)
            ORDER BY DATE(entry_time)
        ''')
        data = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'status': 'success', 'data': data})
    except Exception as e:
        log.error(f"Error in get_daily_pnl: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/weekly_pnl')
@rate_limit
def get_weekly_pnl():
    """Get weekly P&L for the last 90 days"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                strftime('%Y-W%W', entry_time) as week,
                COUNT(*) as trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as weekly_pnl
            FROM trades 
            WHERE DATE(entry_time) >= DATE('now', '-90 days')
            GROUP BY strftime('%Y-W%W', entry_time)
            ORDER BY week
        ''')
        data = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'status': 'success', 'data': data})
    except Exception as e:
        log.error(f"Error in get_weekly_pnl: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/monthly_pnl')
@rate_limit
def get_monthly_pnl():
    """Get monthly P&L for the last 365 days"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                strftime('%Y-%m', entry_time) as month,
                COUNT(*) as trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as monthly_pnl
            FROM trades 
            WHERE DATE(entry_time) >= DATE('now', '-365 days')
            GROUP BY strftime('%Y-%m', entry_time)
            ORDER BY month
        ''')
        data = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'status': 'success', 'data': data})
    except Exception as e:
        log.error(f"Error in get_monthly_pnl: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/performance')
@rate_limit
def get_performance():
    """Get performance metrics including win rate and top performers"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Win rate
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN exit_time IS NOT NULL AND net_pnl > 0 THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END), 0) as win_rate
            FROM trades
        ''')
        result = cursor.fetchone()
        win_rate = result[0] if result and result[0] is not None else 0
        
        # Top performers
        cursor.execute('''
            SELECT 
                symbol,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as total_pnl,
                COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END) as trades_count
            FROM trades WHERE exit_time IS NOT NULL
            GROUP BY symbol ORDER BY total_pnl DESC LIMIT 5
        ''')
        best_symbols = [dict(row) for row in cursor.fetchall()]
        
        # Worst performers
        cursor.execute('''
            SELECT 
                symbol,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as total_pnl,
                COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END) as trades_count
            FROM trades WHERE exit_time IS NOT NULL
            GROUP BY symbol ORDER BY total_pnl ASC LIMIT 3
        ''')
        worst_symbols = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({
            'status': 'success',
            'data': {
                'win_rate': round(win_rate, 2),
                'best_symbols': best_symbols,
                'worst_symbols': worst_symbols
            }
        })
    except Exception as e:
        log.error(f"Error in get_performance: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/market_status')
@rate_limit
def get_market_status():
    """Get current market status"""
    try:
        now = datetime.now()
        day = now.weekday()
        is_weekend = day >= 5
        market_open_time = datetime.strptime('09:15', '%H:%M').time()
        market_close_time = datetime.strptime('15:30', '%H:%M').time()
        current_time = now.time()

        if is_weekend:
            status = '📅 Market Closed - Weekend'
            is_open = False
        elif current_time < market_open_time:
            open_datetime = datetime.combine(now.date(), market_open_time)
            if current_time < market_open_time:
                time_diff = open_datetime - now
                hours = time_diff.seconds // 3600
                minutes = (time_diff.seconds % 3600) // 60
                status = f'⏰ Market Opens at {market_open_time.strftime("%I:%M %p")} ({hours}h {minutes}m)'
            else:
                status = f'⏰ Market Opens at {market_open_time.strftime("%I:%M %p")}'
            is_open = False
        elif current_time >= market_open_time and current_time < market_close_time:
            status = '🟢 Market Open'
            is_open = True
        else:
            status = '🔴 Market Closed - After Hours'
            is_open = False

        return jsonify({
            'status': 'success', 
            'data': status, 
            'is_open': is_open,
            'timestamp': now.isoformat()
        })
    except Exception as e:
        log.error(f"Error in get_market_status: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/health')
@rate_limit
def get_health():
    """Get bot health status from health_status.json file"""
    try:
        health_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'health_status.json')
        if os.path.exists(health_file):
            with open(health_file, 'r') as f:
                health_data = json.load(f)
            
            health_data['api_server'] = True
            health_data['api_server_uptime'] = app.config.get('start_time', datetime.now().isoformat())
            health_data['api_status'] = 'online'
            
            return jsonify({
                'status': 'success',
                'data': health_data,
                'timestamp': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'status': 'success',
                'data': {
                    'status': '⚠️ Bot not running',
                    'all_ok': False,
                    'broker': False,
                    'market': False,
                    'scanner': False,
                    'sync': False,
                    'api_server': True,
                    'api_status': 'online',
                    'last_error': 'No health data available',
                    'error_count': 1,
                    'last_heartbeat': datetime.now().isoformat(),
                    'bot_running': False,
                    'start_time': datetime.now().isoformat()
                },
                'timestamp': datetime.now().isoformat()
            })
    except Exception as e:
        log.error(f"Error in get_health: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/api/health/live')
def live_health():
    """Simple liveness probe for container orchestration"""
    return jsonify({
        'status': 'alive',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/health/ready')
def ready_health():
    """Readiness probe - checks if API is ready to serve requests"""
    try:
        conn = get_db_connection()
        conn.execute('SELECT 1')
        conn.close()
        
        return jsonify({
            'status': 'ready',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        log.error(f"Ready check failed: {e}")
        return jsonify({
            'status': 'not_ready',
            'reason': str(e),
            'timestamp': datetime.now().isoformat()
        }), 503

@app.route('/api/metrics')
@rate_limit
def get_metrics():
    """Get system metrics"""
    try:
        health_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'health_status.json')
        metrics = {}
        
        if os.path.exists(health_file):
            with open(health_file, 'r') as f:
                health_data = json.load(f)
                metrics = health_data.get('metrics', {})
        
        metrics['api_requests'] = {
            'total': getattr(app, 'total_requests', 0),
            'errors': getattr(app, 'error_requests', 0)
        }
        
        return jsonify({
            'status': 'success',
            'data': metrics,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        log.error(f"Error in get_metrics: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/data')
@rate_limit
def get_all_data():
    """Get all data in one request for dashboard"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get trades
        cursor.execute('''
            SELECT 
                id, symbol, entry_price, exit_price, quantity,
                net_pnl as pnl, gross_pnl, strategy, exit_reason, 
                entry_time, exit_time, exit_type,
                CASE WHEN exit_time IS NULL THEN 'OPEN' ELSE 'CLOSED' END as status
            FROM trades 
            ORDER BY entry_time DESC 
            LIMIT 100
        ''')
        trades = [dict(row) for row in cursor.fetchall()]
        
        # Get open positions
        cursor.execute('''
            SELECT 
                id, symbol, entry_price, quantity, strategy, entry_time, status
            FROM positions 
            WHERE status = 'OPEN'
            ORDER BY entry_time DESC
        ''')
        positions = [dict(row) for row in cursor.fetchall()]
        
        # Get stats
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as total_pnl,
                SUM(CASE WHEN exit_time IS NOT NULL AND net_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN exit_time IS NOT NULL AND net_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN 1 ELSE 0 END) as closed_trades,
                SUM(CASE WHEN exit_time IS NULL THEN 1 ELSE 0 END) as open_trades
            FROM trades
        ''')
        stats_overall = dict(cursor.fetchone())
        
        cursor.execute('''
            SELECT 
                COUNT(*) as today_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as today_pnl
            FROM trades WHERE DATE(entry_time) = ?
        ''', (today,))
        stats_today = dict(cursor.fetchone())
        
        # Get win rate
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN exit_time IS NOT NULL AND net_pnl > 0 THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END), 0) as win_rate
            FROM trades
        ''')
        result = cursor.fetchone()
        win_rate = result[0] if result and result[0] is not None else 0
        
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': {
                'trades': trades,
                'positions': positions,
                'stats': {
                    'overall': stats_overall,
                    'today': stats_today,
                    'win_rate': round(win_rate, 2)
                }
            },
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        log.error(f"Error in get_all_data: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.before_request
def before_request():
    """Log incoming requests and track metrics"""
    app.total_requests = getattr(app, 'total_requests', 0) + 1
    log.debug(f"Request: {request.method} {request.path} from {request.remote_addr}")

@app.after_request
def after_request(response):
    """Log response and track errors"""
    if response.status_code >= 400:
        app.error_requests = getattr(app, 'error_requests', 0) + 1
        log.warning(f"Error response: {response.status_code} for {request.path}")
    return response

@app.errorhandler(404)
def not_found(error):
    return jsonify({'status': 'error', 'message': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    log.error(f"Internal server error: {error}")
    return jsonify({'status': 'error', 'message': 'Internal server error'}), 500

@app.errorhandler(429)
def rate_limit_error(error):
    return jsonify({
        'status': 'error',
        'message': 'Rate limit exceeded. Please wait a moment and try again.'
    }), 429

@app.route('/<path:path>')
def serve_static(path):
    """Serve static files from web folder"""
    try:
        return send_from_directory('../web', path)
    except Exception as e:
        log.error(f"Failed to serve static: {e}")
        return jsonify({'status': 'error', 'message': 'File not found'}), 404

if __name__ == '__main__':
    app.config['start_time'] = datetime.now().isoformat()
    
    log.info("=" * 60)
    log.info("🚀 ALPHA Bot API Server Starting")
    log.info("=" * 60)
    log.info(f"📡 Host: {HOST}")
    log.info(f"🔌 Port: {PORT}")
    log.info(f"🐛 Debug: {DEBUG}")
    log.info(f"📊 Rate Limit: {RATE_LIMIT_PER_MINUTE} requests/min")
    log.info(f"🌐 CORS Origins: {CORS_ORIGINS}")
    log.info(f"🗄️ Database: {DB_PATH}")
    log.info("=" * 60)
    
    app.run(debug=DEBUG, host=HOST, port=PORT)