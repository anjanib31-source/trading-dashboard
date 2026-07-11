from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import sqlite3
import json
import os
from datetime import datetime, timedelta

app = Flask(__name__, static_folder='../web', static_url_path='')
CORS(app, origins='*')

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'trades.db')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def serve_dashboard():
    return send_from_directory('../web', 'dashboard.html')

@app.route('/api/trades')
def get_trades():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
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
            LIMIT 500
        ''')
        trades = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'status': 'success', 'data': trades, 'count': len(trades)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/stats')
def get_stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        stats = {}
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
        cursor.execute('''
            SELECT 
                COUNT(*) as today_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as today_pnl
            FROM trades WHERE DATE(entry_time) = ?
        ''', (today,))
        stats['today'] = dict(cursor.fetchone())
        cursor.execute('''
            SELECT 
                COUNT(*) as week_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as week_pnl
            FROM trades WHERE DATE(entry_time) >= ?
        ''', (week_ago,))
        stats['week'] = dict(cursor.fetchone())
        cursor.execute('''
            SELECT 
                COUNT(*) as month_trades,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as month_pnl
            FROM trades WHERE DATE(entry_time) >= ?
        ''', (month_ago,))
        stats['month'] = dict(cursor.fetchone())
        conn.close()
        return jsonify({'status': 'success', 'data': stats})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/daily_pnl')
def get_daily_pnl():
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
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/weekly_pnl')
def get_weekly_pnl():
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
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/monthly_pnl')
def get_monthly_pnl():
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
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/performance')
def get_performance():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 
                COUNT(CASE WHEN exit_time IS NOT NULL AND net_pnl > 0 THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END), 0) as win_rate
            FROM trades
        ''')
        win_rate = cursor.fetchone()[0] or 0
        cursor.execute('''
            SELECT 
                symbol,
                SUM(CASE WHEN exit_time IS NOT NULL THEN net_pnl ELSE 0 END) as total_pnl,
                COUNT(CASE WHEN exit_time IS NOT NULL THEN 1 END) as trades_count
            FROM trades WHERE exit_time IS NOT NULL
            GROUP BY symbol ORDER BY total_pnl DESC LIMIT 5
        ''')
        best_symbols = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({
            'status': 'success',
            'data': {
                'win_rate': round(win_rate, 2),
                'best_symbols': best_symbols
            }
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/market_status')
def get_market_status():
    try:
        now = datetime.now()
        day = now.weekday()
        is_weekend = day >= 5
        current_time = now.strftime('%H:%M')
        is_trading_hours = '09:15' <= current_time <= '15:30'
        is_open = not is_weekend and is_trading_hours
        if is_open:
            status = '🟢 Market Open'
            is_open = True
        elif is_weekend:
            status = '📅 Market Closed - Weekend'
            is_open = False
        else:
            status = '🔴 Market Closed - After Hours'
            is_open = False
        return jsonify({'status': 'success', 'data': status, 'is_open': is_open})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)