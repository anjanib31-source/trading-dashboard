"""
Utility classes for ALPHA Trading Bot
"""

import logging
import threading
from datetime import datetime
from collections import deque
from typing import Optional, Dict, List, Tuple, Any

log = logging.getLogger(__name__)

class ScanManager:
    """Manages scan operations to prevent overlap"""
    
    def __init__(self):
        self.is_scanning = False
        self.last_scan_time = None
        self.scan_thread = None
        self._lock = threading.Lock()
    
    def start_scan(self, bot):
        """Start a scan if not already running"""
        with self._lock:
            if self.is_scanning:
                log.debug("[SCAN] Already running, skipping")
                return
            
            self.is_scanning = True
            self.last_scan_time = datetime.now()
        
        try:
            bot._perform_scan()
        except Exception as e:
            log.error(f"[SCAN] Error: {e}")
        finally:
            with self._lock:
                self.is_scanning = False
    
    def get_status(self) -> str:
        """Get scan manager status"""
        if self.is_scanning:
            return "🔄 Scanning..."
        if self.last_scan_time:
            elapsed = (datetime.now() - self.last_scan_time).total_seconds()
            return f"✅ Last scan: {elapsed:.0f}s ago"
        return "⏸️ Idle"


class MetricsCollector:
    """Collects runtime metrics"""
    
    def __init__(self):
        self.metrics = {
            'api_calls': 0,
            'api_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'scan_times': [],
            'trades_taken': 0,
            'trades_success': 0,
            'ws_messages': 0
        }
        self._lock = threading.Lock()
    
    def record_api_call(self, success: bool = True):
        with self._lock:
            self.metrics['api_calls'] += 1
            if not success:
                self.metrics['api_errors'] += 1
    
    def record_cache_hit(self, hit: bool):
        with self._lock:
            if hit:
                self.metrics['cache_hits'] += 1
            else:
                self.metrics['cache_misses'] += 1
    
    def record_scan_duration(self, duration: float):
        with self._lock:
            self.metrics['scan_times'].append(duration)
            if len(self.metrics['scan_times']) > 100:
                self.metrics['scan_times'] = self.metrics['scan_times'][-50:]
    
    def record_trade(self, success: bool = True):
        with self._lock:
            self.metrics['trades_taken'] += 1
            if success:
                self.metrics['trades_success'] += 1
    
    def record_ws_message(self):
        with self._lock:
            self.metrics['ws_messages'] += 1
    
    def get_summary(self) -> dict:
        with self._lock:
            avg_scan = sum(self.metrics['scan_times']) / len(self.metrics['scan_times']) if self.metrics['scan_times'] else 0
            cache_hits = self.metrics['cache_hits']
            cache_misses = self.metrics['cache_misses']
            hit_rate = cache_hits / (cache_hits + cache_misses) if (cache_hits + cache_misses) > 0 else 0
            return {
                'api_calls': self.metrics['api_calls'],
                'api_errors': self.metrics['api_errors'],
                'cache_hit_rate': hit_rate,
                'avg_scan_time': avg_scan,
                'trades_taken': self.metrics['trades_taken'],
                'trades_success': self.metrics['trades_success']
            }


class BotState:
    """Tracks bot state"""
    
    def __init__(self):
        self.state = "INITIALIZING"
        self.last_update = datetime.now()
        self.error_count = 0
        self._lock = threading.Lock()
    
    def set_state(self, state: str):
        with self._lock:
            self.state = state
            self.last_update = datetime.now()
    
    def get_state(self) -> str:
        with self._lock:
            return self.state


class ErrorRecovery:
    """Handles error recovery"""
    
    def __init__(self, bot):
        self.bot = bot
        self.recovery_attempts = 0
        self.last_recovery = None
        self._lock = threading.Lock()
    
    def attempt_recovery(self, error: Exception) -> bool:
        with self._lock:
            self.recovery_attempts += 1
            self.last_recovery = datetime.now()
        
        log.warning(f"[RECOVERY] Attempt #{self.recovery_attempts} for: {error}")
        
        # Attempt reconnection for network errors
        if "connection" in str(error).lower() or "timeout" in str(error).lower():
            log.info("[RECOVERY] Attempting to reconnect...")
            return self.bot.login()
        
        # Attempt WebSocket reconnect
        if "websocket" in str(error).lower() or "ws" in str(error).lower():
            log.info("[RECOVERY] Attempting WebSocket reconnect...")
            try:
                self.bot.start_websocket()
                return True
            except Exception as e:
                log.error(f"[RECOVERY] WebSocket reconnect failed: {e}")
                return False
        
        return False