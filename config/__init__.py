# config/__init__.py
"""Configuration module for ALPHA Bot"""

import json
import os
from typing import Dict, List, Any

def load_stock_config() -> Dict:
    """Load stock configuration from stocks.json"""
    config_file = os.path.join(os.path.dirname(__file__), 'stocks.json')
    
    default_config = {
        "version": "1.0",
        "sources": {
            "nsepython": {"enabled": True, "indices": ["NIFTY 50"], "max_stocks": 40},
            "yahoo_finance": {"enabled": True, "max_stocks": 20},
            "hardcoded": {"enabled": True, "stocks": []}
        },
        "filters": {"min_price": 20, "max_price": 4000, "min_volume": 100000, "max_stocks": 40},
        "scanning": {"auto_pick_stocks": True, "refresh_interval_hours": 24, "enable_dynamic_discovery": True}
    }
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
            return config
        else:
            return default_config
    except Exception as e:
        print(f"[CONFIG] Failed to load stock config: {e}")
        return default_config

def get_hardcoded_stocks() -> List[str]:
    """Get hardcoded stocks from config"""
    config = load_stock_config()
    return config.get('sources', {}).get('hardcoded', {}).get('stocks', [])

def get_filters() -> Dict:
    """Get filter settings from config"""
    config = load_stock_config()
    return config.get('filters', {})

def get_scanning_settings() -> Dict:
    """Get scanning settings from config"""
    config = load_stock_config()
    return config.get('scanning', {})