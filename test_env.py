"""
Test Environment Variables
Run: python test_env.py
"""

import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

print("=" * 60)
print("🔍 ALPHA Trading Bot - Environment Check")
print("=" * 60)

# Required variables
required_vars = [
    'API_KEY', 
    'CLIENT_CODE', 
    'MPIN', 
    'TOTP_SECRET'
]

all_present = True
print("\n📋 Required Variables:")
for var in required_vars:
    value = os.getenv(var)
    status = "✅" if value else "❌"
    masked = value[:4] + "****" if value else "MISSING"
    print(f"  {status} {var}: {masked}")
    if not value:
        all_present = False

# Optional variables
optional_vars = [
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_ID',
    'GITHUB_PAT',
    'GITHUB_USERNAME',
    'GITHUB_REPO',
    'TRADING_MODE',
    'LOG_LEVEL'
]

print("\n📋 Optional Variables:")
for var in optional_vars:
    value = os.getenv(var)
    status = "✅" if value else "❌"
    masked = value[:4] + "****" if value else "NOT SET"
    print(f"  {status} {var}: {masked}")

# Trading parameters
trading_vars = [
    'CAPITAL',
    'MAX_POSITIONS',
    'LEVERAGE',
    'STOP_LOSS',
    'SCAN_INTERVAL'
]

print("\n📊 Trading Parameters:")
for var in trading_vars:
    value = os.getenv(var)
    status = "✅" if value else "❌"
    print(f"  {status} {var}: {value or 'DEFAULT'}")

# Summary
print("\n" + "=" * 60)
if all_present:
    print("✅ ALL REQUIRED VARIABLES PRESENT")
    print("   Bot is ready to start!")
else:
    print("❌ SOME REQUIRED VARIABLES MISSING")
    print("   Please check your .env file")
print("=" * 60)