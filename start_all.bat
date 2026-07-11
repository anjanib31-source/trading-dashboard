@echo off
echo ========================================
echo 🚀 Starting ALPHA Trading System
echo ========================================
echo.

cd C:\Users\ArandaTech\trading-bot

echo 📊 Starting Flask API server...
start /B python src\app.py
timeout /t 3 /nobreak > nul

echo 🌐 Starting ngrok tunnel...
start /B ngrok http 5000
timeout /t 3 /nobreak > nul

echo 🤖 Starting Trading Bot...
python src\angel_bot_v2.py

pause