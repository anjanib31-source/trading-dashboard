@echo off
setlocal enabledelayedexpansion

echo ========================================
echo 🚀 Starting ALPHA Trading System
echo ========================================
echo.

cd C:\Users\ArandaTech\trading-bot

:: Check if already running
tasklist /FI "IMAGENAME eq python.exe" 2>NUL | find /I "python.exe" >NUL
if "%ERRORLEVEL%"=="0" (
    echo ⚠️ Python is already running. Killing existing processes...
    taskkill /F /IM python.exe 2>NUL
    taskkill /F /IM ngrok.exe 2>NUL
    timeout /t 2 /nobreak > NUL
)

echo 📊 Starting Flask API server...
start "Flask Server" /B python src\app.py
timeout /t 3 /nobreak > nul

echo 🌐 Starting ngrok tunnel...
start "ngrok Tunnel" /B ngrok http 5000
timeout /t 3 /nobreak > nul

echo 🤖 Starting Trading Bot...
python src\angel_bot_v2.py

:: Cleanup when bot stops
echo.
echo 🛑 Bot stopped. Cleaning up...
taskkill /F /IM python.exe 2>NUL
taskkill /F /IM ngrok.exe 2>NUL

pause