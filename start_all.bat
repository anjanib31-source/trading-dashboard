@echo off
setlocal enabledelayedexpansion

echo ========================================
echo 🚀 ALPHA Trading System - Starting
echo ========================================
echo.

:: Set paths
set "PROJECT_DIR=C:\Users\ArandaTech\trading-bot"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "NGROK_URL_FILE=%PROJECT_DIR%\ngrok_url.txt"
set "PID_FILE=%PROJECT_DIR%\.bot_pids"

cd /d "%PROJECT_DIR%"

:: Create logs directory if not exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: ========================================
:: CLEANUP - Kill existing processes
:: ========================================
echo 📋 Checking for existing processes...

:: Kill only our specific processes (not all Python!)
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq python.exe" /NH 2^>NUL ^| findstr /i "app.py angel_bot_v2.py"') do (
    echo Killing existing bot process: %%a
    taskkill /PID %%a /F 2>NUL
)

for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq ngrok.exe" /NH 2^>NUL') do (
    echo Killing existing ngrok process: %%a
    taskkill /PID %%a /F 2>NUL
)

timeout /t 2 /nobreak > NUL

:: Clean up old PID file
if exist "%PID_FILE%" del "%PID_FILE%"

:: ========================================
:: START FLASK API SERVER
:: ========================================
echo.
echo 📊 Starting Flask API server...
start "ALPHA-Flask" /B python src\app.py > "%LOG_DIR%\flask.log" 2>&1

:: Wait for Flask to start
echo Waiting for Flask server...
set "FLASK_READY="
for /l %%i in (1,1,10) do (
    timeout /t 1 /nobreak > NUL
    powershell -Command "try { $r = Invoke-WebRequest -Uri http://localhost:5000/api/health -TimeoutSec 1; if ($r.StatusCode -eq 200) { exit 0 } } catch { exit 1 }" 2>NUL
    if !ERRORLEVEL! EQU 0 (
        set "FLASK_READY=1"
        echo ✅ Flask server ready!
        goto :flask_ready
    )
    echo .%%i
)

:flask_ready
if not defined FLASK_READY (
    echo ⚠️ Flask server may not be ready. Continuing anyway...
)

:: ========================================
:: START NGROK TUNNEL
:: ========================================
echo.
echo 🌐 Starting ngrok tunnel...
start "ALPHA-Ngrok" /B ngrok http 5000 --log=stdout --log-level=info > "%LOG_DIR%\ngrok.log" 2>&1

:: Wait for ngrok and get URL
echo Waiting for ngrok tunnel...
set "NGROK_READY="
for /l %%i in (1,1,15) do (
    timeout /t 1 /nobreak > NUL
    powershell -Command "try { $r = Invoke-RestMethod -Uri http://localhost:4040/api/tunnels -TimeoutSec 2; if ($r.tunnels) { exit 0 } } catch { exit 1 }" 2>NUL
    if !ERRORLEVEL! EQU 0 (
        set "NGROK_READY=1"
        echo ✅ ngrok tunnel ready!
        goto :ngrok_ready
    )
    echo .%%i
)

:ngrok_ready
:: Get ngrok URL
if defined NGROK_READY (
    powershell -Command "try { $r = Invoke-RestMethod -Uri http://localhost:4040/api/tunnels; $url = $r.tunnels | Where-Object { $_.proto -eq 'https' } | Select-Object -First 1 -ExpandProperty public_url; if ($url) { Write-Output $url } } catch { Write-Output '' }" > "%NGROK_URL_FILE%" 2>NUL
    
    if exist "%NGROK_URL_FILE%" (
        set /p NGROK_URL=<"%NGROK_URL_FILE%"
        if defined NGROK_URL (
            echo.
            echo ========================================
            echo 🌐 NGROK PUBLIC URL:
            echo %NGROK_URL%
            echo ========================================
            echo.
            
            :: Save to .env for dashboard
            echo NGROK_URL=%NGROK_URL% >> .env
        )
    )
)

:: ========================================
:: START TRADING BOT
:: ========================================
echo.
echo 🤖 Starting Trading Bot...
echo ========================================
echo.
echo 📅 Start Time: %DATE% %TIME%
echo 📂 Log Directory: %LOG_DIR%
echo.
echo 📌 Commands:
echo    - Press Ctrl+C to stop bot
echo    - Dashboard: http://localhost:5000
if defined NGROK_URL echo    - External: %NGROK_URL%
echo.
echo ========================================
echo.

:: Start the bot
python src\angel_bot_v2.py

:: ========================================
:: CLEANUP - Bot Stopped
:: ========================================
echo.
echo ========================================
echo 🛑 Bot stopped. Cleaning up...
echo ========================================

:: Kill only our specific processes
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq python.exe" /NH 2^>NUL ^| findstr /i "app.py"') do (
    taskkill /PID %%a /F 2>NUL
)

for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq ngrok.exe" /NH 2^>NUL') do (
    taskkill /PID %%a /F 2>NUL
)

echo ✅ Cleanup complete!
echo.
pause