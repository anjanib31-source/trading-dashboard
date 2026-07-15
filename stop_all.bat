@echo off
echo ========================================
echo 🛑 Stopping ALPHA Trading System
echo ========================================
echo.

:: Stop only our Python processes
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq python.exe" /NH 2^>NUL ^| findstr /i "app.py angel_bot_v2.py"') do (
    echo Stopping process: %%a
    taskkill /PID %%a /F 2>NUL
)

:: Stop ngrok
for /f "tokens=2" %%a in ('tasklist /FI "IMAGENAME eq ngrok.exe" /NH 2^>NUL') do (
    echo Stopping ngrok: %%a
    taskkill /PID %%a /F 2>NUL
)

echo.
echo ✅ All processes stopped!
pause