@echo off
echo ========================================
echo 📊 ALPHA System Status
echo ========================================
echo.

:: Check Flask
echo 📊 Flask API Server:
tasklist /FI "IMAGENAME eq python.exe" /NH 2>NUL | findstr "app.py" >NUL
if %ERRORLEVEL% EQU 0 (
    echo    ✅ Running
) else (
    echo    ❌ Not running
)

:: Check Bot
echo.
echo 🤖 Trading Bot:
tasklist /FI "IMAGENAME eq python.exe" /NH 2>NUL | findstr "angel_bot_v2.py" >NUL
if %ERRORLEVEL% EQU 0 (
    echo    ✅ Running
) else (
    echo    ❌ Not running
)

:: Check Ngrok
echo.
echo 🌐 Ngrok Tunnel:
tasklist /FI "IMAGENAME eq ngrok.exe" /NH 2>NUL | findstr "ngrok.exe" >NUL
if %ERRORLEVEL% EQU 0 (
    echo    ✅ Running
    echo.
    echo    Public URL:
    powershell -Command "try { $r = Invoke-RestMethod -Uri http://localhost:4040/api/tunnels -ErrorAction Stop; $url = $r.tunnels | Where-Object { $_.proto -eq 'https' } | Select-Object -First 1 -ExpandProperty public_url; if ($url) { Write-Output $url } } catch { Write-Output '   ⚠️ Not available' }"
) else (
    echo    ❌ Not running
)

:: Check Health
echo.
echo 💚 Health Status:
powershell -Command "try { $r = Invoke-RestMethod -Uri http://localhost:5000/api/health -TimeoutSec 3 -ErrorAction Stop; Write-Output $r.data.status } catch { Write-Output '⚠️ API not responding' }"

echo.
pause