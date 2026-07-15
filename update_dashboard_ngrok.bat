@echo off
echo ========================================
echo 🔄 Updating Dashboard with Ngrok URL
echo ========================================

:: Get ngrok URL
powershell -Command "try { $r = Invoke-RestMethod -Uri http://localhost:4040/api/tunnels; $url = $r.tunnels | Where-Object { $_.proto -eq 'https' } | Select-Object -First 1 -ExpandProperty public_url; if ($url) { Write-Output $url } } catch { Write-Output '' }" > ngrok_url.txt 2>NUL

set /p NGROK_URL=<ngrok_url.txt

if defined NGROK_URL (
    echo ✅ Ngrok URL: %NGROK_URL%
    
    :: Update dashboard.html (if needed)
    :: Uncomment if you want to update dashboard with ngrok URL
    :: powershell -Command "(Get-Content web\dashboard.html) -replace 'const API_BASE = .*;', 'const API_BASE = ''%NGROK_URL%/api'';' | Set-Content web\dashboard.html"
    
    echo 📋 Dashboard available at: %NGROK_URL%
) else (
    echo ❌ Ngrok not running or URL not available
)

pause