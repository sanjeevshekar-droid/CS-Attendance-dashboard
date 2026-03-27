@echo off
title Cityflo Dashboard Server
echo ========================================
echo  Cityflo CS Dashboard - Network Server
echo ========================================

:: Get local IP address
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4" ^| findstr /v "127.0.0.1"') do (
    set IP=%%a
    goto :found
)
:found
set IP=%IP: =%

echo.
echo Dashboard is being served at:
echo   http://%IP%:8765/attendance_dashboard.html
echo.
echo Share this link with your team.
echo Keep this window open while serving.
echo Press Ctrl+C to stop.
echo ========================================
cd /d "%~dp0"
python -m http.server 8765
pause
