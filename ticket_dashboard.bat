@echo off
title Cityflo Ticket Monitoring Dashboard
echo ========================================
echo  Cityflo Ticket Monitoring Dashboard
echo ========================================
echo  Generates a live HTML dashboard showing
echo  which tickets can be closed right now.
echo ========================================
cd /d "%~dp0"

echo.
echo  Generating dashboard and opening browser...
echo  (Auto-refreshes every 5 minutes)
echo.
echo  Press Ctrl+C to stop auto-refresh.
echo ========================================
python ticket_dashboard.py --watch 5
pause
