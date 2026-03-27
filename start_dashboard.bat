@echo off
title Cityflo CS Dashboard
echo ========================================
echo  Cityflo CS Dashboard
echo ========================================
echo  TAB 1 — Ticket Queue (live priority view)
echo  TAB 2 — Agent Audit  (last 7 days)
echo ========================================
cd /d "%~dp0"

echo.
echo  Opening browser and starting auto-refresh (every 5 min)...
echo  Press Ctrl+C to stop.
echo ========================================
python ticket_dashboard.py --watch 5
pause
