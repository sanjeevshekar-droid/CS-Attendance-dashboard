@echo off
title Cityflo CS Monitoring Dashboard
echo =====================================================
echo  Cityflo CS Monitoring Dashboard
echo =====================================================
echo  TAB 1 — Associate Performance  (7-day view)
echo  TAB 2 — Ticket Quality Check
echo  TAB 3 — Released Tickets       (invalid flags)
echo  TAB 4 — Backlog ^& Load
echo  TAB 5 — Auto-Closure Suggestions
echo  TAB 6 — Alerts ^& Flags
echo =====================================================
cd /d "%~dp0"

echo.
echo  Opening browser and starting auto-refresh every 3 min...
echo  Press Ctrl+C to stop.
echo =====================================================
python monitoring_dashboard.py --watch 3
pause
