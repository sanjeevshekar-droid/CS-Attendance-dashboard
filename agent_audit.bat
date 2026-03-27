@echo off
title Cityflo Agent Audit Dashboard
echo ========================================
echo  Cityflo Agent Audit Dashboard
echo ========================================
echo  Shows last 7 days:
echo   1. Tickets assigned per associate
echo   2. Tickets released before logoff
echo   3. Tickets responded + closed
echo   4. Responded but not highlighted
echo ========================================
cd /d "%~dp0"

echo.
echo  Generating dashboard and opening browser...
echo  (Auto-refreshes every 5 minutes)
echo.
echo  Press Ctrl+C to stop.
echo ========================================
python agent_audit.py --watch 5
pause
