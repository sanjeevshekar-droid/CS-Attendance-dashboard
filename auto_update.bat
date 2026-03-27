@echo off
cd /d "%~dp0"

:loop
echo Updating dashboard...

python attendance_dashboard.py

git add .
git diff --quiet || git commit -m "Auto update"
git push origin main

timeout /t 60

goto loop