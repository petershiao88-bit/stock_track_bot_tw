@echo off
REM ============================================================
REM  Windows Task Scheduler helper script
REM
REM  Usage:
REM    1. Open Task Scheduler -> Create Basic Task
REM    2. Action -> Start a program
REM       Program/script : full path to this .bat file
REM       Add arguments  : (leave empty)
REM       Start in       : (leave empty; the script changes dir automatically)
REM    3. Logs are written to  logs\scheduler.log
REM ============================================================

cd /d "%~dp0"
if not exist "logs" mkdir "logs"

REM ── Adjust PY to your Python interpreter ──
REM If you use a virtual environment, point to  venv\Scripts\python.exe  instead.
set "PY=python"

echo === START %date% %time% ===>> "logs\scheduler.log"
"%PY%" "%~dp0main.py" --mode daily>> "logs\scheduler.log" 2>&1
set "RC=%errorlevel%"
echo === END exit=%RC% ===>> "logs\scheduler.log"
exit /b %RC%
