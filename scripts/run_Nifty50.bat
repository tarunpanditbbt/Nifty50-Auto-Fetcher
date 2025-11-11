@echo off
REM ============================================================
REM  NIFTY 50 AUTO RUNNER - CLIENT SAFE VERSION
REM ============================================================

cd /d "C:\Nifty_50" || (echo Failed to access project folder & exit /b 1)

if not exist "nifty50_data" mkdir "nifty50_data"
if not exist "logs" mkdir "logs"

echo Running Nifty50_base.py...
"C:\Nifty_50\venv\Scripts\python.exe" "C:\Nifty_50\Nifty50_base.py" >> "C:\Nifty_50\logs\runner.log" 2>&1

if %ERRORLEVEL%==0 (
    echo SUCCESS: JSON created in nifty50_data
) else (
    echo ERROR: Check logs\runner.log for details
)
exit /b %ERRORLEVEL%