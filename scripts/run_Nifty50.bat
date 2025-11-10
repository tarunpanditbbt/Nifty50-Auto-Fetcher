@echo off
REM ============================================================================
REM NIFTY 50 AUTO RUNNER
REM Run this file to fetch Nifty 50 data manually or via Task Scheduler
REM ============================================================================

echo ============================================================
echo NIFTY 50 DATA FETCHER - AUTO RUN
echo ============================================================
echo.

REM Move to project root (the folder that contains venv and Nifty50_base.py)
cd /d "%~dp0.."
echo Current Directory: %CD%
echo.

REM Make sure folders exist
if not exist "nifty50_data" mkdir nifty50_data
if not exist "logs" mkdir logs

REM Run Python script using your venv (this is very important!)
echo Running Nifty50_base.py...
echo.

venv\Scripts\python.exe Nifty50_base.py >> logs\runner.log 2>&1
set "EXITCODE=%ERRORLEVEL%"

echo.
echo ============================================================
echo Script Completed (exit code=%EXITCODE%)
echo ============================================================
echo.

REM Pause only if run manually (not from Task Scheduler)
if "%~1"=="" pause

exit /b %EXITCODE%
