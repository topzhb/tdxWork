@echo off
title Backtest Service - Port 8765

cd /d "%~dp0"

echo.
echo  ============================================
echo     Quan-Select  Backtest Service 
echo     http://localhost:8765
echo  ============================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found. Please add Python to PATH.
    pause
    exit /b 1
)

echo  [START] python backtest.py --serve
echo  [INFO ] Service started.
echo  [INFO ] Backtest UI: http://localhost:8765/picks/backtest_ui.html
echo  [INFO ] Press Ctrl+C to stop.
echo.
python backtest.py --serve

echo.
echo  [INFO ] Service stopped.
pause
