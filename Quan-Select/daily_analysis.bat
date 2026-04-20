@echo off
chcp 936 >nul 2>&1
cd /d "%~dp0"
set "MODE_ARG="
set "TECH_ARG="
set "STRAT_ARG="
set "SMODE_ARG="
set "QDIFF_ARG="
echo.
echo  ========================================================================
echo    Jian Long Zai Tian - Daily Stock Analysis
echo  ========================================================================
echo.
echo  [0] Run Mode
echo      1. Full Flow (collect + score + report)   [default]
echo      2. Report Only (reuse existing data)
echo.
set /p mode= Please select [1/2/Enter]:
if "%mode%"=="2" (
    set "MODE_ARG=--only-report"
    echo.
    echo  [Report Only] Using existing data...
    echo.
    goto :RUN
)
echo.
echo  [1] Tech Scoring Mode
echo      1. Fundamental + Tech Filter                 [default]
echo      2. Pure Fundamental (skip K-line, fastest)
echo.
set /p tech= Please select [1/2/Enter]:
echo.
echo  ========================================================================
echo   Fund Strategy (surprise is an add-on, not standalone)
echo  ========================================================================
echo.
echo   --- Mid/Long-term Protection ---
echo      1. classic              Stable Value               [default]
echo      2. classic + surprise   Stable + Catalyst
echo      3. growth               Bull Growth
echo      4. growth + surprise    Growth + Catalyst
echo.
echo   --- Short-term Momentum ---
echo      5. single_line         Short-line Burst           (built-in qdiff)
echo.
echo   --- Screening Tool ---
echo      6. surprise only       Surprise Scanner           (no TOP limit)
echo.
set /p strat= Please select [1-6/Enter]:
:::: Check if qdiff/surprise mode selection is needed
if "%strat%"=="5" goto :QDIFF_PROMPT
if "%strat%"=="6" (
    set "STRAT_ARG=--fund-strategy surprise"
    goto :SMODE_PROMPT
)
if "%strat%"=="2" set "STRAT_ARG=--fund-strategy classic,surprise"
if "%strat%"=="4" set "STRAT_ARG=--fund-strategy growth,surprise"
goto :SMODE_PROMPT

:QDIFF_PROMPT
set "STRAT_ARG=--fund-strategy single_line"
echo.
echo  [A] Qdiff Data Source (single_line built-in expectation gap)
echo      1. quarter   Quarterly consensus vs single-quarter profit   [default]
echo                 ^|-- forward: find catalyst gap (expect ^> actual)
echo                 +-- actual:  find earnings beat (actual ^> expect)
echo      2. ttm       Annual EPS consensus vs TTM profit growth
echo                 ^|-- forward: find annual acceleration signal
echo                 +-- actual:  find TTM beat consensus
echo.
set /p qdiff= Please select [1/2/Enter]:
if "%qdiff%"=="2" set "QDIFF_ARG=--qdiff-mode ttm"

:SMODE_PROMPT
echo.
echo  [B] Expectation Direction
echo      1. auto      Auto-detect per stock (forward or actual)  [default]
echo      2. forward   Market expects future better (find catalyst)
echo      3. actual    Actual beats expectation (verify beat)
echo.
set /p smode= Please select [1/2/3/Enter]:
:::: Set tech arg
if "%tech%"=="2" (
    set "TECH_ARG=--skip-tech"
) else (
    set "TECH_ARG="
)
:::: Set strategy arg for options without explicit set above
if "%strat%"=="" set "STRAT_ARG=--fund-strategy classic"
if "%strat%"=="1" set "STRAT_ARG=--fund-strategy classic"
if "%strat%"=="3" set "STRAT_ARG=--fund-strategy growth"
:::: Set surprise mode arg
if "%smode%"=="2" (
    set "SMODE_ARG=--surprise-mode forward"
) else if "%smode%"=="3" (
    set "SMODE_ARG=--surprise-mode actual"
) else (
    set "SMODE_ARG="
)
:RUN
echo.
echo  === Running ===
echo  python main.py %MODE_ARG% %TECH_ARG% %STRAT_ARG% %SMODE_ARG% %QDIFF_ARG%
echo.
python main.py %MODE_ARG% %TECH_ARG% %STRAT_ARG% %SMODE_ARG% %QDIFF_ARG%
pause
