@echo off
setlocal enabledelayedexpansion
:: Resync by Date - KBS Sync
:: Usage: resync.bat [YYYY-MM-DD]
:: Example: resync.bat 2025-01-15
:: If no date provided, will prompt for date

if "%~1"=="" (
    echo.
    echo ========================================
    echo KBS Sync - Resync by Date
    echo ========================================
    echo.
    set /p RESYNC_DATE="Enter date to resync (YYYY-MM-DD): "
    if "!RESYNC_DATE!"=="" (
        echo Error: Date is required
        pause
        exit /b 1
    )
) else (
    set RESYNC_DATE=%~1
)

echo.
echo Starting resync for date: %RESYNC_DATE%
echo.

python KBS_Sync_GUI.py --resync-date %RESYNC_DATE%

if %errorLevel% neq 0 (
    echo.
    echo Error: Resync failed
    pause
)

