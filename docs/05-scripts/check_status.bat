@echo off
REM UBS Local Connector - Status Checker
REM This batch file checks if syncs are running

echo ========================================
echo UBS Local Connector - Status Check
echo ========================================
echo.

REM Change to project root directory (go up 2 levels from scripts folder)
cd /d "%~dp0\..\..\php_sync_server"

REM Check PHP sync lock
if exist "locks\php_sync.lock" (
    echo PHP Sync: RUNNING
    type locks\php_sync.lock
) else (
    echo PHP Sync: NOT RUNNING
)

echo.

REM Check Python sync lock
if exist "locks\python_sync.lock" (
    echo Python Sync: RUNNING
    type locks\python_sync.lock
) else (
    echo Python Sync: NOT RUNNING
)

echo.
echo ========================================
pause
