@echo off
REM UBS Local Connector - Sync Runner
REM This batch file runs both Python and PHP syncs in sequence

echo ========================================
echo UBS Local Connector - Sync Process
echo ========================================
echo.

REM Change to project root directory (go up 2 levels from scripts folder)
cd /d "%~dp0\..\.."

REM Step 1: Run Python Sync (DBF to Local MySQL)
echo [1/2] Running Python sync (DBF to Local MySQL)...
echo.
cd python_sync_local
python main.py
if errorlevel 1 (
    echo.
    echo ERROR: Python sync failed!
    echo Please check the error messages above.
    pause
    exit /b 1
)
echo.
echo Python sync completed successfully!
echo.

REM Step 2: Run PHP Sync (Local MySQL to Remote MySQL)
echo [2/2] Running PHP sync (Local MySQL to Remote MySQL)...
echo.
cd ..\php_sync_server
php main.php
if errorlevel 1 (
    echo.
    echo ERROR: PHP sync failed!
    echo Please check the error messages above.
    pause
    exit /b 1
)
echo.
echo PHP sync completed successfully!
echo.

echo ========================================
echo Sync process completed!
echo ========================================
pause
