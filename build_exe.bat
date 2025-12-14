@echo off
:: Build EXE from Python GUI
:: This will create KBS_Sync_GUI.exe in the dist folder

echo.
echo ========================================
echo Building KBS_Sync_GUI.exe
echo ========================================
echo.

:: Check if PyInstaller is installed
python -c "import PyInstaller" 2>nul
if %errorLevel% neq 0 (
    echo PyInstaller not found. Installing...
    pip install pyinstaller
    if %errorLevel% neq 0 (
        echo Error: Failed to install PyInstaller
        pause
        exit /b 1
    )
)

echo.
echo Building executable...
echo.

:: Build EXE (one file, no console window, with name)
pyinstaller --onefile --windowed --name="KBS_Sync_GUI" --clean KBS_Sync_GUI.py

if %errorLevel% neq 0 (
    echo.
    echo Error: Failed to build EXE
    pause
    exit /b 1
)

echo.
echo ========================================
echo Build Complete!
echo ========================================
echo.
echo EXE file location: dist\KBS_Sync_GUI.exe
echo.
echo You can now copy this EXE to any Windows PC and run it
echo without needing Python installed!
echo.
pause

