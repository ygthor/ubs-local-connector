@echo off
:: Run the Python GUI version (Normal Sync)
python KBS_Sync_GUI.py
if %errorLevel% neq 0 (
    echo.
    echo Error: Failed to run Python GUI
    echo Make sure Python is installed and tkinter is available
    pause
)

