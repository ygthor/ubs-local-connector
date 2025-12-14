# How to Convert PS1 to EXE

## Method 1: PS2EXE (Recommended - Free & Easy)

### Step 1: Install PS2EXE Module

Open PowerShell as Administrator and run:

```powershell
Install-Module -Name ps2exe -Force
```

If you get an error about execution policy, run:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Step 2: Convert PS1 to EXE

Navigate to your project directory and run:

```powershell
cd C:\xampp\htdocs\ubs-local-connector
Invoke-ps2exe -inputFile "KBS_Sync_GUI.ps1" -outputFile "KBS_Sync_GUI.exe" -title "KBS Sync" -description "KBS Sync Process Monitor"
```

### Step 3: Optional - Add Icon

If you have an icon file (.ico):

```powershell
Invoke-ps2exe -inputFile "KBS_Sync_GUI.ps1" -outputFile "KBS_Sync_GUI.exe" -iconFile "icon.ico" -title "KBS Sync" -description "KBS Sync Process Monitor"
```

### Step 4: Optional - Hide Console Window

To hide the console window (GUI only):

```powershell
Invoke-ps2exe -inputFile "KBS_Sync_GUI.ps1" -outputFile "KBS_Sync_GUI.exe" -noConsole -title "KBS Sync"
```

### Complete Command with All Options

```powershell
Invoke-ps2exe `
    -inputFile "KBS_Sync_GUI.ps1" `
    -outputFile "KBS_Sync_GUI.exe" `
    -title "KBS Sync" `
    -description "KBS Sync Process Monitor with Progress Display" `
    -company "Your Company Name" `
    -product "KBS Sync" `
    -copyright "Copyright © 2025" `
    -version "1.0.0.0" `
    -noConsole `
    -requireAdmin
```

## Method 2: PS2EXE-GUI (Graphical Interface)

### Step 1: Download PS2EXE-GUI

Download from: https://github.com/MScholtes/PS2EXE/releases

### Step 2: Use GUI

1. Run `PS2EXE-GUI.exe`
2. Select your `KBS_Sync_GUI.ps1` file
3. Configure options (icon, version, etc.)
4. Click "Create EXE"

## Method 3: Online Converter (Quick but Less Secure)

1. Visit: https://tools-4all.com/ps-to-exe
2. Upload your `KBS_Sync_GUI.ps1`
3. Configure options
4. Download the EXE

## Important Notes

### For KBS_Sync_GUI.ps1 specifically:

1. **Keep Console Visible**: Since the script shows progress in a GUI window, you can use `-noConsole` to hide the PowerShell console window.

2. **Admin Rights**: The script requires admin rights, so use `-requireAdmin` flag.

3. **Dependencies**: The EXE will still need:
   - PowerShell installed on target machine
   - Python executable (if path is hardcoded)
   - PHP executable (if path is hardcoded)
   - Access to the sync scripts

4. **Paths**: Make sure paths in the script are correct or use relative paths.

## Recommended Command for KBS_Sync_GUI.ps1

```powershell
Invoke-ps2exe `
    -inputFile "KBS_Sync_GUI.ps1" `
    -outputFile "KBS_Sync_GUI.exe" `
    -title "KBS Sync" `
    -description "KBS Sync Process Monitor" `
    -noConsole `
    -requireAdmin
```

This will:
- ✅ Hide the PowerShell console (GUI only)
- ✅ Require admin rights automatically
- ✅ Create a standalone EXE file

## Testing the EXE

After conversion:
1. Double-click `KBS_Sync_GUI.exe`
2. It should show the GUI window
3. Test the sync process

## Troubleshooting

### Error: "Module not found"
```powershell
# Install from PowerShell Gallery
Install-Module -Name ps2exe -Force -Scope CurrentUser
```

### Error: "Execution Policy"
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### EXE doesn't run
- Check if PowerShell is installed
- Run as Administrator
- Check Windows Defender/Antivirus (may block EXE)

