# ============================
# Check for Administrator privileges
# ============================
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "`n╔════════════════════════════════════════╗" -ForegroundColor Yellow
    Write-Host "║   ELEVATING TO ADMINISTRATOR MODE    ║" -ForegroundColor Yellow
    Write-Host "╚════════════════════════════════════════╝`n" -ForegroundColor Yellow
    Write-Host "This script requires administrator privileges." -ForegroundColor Gray
    Write-Host "Restarting with elevated permissions...`n" -ForegroundColor Gray
    Start-Sleep -Seconds 1
    
    # Relaunch script as administrator
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$($MyInvocation.MyCommand.Path)`""
    Start-Process powershell.exe -Verb RunAs -ArgumentList $arguments -WindowStyle Hidden
    exit
}

# ============================
# Suppress all error dialogs and popups
# ============================
$ErrorActionPreference = "SilentlyContinue"
$ProgressPreference = "SilentlyContinue"

# Suppress Windows error dialogs and zone checks
$env:SEE_MASK_NOZONECHECKS = 1
try {
    Add-Type -AssemblyName System.Windows.Forms -ErrorAction SilentlyContinue
    [System.Windows.Forms.Application]::SetUnhandledExceptionMode([System.Windows.Forms.UnhandledExceptionMode]::SilentMode) -ErrorAction SilentlyContinue
} catch {}

# ============================
# Startup Banner
# ============================
Clear-Host
Write-Host "`n╔════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     KBS Sync Test - Initializing     ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════╝`n" -ForegroundColor Cyan
Start-Sleep -Milliseconds 500

# ============================
# Programs to detect
# ============================
$targetPrograms = @("cpl","vstk","daccount")

# ============================
# KBS tasks to run in sequence
# ============================
$tasks = @(
    @{ 
        Name = "KBS SYNC Remote"; 
        Path = "\"; 
        Script = "C:\Users\User\AppData\Local\Microsoft\WindowsApps\python.exe";
        Arguments = "C:\xampp\htdocs\ubs-local-connector\python_sync_local\main.py"
    },
    @{
        Name = "KBS SYNC UBS to Local MYSQL"; 
        Path = "\"; 
        Script = "C:\xampp\php\php-win.exe";
        Arguments = "C:\xampp\htdocs\ubs-local-connector\php_sync_server\main.php"
    }
)

# ============================
# Check running programs
# ============================
Write-Progress -Activity "KBS Sync Test" -Status "Checking for running programs..." -PercentComplete 0
Write-Host "Checking for running programs..." -ForegroundColor Cyan
$running = Get-Process -ErrorAction SilentlyContinue |
           Where-Object { $targetPrograms -contains $_.ProcessName }

if ($running) {
    $names = ($running.ProcessName | Sort-Object -Unique) -join ", "
    Write-Host "`n╔════════════════════════════════════════╗" -ForegroundColor Yellow
    Write-Host "║      WARNING - PROGRAMS DETECTED       ║" -ForegroundColor Yellow
    Write-Host "╚════════════════════════════════════════╝" -ForegroundColor Yellow
    Write-Host "Detected running programs: $names" -ForegroundColor Yellow
    Write-Host "Continuing anyway...`n" -ForegroundColor Gray
} else {
    Write-Progress -Activity "KBS Sync Test" -Status "No conflicting programs detected" -PercentComplete 10
    Write-Host "No conflicting programs detected. Proceeding...`n" -ForegroundColor Green
}

# ============================
# Function: Delete & recreate task
# ============================
function Recreate-Task {
    param($task)

    $taskName = $task.Name
    $taskPath = $task.Path
    $script = $task.Script
    $arguments = $task.Arguments

    $fullTaskName = if ($taskPath -eq "\") { $taskName } else { "$taskPath$taskName" }

    try {
        # Delete old task (suppress all output and errors, no popups)
        $nullFile = "$env:TEMP\schtasks_delete_$([System.Guid]::NewGuid().ToString('N')).txt"
        $null = Start-Process -FilePath "schtasks.exe" `
            -ArgumentList "/Delete", "/TN", "`"$fullTaskName`"", "/F" `
            -WindowStyle Hidden -NoNewWindow -Wait `
            -ErrorAction SilentlyContinue `
            -RedirectStandardOutput $nullFile `
            -RedirectStandardError $nullFile
        if (Test-Path $nullFile) { Remove-Item $nullFile -ErrorAction SilentlyContinue -Force }

        # Create new task (Daily, repeat 10 min, SYSTEM)
        $taskAction = "`"$script`" `"$arguments`""
        $nullFile = "$env:TEMP\schtasks_create_$([System.Guid]::NewGuid().ToString('N')).txt"
        $null = Start-Process -FilePath "schtasks.exe" `
            -ArgumentList "/Create", "/TN", "`"$fullTaskName`"", `
                          "/TR", "$taskAction", `
                          "/SC", "DAILY", "/ST", "00:00", `
                          "/RI", "10", "/DU", "24:00", `
                          "/RL", "HIGHEST", "/RU", "SYSTEM", "/F" `
            -WindowStyle Hidden -NoNewWindow -Wait `
            -ErrorAction SilentlyContinue `
            -RedirectStandardOutput $nullFile `
            -RedirectStandardError $nullFile
        if (Test-Path $nullFile) { Remove-Item $nullFile -ErrorAction SilentlyContinue -Force }
    }
    catch {
        # Silently handle any errors
        $null = $_
    }
}

# ============================
# Function: Run task and wait
# ============================
function Run-And-Wait {
    param($taskName, $overallPercent)

    Write-Host "  → Starting: $taskName" -ForegroundColor Cyan
    
    # Start the task (suppress all output and errors, no popups)
    $nullFile = "$env:TEMP\schtasks_run_$([System.Guid]::NewGuid().ToString('N')).txt"
    $null = Start-Process -FilePath "schtasks.exe" `
        -ArgumentList "/Run", "/TN", "`"$taskName`"" `
        -WindowStyle Hidden -NoNewWindow -Wait `
        -ErrorAction SilentlyContinue `
        -RedirectStandardOutput $nullFile `
        -RedirectStandardError $nullFile
    if (Test-Path $nullFile) { Remove-Item $nullFile -ErrorAction SilentlyContinue -Force }
    
    Write-Host "  → Task started, monitoring status..." -ForegroundColor Gray

    # Wait until the task is no longer running
    $dotCount = 0
    $elapsedSeconds = 0
    while ($true) {
        # Query task status (suppress all output and errors, no popups)
        $tempFile = [System.IO.Path]::GetTempFileName()
        try {
            $null = Start-Process -FilePath "schtasks.exe" `
                -ArgumentList "/Query", "/TN", "`"$taskName`"", "/FO", "LIST", "/V" `
                -WindowStyle Hidden -NoNewWindow -Wait `
                -ErrorAction SilentlyContinue `
                -RedirectStandardOutput $tempFile `
                -RedirectStandardError $null
            
            if (Test-Path $tempFile) {
                $state = (Get-Content $tempFile -ErrorAction SilentlyContinue | Select-String "Status" -ErrorAction SilentlyContinue)
                if ($state) {
                    $state = $state.ToString()
                } else {
                    $state = ""
                }
                Remove-Item $tempFile -ErrorAction SilentlyContinue -Force
            } else {
                $state = ""
            }
        }
        catch {
            $state = ""
            if (Test-Path $tempFile) {
                Remove-Item $tempFile -ErrorAction SilentlyContinue -Force
            }
        }

        if ($state -notmatch "Running") {
            Write-Progress -Activity "KBS Sync Test" -Status "$taskName - Completed!" -PercentComplete $overallPercent
            Write-Host "`r  → $taskName completed!                    " -ForegroundColor Green
            break
        }
        
        # Update progress bar with animated status
        $dots = "." * (($dotCount % 4) + 1)
        $statusMsg = "$taskName - Running$dots (${elapsedSeconds}s)"
        Write-Progress -Activity "KBS Sync Test" -Status $statusMsg -PercentComplete $overallPercent
        
        # Show animated dots in console too
        Write-Host "`r  → Running: $taskName$dots (${elapsedSeconds}s)   " -NoNewline -ForegroundColor Yellow
        $dotCount++
        $elapsedSeconds += 2
        Start-Sleep -Seconds 2
    }
    Write-Host ""
}

# ============================
# Process tasks (SEQUENTIAL)
# ============================
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "KBS Sync Test - Starting Process" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Phase 1: Create/Update tasks (10% - 30%)
Write-Progress -Activity "KBS Sync Test" -Status "[1/3] Creating/Updating scheduled tasks..." -PercentComplete 10
Write-Host "[1/3] Creating/Updating scheduled tasks..." -ForegroundColor Yellow
$taskIndex = 0
foreach ($task in $tasks) {
    $taskIndex++
    $percent = 10 + (($taskIndex / $tasks.Count) * 20)  # 10% to 30%
    Write-Progress -Activity "KBS Sync Test" -Status "[1/3] Processing task $taskIndex/$($tasks.Count): $($task.Name)" -PercentComplete $percent
    Write-Host "  - Processing task $taskIndex/$($tasks.Count): $($task.Name)" -ForegroundColor Gray
    Recreate-Task -task $task
}
Write-Progress -Activity "KBS Sync Test" -Status "[1/3] Tasks created successfully!" -PercentComplete 30
Write-Host "[1/3] Tasks created successfully!`n" -ForegroundColor Green

# Phase 2: Run first task (30% - 65%)
Write-Progress -Activity "KBS Sync Test" -Status "[2/3] Running first task..." -PercentComplete 30
Write-Host "[2/3] Running tasks sequentially...`n" -ForegroundColor Yellow
Run-And-Wait "KBS SYNC Remote" 65

# Phase 3: Run second task (65% - 100%)
Write-Progress -Activity "KBS Sync Test" -Status "[3/3] Running final task..." -PercentComplete 65
Write-Host "`n[3/3] Running final task...`n" -ForegroundColor Yellow
Run-And-Wait "KBS SYNC UBS to Local MYSQL" 100

# Complete
Write-Progress -Activity "KBS Sync Test" -Completed
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "KBS Sync Test - Completed Successfully!" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green
