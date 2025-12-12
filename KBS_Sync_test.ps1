Add-Type -AssemblyName System.Windows.Forms

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
    Write-Progress -Activity "KBS Sync Test" -Completed
    $names = ($running.ProcessName | Sort-Object -Unique) -join ", "
    Write-Host "Warning: Detected running programs: $names" -ForegroundColor Red
    [System.Windows.Forms.MessageBox]::Show(
        "Please close the following program(s) before running the tasks:`n$names",
        "Close Programs First",
        [System.Windows.Forms.MessageBoxButtons]::OK,
        [System.Windows.Forms.MessageBoxIcon]::Warning
    )
    exit 0
}

Write-Progress -Activity "KBS Sync Test" -Status "No conflicting programs detected" -PercentComplete 10
Write-Host "No conflicting programs detected. Proceeding...`n" -ForegroundColor Green

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
        # Delete old task
        schtasks.exe /Delete /TN "$fullTaskName" /F | Out-Null

        # Create new task (Daily, repeat 10 min, SYSTEM)
        $taskAction = "`"$script`" `"$arguments`""
        schtasks.exe /Create /TN "$fullTaskName" `
                      /TR "$taskAction" `
                      /SC DAILY /ST 00:00 `
                      /RI 10 /DU 24:00 `
                      /RL HIGHEST /RU SYSTEM /F | Out-Null
    }
    catch {}
}

# ============================
# Function: Run task and wait
# ============================
function Run-And-Wait {
    param($taskName, $overallPercent)

    Write-Host "  → Starting: $taskName" -ForegroundColor Cyan
    
    # Start the task
    schtasks.exe /Run /TN "$taskName" | Out-Null
    
    Write-Host "  → Task started, monitoring status..." -ForegroundColor Gray

    # Wait until the task is no longer running
    $dotCount = 0
    $elapsedSeconds = 0
    while ($true) {
        $state = (schtasks.exe /Query /TN "$taskName" /FO LIST /V |
                  Select-String "Status").ToString()

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
