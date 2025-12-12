Add-Type -AssemblyName System.Windows.Forms

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
$running = Get-Process -ErrorAction SilentlyContinue |
           Where-Object { $targetPrograms -contains $_.ProcessName }

if ($running) {
    $names = ($running.ProcessName | Sort-Object -Unique) -join ", "
    [System.Windows.Forms.MessageBox]::Show(
        "Please close the following program(s) before running the tasks:`n$names",
        "Close Programs First",
        [System.Windows.Forms.MessageBoxButtons]::OK,
        [System.Windows.Forms.MessageBoxIcon]::Warning
    )
    exit 0
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
    param($taskName)

    # Start the task
    schtasks.exe /Run /TN "$taskName" | Out-Null

    # Wait until the task is no longer running
    Write-Host "Waiting for $taskName to finish..."
    while ($true) {
        $state = (schtasks.exe /Query /TN "$taskName" /FO LIST /V |
                  Select-String "Status").ToString()

        if ($state -notmatch "Running") {
            break
        }
        Start-Sleep -Seconds 2
    }
}

# ============================
# Process tasks (SEQUENTIAL)
# ============================
foreach ($task in $tasks) {
    Recreate-Task -task $task
}

# Run tasks **in order**
Run-And-Wait "KBS SYNC Remote"
Run-And-Wait "KBS SYNC UBS to Local MYSQL"
