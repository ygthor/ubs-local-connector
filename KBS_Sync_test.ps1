Add-Type -AssemblyName System.Windows.Forms

# ============================
# Programs to detect
# ============================
$targetPrograms = @("cpl","vstk","daccount")

# ============================
# KBS tasks to run
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
# Delete, recreate, and run tasks
# ============================
foreach ($task in $tasks) {
    $taskName = $task.Name
    $taskPath = $task.Path
    $script = $task.Script
    $arguments = $task.Arguments

    # Full task name for schtasks.exe
    $fullTaskName = if ($taskPath -eq "\") { $taskName } else { "$taskPath$taskName" }

    try {
        # Delete task if exists
        schtasks.exe /Delete /TN "$fullTaskName" /F | Out-Null

        # Recreate task: daily, repeat every 10 minutes for 24 hours, run as SYSTEM
        $taskAction = "`"$script`" `"$arguments`""
        schtasks.exe /Create /TN "$fullTaskName" /TR "$taskAction" /SC DAILY /ST 00:00 /RI 10 /DU 24:00 /RL HIGHEST /RU SYSTEM /F | Out-Null

        # Run task immediately
        schtasks.exe /Run /TN "$fullTaskName" | Out-Null
    }
    catch {
        # Ignore errors silently
    }
}
