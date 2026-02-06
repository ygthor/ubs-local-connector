import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import threading
import os
import sys
import re
import time
import shutil
from datetime import datetime
import platform
import atexit

# Try to import psutil for process checking, fallback to tasklist if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

class SyncGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("KBS Sync - Progress Monitor")
        self.root.geometry("800x600")
        self.root.resizable(False, False)
        
        # Configuration - Auto-detect executables and scripts
        # Get script directory (where this Python file is located)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Script paths (relative to this file's location)
        self.python_script = os.path.join(script_dir, "python_sync_local", "main.py")
        self.php_script = os.path.join(script_dir, "php_sync_server", "main.php")
        
        # Auto-detect executables
        self.python_exe = self.find_python()
        self.php_exe = self.find_php()
        
        # Progress tracking
        self.python_total_files = 11
        self.php_total_tables = 6
        self.current_percent = 0
        self.current_file_num = 0
        self.current_table_num = 0
        self.tables_found = False

        # Log behavior (prevent UI crash from extremely verbose output)
        self.max_log_lines = 4000
        self.log_trim_chunk = 200
        self.max_log_line_length = 500
        self.drop_debug_lines = True
        self.log_line_count = 0
        self.log_queue = []
        self.log_flush_scheduled = False
        self.log_flush_interval_ms = 300
        self.gui_show_summaries_only = True
        self.log_file = None
        self.log_file_path = None
        self._init_log_file()
        
        # Programs to detect (same as PowerShell script)
        self.target_programs = ["cpl", "vstk", "daccount"]
        
        # Check admin
        if not self.is_admin():
            self.request_admin()
            return
        
        # Resync mode - check command line arguments
        self.resync_date = None
        
        # Parse command line arguments
        args = sys.argv[1:]
        for i, arg in enumerate(args):
            if arg == '--resync-date' and i + 1 < len(args):
                self.resync_date = args[i + 1]
                break
        
        # Log detected paths
        print(f"Detected Python: {self.python_exe}")
        print(f"Detected PHP: {self.php_exe}")
        
        # If resync_date is set via command line, validate it
        if self.resync_date:
            try:
                datetime.strptime(self.resync_date, "%Y-%m-%d")
                print(f"Resync mode: {self.resync_date}")
            except ValueError:
                print(f"Invalid date format: {self.resync_date}. Using normal sync.")
                self.resync_date = None
        
        # Skip dialog - go straight to sync (default to normal sync)
        self.setup_ui()
        # Check for running programs before starting sync
        if self.check_running_programs():
            # Programs detected, exit - don't start sync
            return
        self.start_sync()
    
    def find_python(self):
        """Auto-detect Python executable"""
        # Try using 'python' command (works if in PATH)
        python_cmd = shutil.which('python')
        if python_cmd and os.path.exists(python_cmd):
            return python_cmd
        
        # Try 'python3'
        python_cmd = shutil.which('python3')
        if python_cmd and os.path.exists(python_cmd):
            return python_cmd
        
        # Try common Windows locations
        common_paths = [
            r"C:\Python*\python.exe",
            r"C:\Program Files\Python*\python.exe",
            r"C:\Program Files (x86)\Python*\python.exe",
            os.path.expanduser(r"~\AppData\Local\Programs\Python\Python*\python.exe"),
            os.path.expanduser(r"~\AppData\Local\Microsoft\WindowsApps\python.exe"),
        ]
        
        import glob
        for pattern in common_paths:
            matches = glob.glob(pattern)
            if matches:
                # Return the first match, prefer newer versions
                matches.sort(reverse=True)
                return matches[0]
        
        # Fallback: use sys.executable (the Python running this script)
        return sys.executable
    
    def find_php(self):
        """Auto-detect PHP executable"""
        # Prefer php.exe over php-win.exe when we're capturing output
        # php-win.exe is for GUI apps, php.exe works better with subprocess
        
        # Try common XAMPP location first (prefer php.exe)
        xampp_php_exe = r"C:\xampp\php\php.exe"
        if os.path.exists(xampp_php_exe):
            return xampp_php_exe
        
        # Try using 'php' command (works if in PATH)
        php_cmd = shutil.which('php')
        if php_cmd and os.path.exists(php_cmd):
            return php_cmd
        
        # Try common locations
        common_paths = [
            r"C:\xampp\php\php.exe",
            r"C:\xampp\php\php-win.exe",  # Fallback to php-win.exe
            r"C:\php\php.exe",
            r"C:\Program Files\PHP\php.exe",
        ]
        
        for path in common_paths:
            if os.path.exists(path):
                return path
        
        return r"C:\xampp\php\php.exe"  # Default fallback to php.exe
    
    def is_admin(self):
        try:
            return os.getuid() == 0
        except:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
    
    def request_admin(self):
        import ctypes
        ctypes.windll.shell32.ShellExecuteW(
            None, "runas", sys.executable, " ".join(sys.argv), None, 1
        )
        sys.exit()
    
    def check_running_programs(self):
        """Check for running programs that might conflict with sync
        Returns True if programs are detected (should halt), False otherwise"""
        self.update_progress(0, "Checking for running programs...", 
                           "Checking for running programs...", "")
        
        running_programs = []
        
        if PSUTIL_AVAILABLE:
            # Use psutil if available (cleaner approach)
            try:
                for proc in psutil.process_iter(['name']):
                    try:
                        proc_name = proc.info['name'].lower()
                        # Remove .exe extension if present for comparison
                        proc_name_no_ext = proc_name.replace('.exe', '')
                        # Check if process name (with or without extension) matches target programs
                        if proc_name_no_ext in self.target_programs or proc_name in self.target_programs:
                            # Store without extension to match target_programs format
                            running_programs.append(proc_name_no_ext)
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
            except Exception as e:
                # Fallback to tasklist if psutil fails
                running_programs = self._check_running_programs_tasklist()
        else:
            # Use tasklist command on Windows
            running_programs = self._check_running_programs_tasklist()
        
        # Remove duplicates and sort
        running_programs = sorted(list(set(running_programs)))
        
        if running_programs:
            # Show error message and exit immediately
            names = ", ".join(running_programs)
            error_msg = f"ERROR - PROGRAMS DETECTED\n\nDetected running programs: {names}\n\nPlease close these programs before running the sync.\n\nThe application will now exit."
            
            # Show messagebox error (blocking - waits for user to click OK)
            messagebox.showerror(
                "ERROR - PROGRAMS DETECTED",
                error_msg
            )
            
            # Exit the application immediately after messagebox is closed
            self.root.destroy()
            sys.exit(1)
        else:
            self.update_progress(0, "No conflicting programs detected", 
                               "No conflicting programs detected. Proceeding...", "")
            return False  # Return False to indicate no programs detected, can proceed
    
    def _check_running_programs_tasklist(self):
        """Check for running programs using Windows tasklist command"""
        running_programs = []
        
        if platform.system() != 'Windows':
            return running_programs
        
        try:
            # Run tasklist command
            result = subprocess.run(
                ['tasklist', '/FO', 'CSV', '/NH'],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=subprocess.CREATE_NO_WINDOW if platform.system() == 'Windows' else 0
            )
            
            if result.returncode == 0:
                # Parse output - CSV format: "Image Name","PID","Session Name","Session#","Mem Usage"
                for line in result.stdout.strip().split('\n'):
                    if line:
                        # Extract process name from CSV (first field)
                        parts = line.split(',')
                        if parts:
                            proc_name = parts[0].strip('"').lower()
                            proc_name_no_ext = proc_name.replace('.exe', '')
                            # Check if process name (with or without extension) matches target programs
                            if proc_name_no_ext in self.target_programs or proc_name in self.target_programs:
                                # Store without extension to match target_programs format
                                running_programs.append(proc_name_no_ext)
        except (subprocess.TimeoutExpired, subprocess.SubprocessError, Exception):
            # Silently fail if tasklist doesn't work
            pass
        
        return running_programs
    
    def ask_sync_mode(self):
        """Ask user to choose between normal sync and resync"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Sync Mode")
        dialog.geometry("400x200")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (400 // 2)
        y = (dialog.winfo_screenheight() // 2) - (200 // 2)
        dialog.geometry(f"400x200+{x}+{y}")
        
        tk.Label(dialog, text="Select Sync Mode", 
                font=("Segoe UI", 12, "bold")).pack(pady=20)
        
        def normal_sync():
            dialog.destroy()
            self.setup_ui()
            self.start_sync()
        
        def resync():
            dialog.destroy()
            self.ask_resync_date()
        
        btn_frame = tk.Frame(dialog)
        btn_frame.pack(pady=20)
        
        normal_btn = tk.Button(btn_frame, text="Normal Sync", 
                              command=normal_sync, width=15, height=2)
        normal_btn.pack(side=tk.LEFT, padx=10)
        
        resync_btn = tk.Button(btn_frame, text="Resync by Date", 
                              command=resync, width=15, height=2)
        resync_btn.pack(side=tk.LEFT, padx=10)
    
    def ask_resync_date(self):
        """Ask user to enter a date for resync"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Resync by Date")
        dialog.geometry("400x250")
        dialog.resizable(False, False)
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (400 // 2)
        y = (dialog.winfo_screenheight() // 2) - (250 // 2)
        dialog.geometry(f"400x250+{x}+{y}")
        
        tk.Label(dialog, text="Resync by Date", 
                font=("Segoe UI", 12, "bold")).pack(pady=10)
        
        tk.Label(dialog, text="Enter date to resync (YYYY-MM-DD):", 
                font=("Segoe UI", 9)).pack(pady=5)
        
        tk.Label(dialog, text="This will sync all records where\ncreated_at or updated_at matches this date", 
                font=("Segoe UI", 8), fg="gray").pack(pady=5)
        
        date_entry = tk.Entry(dialog, font=("Segoe UI", 11), width=20)
        date_entry.pack(pady=10)
        date_entry.focus()
        
        # Set default to today
        today = datetime.now().strftime("%Y-%m-%d")
        date_entry.insert(0, today)
        date_entry.select_range(0, tk.END)
        
        def start_resync():
            date_str = date_entry.get().strip()
            try:
                # Validate date format
                datetime.strptime(date_str, "%Y-%m-%d")
                self.resync_date = date_str
                dialog.destroy()
                self.setup_ui()
                self.start_sync()
            except ValueError:
                tk.messagebox.showerror("Invalid Date", 
                    "Please enter date in YYYY-MM-DD format (e.g., 2025-01-15)")
        
        btn_frame = tk.Frame(dialog)
        btn_frame.pack(pady=20)
        
        cancel_btn = tk.Button(btn_frame, text="Cancel", 
                              command=lambda: (dialog.destroy(), self.root.quit()), width=12)
        cancel_btn.pack(side=tk.LEFT, padx=5)
        
        ok_btn = tk.Button(btn_frame, text="Start Resync", 
                          command=start_resync, width=12)
        ok_btn.pack(side=tk.LEFT, padx=5)
        
        # Bind Enter key
        date_entry.bind('<Return>', lambda e: start_resync())
    
    def setup_ui(self):
        # Title
        title = tk.Label(self.root, text="KBS Sync Process", 
                        font=("Segoe UI", 16, "bold"))
        title.pack(pady=15, anchor="w", padx=20)
        
        # Status
        self.status_label = tk.Label(self.root, text="Initializing...",
                                     font=("Segoe UI", 10))
        self.status_label.pack(pady=5, anchor="w", padx=20)
        
        # Progress bar
        self.progress = ttk.Progressbar(self.root, length=760, mode='determinate', maximum=100)
        self.progress.pack(pady=10, padx=20)
        
        # Percentage and Phase (side by side)
        progress_frame = tk.Frame(self.root)
        progress_frame.pack(fill=tk.X, padx=20)
        
        self.percent_label = tk.Label(progress_frame, text="0%",
                                     font=("Segoe UI", 11, "bold"))
        self.percent_label.pack(side=tk.LEFT)
        
        self.phase_label = tk.Label(progress_frame, text="", 
                                   font=("Segoe UI", 9, "italic"),
                                   fg="darkblue")
        self.phase_label.pack(side=tk.LEFT, padx=(10, 0))
        
        # Log output
        self.log_text = scrolledtext.ScrolledText(
            self.root, height=20, width=95,
            font=("Consolas", 9),
            bg="black", fg="lime", wrap=tk.WORD
        )
        self.log_text.pack(pady=10, padx=20, fill=tk.BOTH, expand=True)
        
        # Close button
        self.close_btn = tk.Button(self.root, text="Close", 
                                   command=self.root.quit, state="disabled")
        self.close_btn.pack(pady=10)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _init_log_file(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = os.path.join(log_dir, f"gui_sync_{timestamp}.log")
            self.log_file = open(self.log_file_path, "a", encoding="utf-8", buffering=1)
            atexit.register(self._close_log_file)
        except Exception:
            self.log_file = None

    def _close_log_file(self):
        try:
            if self.log_file:
                self.log_file.close()
        except Exception:
            pass

    def _on_close(self):
        self._close_log_file()
        self.root.destroy()
    
    def update_progress(self, percent, status, detail="", phase=""):
        """Thread-safe progress update"""
        def update():
            self.current_percent = percent
            self.progress['value'] = percent
            self.percent_label['text'] = f"{int(percent)}%"
            self.status_label['text'] = status
            if phase:
                self.phase_label['text'] = phase
            if detail:
                self.append_log(detail)
        
        self.root.after(0, update)

    def should_log_line(self, line):
        if self.drop_debug_lines and '🔍 DEBUG' in line:
            return False
        return True

    def is_summary_line(self, line):
        if not self.gui_show_summaries_only:
            return True
        summary_keywords = [
            "INFO:", "WARN", "WARNING", "ERROR", "FAILED", "SUCCESS",
            "Starting", "Completed", "Processing table", "Found", "SYNC",
            "✅", "❌", "⚠️", "⬆️", "⬇️", "📊", "📦"
        ]
        return any(k in line for k in summary_keywords)

    def append_log(self, detail):
        if not self.should_log_line(detail):
            return
        if self.log_file:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.log_file.write(f"[{timestamp}] {detail}\n")
            except Exception:
                pass
        if not self.is_summary_line(detail):
            return
        if self.max_log_line_length and len(detail) > self.max_log_line_length:
            detail = detail[:self.max_log_line_length] + "..."
        self.log_queue.append(detail)
        if not self.log_flush_scheduled:
            self.log_flush_scheduled = True
            self.root.after(self.log_flush_interval_ms, self._flush_log_queue)

    def _flush_log_queue(self):
        if not self.log_queue:
            self.log_flush_scheduled = False
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        lines = [f"[{timestamp}] {line}" for line in self.log_queue]
        self.log_queue.clear()
        self.log_text.insert(tk.END, "\n".join(lines) + "\n")
        self.log_line_count += len(lines)
        if self.log_line_count > self.max_log_lines:
            self.log_text.delete("1.0", f"{self.log_trim_chunk + 1}.0")
            self.log_line_count -= self.log_trim_chunk
        self.log_text.see(tk.END)
        self.log_flush_scheduled = False
    
    def parse_python_progress(self, line):
        """Parse Python sync output for progress"""
        # Parse [X/Y] format for file processing
        match = re.search(r'\[(\d+)/(\d+)\]', line)
        if match:
            file_num = int(match.group(1))
            total_files = int(match.group(2))
            self.current_file_num = file_num
            # Calculate: 10% (start) + (file_num / total_files) * 40% = 10-50%
            percent = 10 + round((file_num / total_files) * 40)
            return percent, f"Phase 1: Python Sync ({file_num}/{total_files} files)"
        
        # Parse "Reading records: X read" for DBF reading progress
        match = re.search(r'Reading records: ([\d,]+) read', line)
        if match:
            records_read = int(match.group(1).replace(',', ''))
            # Keep current file progress, just update detail
            return self.current_percent, None
        
        # Parse "Read X records" completion message
        match = re.search(r'Read ([\d,]+) records in', line)
        if match:
            # File reading complete, maintain progress
            return self.current_percent, None
        
        # Parse "Syncing X records to database" message
        if 'Syncing' in line and 'records to database' in line:
            return self.current_percent, None
        
        # Parse "completed in Xs" for file completion
        match = re.search(r'completed in ([\d.]+)s', line)
        if match:
            # File sync complete, maintain progress
            return self.current_percent, None
        
        # Check for completion
        if 'SYNC COMPLETED' in line:
            return 50, "Phase 1: Python Sync - COMPLETE"
        
        return None, None
    
    def parse_php_progress(self, line):
        """Parse PHP sync output for progress"""
        # STEP 1: Find total table count
        match = re.search(r'Found (\d+) tables to sync', line)
        if match:
            self.php_total_tables = int(match.group(1))
            self.tables_found = True
            return 55, "Phase 2: PHP Sync"
        
        # STEP 2: Parse "Processing table X/Y"
        match = re.search(r'Processing table (\d+)/(\d+)', line)
        if match:
            table_num = int(match.group(1))
            tables_total = int(match.group(2))
            self.current_table_num = table_num
            if not self.tables_found:
                self.php_total_tables = tables_total
            
            # Calculate: 55% (start) + (table_num / tables_total) * 38% = 55-93%
            if tables_total > 0:
                percent = 55 + round((table_num / tables_total) * 38, 1)
                if percent > 93:
                    percent = 93
                return percent, f"Phase 2: PHP Sync (Table {table_num}/{tables_total})"
        
        # STEP 3: Recalculation (keep current percent)
        if re.search(r'Recalculating artran totals|Processing REFNO:', line):
            return self.current_percent, None
        
        # STEP 4: Duplicate cleanup starts
        if re.search(r'Starting duplicate orders|Checking for duplicate|duplicate.*validation', line):
            return 93, "Phase 2: PHP Sync (Duplicate cleanup)"
        
        # STEP 5: Duplicate cleanup completed
        if re.search(r'Duplicate validation completed|cleaned up.*order|No cleanup needed', line):
            return 95, "Phase 2: PHP Sync (Cleanup complete)"
        
        # STEP 6: Summary starts
        if re.search(r'SYNC RESULTS SUMMARY|DETAILED SYNC SUMMARY', line):
            return 96, "Phase 2: PHP Sync (Generating summary)"
        
        # STEP 7: Completed
        if re.search(r'completed successfully|🎉.*completed|All.*tables processed', line):
            return 100, "Phase 2: PHP Sync - COMPLETE"
        
        return None, None
    
    def run_python_sync(self):
        """Run Python sync with real-time progress tracking"""
        self.update_progress(5, "Starting Python Sync (DBF → Local MySQL)...", 
                           "═══════════════════════════════════════════════════════════", 
                           "Phase 1: Python Sync")
        self.update_progress(5, "Starting Python Sync...", "Checking Python executable...", 
                           "Phase 1: Python Sync")
        
        if not os.path.exists(self.python_exe):
            self.update_progress(0, "Error: Python executable not found!", 
                               f"❌ Python not found at: {self.python_exe}", "ERROR")
            return False
        
        if not os.path.exists(self.python_script):
            self.update_progress(0, "Error: Python script not found!", 
                               f"❌ Script not found at: {self.python_script}", "ERROR")
            return False
        
        self.update_progress(10, "Python Sync: Starting...", 
                           "🚀 Starting FAST DBF to MySQL sync...", 
                           "Phase 1: Python Sync")
        
        # Start process with UTF-8 encoding
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONUTF8'] = '1'
        
        # Prevent console window from flashing on Windows
        popen_kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'text': True,
            'encoding': 'utf-8',
            'errors': 'replace',
            'bufsize': 1,
            'cwd': os.path.dirname(self.python_script),
            'env': env
        }
        
        if platform.system() == 'Windows':
            # CREATE_NO_WINDOW flag prevents new console window from appearing
            popen_kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
            
            # Also set startupinfo to hide window completely
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
            popen_kwargs['startupinfo'] = startupinfo
        
        process = subprocess.Popen(
            [self.python_exe, self.python_script],
            **popen_kwargs
        )
        
        # Real-time output reading with progress parsing
        start_time = time.time()
        current_percent = 10
        
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            
            line = line.strip()
            if not line:
                continue
            
            # Parse progress from line
            percent, phase = self.parse_python_progress(line)
            if percent is not None:
                current_percent = percent
                if phase:
                    self.update_progress(percent, "Python Sync: Processing files...", 
                                       line, phase)
                else:
                    self.update_progress(percent, "Python Sync: Processing files...", line)
            else:
                # Show all lines in log for better visibility
                # Check if it's a progress-related message (don't show verbose debug info)
                is_progress_line = any(keyword in line for keyword in [
                    'Reading', 'Read', 'records', 'Processing', 'Syncing', 
                    'completed', 'Filtering', 'File size', 'Opening', 'Found'
                ])
                
                if is_progress_line or len(line) < 100:  # Show progress lines or short messages
                    self.update_progress(current_percent, "Python Sync: Processing files...", line)
                # Time-based fallback if no progress detected for a while
                elapsed = time.time() - start_time
                if current_percent < 45 and elapsed > 10 and not is_progress_line:
                    # Only use time-based estimation if we haven't seen progress in 10+ seconds
                    estimated = 10 + min(35, round(elapsed / 5))
                    if estimated > current_percent:
                        current_percent = estimated
                        self.update_progress(current_percent, 
                                           f"Python Sync: Running... ({int(elapsed)} seconds)", 
                                           "", "Phase 1: Python Sync")
        
        process.wait()
        
        # Check for errors
        stderr_output = process.stderr.read()
        if process.returncode == 0:
            self.update_progress(50, "Python Sync: Completed successfully!", 
                               "✅ Python sync completed successfully!", 
                               "Phase 1: Python Sync - COMPLETE")
            return True
        else:
            self.update_progress(50, "Python Sync: Failed!", 
                               f"❌ Python sync failed with exit code: {process.returncode}", 
                               "Phase 1: Python Sync - FAILED")
            if stderr_output:
                self.update_progress(50, "Python Sync: Failed!", 
                                   f"Error: {stderr_output}", 
                                   "Phase 1: Python Sync - FAILED")
            return False
    
    def run_php_sync(self):
        """Run PHP sync with real-time progress tracking"""
        if self.resync_date:
            self.update_progress(50, f"Starting PHP Resync for date: {self.resync_date}...", 
                               "═══════════════════════════════════════════════════════════", 
                               "Phase 2: PHP Resync")
            self.update_progress(50, f"Resyncing records for {self.resync_date}...", 
                               f"Checking PHP executable... (Resync mode: {self.resync_date})", 
                               "Phase 2: PHP Resync")
        else:
            self.update_progress(50, "Starting PHP Sync (Local MySQL ↔ Remote MySQL)...", 
                               "═══════════════════════════════════════════════════════════", 
                               "Phase 2: PHP Sync")
            self.update_progress(50, "Starting PHP Sync...", "Checking PHP executable...", 
                               "Phase 2: PHP Sync")
        
        if not os.path.exists(self.php_exe):
            self.update_progress(50, "Error: PHP executable not found!", 
                               f"❌ PHP not found at: {self.php_exe}", "ERROR")
            return False
        
        if not os.path.exists(self.php_script):
            self.update_progress(50, "Error: PHP script not found!", 
                               f"❌ Script not found at: {self.php_script}", "ERROR")
            return False
        
        self.update_progress(55, "PHP Sync: Starting...", 
                           "🚀 Starting UBS Local Connector Sync Process...", 
                           "Phase 2: PHP Sync")
        
        # Start process with UTF-8 encoding
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'  # For any Python subprocesses PHP might call
        
        # Add resync date as command line argument if set
        php_args = [self.php_exe, self.php_script]
        if self.resync_date:
            php_args.append('--resync-date')
            php_args.append(self.resync_date)
        
        # Prevent console window from flashing on Windows
        popen_kwargs = {
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'text': True,
            'encoding': 'utf-8',
            'errors': 'replace',
            'bufsize': 1,
            'cwd': os.path.dirname(self.php_script),
            'env': env
        }
        
        if platform.system() == 'Windows':
            # CREATE_NO_WINDOW flag prevents new console window from appearing
            popen_kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
            
            # Also set startupinfo to hide window completely
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
            popen_kwargs['startupinfo'] = startupinfo
        
        process = subprocess.Popen(
            php_args,
            **popen_kwargs
        )
        
        # Real-time output reading with progress parsing
        start_time = time.time()
        current_percent = 55
        
        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            
            line = line.strip()
            if not line:
                continue
            
            # Parse progress from line
            percent, phase = self.parse_php_progress(line)
            if percent is not None:
                current_percent = percent
                if phase:
                    self.update_progress(percent, "PHP Sync: Processing tables...", 
                                       line, phase)
                else:
                    self.update_progress(percent, "PHP Sync: Processing tables...", line)
            else:
                # Time-based fallback if tables not found yet
                elapsed = time.time() - start_time
                if not self.tables_found and current_percent < 70 and elapsed > 3:
                    estimated = 55 + min(15, round(elapsed / 2))
                    if estimated > current_percent:
                        current_percent = estimated
                        self.update_progress(current_percent, 
                                           f"PHP Sync: Initializing... ({int(elapsed)} seconds)", 
                                           "", "Phase 2: PHP Sync (detecting tables...)")
                else:
                    phase_text = "Phase 2: PHP Sync"
                    if self.current_table_num > 0 and self.php_total_tables > 0:
                        phase_text = f"Phase 2: PHP Sync (Table {self.current_table_num}/{self.php_total_tables})"
                    elif self.tables_found and self.php_total_tables > 0:
                        phase_text = f"Phase 2: PHP Sync ({self.php_total_tables} tables total)"
                    self.update_progress(current_percent, "PHP Sync: Processing tables...", 
                                       line, phase_text)
        
        process.wait()
        
        # Check for errors
        stderr_output = process.stderr.read()
        if process.returncode == 0:
            self.update_progress(100, "PHP Sync: Completed successfully!", 
                               "✅ PHP sync completed successfully!", 
                               "Phase 2: PHP Sync - COMPLETE")
            return True
        else:
            self.update_progress(100, "PHP Sync: Failed!", 
                               f"❌ PHP sync failed with exit code: {process.returncode}", 
                               "Phase 2: PHP Sync - FAILED")
            if stderr_output:
                self.update_progress(100, "PHP Sync: Failed!", 
                                   f"Error: {stderr_output}", 
                                   "Phase 2: PHP Sync - FAILED")
            return False
    
    def start_sync(self):
        """Start sync process in background thread"""
        def sync_thread():
            try:
                # Phase 1: Python Sync (0-50%)
                if not self.run_python_sync():
                    self.update_progress(50, "Process failed at Python Sync", 
                                       "═══════════════════════════════════════════════════════════", 
                                       "FAILED")
                    self.update_progress(50, "Process failed", 
                                       "❌ Sync process stopped due to Python sync failure", 
                                       "FAILED")
                    self.root.after(0, lambda: self.close_btn.config(state='normal'))
                    return
                
                time.sleep(0.5)
                
                # Phase 2: PHP Sync (50-100%)
                if not self.run_php_sync():
                    self.update_progress(100, "Process failed at PHP Sync", 
                                       "═══════════════════════════════════════════════════════════", 
                                       "FAILED")
                    self.update_progress(100, "Process failed", 
                                       "❌ Sync process stopped due to PHP sync failure", 
                                       "FAILED")
                    self.root.after(0, lambda: self.close_btn.config(state='normal'))
                    return
                
                # Success!
                self.update_progress(100, "All sync processes completed successfully!", 
                                   "═══════════════════════════════════════════════════════════", 
                                   "COMPLETE")
                self.update_progress(100, "Completed!", 
                                   "🎉 All sync tasks completed successfully!", 
                                   "COMPLETE")
                self.update_progress(100, "Completed!", 
                                   "Python Sync: ✅ | PHP Sync: ✅", 
                                   "COMPLETE")
                
                self.root.after(0, lambda: self.close_btn.config(state='normal'))
                
            except Exception as e:
                self.update_progress(0, f"Error: {str(e)}", str(e), "ERROR")
                self.root.after(0, lambda: self.close_btn.config(state='normal'))
        
        threading.Thread(target=sync_thread, daemon=True).start()

if __name__ == "__main__":
    root = tk.Tk()
    app = SyncGUI(root)
    root.mainloop()
