"""
Lock file system for preventing concurrent syncs between Python and PHP
"""
import os
import sys
import time
import psutil
from pathlib import Path


def get_lock_dir():
    """Get the lock directory path"""
    # Get the directory of this script
    script_dir = Path(__file__).parent
    # Go up one level to ubs-local-connector, then to php_sync_server/locks
    lock_dir = script_dir.parent / 'php_sync_server' / 'locks'
    return lock_dir


def is_process_running(pid):
    """Check if a process is still running"""
    try:
        return psutil.pid_exists(pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return False


def acquire_sync_lock(lock_type='python'):
    """
    Acquire a sync lock
    Returns True if lock acquired, False if already locked
    """
    lock_dir = get_lock_dir()
    lock_dir.mkdir(parents=True, exist_ok=True)
    
    lock_file = lock_dir / f'{lock_type}_sync.lock'
    pid_file = lock_dir / f'{lock_type}_sync.pid'
    
    # Check if lock exists and if process is still running
    if lock_file.exists():
        try:
            if pid_file.exists():
                pid = int(pid_file.read_text().strip())
                if is_process_running(pid):
                    return False  # Lock is held by running process
            # Stale lock, remove it
            lock_file.unlink()
            if pid_file.exists():
                pid_file.unlink()
        except (ValueError, OSError) as e:
            print(f"⚠️  Error checking lock: {e}")
            # Try to remove stale lock
            try:
                lock_file.unlink()
                if pid_file.exists():
                    pid_file.unlink()
            except:
                pass
    
    # Create lock file
    try:
        lock_file.write_text(time.strftime('%Y-%m-%d %H:%M:%S'))
        pid_file.write_text(str(os.getpid()))
        return True
    except Exception as e:
        print(f"❌ Error creating lock: {e}")
        return False


def release_sync_lock(lock_type='python'):
    """Release sync lock"""
    lock_dir = get_lock_dir()
    lock_file = lock_dir / f'{lock_type}_sync.lock'
    pid_file = lock_dir / f'{lock_type}_sync.pid'
    
    try:
        if lock_file.exists():
            lock_file.unlink()
        if pid_file.exists():
            pid_file.unlink()
    except Exception as e:
        print(f"⚠️  Error releasing lock: {e}")


def is_sync_running(lock_type='python'):
    """Check if sync is currently running"""
    lock_dir = get_lock_dir()
    lock_file = lock_dir / f'{lock_type}_sync.lock'
    pid_file = lock_dir / f'{lock_type}_sync.pid'
    
    if not lock_file.exists():
        return False
    
    try:
        if pid_file.exists():
            pid = int(pid_file.read_text().strip())
            return is_process_running(pid)
        return False
    except (ValueError, OSError):
        return False


def get_sync_status():
    """Get sync status for both Python and PHP"""
    return {
        'python': {
            'running': is_sync_running('python'),
            'lock_file': str(get_lock_dir() / 'python_sync.lock'),
            'pid_file': str(get_lock_dir() / 'python_sync.pid'),
        },
        'php': {
            'running': is_sync_running('php'),
            'lock_file': str(get_lock_dir() / 'php_sync.lock'),
            'pid_file': str(get_lock_dir() / 'php_sync.pid'),
        },
    }
