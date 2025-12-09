#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Standalone script to sync only icgroup.dbf to local MySQL
Usage: python sync_icgroup.py
"""

from main import sync_icgroup_only
from sync_lock import acquire_sync_lock, release_sync_lock, is_sync_running
import sys
import atexit

def main():
    # Check if PHP sync is running
    if is_sync_running('php'):
        print("❌ PHP sync is currently running. Please wait for it to complete.")
        sys.exit(1)
    
    # Acquire Python sync lock
    if not acquire_sync_lock('python'):
        print("❌ Python sync is already running or lock file exists. Please check and remove lock file if needed.")
        sys.exit(1)
    
    # Register cleanup function to release lock on exit
    atexit.register(lambda: release_sync_lock('python'))
    
    try:
        success = sync_icgroup_only()
        
        if success:
            print("\n🎉 icgroup sync completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ icgroup sync failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        release_sync_lock('python')
        sys.exit(1)
    finally:
        # Ensure lock is released
        release_sync_lock('python')


if __name__ == "__main__":
    main()
