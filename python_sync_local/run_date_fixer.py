#!/usr/bin/env python3
"""
Simple runner script for DBF Date Fixer
"""

from fix_updated_on_dates import DBFDateFixer

def main():
    print("🚀 Starting DBF UPDATED_ON Date Fixer...")
    print("This script will fix all invalid UPDATED_ON dates in DBF files.\n")
    
    # Create and run the fixer
    fixer = DBFDateFixer()
    fixer.process_all_tables()

if __name__ == "__main__":
    main()
