#!/usr/bin/env python3
"""
DBF UPDATED_ON Date Fixer Script

This script loops through all DBF tables and fixes invalid UPDATED_ON dates
by updating them to the current date.

Flow:
1. Loop through all tables to sync (similar to main.py)
2. Check UPDATED_ON column for invalid dates
3. Update invalid dates to current date
4. Report progress and summary
"""

import os
import dbf
import datetime
from dotenv import load_dotenv
import time
from typing import List, Dict, Any, Optional

# Load environment variables
load_dotenv()

class DBFDateFixer:
    def __init__(self):
        self.fixed_records = 0
        self.null_converted_records = 0
        self.total_records_processed = 0
        self.tables_processed = 0
        self.start_time = time.time()
        
    def get_tables_to_process(self) -> Dict[str, List[str]]:
        """
        Get list of DBF tables to process (similar to main.py structure)
        """
        return {
            "UBSACC2015": [
                "arcust",
                "apvend",
                "arpay",
                "arpost",
                "gldata",
                "glbatch",
                "glpost",
            ],
            "UBSSTK2015": [
                "icitem",
                "artran",
                "ictran",
                "arpso",
                "icpso",
            ],
        }
    
    def get_dbf_path(self, directory_name: str, dbf_name: str) -> str:
        """
        Get full path to DBF file
        """
        dbf_subpath = os.getenv("DBF_SUBPATH", "Sample")
        directory_path = f"C:/{directory_name}/{dbf_subpath}"
        file_name = dbf_name + ".dbf"
        return os.path.join(directory_path, file_name)
    
    def is_valid_datetime_data(self, date_value: Any) -> bool:
        """
        Check if a value is valid datetime data (datetime, date, or None)
        """
        if date_value is None:
            return True  # None is valid (null)
        
        # Valid datetime types
        if isinstance(date_value, (datetime.datetime, datetime.date)):
            return True
        
        # Invalid types (strings, numbers, etc.)
        return False
    
    def should_convert_to_null(self, date_value: Any) -> bool:
        """
        Check if a datetime field value should be converted to null
        """
        if date_value is None:
            return False  # Already null
        
        # Convert non-datetime data to null
        if not isinstance(date_value, (datetime.datetime, datetime.date)):
            return True
        
        # For datetime objects, check if they're invalid dates
        if isinstance(date_value, datetime.datetime):
            if date_value.year < 1900 or date_value.year > 2100:
                return True
            if (date_value.year == 1900 and date_value.month == 1 and date_value.day == 1) or \
               (date_value.year == 1970 and date_value.month == 1 and date_value.day == 1):
                return True
        
        # For date objects, check if they're invalid dates
        if isinstance(date_value, datetime.date):
            if date_value.year < 1900 or date_value.year > 2100:
                return True
            if (date_value.year == 1900 and date_value.month == 1 and date_value.day == 1) or \
               (date_value.year == 1970 and date_value.month == 1 and date_value.day == 1):
                return True
        
        return False
    
    def get_current_datetime(self) -> datetime.datetime:
        """
        Get current datetime object
        """
        return datetime.datetime.now()
    
    def fix_updated_on_field(self, file_path: str, table_name: str) -> Dict[str, int]:
        """
        Fix UPDATED_ON field in a DBF file
        """
        if not os.path.exists(file_path):
            print(f"⚠️  Warning: File {file_path} does not exist, skipping...")
            return {"processed": 0, "fixed": 0}
        
        print(f"🔍 Processing {table_name}...")
        print(f"📁 File: {file_path}")
        
        try:
            # Open DBF file for reading and writing
            table = dbf.Table(file_path)
            table.open(mode=dbf.READ_WRITE)
            
            # Check if UPDATED_ON or CREATED_ON field exists
            field_names = list(table.field_names)
            datetime_fields = []
            
            if 'UPDATED_ON' in field_names:
                datetime_fields.append('UPDATED_ON')
            if 'CREATED_ON' in field_names:
                datetime_fields.append('CREATED_ON')
            
            if not datetime_fields:
                print(f"ℹ️  No UPDATED_ON or CREATED_ON field found in {table_name}, skipping...")
                table.close()
                return {"processed": 0, "fixed": 0, "null_converted": 0}
            
            print(f"📋 Found datetime fields: {datetime_fields}")
            
            processed_count = 0
            fixed_count = 0
            null_converted_count = 0
            current_datetime = self.get_current_datetime()
            
            print(f"📊 Found {len(table)} records to process")
            
            # Process each record
            for i, record in enumerate(table):
                try:
                    processed_count += 1
                    
                    # Process each datetime field
                    for field_name in datetime_fields:
                        current_value = getattr(record, field_name)
                        
                        # Check if we should convert to null (non-datetime data)
                        if self.should_convert_to_null(current_value):
                            with record:
                                setattr(record, field_name, None)
                            null_converted_count += 1
                            
                            if null_converted_count <= 5:  # Show first 5 null conversions
                                print(f"  🔄 Converted to null record {i+1} {field_name}: '{current_value}' ({type(current_value).__name__}) → None")
                            elif null_converted_count == 6:
                                print(f"  ... (showing first 5 null conversions)")
                        
                        # Check if date is invalid but is datetime data
                        elif not self.is_valid_datetime_data(current_value) or \
                             (isinstance(current_value, (datetime.datetime, datetime.date)) and 
                              self.should_convert_to_null(current_value)):
                            # Update to current datetime
                            with record:
                                setattr(record, field_name, current_datetime)
                            fixed_count += 1
                            
                            if fixed_count <= 5:  # Show first 5 fixes
                                print(f"  🔧 Fixed record {i+1} {field_name}: '{current_value}' → '{current_datetime}'")
                            elif fixed_count == 6:
                                print(f"  ... (showing first 5 fixes)")
                    
                    # Progress update every 1000 records
                    if processed_count % 1000 == 0:
                        print(f"  📈 Progress: {processed_count}/{len(table)} records processed, {fixed_count} fixed, {null_converted_count} converted to null")
                
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not process record {i+1}: {e}")
                    continue
            
            table.close()
            
            print(f"✅ Completed {table_name}: {processed_count} records processed, {fixed_count} dates fixed, {null_converted_count} converted to null")
            return {"processed": processed_count, "fixed": fixed_count, "null_converted": null_converted_count}
            
        except Exception as e:
            print(f"❌ Error processing {table_name}: {e}")
            return {"processed": 0, "fixed": 0, "null_converted": 0}
    
    def process_all_tables(self):
        """
        Process all DBF tables and fix UPDATED_ON dates
        """
        print("🚀 Starting DBF UPDATED_ON Date Fixing Process")
        print("=" * 60)
        
        tables_config = self.get_tables_to_process()
        total_tables = sum(len(tables) for tables in tables_config.values())
        
        print(f"📋 Found {total_tables} tables to process:")
        for directory, tables in tables_config.items():
            print(f"  📁 {directory}: {', '.join(tables)}")
        print()
        
        current_table = 0
        
        for directory_name, dbf_list in tables_config.items():
            print(f"📂 Processing directory: {directory_name}")
            print("-" * 40)
            
            for dbf_name in dbf_list:
                current_table += 1
                table_name = f"{directory_name}_{dbf_name}"
                
                print(f"[{current_table}/{total_tables}] Processing {table_name}")
                
                # Get file path
                file_path = self.get_dbf_path(directory_name, dbf_name)
                
                # Fix UPDATED_ON field
                result = self.fix_updated_on_field(file_path, table_name)
                
                # Update counters
                self.total_records_processed += result["processed"]
                self.fixed_records += result["fixed"]
                self.null_converted_records += result["null_converted"]
                self.tables_processed += 1
                
                print()
        
        # Generate final report
        self.generate_report()
    
    def generate_report(self):
        """
        Generate final processing report
        """
        elapsed_time = time.time() - self.start_time
        
        print("=" * 60)
        print("📊 PROCESSING SUMMARY")
        print("=" * 60)
        print(f"⏱️  Total Time: {elapsed_time:.2f} seconds")
        print(f"📋 Tables Processed: {self.tables_processed}")
        print(f"📄 Total Records Processed: {self.total_records_processed:,}")
        print(f"🔧 Total Dates Fixed: {self.fixed_records:,}")
        print(f"🔄 Total Converted to Null: {self.null_converted_records:,}")
        
        if self.total_records_processed > 0:
            fix_rate = (self.fixed_records / self.total_records_processed) * 100
            null_rate = (self.null_converted_records / self.total_records_processed) * 100
            print(f"📈 Fix Rate: {fix_rate:.2f}%")
            print(f"📈 Null Conversion Rate: {null_rate:.2f}%")
        
        total_changes = self.fixed_records + self.null_converted_records
        if total_changes > 0:
            print(f"\n✅ Successfully processed {total_changes:,} UPDATED_ON fields!")
            if self.fixed_records > 0:
                print(f"🔧 Fixed {self.fixed_records:,} invalid dates")
            if self.null_converted_records > 0:
                print(f"🔄 Converted {self.null_converted_records:,} non-datetime values to null")
            print("🎯 All UPDATED_ON fields now contain valid datetime data or null.")
        else:
            print(f"\n✅ No UPDATED_ON fields needed processing!")
            print("🎯 All UPDATED_ON fields were already valid.")
        
        print("\n" + "=" * 60)

def main():
    """
    Main function to run the DBF date fixer
    """
    print("🔧 DBF UPDATED_ON Date Fixer")
    print("This script will fix all invalid UPDATED_ON dates in DBF files.")
    print("Invalid dates will be updated to the current date.")
    print()
    
    # Ask for confirmation
    response = input("Do you want to proceed? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("❌ Operation cancelled by user.")
        return
    
    print()
    
    # Create and run the fixer
    fixer = DBFDateFixer()
    fixer.process_all_tables()

if __name__ == "__main__":
    main()
