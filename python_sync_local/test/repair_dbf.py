#!/usr/bin/env python3
"""
DBF File Repair Utility
Fixes common DBF file issues like record length mismatches
"""

import os
import struct
import shutil
from datetime import datetime

def repair_dbf_file(input_path, output_path=None):
    """
    Repair a DBF file with record length mismatches
    
    Args:
        input_path: Path to the problematic DBF file
        output_path: Path for the repaired file (optional, defaults to input_path + '.repaired')
    
    Returns:
        bool: True if repair was successful, False otherwise
    """
    if not os.path.exists(input_path):
        print(f"❌ File not found: {input_path}")
        return False
    
    if output_path is None:
        output_path = input_path + '.repaired'
    
    print(f"🔧 Repairing DBF file: {input_path}")
    print(f"📁 Output file: {output_path}")
    
    try:
        # Create backup
        backup_path = input_path + '.backup'
        shutil.copy2(input_path, backup_path)
        print(f"💾 Created backup: {backup_path}")
        
        with open(input_path, 'rb') as infile, open(output_path, 'wb') as outfile:
            # Read and copy header
            header = infile.read(32)
            if len(header) < 32:
                print("❌ Invalid DBF file: header too short")
                return False
            
            # Get header information
            num_records = struct.unpack('<I', header[4:8])[0]
            header_length = struct.unpack('<H', header[8:10])[0]
            record_length = struct.unpack('<H', header[10:12])[0]
            
            print(f"📊 Records: {num_records}, Header length: {header_length}, Record length: {record_length}")
            
            # Copy header and field definitions
            infile.seek(0)
            header_and_fields = infile.read(header_length)
            outfile.write(header_and_fields)
            
            # Process records
            repaired_count = 0
            skipped_count = 0
            
            for i in range(num_records):
                record_data = infile.read(record_length)
                
                if len(record_data) == 0:
                    # End of file reached
                    print(f"⚠️  End of file reached at record {i}")
                    break
                elif len(record_data) < record_length:
                    # Record too short - pad with null bytes
                    padding_needed = record_length - len(record_data)
                    record_data = record_data + b'\x00' * padding_needed
                    repaired_count += 1
                    if repaired_count <= 5:  # Show first 5 repairs
                        print(f"🔧 Repaired record {i}: padded {padding_needed} bytes")
                elif len(record_data) > record_length:
                    # Record too long - truncate
                    record_data = record_data[:record_length]
                    repaired_count += 1
                    if repaired_count <= 5:  # Show first 5 repairs
                        print(f"🔧 Repaired record {i}: truncated {len(record_data) - record_length} bytes")
                
                # Skip deleted records
                if record_data[0] == 0x2A:  # Deleted record marker
                    skipped_count += 1
                    continue
                
                outfile.write(record_data)
            
            print(f"✅ Repair completed!")
            print(f"📊 Repaired records: {repaired_count}")
            print(f"🗑️  Skipped deleted records: {skipped_count}")
            print(f"💾 Repaired file saved as: {output_path}")
            
            return True
            
    except Exception as e:
        print(f"❌ Error repairing file: {e}")
        return False

def main():
    """Main function for command-line usage"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python repair_dbf.py <dbf_file_path> [output_path]")
        print("Example: python repair_dbf.py C:/UBSSTK2015/Sample/ictran.dbf")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    success = repair_dbf_file(input_file, output_file)
    
    if success:
        print("\n🎉 DBF file repair completed successfully!")
        print("💡 You can now try syncing the repaired file.")
    else:
        print("\n❌ DBF file repair failed.")
        print("💡 You may need to restore from backup or contact support.")

if __name__ == "__main__":
    main()
