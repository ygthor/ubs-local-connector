#!/usr/bin/env python3
"""
Check record sizes in DBF files to diagnose packet size issues
"""

import dbf
import os

def check_record_size(dbf_path):
    try:
        table = dbf.Table(dbf_path)
        table.open()
        
        print(f"📊 Analyzing: {os.path.basename(dbf_path)}")
        print(f"Total records: {len(table)}")
        print(f"Total fields: {len(table.field_names)}")
        
        # Check first few records for size
        total_size = 0
        max_record_size = 0
        
        for i, record in enumerate(table):
            if i >= 5:  # Check first 5 records
                break
                
            record_size = 0
            record_data = {}
            
            for field_name in table.field_names:
                try:
                    value = getattr(record, field_name)
                    if isinstance(value, str):
                        record_size += len(value.encode('utf-8'))
                        record_data[field_name] = len(value)
                    elif isinstance(value, bytes):
                        record_size += len(value)
                        record_data[field_name] = len(value)
                    else:
                        record_data[field_name] = str(value)[:50]  # Truncate for display
                except:
                    record_data[field_name] = "ERROR"
            
            max_record_size = max(max_record_size, record_size)
            total_size += record_size
            
            print(f"\nRecord {i+1} size: {record_size:,} bytes")
            # Show largest fields
            large_fields = [(k, v) for k, v in record_data.items() if isinstance(v, int) and v > 100]
            if large_fields:
                print("Large fields:")
                for field, size in sorted(large_fields, key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {field}: {size:,} bytes")
        
        avg_size = total_size / min(5, len(table))
        estimated_total = avg_size * len(table)
        
        print(f"\n📈 Size Analysis:")
        print(f"Average record size: {avg_size:,.0f} bytes")
        print(f"Max record size: {max_record_size:,} bytes")
        print(f"Estimated total size: {estimated_total:,.0f} bytes ({estimated_total/1024/1024:.1f} MB)")
        
        # Check if any single record exceeds packet size
        if max_record_size > 20 * 1024 * 1024:  # 20MB
            print(f"⚠️  WARNING: Single record exceeds 20MB packet size!")
        elif max_record_size > 16 * 1024 * 1024:  # 16MB
            print(f"⚠️  WARNING: Single record close to packet size limit")
        else:
            print(f"✅ Record sizes are within packet limits")
            
        table.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Check the problematic file
    check_record_size("C:/UBSSTK2015/Sample/TESTMODE/ictran.dbf")
