#!/usr/bin/env python3
"""
Performance test script to demonstrate sync speed improvements
"""

import os
import time
import sys
from sync_database import sync_to_database

def create_test_data(num_records=2000):
    """Create test data structure similar to DBF data"""
    structures = [
        {"name": "id", "type": "N", "size": 10, "decs": 0},
        {"name": "name", "type": "C", "size": 50, "decs": 0},
        {"name": "amount", "type": "N", "size": 10, "decs": 2},
        {"name": "date", "type": "D", "size": 8, "decs": 0},
        {"name": "description", "type": "C", "size": 255, "decs": 0}
    ]
    
    rows = []
    for i in range(num_records):
        row = {
            "id": i + 1,
            "name": f"Test Record {i + 1}",
            "amount": round(100.50 + i * 0.1, 2),
            "date": "20240101",
            "description": f"This is a test description for record {i + 1} with some additional text to make it longer."
        }
        rows.append(row)
    
    return {
        "structure": structures,
        "rows": rows
    }

def test_performance():
    """Test the performance of the optimized sync function"""
    print("🚀 Testing optimized sync performance...")
    print("=" * 50)
    
    # Test with different record counts
    test_sizes = [100, 500, 1000, 2000, 5000]
    
    for size in test_sizes:
        print(f"\n📊 Testing with {size} records:")
        
        # Create test data
        data = create_test_data(size)
        
        # Test sync performance
        start_time = time.time()
        try:
            sync_to_database("test_performance.dbf", data, "test")
            total_time = time.time() - start_time
            
            records_per_second = size / total_time if total_time > 0 else 0
            
            print(f"✅ Completed: {total_time:.2f}s")
            print(f"⚡ Speed: {records_per_second:.0f} records/sec")
            
            # Performance rating
            if records_per_second > 1000:
                rating = "🔥 EXCELLENT"
            elif records_per_second > 500:
                rating = "🚀 VERY GOOD"
            elif records_per_second > 200:
                rating = "✅ GOOD"
            elif records_per_second > 100:
                rating = "⚠️  ACCEPTABLE"
            else:
                rating = "🐌 SLOW"
            
            print(f"📈 Rating: {rating}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 Performance Test Complete!")
    print("\n💡 Expected improvements:")
    print("   • 10-50x faster than individual INSERTs")
    print("   • Batch operations reduce database round-trips")
    print("   • Single transaction reduces commit overhead")
    print("   • Prepared statements improve SQL parsing")

if __name__ == "__main__":
    # Set environment variables for testing
    os.environ["DB_TYPE"] = "mysql"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_USER"] = "root"
    os.environ["DB_PASSWORD"] = ""
    os.environ["DB_NAME"] = "ubs_data"
    
    test_performance()
