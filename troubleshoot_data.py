#!/usr/bin/env python3
"""
Test script for checking data loading functionality
"""

import os
import sys

def test_data_loading():
    print("🔍 Testing data loading...")
    
    # Check if data directory exists
    if not os.path.exists('data/'):
        print("❌ Data directory not found")
        print("💡 Run: python src/data_generation/sample_data_generator.py")
        return False
    
    print("✅ Data directory exists")
    
    # List data files
    data_files = os.listdir('data/')
    if not data_files:
        print("❌ No data files found")
        print("💡 Generate sample data first")
        return False
    
    print(f"✅ Found {len(data_files)} data files:")
    for file in data_files[:10]:  # Show first 10 files
        file_path = os.path.join('data', file)
        size = os.path.getsize(file_path)
        print(f"   - {file} ({size:,} bytes)")
    
    if len(data_files) > 10:
        print(f"   ... and {len(data_files) - 10} more files")
    
    # Test loading with pandas
    print("\n🔍 Testing pandas loading...")
    try:
        import pandas as pd
        
        # Find a CSV file to test
        csv_files = [f for f in data_files if f.endswith('.csv')]
        if csv_files:
            test_file = os.path.join('data', csv_files[0])
            df = pd.read_csv(test_file)
            print(f"✅ Pandas loading successful: {len(df)} rows, {len(df.columns)} columns")
            print(f"   Columns: {list(df.columns)}")
            print(f"   Sample data:\n{df.head(3)}")
        else:
            print("⚠️  No CSV files found for testing")
            
    except Exception as e:
        print(f"❌ Pandas loading failed: {e}")
        return False
    
    # Test with sample data generator
    print("\n🔍 Testing sample data generator...")
    try:
        sys.path.append('src')
        from data_generation.sample_data_generator import SampleDataGenerator
        
        generator = SampleDataGenerator()
        print("✅ Sample data generator imported successfully")
        
        # Test generating a small dataset
        test_customers = generator.generate_customers(5)
        print(f"✅ Generated {len(test_customers)} test customer records")
        
    except ImportError as e:
        print(f"❌ Cannot import sample data generator: {e}")
        return False
    except Exception as e:
        print(f"❌ Sample data generation failed: {e}")
        return False
    
    return True

def test_spark_data_loading():
    print("\n🔍 Testing Spark data loading...")
    
    try:
        from pyspark.sql import SparkSession
        
        # Create minimal Spark session
        spark = SparkSession.builder \
            .appName("DataLoadTest") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.default.parallelism", "1") \
            .getOrCreate()
        
        print("✅ Spark session created for data loading test")
        
        # Test loading data files
        data_files = os.listdir('data/')
        csv_files = [f for f in data_files if f.endswith('.csv')]
        
        if csv_files:
            test_file = os.path.join('data', csv_files[0])
            
            try:
                # Test Spark CSV loading
                spark_df = spark.read.csv(test_file, header=True, inferSchema=True)
                count = spark_df.count()
                print(f"✅ Spark CSV loading successful: {count} rows")
                
                # Test basic operations
                spark_df.show(3)
                print("✅ Spark show() operation successful")
                
            except Exception as load_error:
                print(f"❌ Spark data loading failed: {load_error}")
                return False
        
        spark.stop()
        print("✅ Spark data loading test completed")
        return True
        
    except Exception as e:
        print(f"❌ Spark data loading test failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing data loading functionality...")
    print("=" * 40)
    
    # Test basic data loading
    basic_success = test_data_loading()
    
    if basic_success:
        # Test Spark data loading if basic test passes
        spark_success = test_spark_data_loading()
        
        if basic_success and spark_success:
            print("\n🎉 All data loading tests passed!")
        elif basic_success:
            print("\n⚠️  Basic data loading works, but Spark has issues")
            print("💡 You can still use pandas-only mode")
    else:
        print("\n❌ Basic data loading failed")
        print("💡 Generate sample data first")
