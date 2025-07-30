#!/usr/bin/env python3
"""
Test script for checking Spark functionality
"""

def test_spark():
    try:
        from pyspark.sql import SparkSession
        print("✅ PySpark import successful")
        
        # Create Spark session with minimal config
        spark = SparkSession.builder \
            .appName("TroubleshootTest") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.default.parallelism", "1") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        print(f"✅ Spark session created successfully")
        print(f"   Spark version: {spark.version}")
        print(f"   Python version: {spark.sparkContext.pythonVer}")
        
        # Test simple operation
        import pandas as pd
        test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': ['a', 'b', 'c', 'd', 'e'],
            'number': [10, 20, 30, 40, 50]
        })
        
        print("✅ Test data created")
        
        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(test_data)
        print("✅ Spark DataFrame created")
        
        # Test basic operations
        count = spark_df.count()
        print(f"✅ Count operation successful: {count} rows")
        
        # Test filter operation
        filtered = spark_df.filter(spark_df.number > 20)
        filtered_count = filtered.count()
        print(f"✅ Filter operation successful: {filtered_count} rows after filter")
        
        # Test collect operation (this often fails with Python worker issues)
        try:
            collected = spark_df.limit(2).collect()
            print(f"✅ Collect operation successful: retrieved {len(collected)} rows")
        except Exception as collect_error:
            print(f"❌ Collect operation failed: {collect_error}")
            print("   This is often where Python worker errors occur")
            return False
        
        # Test conversion back to pandas
        try:
            pandas_result = spark_df.limit(3).toPandas()
            print(f"✅ toPandas operation successful: {len(pandas_result)} rows")
        except Exception as pandas_error:
            print(f"❌ toPandas operation failed: {pandas_error}")
            return False
        
        spark.stop()
        print("✅ Spark session stopped cleanly")
        print("\n🎉 All Spark tests passed!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Install PySpark: pip install pyspark==3.5.0")
        return False
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        
        if "Python worker" in str(e):
            print("\n🔧 Python Worker Error Detected!")
            print("Common solutions:")
            print("1. Check Java installation: java -version")
            print("2. Set JAVA_HOME environment variable")
            print("3. Restart the terminal/IDE")
            print("4. Try with smaller datasets")
            print("5. Use pandas-only mode")
            
        elif "java.lang.OutOfMemoryError" in str(e):
            print("\n🔧 Out of Memory Error!")
            print("Solutions:")
            print("1. Close other applications")
            print("2. Restart your computer")
            print("3. Use smaller datasets")
            print("4. Increase system memory")
            
        return False

if __name__ == "__main__":
    print("🔍 Testing Spark functionality...")
    print("=" * 40)
    
    success = test_spark()
    
    if not success:
        print("\n💡 Suggestions:")
        print("- Try running the app in pandas-only mode")
        print("- Check TROUBLESHOOTING.md for detailed solutions")
        print("- Use the Jupyter notebook for more control")
