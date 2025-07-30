# 🛠️ Data Lineage Pipeline - Troubleshooting Guide

## 🚨 Common Issues and Solutions

### 1. Spark Python Worker Connection Errors

**Error:** `py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.api.python.PythonRDD.collectAndServe`

**Symptoms:**
- Spark session initializes successfully
- Error occurs when loading or processing data
- Python worker fails to connect back to JVM

**Solutions:**

#### Quick Fixes:
1. **Restart the Application**
   ```bash
   # Stop the current app (Ctrl+C) and restart
   streamlit run ui/app.py
   ```

2. **Use Smaller Datasets**
   - Try with smaller sample sizes
   - Use the dataset preview with limited rows

3. **Switch to Pandas Fallback Mode**
   - The app automatically detects and offers pandas fallback
   - Look for "🐼 Pandas mode" indicators in the UI

#### Environment Fixes:

1. **Check Java Installation**
   ```bash
   # Verify Java is installed and accessible
   java -version
   
   # Should show Java 8, 11, or 17
   ```

2. **Set JAVA_HOME Environment Variable**
   ```bash
   # Windows
   set JAVA_HOME=C:\Program Files\Java\jdk-11.0.x
   
   # Linux/Mac
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
   ```

3. **Free Up Memory**
   - Close other memory-intensive applications
   - Restart your system if needed
   - Use smaller datasets for testing

#### Configuration Fixes:

1. **Enhanced Spark Configuration** (Already Applied)
   - Disabled Arrow execution
   - Reduced parallelism settings
   - Increased timeouts
   - Disabled Python worker reuse

2. **Alternative Spark Settings**
   ```python
   # If issues persist, try minimal Spark config
   spark = SparkSession.builder \
       .appName("DataLineage") \
       .config("spark.sql.adaptive.enabled", "false") \
       .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
       .config("spark.default.parallelism", "1") \
       .getOrCreate()
   ```

### 2. Module Import Errors

**Error:** `ModuleNotFoundError: No module named 'pyspark'`

**Solution:**
```bash
# Install all required packages
pip install -r requirements.txt

# Or install individually
pip install pyspark==3.5.0 streamlit==1.47.0
```

### 3. Airflow Setup Issues

**Error:** Airflow not starting or DAG not visible

**Solutions:**

1. **Initialize Airflow Database**
   ```bash
   airflow db init
   airflow db migrate
   ```

2. **Create Admin User**
   ```bash
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

3. **Check DAG Folder**
   ```bash
   # Ensure DAGs are in the correct folder
   cp dags/data_lineage_dag.py $AIRFLOW_HOME/dags/
   ```

### 4. Data Loading Issues

**Symptoms:**
- Empty datasets
- Schema errors
- Sample data generation fails

**Solutions:**

1. **Check Data Directory**
   ```bash
   # Verify data files exist
   ls -la data/
   ```

2. **Regenerate Sample Data**
   ```bash
   # Run data generation script
   python src/data_generation/sample_data_generator.py
   ```

3. **Use Pandas Fallback**
   - If Spark fails, the app automatically uses pandas
   - Look for "pandas mode" indicators

### 5. UI/Streamlit Issues

**Error:** Streamlit app not loading or crashing

**Solutions:**

1. **Clear Cache**
   ```bash
   # Clear Streamlit cache
   streamlit cache clear
   ```

2. **Check Port Availability**
   ```bash
   # Use different port if 8501 is busy
   streamlit run ui/app.py --server.port 8502
   ```

3. **Browser Issues**
   - Clear browser cache
   - Try incognito/private mode
   - Use different browser

## 🎯 Performance Optimization

### Memory Usage
1. **Monitor Memory**
   - Use Task Manager/Activity Monitor
   - Close unnecessary applications
   - Restart if memory usage is high

2. **Dataset Size**
   - Start with small datasets (< 1000 rows)
   - Gradually increase size
   - Use sampling for large datasets

### Spark Optimization
1. **Partition Management**
   ```python
   # Optimize partitions
   df = df.repartition(2)  # For small datasets
   ```

2. **Caching Strategy**
   ```python
   # Cache frequently used DataFrames
   df.cache()
   df.count()  # Trigger caching
   ```

## 🔍 Debugging Steps

### 1. Check Application Logs
```bash
# Terminal output shows errors and warnings
# Look for Python worker, Java, or memory errors
```

### 2. Test Components Individually

1. **Test Spark Session**
   ```python
   # In Jupyter notebook
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("test").getOrCreate()
   print(spark.version)
   ```

2. **Test Data Loading**
   ```python
   # Test with small pandas DataFrame
   import pandas as pd
   df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
   spark_df = spark.createDataFrame(df)
   spark_df.show()
   ```

3. **Test Transformations**
   ```python
   # Test simple transformations
   df.filter("a > 1").show()
   ```

### 3. Environment Verification
```bash
# Check Python version
python --version

# Check installed packages
pip list | grep -E "(pyspark|streamlit|pandas)"

# Check Java
java -version
echo $JAVA_HOME
```

## 🆘 When All Else Fails

### Alternative Approaches

1. **Use Jupyter Notebook**
   - Open `notebooks/data_lineage_exploration.ipynb`
   - More direct control and debugging capabilities

2. **Pandas-Only Mode**
   - Modify code to use only pandas
   - Less scalable but more reliable for small datasets

3. **Local Python Scripts**
   - Run transformations directly with Python scripts
   - Bypass Spark entirely for testing

### Get Help

1. **Check Error Messages**
   - Read full error traces
   - Look for specific error types mentioned above

2. **Enable Debug Mode**
   ```bash
   # Run with debug info
   streamlit run ui/app.py --logger.level=debug
   ```

3. **Community Resources**
   - PySpark documentation
   - Streamlit community forum
   - Stack Overflow

## 📋 System Requirements

### Minimum Requirements
- **RAM:** 8GB (16GB recommended)
- **Java:** Version 8, 11, or 17
- **Python:** 3.8+
- **Disk Space:** 2GB free

### Tested Environments
- ✅ Windows 10/11 with Java 11
- ✅ Ubuntu 20.04+ with Java 8/11
- ✅ macOS 10.15+ with Java 11

## 🔧 Configuration Files

### Spark Configuration
The app uses optimized Spark settings in `src/transformations/base.py`:
- Reduced parallelism for stability
- Disabled Arrow execution
- Increased timeouts
- Memory optimization

### Environment Variables
```bash
# Optional - set these if needed
export JAVA_HOME=/path/to/java
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$PYTHONPATH:/path/to/project
```

---

**Need more help?** Check the main README.md or create an issue with:
- Full error message
- Your system information
- Steps to reproduce the issue
