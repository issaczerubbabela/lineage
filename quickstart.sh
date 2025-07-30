#!/bin/bash

# Quick Start Script for Data Lineage Pipeline
# This script provides easy commands for common tasks

echo "🚀 Data Lineage Pipeline - Quick Start"
echo "====================================="

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
echo "🔍 Checking prerequisites..."

if ! command_exists python; then
    echo "❌ Python not found. Please install Python 3.8+"
    exit 1
fi

if ! command_exists java; then
    echo "⚠️  Java not found. Install Java 8, 11, or 17 for full functionality"
    echo "   App will try to run in pandas-only mode"
fi

echo "✅ Prerequisites check completed"
echo ""

# Show menu
echo "Choose an option:"
echo "1. 🏗️  Install dependencies"
echo "2. 🎲 Generate sample data"
echo "3. 🚀 Start Streamlit app"
echo "4. 📊 Start Jupyter notebook"
echo "5. 🌪️  Start Airflow (requires Airflow installation)"
echo "6. 🧪 Run tests"
echo "7. 🧹 Clean cache"
echo "8. 📋 Show system info"
echo "9. 🔧 Troubleshooting mode"

read -p "Enter your choice (1-9): " choice

case $choice in
    1)
        echo "📦 Installing dependencies..."
        pip install -r requirements.txt
        echo "✅ Dependencies installed"
        ;;
    2)
        echo "🎲 Generating sample data..."
        python src/data_generation/sample_data_generator.py
        echo "✅ Sample data generated"
        ;;
    3)
        echo "🚀 Starting Streamlit app..."
        echo "   App will open at http://localhost:8501"
        echo "   Press Ctrl+C to stop"
        streamlit run ui/app.py
        ;;
    4)
        echo "📊 Starting Jupyter notebook..."
        echo "   Notebook will open in your browser"
        echo "   Navigate to notebooks/data_lineage_exploration.ipynb"
        jupyter notebook
        ;;
    5)
        echo "🌪️  Starting Airflow..."
        if ! command_exists airflow; then
            echo "❌ Airflow not found. Install with: pip install apache-airflow"
            exit 1
        fi
        
        # Initialize Airflow if needed
        if [ ! -d "$AIRFLOW_HOME/dags" ]; then
            echo "🏗️  Initializing Airflow..."
            airflow db init
        fi
        
        # Copy DAG
        cp dags/data_lineage_dag.py $AIRFLOW_HOME/dags/
        
        echo "Starting Airflow webserver and scheduler..."
        echo "Access at http://localhost:8080"
        airflow webserver --port 8080 &
        airflow scheduler
        ;;
    6)
        echo "🧪 Running tests..."
        if [ -d "tests" ]; then
            python -m pytest tests/ -v
        else
            echo "⚠️  No tests directory found"
            echo "Running basic import tests..."
            python -c "
import sys
try:
    from src.transformations.base import SparkTransformationEngine
    print('✅ Core modules import successfully')
except Exception as e:
    print(f'❌ Import error: {e}')
    
try:
    import pyspark
    print('✅ PySpark available')
except:
    print('⚠️  PySpark not available')
    
try:
    import streamlit
    print('✅ Streamlit available')
except:
    print('❌ Streamlit not available')
"
        fi
        ;;
    7)
        echo "🧹 Cleaning cache..."
        
        # Clear Python cache
        find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
        find . -name "*.pyc" -delete 2>/dev/null || true
        
        # Clear Streamlit cache
        if command_exists streamlit; then
            streamlit cache clear
        fi
        
        # Clear Jupyter cache
        if [ -d ".ipynb_checkpoints" ]; then
            rm -rf .ipynb_checkpoints
        fi
        
        echo "✅ Cache cleaned"
        ;;
    8)
        echo "📋 System Information:"
        echo "====================="
        echo "Python version: $(python --version)"
        echo "Java version: $(java -version 2>&1 | head -1 || echo 'Not installed')"
        echo "OS: $(uname -s 2>/dev/null || echo 'Windows')"
        echo "Available RAM: $(free -h 2>/dev/null | grep Mem | awk '{print $2}' || echo 'Unknown')"
        echo ""
        echo "Installed packages:"
        pip list | grep -E "(pyspark|streamlit|pandas|airflow)" || echo "No relevant packages found"
        ;;
    9)
        echo "🔧 Troubleshooting Mode"
        echo "======================"
        echo ""
        echo "🔍 Testing Spark connection..."
        python -c "
try:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('test').getOrCreate()
    print('✅ Spark session created successfully')
    print(f'Spark version: {spark.version}')
    
    # Test simple operation
    import pandas as pd
    test_df = spark.createDataFrame(pd.DataFrame({'test': [1, 2, 3]}))
    count = test_df.count()
    print(f'✅ Test operation successful - count: {count}')
    spark.stop()
    
except Exception as e:
    print(f'❌ Spark test failed: {e}')
    print('💡 Try pandas-only mode or check Java installation')
"
        echo ""
        echo "🔍 Testing data loading..."
        python -c "
try:
    import os
    if os.path.exists('data/'):
        files = os.listdir('data/')
        print(f'✅ Data directory exists with {len(files)} files')
        for f in files[:5]:  # Show first 5 files
            print(f'   - {f}')
    else:
        print('⚠️  Data directory not found')
        print('💡 Run option 2 to generate sample data')
except Exception as e:
    print(f'❌ Data check failed: {e}')
"
        echo ""
        echo "🔍 For more detailed troubleshooting, see TROUBLESHOOTING.md"
        ;;
    *)
        echo "❌ Invalid choice. Please run the script again."
        exit 1
        ;;
esac

echo ""
echo "🎉 Task completed!"
echo "💡 For more help, check README.md and TROUBLESHOOTING.md"
