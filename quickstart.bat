@echo off
REM Quick Start Script for Data Lineage Pipeline (Windows)
REM This script provides easy commands for common tasks

echo 🚀 Data Lineage Pipeline - Quick Start
echo =====================================

REM Check prerequisites
echo 🔍 Checking prerequisites...

python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python 3.8+
    pause
    exit /b 1
)

java -version >nul 2>&1
if errorlevel 1 (
    echo ⚠️  Java not found. Install Java 8, 11, or 17 for full functionality
    echo    App will try to run in pandas-only mode
) else (
    echo ✅ Java found
)

echo ✅ Prerequisites check completed
echo.

REM Show menu
echo Choose an option:
echo 1. 🏗️  Install dependencies
echo 2. 🎲 Generate sample data
echo 3. 🚀 Start Streamlit app
echo 4. 📊 Start Jupyter notebook
echo 5. 🌪️  Start Airflow (requires Airflow installation)
echo 6. 🧪 Run tests
echo 7. 🧹 Clean cache
echo 8. 📋 Show system info
echo 9. 🔧 Troubleshooting mode

set /p choice=Enter your choice (1-9): 

if "%choice%"=="1" (
    echo 📦 Installing dependencies...
    pip install -r requirements.txt
    echo ✅ Dependencies installed
    goto end
)

if "%choice%"=="2" (
    echo 🎲 Generating sample data...
    python src/data_generation/sample_data_generator.py
    echo ✅ Sample data generated
    goto end
)

if "%choice%"=="3" (
    echo 🚀 Starting Streamlit app...
    echo    App will open at http://localhost:8501
    echo    Press Ctrl+C to stop
    streamlit run ui/app.py
    goto end
)

if "%choice%"=="4" (
    echo 📊 Starting Jupyter notebook...
    echo    Notebook will open in your browser
    echo    Navigate to notebooks/data_lineage_exploration.ipynb
    jupyter notebook
    goto end
)

if "%choice%"=="5" (
    echo 🌪️  Starting Airflow...
    airflow version >nul 2>&1
    if errorlevel 1 (
        echo ❌ Airflow not found. Install with: pip install apache-airflow
        goto end
    )
    
    REM Initialize Airflow if needed
    if not exist "%AIRFLOW_HOME%\dags" (
        echo 🏗️  Initializing Airflow...
        airflow db init
    )
    
    REM Copy DAG
    copy dags\data_lineage_dag.py %AIRFLOW_HOME%\dags\
    
    echo Starting Airflow webserver and scheduler...
    echo Access at http://localhost:8080
    start "Airflow Webserver" airflow webserver --port 8080
    airflow scheduler
    goto end
)

if "%choice%"=="6" (
    echo 🧪 Running tests...
    if exist "tests" (
        python -m pytest tests/ -v
    ) else (
        echo ⚠️  No tests directory found
        echo Running basic import tests...
        python -c "import sys; exec(open('test_imports.py').read())"
    )
    goto end
)

if "%choice%"=="7" (
    echo 🧹 Cleaning cache...
    
    REM Clear Python cache
    for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
    del /s /q *.pyc >nul 2>&1
    
    REM Clear Streamlit cache
    streamlit cache clear >nul 2>&1
    
    REM Clear Jupyter cache
    if exist ".ipynb_checkpoints" rd /s /q ".ipynb_checkpoints"
    
    echo ✅ Cache cleaned
    goto end
)

if "%choice%"=="8" (
    echo 📋 System Information:
    echo =====================
    python --version
    echo Java version:
    java -version 2>&1 | findstr "version"
    echo OS: %OS%
    echo.
    echo Installed packages:
    pip list | findstr /i "pyspark streamlit pandas airflow"
    goto end
)

if "%choice%"=="9" (
    echo 🔧 Troubleshooting Mode
    echo ======================
    echo.
    echo 🔍 Testing Spark connection...
    python -c "exec(open('troubleshoot_spark.py').read())"
    echo.
    echo 🔍 Testing data loading...
    python -c "exec(open('troubleshoot_data.py').read())"
    echo.
    echo 🔍 For more detailed troubleshooting, see TROUBLESHOOTING.md
    goto end
)

echo ❌ Invalid choice. Please run the script again.

:end
echo.
echo 🎉 Task completed!
echo 💡 For more help, check README.md and TROUBLESHOOTING.md
pause
