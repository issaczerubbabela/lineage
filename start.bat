@echo off
echo.
echo ========================================
echo  Data Lineage Pipeline Startup
echo ========================================
echo.

echo Checking Python installation...
python --version
if %errorlevel% neq 0 (
    echo ❌ Python not found! Please install Python 3.8+
    pause
    exit /b 1
)

echo.
echo Checking dependencies...
python -c "import pyspark, streamlit, pandas, plotly; print('✅ All core dependencies found!')"
if %errorlevel% neq 0 (
    echo ❌ Missing dependencies! Installing...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo ❌ Failed to install dependencies!
        pause
        exit /b 1
    )
)

echo.
echo ========================================
echo  Choose how to run the application:
echo ========================================
echo.
echo [1] Streamlit Web UI (Recommended)
echo [2] Jupyter Notebook
echo [3] Check installation only
echo.
set /p choice="Enter your choice (1-3): "

if "%choice%"=="1" (
    echo.
    echo 🚀 Starting Streamlit Web UI...
    echo Open your browser to: http://localhost:8501
    echo Press Ctrl+C to stop
    echo.
    streamlit run ui/app.py
) else if "%choice%"=="2" (
    echo.
    echo 📓 Starting Jupyter Notebook...
    echo.
    jupyter notebook notebooks/data_lineage_exploration.ipynb
) else if "%choice%"=="3" (
    echo.
    echo ✅ Installation check complete!
    echo.
    echo Available commands:
    echo   - streamlit run ui/app.py           (Web UI)
    echo   - jupyter notebook notebooks/       (Notebook)
    echo   - python -c "import ui.app"         (Test import)
    echo.
) else (
    echo Invalid choice. Please run the script again.
)

echo.
pause
