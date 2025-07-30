#!/bin/bash

echo ""
echo "========================================"
echo "  Data Lineage Pipeline Startup"
echo "========================================"
echo ""

echo "Checking Python installation..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python not found! Please install Python 3.8+"
    exit 1
fi

python3 --version

echo ""
echo "Checking dependencies..."
python3 -c "import pyspark, streamlit, pandas, plotly; print('✅ All core dependencies found!')" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ Missing dependencies! Installing..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "❌ Failed to install dependencies!"
        exit 1
    fi
fi

echo ""
echo "========================================"
echo "  Choose how to run the application:"
echo "========================================"
echo ""
echo "[1] Streamlit Web UI (Recommended)"
echo "[2] Jupyter Notebook"
echo "[3] Check installation only"
echo ""
read -p "Enter your choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo "🚀 Starting Streamlit Web UI..."
        echo "Open your browser to: http://localhost:8501"
        echo "Press Ctrl+C to stop"
        echo ""
        streamlit run ui/app.py
        ;;
    2)
        echo ""
        echo "📓 Starting Jupyter Notebook..."
        echo ""
        jupyter notebook notebooks/data_lineage_exploration.ipynb
        ;;
    3)
        echo ""
        echo "✅ Installation check complete!"
        echo ""
        echo "Available commands:"
        echo "  - streamlit run ui/app.py           (Web UI)"
        echo "  - jupyter notebook notebooks/       (Notebook)"
        echo "  - python3 -c \"import ui.app\"         (Test import)"
        echo ""
        ;;
    *)
        echo "Invalid choice. Please run the script again."
        ;;
esac

echo ""
