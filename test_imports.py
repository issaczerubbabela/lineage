#!/usr/bin/env python3
"""
Test script for checking basic imports
"""

def test_imports():
    print("🔍 Testing basic imports...")
    print("=" * 30)
    
    # Test core Python packages
    try:
        import sys
        print(f"✅ Python version: {sys.version}")
    except:
        print("❌ Python import failed")
        return False
    
    # Test pandas
    try:
        import pandas as pd
        print(f"✅ Pandas version: {pd.__version__}")
    except ImportError:
        print("❌ Pandas not available")
        print("💡 Install with: pip install pandas")
        return False
    
    # Test PySpark
    try:
        import pyspark
        print(f"✅ PySpark version: {pyspark.__version__}")
    except ImportError:
        print("❌ PySpark not available")
        print("💡 Install with: pip install pyspark==3.5.0")
        print("   You can still use pandas-only mode")
    
    # Test Streamlit
    try:
        import streamlit as st
        print(f"✅ Streamlit available")
    except ImportError:
        print("❌ Streamlit not available")
        print("💡 Install with: pip install streamlit")
        return False
    
    # Test plotting libraries
    try:
        import plotly
        print(f"✅ Plotly available")
    except ImportError:
        print("⚠️  Plotly not available - visualizations may not work")
        print("💡 Install with: pip install plotly")
    
    try:
        import networkx as nx
        print(f"✅ NetworkX available")
    except ImportError:
        print("⚠️  NetworkX not available - lineage graphs may not work")
        print("💡 Install with: pip install networkx")
    
    # Test project modules
    print("\n🔍 Testing project modules...")
    try:
        import sys
        import os
        sys.path.append(os.path.join(os.getcwd(), 'src'))
        
        from transformations.base import SparkTransformationEngine
        print("✅ Core transformation engine imported")
    except ImportError as e:
        print(f"❌ Cannot import transformation engine: {e}")
        print("💡 Make sure you're in the project root directory")
        return False
    
    try:
        from lineage.tracker import DataLineageTracker
        print("✅ Lineage tracker imported")
    except ImportError as e:
        print(f"❌ Cannot import lineage tracker: {e}")
    
    print("\n🎉 Core imports successful!")
    return True

if __name__ == "__main__":
    test_imports()
