"""
Complete Data Lineage Pipeline UI with Full Functionality
Interactive Streamlit application for data transformations and lineage tracking
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
from typing import Dict, List, Any, Optional
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import networkx as nx
from datetime import datetime
import time

# Add project root to path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(project_root)

# Import our modules
try:
    from src.transformations import (
        SparkTransformationEngine, 
        AVAILABLE_TRANSFORMATIONS,
        DataLineageTracker
    )
    from src.lineage import create_lineage_visualization
except ImportError as e:
    st.error(f"Failed to import modules: {e}")
    st.info("Make sure you're running from the project root directory and all dependencies are installed.")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="Data Lineage Pipeline",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1e88e5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem;
    }
    .transformation-card {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 1rem;
        margin: 0.5rem 0;
        background: #f9f9f9;
    }
    .transformation-card:hover {
        border-color: #1e88e5;
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    }
    .success-msg {
        background: #4caf50;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .error-msg {
        background: #f44336;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
def initialize_session_state():
    """Initialize all session state variables"""
    if 'spark_engine' not in st.session_state:
        st.session_state.spark_engine = None
    if 'loaded_datasets' not in st.session_state:
        st.session_state.loaded_datasets = {}
    if 'transformation_history' not in st.session_state:
        st.session_state.transformation_history = []
    if 'lineage_data' not in st.session_state:
        st.session_state.lineage_data = None
    if 'pipeline_executor' not in st.session_state:
        st.session_state.pipeline_executor = None
    if 'execution_results' not in st.session_state:
        st.session_state.execution_results = []
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 1

initialize_session_state()

def initialize_spark():
    """Initialize Spark engine with robust error handling"""
    if st.session_state.spark_engine is None:
        try:
            with st.spinner("Initializing Spark session..."):
                st.session_state.spark_engine = SparkTransformationEngine("DataLineageUI")
                
                # Test the Spark session with a simple operation
                test_df = st.session_state.spark_engine.spark.range(1, 3).toDF("test")
                test_count = test_df.count()
                if test_count == 2:
                    st.success("✅ Spark session initialized and tested successfully!")
                    return True
                else:
                    st.error("❌ Spark session test failed")
                    return False
                    
        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ Failed to initialize Spark: {error_msg}")
            
            # Provide specific troubleshooting for common issues
            if "Python worker failed to connect back" in error_msg:
                st.error("""
                **Python Worker Connection Error Detected**
                
                This is a common issue with Spark and Python communication. Try these solutions:
                
                1. **Restart the application** - Close and reopen Streamlit
                2. **Clear Python cache** - Delete __pycache__ folders
                3. **Check Java installation** - Ensure Java 8 or 11 is installed
                4. **Reduce memory usage** - Close other applications
                5. **Use simpler mode** - Try the fallback mode below
                """)
                
                # Offer a fallback option
                if st.button("🔧 Try Fallback Spark Configuration"):
                    try:
                        # Create a simpler Spark session
                        from pyspark.sql import SparkSession
                        spark_simple = SparkSession.builder \
                            .appName("DataLineage_Fallback") \
                            .config("spark.sql.adaptive.enabled", "false") \
                            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                            .config("spark.driver.memory", "1g") \
                            .config("spark.executor.memory", "1g") \
                            .config("spark.default.parallelism", "1") \
                            .config("spark.sql.shuffle.partitions", "1") \
                            .getOrCreate()
                        
                        # Test the fallback session
                        test_df = spark_simple.range(1, 2).toDF("test")
                        test_count = test_df.count()
                        
                        if test_count == 1:
                            st.success("✅ Fallback Spark configuration works!")
                            st.info("You can proceed with basic operations, but performance may be limited.")
                            # Store the simple session
                            st.session_state.spark_fallback = spark_simple
                            return True
                        else:
                            st.error("❌ Even fallback configuration failed")
                            
                    except Exception as e2:
                        st.error(f"❌ Fallback configuration also failed: {e2}")
            
            elif "Java" in error_msg or "JAVA_HOME" in error_msg:
                st.error("""
                **Java Installation Issue**
                
                Please ensure:
                1. Java 8 or Java 11 is installed
                2. JAVA_HOME environment variable is set
                3. Java is in your system PATH
                
                Download Java from: https://adoptopenjdk.net/
                """)
            
            elif "memory" in error_msg.lower():
                st.error("""
                **Memory Issue**
                
                Try:
                1. Close other applications to free memory
                2. Restart your computer
                3. Use smaller datasets for testing
                """)
            
            return False
    return True

def generate_fallback_data():
    """Generate minimal sample data as fallback"""
    try:
        import pandas as pd
        import numpy as np
        from faker import Faker
        import random
        
        fake = Faker()
        fake.seed(42)
        np.random.seed(42)
        random.seed(42)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Create data directory
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Generate customers
        status_text.text("Generating customers data...")
        progress_bar.progress(20)
        customers_data = []
        for i in range(1000):
            customer = {
                'customer_id': f"CUST_{i+1:06d}",
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'city': fake.city(),
                'state': fake.state(),
                'age': random.randint(18, 80),
                'registration_date': fake.date_between(start_date='-2y', end_date='today'),
                'is_active': np.random.choice([True, False], p=[0.8, 0.2])
            }
            customers_data.append(customer)
        
        customers_df = pd.DataFrame(customers_data)
        customers_df.to_csv(os.path.join(data_dir, 'customers.csv'), index=False)
        
        # Generate products
        status_text.text("Generating products data...")
        progress_bar.progress(40)
        products_data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Beauty']
        
        for i in range(200):
            product = {
                'product_id': f"PROD_{i+1:06d}",
                'product_name': fake.catch_phrase(),
                'category': random.choice(categories),
                'price': round(random.uniform(10, 500), 2),
                'cost': round(random.uniform(5, 300), 2),
                'stock_quantity': random.randint(0, 100),
                'weight': round(random.uniform(0.1, 10), 2),
                'supplier_id': f"SUPP_{random.randint(1, 50):03d}",
                'rating': round(random.uniform(1, 5), 1),
                'is_discontinued': np.random.choice([True, False], p=[0.1, 0.9])
            }
            products_data.append(product)
        
        products_df = pd.DataFrame(products_data)
        products_df.to_csv(os.path.join(data_dir, 'products.csv'), index=False)
        
        # Generate orders
        status_text.text("Generating orders data...")
        progress_bar.progress(60)
        orders_data = []
        for i in range(1000):
            order = {
                'order_id': f"ORDER_{i+1:08d}",
                'customer_id': f"CUST_{random.randint(1, 1000):06d}",
                'order_date': fake.date_between(start_date='-1y', end_date='today'),
                'total_amount': round(random.uniform(20, 1000), 2),
                'status': random.choice(['Completed', 'Pending', 'Cancelled', 'Processing']),
                'shipping_address': fake.address().replace('\n', ', '),
                'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer'])
            }
            orders_data.append(order)
        
        orders_df = pd.DataFrame(orders_data)
        orders_df.to_csv(os.path.join(data_dir, 'orders.csv'), index=False)
        
        # Generate order_items
        status_text.text("Generating order items data...")
        progress_bar.progress(80)
        order_items_data = []
        for i, order in enumerate(orders_data):
            num_items = random.randint(1, 5)
            for j in range(num_items):
                item = {
                    'order_item_id': f"ITEM_{i+1:08d}_{j+1}",
                    'order_id': order['order_id'],
                    'product_id': f"PROD_{random.randint(1, 200):06d}",
                    'quantity': random.randint(1, 5),
                    'unit_price': round(random.uniform(10, 200), 2),
                    'discount': round(random.uniform(0, 0.2), 2)
                }
                order_items_data.append(item)
        
        order_items_df = pd.DataFrame(order_items_data)
        order_items_df.to_csv(os.path.join(data_dir, 'order_items.csv'), index=False)
        
        # Generate inventory
        status_text.text("Generating inventory data...")
        progress_bar.progress(90)
        inventory_data = []
        for i in range(200):
            inventory = {
                'product_id': f"PROD_{i+1:06d}",
                'warehouse_location': random.choice(['Warehouse_A', 'Warehouse_B', 'Warehouse_C', 'Warehouse_D']),
                'quantity_available': random.randint(0, 500),
                'reserved_quantity': random.randint(0, 50),
                'last_updated': fake.date_between(start_date='-30d', end_date='today'),
                'reorder_level': random.randint(10, 100)
            }
            inventory_data.append(inventory)
        
        inventory_df = pd.DataFrame(inventory_data)
        inventory_df.to_csv(os.path.join(data_dir, 'inventory.csv'), index=False)
        
        progress_bar.progress(100)
        status_text.text("Data generation completed!")
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
        
        return True
        
    except Exception as e:
        st.error(f"Failed to generate fallback data: {e}")
        return False

def load_sample_data():
    """Load sample datasets with enhanced error handling"""
    if not initialize_spark():
        return False
    
    data_dir = os.path.join(project_root, 'data')
    
    try:
        datasets = {}
        dataset_files = {
            'customers': 'customers.csv',
            'products': 'products.csv', 
            'orders': 'orders.csv',
            'order_items': 'order_items.csv',
            'inventory': 'inventory.csv'
        }
        
        progress_bar = st.progress(0)
        total_files = len(dataset_files)
        
        for i, (dataset_name, filename) in enumerate(dataset_files.items()):
            file_path = os.path.join(data_dir, filename)
            
            if os.path.exists(file_path):
                try:
                    st.info(f"Loading {dataset_name}...")
                    
                    # Use the engine's improved load_data method
                    df = st.session_state.spark_engine.load_data(file_path, dataset_name, 'csv')
                    
                    # Safely get row count with error handling
                    try:
                        row_count = df.count()
                        col_count = len(df.columns)
                        
                        datasets[dataset_name] = df
                        st.success(f"✅ Loaded {dataset_name}: {row_count:,} rows, {col_count} columns")
                        
                    except Exception as count_error:
                        # If count fails due to Python worker issues, still store the DataFrame
                        # but don't try to count rows
                        col_count = len(df.columns)
                        datasets[dataset_name] = df
                        st.warning(f"⚠️ Loaded {dataset_name}: {col_count} columns (row count unavailable due to Spark issue)")
                        st.info("💡 Data is loaded but some operations may be limited due to Spark Python worker issues")
                    
                except Exception as load_error:
                    error_msg = str(load_error)
                    
                    if "Python worker failed to connect back" in error_msg:
                        st.error(f"❌ Python worker error loading {dataset_name}")
                        st.info("""
                        **Workaround Options:**
                        1. Try restarting the Streamlit app
                        2. Use the Jupyter notebook instead (`notebooks/data_lineage_exploration.ipynb`)
                        3. Reduce dataset size
                        4. Use pandas-only mode (limited functionality)
                        """)
                        
                        # Offer pandas fallback
                        if st.button(f"📊 Load {dataset_name} with Pandas (Limited Features)", key=f"pandas_{dataset_name}"):
                            try:
                                import pandas as pd
                                pandas_df = pd.read_csv(file_path)
                                st.success(f"✅ Loaded {dataset_name} with Pandas: {len(pandas_df):,} rows")
                                st.warning("⚠️ Using Pandas mode - transformations will be limited")
                                # Store as pandas DataFrame with special marker
                                datasets[f"{dataset_name}_pandas"] = pandas_df
                            except Exception as pandas_error:
                                st.error(f"❌ Pandas fallback also failed: {pandas_error}")
                    else:
                        st.error(f"❌ Failed to load {dataset_name}: {load_error}")
            else:
                st.warning(f"⚠️ {filename} not found")
            
            progress_bar.progress((i + 1) / total_files)
        
        progress_bar.empty()
        
        if datasets:
            st.session_state.loaded_datasets = datasets
            st.success(f"✅ Successfully loaded {len(datasets)} datasets")
            
            # Check if we have any Spark DataFrames vs Pandas DataFrames
            spark_datasets = [k for k in datasets.keys() if not k.endswith('_pandas')]
            pandas_datasets = [k for k in datasets.keys() if k.endswith('_pandas')]
            
            if pandas_datasets:
                st.info(f"📊 Using Pandas mode for: {', '.join(pandas_datasets)}")
                st.warning("⚠️ Some advanced features may be unavailable in Pandas mode")
            
            return True
        else:
            st.error("❌ No datasets were loaded successfully")
            return False
        
    except Exception as e:
        st.error(f"Failed to load datasets: {e}")
        
        # Offer complete fallback to pandas
        st.info("🔧 **Complete Pandas Fallback Mode**")
        if st.button("📊 Load All Data with Pandas Only"):
            try:
                import pandas as pd
                datasets = {}
                
                for dataset_name, filename in dataset_files.items():
                    file_path = os.path.join(data_dir, filename)
                    if os.path.exists(file_path):
                        pandas_df = pd.read_csv(file_path)
                        datasets[f"{dataset_name}_pandas"] = pandas_df
                        st.success(f"✅ Loaded {dataset_name}: {len(pandas_df):,} rows (Pandas)")
                
                if datasets:
                    st.session_state.loaded_datasets = datasets
                    st.warning("⚠️ Running in Pandas-only mode - Spark transformations unavailable")
                    return True
                else:
                    st.error("❌ No datasets found")
                    return False
                    
            except Exception as pandas_error:
                st.error(f"❌ Pandas fallback failed: {pandas_error}")
                return False
        
        return False

def show_dataset_preview(dataset_name: str, df):
    """Show a comprehensive preview of the dataset with error handling"""
    st.subheader(f"📊 {dataset_name.title()} Dataset")
    
    # Check if this is a pandas DataFrame (fallback mode)
    is_pandas = dataset_name.endswith('_pandas') or hasattr(df, 'shape')
    
    if is_pandas:
        # Pandas DataFrame handling
        st.info("📊 Viewing in Pandas mode")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Rows", f"{len(df):,}")
        
        with col2:
            st.metric("Total Columns", len(df.columns))
        
        with col3:
            null_count = df.isnull().sum().sum()
            total_cells = len(df) * len(df.columns)
            null_percentage = (null_count / total_cells * 100) if total_cells > 0 else 0
            st.metric("Null Values", f"{null_percentage:.1f}%")
        
        with col4:
            memory_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)
            st.metric("Memory Usage", f"{memory_usage:.1f} MB")
        
        # Schema information for pandas
        st.write("### 🏗️ Schema Information")
        schema_data = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            sample_values = df[col].dropna().head(3).tolist()
            sample_str = ", ".join([str(v) for v in sample_values][:3])
            
            schema_data.append({
                "Column": col,
                "Type": dtype,
                "Non-Null Count": df[col].count(),
                "Sample Values": sample_str[:50] + "..." if len(sample_str) > 50 else sample_str
            })
        
        st.dataframe(pd.DataFrame(schema_data), use_container_width=True)
        
        # Sample data
        st.write("### 👀 Sample Data")
        sample_size = st.slider("Sample size", min_value=5, max_value=100, value=10)
        sample_df = df.head(sample_size)
        st.dataframe(sample_df, use_container_width=True)
        
    else:
        # Spark DataFrame handling with error recovery
        col1, col2, col3, col4 = st.columns(4)
        
        try:
            # Try to get row count safely
            with col1:
                try:
                    row_count = df.count()
                    st.metric("Total Rows", f"{row_count:,}")
                except Exception as e:
                    st.metric("Total Rows", "Error - see below")
                    st.error(f"Row count failed: {e}")
            
            with col2:
                st.metric("Total Columns", len(df.columns))
            
            with col3:
                try:
                    # Try to calculate null percentage
                    total_cells = df.count() * len(df.columns)
                    null_count = sum([df.filter(df[col].isNull()).count() for col in df.columns])
                    null_percentage = (null_count / total_cells * 100) if total_cells > 0 else 0
                    st.metric("Null Values", f"{null_percentage:.1f}%")
                except Exception as e:
                    st.metric("Null Values", "Calculation failed")
                    st.warning("Could not calculate null percentage due to Spark issues")
            
            with col4:
                # Memory usage estimation
                try:
                    memory_mb = df.count() * len(df.columns) * 8 / (1024 * 1024)
                    st.metric("Est. Memory", f"{memory_mb:.1f} MB")
                except:
                    st.metric("Est. Memory", "Unknown")
            
            # Schema information
            st.write("### 🏗️ Schema Information")
            schema_data = []
            for field in df.schema.fields:
                try:
                    # Try to get sample values safely
                    sample_values = df.select(field.name).limit(3).collect()
                    sample_str = ", ".join([str(row[0]) for row in sample_values if row[0] is not None][:3])
                except:
                    sample_str = "Unable to retrieve samples"
                
                schema_data.append({
                    "Column": field.name,
                    "Type": str(field.dataType).replace("Type()", ""),
                    "Nullable": "✅" if field.nullable else "❌",
                    "Sample Values": sample_str[:50] + "..." if len(sample_str) > 50 else sample_str
                })
            
            st.dataframe(pd.DataFrame(schema_data), use_container_width=True)
            
            # Data quality metrics
            st.write("### 📊 Data Quality Metrics")
            try:
                quality_metrics = []
                
                for col_name in df.columns:
                    try:
                        total_count = df.count()
                        null_count = df.filter(df[col_name].isNull()).count()
                        distinct_count = df.select(col_name).distinct().count()
                        
                        quality_metrics.append({
                            "Column": col_name,
                            "Completeness": f"{((total_count - null_count) / total_count * 100):.1f}%",
                            "Uniqueness": f"{(distinct_count / total_count * 100):.1f}%",
                            "Null Count": null_count,
                            "Distinct Values": distinct_count
                        })
                    except Exception as col_error:
                        quality_metrics.append({
                            "Column": col_name,
                            "Completeness": "Error",
                            "Uniqueness": "Error", 
                            "Null Count": "Error",
                            "Distinct Values": "Error"
                        })
                
                st.dataframe(pd.DataFrame(quality_metrics), use_container_width=True)
                
            except Exception as e:
                st.warning("Could not calculate data quality metrics due to Spark issues")
                st.info("💡 Try using the Pandas fallback mode for full functionality")
            
            # Sample data with error handling
            st.write("### 👀 Sample Data")
            
            col1, col2 = st.columns([3, 1])
            with col1:
                sample_size = st.slider("Sample size", min_value=5, max_value=100, value=10)
            with col2:
                if st.button("🔄 Refresh Sample"):
                    st.rerun()
            
            try:
                sample_df = df.limit(sample_size).toPandas()
                st.dataframe(sample_df, use_container_width=True)
                
            except Exception as e:
                st.error(f"❌ Could not retrieve sample data: {e}")
                
                if "Python worker" in str(e):
                    st.info("""
                    **Python Worker Error**
                    
                    Try these options:
                    1. Restart the Streamlit app
                    2. Use smaller sample size
                    3. Switch to Pandas mode
                    4. Use the Jupyter notebook instead
                    """)
                    
                    # Offer to convert to Pandas
                    if st.button("🔄 Convert to Pandas Mode", key=f"convert_{dataset_name}"):
                        try:
                            # This might fail, but worth trying
                            pandas_df = df.toPandas()
                            st.session_state.loaded_datasets[f"{dataset_name}_pandas"] = pandas_df
                            del st.session_state.loaded_datasets[dataset_name]
                            st.success("✅ Converted to Pandas mode!")
                            st.rerun()
                        except Exception as convert_error:
                            st.error(f"❌ Conversion failed: {convert_error}")
                
        except Exception as e:
            st.error(f"❌ Error displaying dataset preview: {e}")
            st.info("💡 This dataset may be corrupted or there's a Spark configuration issue")

def create_transformation_interface():
    """Create interactive transformation selection interface"""
    st.header("🔧 Data Transformations")
    
    if not st.session_state.loaded_datasets:
        st.warning("Please load datasets first before applying transformations.")
        return
    
    # Transformation pipeline builder
    st.subheader("📋 Build Transformation Pipeline")
    
    # Step-by-step pipeline builder
    current_step = st.session_state.current_step
    
    st.write(f"### Step {current_step}: Select Transformation")
    
    # Input dataset selection
    col1, col2 = st.columns(2)
    
    with col1:
        available_datasets = list(st.session_state.loaded_datasets.keys())
        if st.session_state.execution_results:
            # Add previously created datasets
            for result in st.session_state.execution_results:
                if result['output_name'] not in available_datasets:
                    available_datasets.append(result['output_name'])
        
        input_dataset = st.selectbox(
            "Select input dataset:",
            available_datasets,
            key=f"input_dataset_{current_step}"
        )
    
    with col2:
        transformation_categories = list(AVAILABLE_TRANSFORMATIONS.keys())
        selected_category = st.selectbox(
            "Select transformation category:",
            transformation_categories,
            key=f"category_{current_step}"
        )
    
    # Transformation selection
    if selected_category:
        transformations = AVAILABLE_TRANSFORMATIONS[selected_category]
        
        # Extract transformation options from the new structure
        transformation_options = [(key, config['name']) for key, config in transformations.items()]
        transformation_keys = [key for key, _ in transformation_options]
        transformation_names = [name for _, name in transformation_options]
        
        if not transformation_names:
            st.warning("No transformations available for this category")
            return
        
        selected_transformation_index = st.selectbox(
            "Select transformation:",
            range(len(transformation_names)),
            format_func=lambda x: transformation_names[x],
            key=f"transformation_{current_step}"
        )
        
        if selected_transformation_index is not None:
            selected_transformation_key = transformation_keys[selected_transformation_index]
            selected_transformation_config = transformations[selected_transformation_key]
            transformation_class = selected_transformation_config['class']
            
            # Show transformation details
            st.info(f"**{selected_transformation_config['name']}**")
            st.write(selected_transformation_config['description'])
            
            # Parameter configuration
            st.write("### ⚙️ Parameters")
            
            # Get available columns from input dataset
            available_columns = []
            if input_dataset and input_dataset in st.session_state.loaded_datasets:
                try:
                    df = st.session_state.loaded_datasets[input_dataset]
                    if hasattr(df, 'columns'):
                        available_columns = list(df.columns)
                    elif hasattr(df, 'schema'):
                        available_columns = [field.name for field in df.schema.fields]
                except Exception as e:
                    st.warning(f"Could not extract columns: {e}")
            
            # Common parameters
            output_name = st.text_input(
                "Output dataset name:",
                value=f"{input_dataset}_{selected_transformation_key}",
                key=f"output_name_{current_step}"
            )
            
            # Transformation-specific parameters from config
            params = {}
            
            for param_name, param_config in selected_transformation_config['parameters'].items():
                param_type = param_config['type']
                param_desc = param_config.get('description', '')
                param_default = param_config.get('default', None)
                
                st.write(f"**{param_name.replace('_', ' ').title()}**")
                if param_desc:
                    st.caption(param_desc)
                
                if param_type == 'text':
                    params[param_name] = st.text_input(
                        f"Value for {param_name}:",
                        value=str(param_default) if param_default is not None else "",
                        key=f"param_{param_name}_{current_step}"
                    )
                
                elif param_type == 'number':
                    params[param_name] = st.number_input(
                        f"Value for {param_name}:",
                        value=float(param_default) if param_default is not None else 0.0,
                        key=f"param_{param_name}_{current_step}"
                    )
                
                elif param_type == 'select':
                    options = param_config.get('options', [])
                    default_index = 0
                    if param_default and param_default in options:
                        default_index = options.index(param_default)
                    
                    params[param_name] = st.selectbox(
                        f"Select {param_name}:",
                        options,
                        index=default_index,
                        key=f"param_{param_name}_{current_step}"
                    )
                
                elif param_type == 'multiselect':
                    options = param_config.get('options', available_columns)
                    default_values = param_config.get('default', [])
                    if isinstance(default_values, str):
                        default_values = [default_values]
                    
                    params[param_name] = st.multiselect(
                        f"Select {param_name}:",
                        options,
                        default=default_values,
                        key=f"param_{param_name}_{current_step}"
                    )
                
                elif param_type == 'dict':
                    st.write("Enter as JSON format:")
                    json_input = st.text_area(
                        f"JSON for {param_name}:",
                        value='{}',
                        key=f"param_{param_name}_{current_step}",
                        help="Example: {\"column1\": \"string\", \"column2\": \"integer\"}"
                    )
                    
                    try:
                        import json
                        params[param_name] = json.loads(json_input)
                    except json.JSONDecodeError:
                        st.error("Invalid JSON format")
                        params[param_name] = {}
            
            # Execute transformation
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("▶️ Execute Step", key=f"execute_{current_step}"):
                    execute_transformation_step(
                        input_dataset, transformation_class, output_name, params
                    )
            
            with col2:
                if st.button("➕ Add Step", key=f"add_step_{current_step}"):
                    st.session_state.current_step += 1
                    st.rerun()
            
            with col3:
                if st.button("🔄 Reset Pipeline", key=f"reset_{current_step}"):
                    st.session_state.execution_results = []
                    st.session_state.current_step = 1
                    st.rerun()
    
    # Show execution history
    if st.session_state.execution_results:
        st.write("### 📝 Execution History")
        
        history_data = []
        for i, result in enumerate(st.session_state.execution_results):
            history_data.append({
                "Step": i + 1,
                "Input": result['input_dataset'],
                "Transformation": result['transformation'],
                "Output": result['output_name'],
                "Rows": f"{result['output_rows']:,}",
                "Columns": result['output_cols'],
                "Status": "✅ Success" if result.get('success', True) else "❌ Failed"
            })
        
        st.dataframe(pd.DataFrame(history_data), use_container_width=True)

def execute_transformation_step(input_dataset, transformation_class, output_name, params):
    """Execute a single transformation step with enhanced error handling"""
    try:
        with st.spinner(f"Executing {transformation_class.__name__}..."):
            # Get the input DataFrame with error handling
            input_df = None
            
            if input_dataset in st.session_state.loaded_datasets:
                input_df = st.session_state.loaded_datasets[input_dataset]
            else:
                # Look for it in previous results
                for result in st.session_state.execution_results:
                    if result['output_name'] == input_dataset:
                        if input_dataset in st.session_state.loaded_datasets:
                            input_df = st.session_state.loaded_datasets[input_dataset]
                        break
                
                if input_df is None:
                    st.error(f"❌ Dataset {input_dataset} not found!")
                    return
            
            # Check if input is pandas DataFrame (fallback mode)
            is_pandas_input = hasattr(input_df, 'shape')  # pandas DataFrame
            
            if is_pandas_input:
                st.info("🐼 Input is in Pandas mode - attempting transformation")
                
                # For pandas mode, we need to handle transformations differently
                try:
                    # Convert to Spark if possible
                    if st.session_state.spark_session:
                        try:
                            input_df = st.session_state.spark_session.createDataFrame(input_df)
                            st.info("✅ Successfully converted to Spark DataFrame")
                        except Exception as convert_error:
                            st.warning(f"⚠️ Could not convert to Spark: {convert_error}")
                            st.info("Continuing with pandas-only transformation...")
                            # Would need pandas-only transformation logic here
                            st.error("❌ Pandas-only transformations not yet implemented")
                            return
                except Exception as e:
                    st.error(f"❌ Error handling pandas input: {e}")
                    return
            
            # Create transformation instance
            try:
                transformation = transformation_class()
            except Exception as e:
                st.error(f"❌ Failed to create transformation instance: {e}")
                return
            
            # Execute transformation with enhanced error handling
            output_df = None
            try:
                output_df = transformation.transform(input_df, **params)
                
                # Validate the output
                if output_df is None:
                    raise ValueError("Transformation returned None")
                
                # Try to get basic info about the result
                try:
                    output_rows = output_df.count()
                    output_cols = len(output_df.columns)
                except Exception as count_error:
                    # If count fails, still continue but with warning
                    st.warning(f"⚠️ Could not get row count: {count_error}")
                    output_rows = "Unknown"
                    output_cols = len(output_df.columns) if hasattr(output_df, 'columns') else "Unknown"
                
            except Exception as transform_error:
                st.error(f"❌ Transformation execution failed: {transform_error}")
                
                # Provide specific guidance based on error type
                if "Python worker" in str(transform_error):
                    st.info("""
                    **Python Worker Error Solutions:**
                    1. 🔄 Restart the Streamlit app
                    2. 📉 Use smaller datasets
                    3. 🐼 Try simpler transformations
                    4. ⚙️ Check Java installation
                    """)
                elif "AnalysisException" in str(transform_error):
                    st.info("💡 Check your column names and transformation parameters")
                elif "IllegalArgumentException" in str(transform_error):
                    st.info("💡 Check your parameter values and data types")
                
                # Record failed execution
                result = {
                    'step': len(st.session_state.execution_results) + 1,
                    'input_dataset': input_dataset,
                    'transformation': transformation_class.__name__,
                    'output_name': output_name,
                    'error': str(transform_error),
                    'timestamp': datetime.now().isoformat(),
                    'success': False
                }
                st.session_state.execution_results.append(result)
                return
            
            # Store result if successful
            if output_df is not None:
                st.session_state.loaded_datasets[output_name] = output_df
                
                # Record successful execution
                result = {
                    'step': len(st.session_state.execution_results) + 1,
                    'input_dataset': input_dataset,
                    'transformation': transformation_class.__name__,
                    'output_name': output_name,
                    'output_rows': output_rows,
                    'output_cols': output_cols,
                    'parameters': params,
                    'timestamp': datetime.now().isoformat(),
                    'success': True
                }
                
                st.session_state.execution_results.append(result)
                
                # Track lineage with error handling
                try:
                    if st.session_state.spark_engine:
                        st.session_state.spark_engine.lineage_tracker.add_transformation(
                            transformation_class.__name__,
                            input_dataset,
                            output_name,
                            params
                        )
                except Exception as lineage_error:
                    st.warning(f"⚠️ Lineage tracking failed: {lineage_error}")
                
                # Show success message
                if isinstance(output_rows, int):
                    st.success(f"✅ Transformation completed! Created {output_name} with {output_rows:,} rows")
                else:
                    st.success(f"✅ Transformation completed! Created {output_name}")
                
                # Offer to preview the result
                if st.button(f"👀 Preview {output_name}", key=f"preview_{output_name}"):
                    try:
                        if hasattr(output_df, 'limit'):  # Spark
                            preview_df = output_df.limit(5).toPandas()
                        else:  # Pandas
                            preview_df = output_df.head(5)
                        
                        st.dataframe(preview_df, use_container_width=True)
                        
                    except Exception as preview_error:
                        st.warning(f"⚠️ Could not preview result: {preview_error}")
            
    except Exception as e:
        st.error(f"❌ Unexpected error during transformation: {e}")
        
        # Record failed execution
        result = {
            'step': len(st.session_state.execution_results) + 1,
            'input_dataset': input_dataset,
            'transformation': transformation_class.__name__,
            'output_name': output_name,
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'success': False
        }
        st.session_state.execution_results.append(result)
        
        # General troubleshooting advice
        st.info("""
        **General Troubleshooting:**
        - Check your input data format
        - Verify transformation parameters
        - Try with a smaller dataset
        - Restart the application if issues persist
        """)

def create_lineage_visualization():
    """Create interactive lineage visualization"""
    st.header("🌊 Data Lineage Tracking")
    
    if not st.session_state.execution_results:
        st.info("No transformations executed yet. Please run some transformations to see lineage.")
        return
    
    # Create lineage graph with proper flow
    G = nx.DiGraph()
    
    # First, identify all unique datasets and their relationships
    datasets = set()
    transformations = []
    
    for result in st.session_state.execution_results:
        if result.get('success', True):  # Only include successful transformations
            datasets.add(result['input_dataset'])
            datasets.add(result['output_name'])
            transformations.append(result)
    
    # Add dataset nodes
    for dataset in datasets:
        G.add_node(dataset, type='dataset', label=dataset, shape='ellipse')
    
    # Add transformation nodes and edges to create proper flow
    for result in transformations:
        input_dataset = result['input_dataset']
        output_dataset = result['output_name']
        transformation = result['transformation']
        step = result['step']
        
        # Create unique transformation node name
        transform_node = f"{transformation}_step_{step}"
        
        # Add transformation node
        G.add_node(transform_node, 
                  type='transformation', 
                  label=transformation,
                  shape='rectangle',
                  step=step,
                  rows_out=result.get('output_rows', 'N/A'),
                  timestamp=result.get('timestamp', ''))
        
        # Add edges: input -> transformation -> output
        G.add_edge(input_dataset, transform_node, 
                  edge_type='data_flow',
                  rows=result.get('output_rows', 'N/A'))
        G.add_edge(transform_node, output_dataset, 
                  edge_type='data_flow', 
                  rows=result.get('output_rows', 'N/A'))
    
    # Use hierarchical layout to show proper flow direction
    try:
        # Try to use a hierarchical layout that shows flow from left to right
        pos = nx.nx_agraph.graphviz_layout(G, prog='dot', args='-Grankdir=LR')
    except:
        # Fallback to spring layout with constraints for better flow visualization
        pos = nx.spring_layout(G, k=3, iterations=100, seed=42)
    
    # Separate nodes by type for different styling
    dataset_nodes = [node for node, data in G.nodes(data=True) if data.get('type') == 'dataset']
    transformation_nodes = [node for node, data in G.nodes(data=True) if data.get('type') == 'transformation']
    
    # Create edge traces with arrows
    edge_traces = []
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        
        # Create arrow effect by offsetting the end point
        dx = x1 - x0
        dy = y1 - y0
        length = (dx**2 + dy**2)**0.5
        if length > 0:
            # Shorten the line to make room for the arrow head
            arrow_length = 0.03 * length
            x1_arrow = x1 - (dx/length) * arrow_length
            y1_arrow = y1 - (dy/length) * arrow_length
        else:
            x1_arrow, y1_arrow = x1, y1
        
        edge_trace = go.Scatter(
            x=[x0, x1_arrow, None], 
            y=[y0, y1_arrow, None], 
            mode='lines',
            line=dict(width=3, color='#666'),
            hoverinfo='none',
            showlegend=False,
            name=f"Flow: {edge[0]} → {edge[1]}"
        )
        edge_traces.append(edge_trace)
        
        # Add arrow head
        if length > 0:
            arrow_head = go.Scatter(
                x=[x1], y=[y1],
                mode='markers',
                marker=dict(
                    symbol='triangle-right',
                    size=12,
                    color='#666',
                    line=dict(width=1, color='#333')
                ),
                hoverinfo='none',
                showlegend=False
            )
            edge_traces.append(arrow_head)
    
    # Dataset nodes (sources and sinks)
    dataset_x = [pos[node][0] for node in dataset_nodes]
    dataset_y = [pos[node][1] for node in dataset_nodes]
    
    # Determine if dataset is source, intermediate, or sink
    dataset_colors = []
    dataset_hover_texts = []
    for node in dataset_nodes:
        in_degree = G.in_degree(node)
        out_degree = G.out_degree(node)
        
        if in_degree == 0:  # Source
            color = 'lightgreen'
            node_type = "Data Source"
        elif out_degree == 0:  # Sink
            color = 'lightcoral'
            node_type = "Final Output"
        else:  # Intermediate
            color = 'lightblue'
            node_type = "Intermediate Dataset"
        
        dataset_colors.append(color)
        dataset_hover_texts.append(f"<b>{node}</b><br>Type: {node_type}<br>Connections: {in_degree} in, {out_degree} out")
    
    dataset_trace = go.Scatter(
        x=dataset_x, y=dataset_y,
        mode='markers+text',
        marker=dict(
            size=25, 
            color=dataset_colors, 
            line=dict(width=2, color='#333'),
            symbol='circle'
        ),
        text=dataset_nodes,
        textposition="middle center",
        textfont=dict(size=10, color='black'),
        name="Datasets",
        hovertemplate="%{hovertext}<extra></extra>",
        hovertext=dataset_hover_texts
    )
    
    # Transformation nodes
    transformation_x = [pos[node][0] for node in transformation_nodes]
    transformation_y = [pos[node][1] for node in transformation_nodes]
    
    transformation_hover_texts = []
    transformation_labels = []
    for node in transformation_nodes:
        node_data = G.nodes[node]
        label = node_data.get('label', node)
        transformation_labels.append(label)
        
        hover_text = f"<b>{label}</b><br>Step: {node_data.get('step', 'N/A')}<br>Output Rows: {node_data.get('rows_out', 'N/A')}"
        transformation_hover_texts.append(hover_text)
    
    transformation_trace = go.Scatter(
        x=transformation_x, y=transformation_y,
        mode='markers+text',
        marker=dict(
            size=20, 
            color='orange', 
            symbol='square',
            line=dict(width=2, color='#333')
        ),
        text=transformation_labels,
        textposition="middle center",
        textfont=dict(size=9, color='black'),
        name="Transformations",
        hovertemplate="%{hovertext}<extra></extra>",
        hovertext=transformation_hover_texts
    )
    
    # Create figure with all traces
    all_traces = edge_traces + [dataset_trace, transformation_trace]
    fig = go.Figure(data=all_traces)
    
    fig.update_layout(
        title="📊 Data Lineage Flow: From Source to Destination",
        showlegend=True,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=60),
        annotations=[
            dict(
                text="🟢 Sources → 🟠 Transformations → 🔴 Final Outputs<br>Hover over nodes for details",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                xanchor='left', yanchor='bottom',
                font=dict(color='#666', size=11)
            )
        ],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add flow statistics
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate statistics
    sources = [node for node in dataset_nodes if G.in_degree(node) == 0]
    sinks = [node for node in dataset_nodes if G.out_degree(node) == 0]
    intermediates = [node for node in dataset_nodes if G.in_degree(node) > 0 and G.out_degree(node) > 0]
    
    with col1:
        st.metric("📥 Data Sources", len(sources))
    with col2:
        st.metric("🔄 Transformations", len(transformation_nodes))
    with col3:
        st.metric("📤 Final Outputs", len(sinks))
    with col4:
        st.metric("🔗 Flow Connections", G.number_of_edges())
    
    # Show flow path analysis
    if sources and sinks:
        st.write("### 🛤️ Data Flow Paths")
        
        for source in sources:
            for sink in sinks:
                try:
                    # Find the shortest path from source to sink
                    if nx.has_path(G, source, sink):
                        path = nx.shortest_path(G, source, sink)
                        path_length = len(path) - 1  # Number of edges
                        
                        # Create a readable path string
                        path_str = " → ".join([
                            f"**{node}**" if node in dataset_nodes else f"*{G.nodes[node].get('label', node)}*"
                            for node in path
                        ])
                        
                        st.write(f"**{source}** to **{sink}** ({path_length} steps):")
                        st.write(path_str)
                        st.write("---")
                except:
                    continue
    
    # Enhanced lineage summary table
    st.write("### 📋 Transformation Pipeline Summary")
    
    # Create a more detailed summary showing the complete flow
    lineage_data = []
    for i, result in enumerate(st.session_state.execution_results):
        if result.get('success', True):  # Only show successful transformations
            # Calculate previous dataset info for better flow tracking
            prev_rows = "Initial Data"
            if i > 0:
                prev_result = st.session_state.execution_results[i-1]
                if prev_result.get('success', True):
                    prev_rows = f"{prev_result.get('output_rows', 'N/A'):,}" if isinstance(prev_result.get('output_rows'), int) else prev_result.get('output_rows', 'N/A')
            
            current_rows = f"{result['output_rows']:,}" if isinstance(result.get('output_rows'), int) else result.get('output_rows', 'N/A')
            
            # Calculate data change
            data_change = "N/A"
            if isinstance(result.get('output_rows'), int) and i > 0:
                prev_result = st.session_state.execution_results[i-1]
                if isinstance(prev_result.get('output_rows'), int):
                    change = result['output_rows'] - prev_result['output_rows']
                    change_pct = (change / prev_result['output_rows']) * 100
                    data_change = f"{change:+,} ({change_pct:+.1f}%)"
            
            lineage_data.append({
                "Step": result['step'],
                "Input Dataset": result['input_dataset'],
                "Transformation": result['transformation'],
                "Output Dataset": result['output_name'],
                "Input Rows": prev_rows,
                "Output Rows": current_rows,
                "Data Change": data_change,
                "Timestamp": result['timestamp'][:19].replace('T', ' ')
            })
    
    if lineage_data:
        df = pd.DataFrame(lineage_data)
        st.dataframe(df, use_container_width=True)
        
        # Show overall pipeline statistics
        st.write("### 📈 Pipeline Statistics")
        
        total_steps = len(lineage_data)
        start_dataset = lineage_data[0]['Input Dataset'] if lineage_data else "N/A"
        end_dataset = lineage_data[-1]['Output Dataset'] if lineage_data else "N/A"
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total Steps", total_steps)
        with col2:
            st.metric("🎯 Start Dataset", start_dataset)
        with col3:
            st.metric("🏁 Final Dataset", end_dataset)
    else:
        st.info("No successful transformations to display.")

def create_advanced_visualizations():
    """Create advanced pipeline visualizations"""
    st.header("📈 Advanced Visualizations")
    
    if not st.session_state.execution_results:
        st.info("No data to visualize. Please execute some transformations first.")
        return
    
    # Visualization options
    viz_type = st.selectbox(
        "Select visualization type:",
        ["Pipeline Flow", "Data Volume Changes", "Transformation Performance", "Data Quality Metrics"]
    )
    
    if viz_type == "Pipeline Flow":
        create_sankey_diagram()
    elif viz_type == "Data Volume Changes":
        create_volume_chart()
    elif viz_type == "Transformation Performance":
        create_performance_chart()
    elif viz_type == "Data Quality Metrics":
        create_quality_metrics()

def create_sankey_diagram():
    """Create Sankey diagram for data flow"""
    st.subheader("🌊 Data Flow Sankey Diagram")
    
    # Prepare data for Sankey
    nodes = []
    links = []
    node_dict = {}
    
    # Collect all unique datasets and transformations
    all_entities = set()
    for result in st.session_state.execution_results:
        all_entities.add(result['input_dataset'])
        all_entities.add(result['output_name'])
        all_entities.add(result['transformation'])
    
    # Create node mapping
    for i, entity in enumerate(all_entities):
        node_dict[entity] = i
        nodes.append(entity)
    
    # Create links
    for result in st.session_state.execution_results:
        # Input -> Transformation
        links.append({
            'source': node_dict[result['input_dataset']],
            'target': node_dict[result['transformation']],
            'value': result['output_rows']
        })
        
        # Transformation -> Output
        links.append({
            'source': node_dict[result['transformation']],
            'target': node_dict[result['output_name']],
            'value': result['output_rows']
        })
    
    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color="lightblue"
        ),
        link=dict(
            source=[link['source'] for link in links],
            target=[link['target'] for link in links],
            value=[link['value'] for link in links]
        )
    )])
    
    fig.update_layout(title_text="Data Flow Sankey Diagram", font_size=10)
    st.plotly_chart(fig, use_container_width=True)

def create_volume_chart():
    """Create data volume changes chart"""
    st.subheader("📊 Data Volume Changes")
    
    steps = []
    input_volumes = []
    output_volumes = []
    transformations = []
    
    for result in st.session_state.execution_results:
        steps.append(f"Step {result['step']}")
        output_volumes.append(result['output_rows'])
        transformations.append(result['transformation'])
        
        # For input volumes, we need to look up the previous step or original dataset
        # This is simplified - in a real implementation, you'd track this properly
        if result['step'] == 1:
            input_volumes.append(result['output_rows'])  # Approximate
        else:
            input_volumes.append(st.session_state.execution_results[result['step']-2]['output_rows'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Input Rows',
        x=steps,
        y=input_volumes,
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name='Output Rows',
        x=steps,
        y=output_volumes,
        marker_color='darkblue'
    ))
    
    fig.update_layout(
        title='Data Volume Changes Through Pipeline',
        xaxis_title='Pipeline Steps',
        yaxis_title='Number of Rows',
        barmode='group'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_performance_chart():
    """Create transformation performance metrics"""
    st.subheader("⚡ Transformation Performance")
    st.info("Performance metrics would be available with execution timing data.")
    
    # Placeholder for performance visualization
    # In a real implementation, you'd track execution times
    performance_data = []
    for result in st.session_state.execution_results:
        performance_data.append({
            'Transformation': result['transformation'],
            'Step': result['step'],
            'Output Rows': result['output_rows'],
            'Complexity': 'Medium'  # Placeholder
        })
    
    if performance_data:
        st.dataframe(pd.DataFrame(performance_data), use_container_width=True)

def create_quality_metrics():
    """Create data quality metrics visualization"""
    st.subheader("🎯 Data Quality Metrics")
    
    if not st.session_state.loaded_datasets:
        st.info("No datasets loaded for quality analysis.")
        return
    
    quality_data = []
    
    for dataset_name, df in st.session_state.loaded_datasets.items():
        total_rows = df.count()
        total_cols = len(df.columns)
        
        # Calculate completeness
        null_counts = []
        for col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            null_counts.append(null_count)
        
        total_nulls = sum(null_counts)
        completeness = ((total_rows * total_cols - total_nulls) / (total_rows * total_cols)) * 100
        
        quality_data.append({
            'Dataset': dataset_name,
            'Rows': total_rows,
            'Columns': total_cols,
            'Completeness': f"{completeness:.1f}%",
            'Total Nulls': total_nulls
        })
    
    if quality_data:
        df_quality = pd.DataFrame(quality_data)
        
        # Create quality metrics chart
        fig = px.bar(
            df_quality, 
            x='Dataset', 
            y='Rows',
            title='Dataset Size Comparison',
            color='Completeness'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Quality metrics table
        st.dataframe(df_quality, use_container_width=True)

def sidebar_controls():
    """Create sidebar controls"""
    st.sidebar.title("🎛️ Pipeline Control")
    
    # Data Management Section
    st.sidebar.header("📊 Data Management")
    
    # Check if sample data exists
    data_dir = os.path.join(project_root, 'data')
    customers_file = os.path.join(data_dir, 'customers.csv')
    
    if not os.path.exists(customers_file):
        st.sidebar.warning("⚠️ Sample data not found!")
        if st.sidebar.button("🎲 Generate Sample Data"):
            with st.spinner("Generating sample data..."):
                try:
                    from data.generate_sample_data import save_sample_data
                    save_sample_data()
                    st.sidebar.success("✅ Sample data generated!")
                    st.rerun()
                except Exception as e:
                    st.sidebar.warning(f"Main generator failed: {e}")
                    st.sidebar.info("🔧 Using fallback generator...")
                    if generate_fallback_data():
                        st.sidebar.success("✅ Fallback data generated!")
                        st.rerun()
                    else:
                        st.sidebar.error("❌ All generators failed")
    else:
        st.sidebar.success("✅ Sample data available!")
        
        if st.sidebar.button("📥 Load Datasets"):
            if load_sample_data():
                st.sidebar.success("✅ Datasets loaded!")
                st.rerun()
    
    # Spark Session Management
    st.sidebar.header("⚡ Spark Session")
    
    if st.session_state.spark_engine is None:
        if st.sidebar.button("🚀 Initialize Spark"):
            initialize_spark()
            st.rerun()
    else:
        st.sidebar.success("✅ Spark session active")
        if st.sidebar.button("🔄 Restart Spark"):
            st.session_state.spark_engine = None
            st.rerun()
    
    # Pipeline Status
    st.sidebar.header("📋 Pipeline Status")
    
    st.sidebar.metric("Datasets Loaded", len(st.session_state.loaded_datasets))
    st.sidebar.metric("Transformations", len(st.session_state.execution_results))
    st.sidebar.metric("Current Step", st.session_state.current_step)
    
    # Quick Actions
    st.sidebar.header("⚡ Quick Actions")
    
    if st.sidebar.button("🗑️ Clear All Data"):
        st.session_state.loaded_datasets = {}
        st.session_state.execution_results = []
        st.session_state.current_step = 1
        st.sidebar.success("✅ Data cleared!")
        st.rerun()
    
    if st.sidebar.button("💾 Export Results"):
        if st.session_state.execution_results:
            export_data = {
                'execution_results': st.session_state.execution_results,
                'timestamp': datetime.now().isoformat()
            }
            st.sidebar.download_button(
                "📥 Download JSON",
                data=json.dumps(export_data, indent=2),
                file_name=f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        else:
            st.sidebar.warning("No results to export")

def main():
    """Main Streamlit application"""
    
    # Custom header
    st.markdown("""
    <div class="main-header">
        🔄 Data Lineage Pipeline
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem; color: #666;">
        <b>Complete Interactive Data Transformation & Lineage Tracking Platform</b><br>
        Transform data with PySpark • Track lineage in real-time • Visualize data flows • Generate production DAGs
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar controls
    sidebar_controls()
    
    # Main content area
    if len(st.session_state.loaded_datasets) == 0:
        # Welcome screen
        st.markdown("""
        ## 🎯 Welcome to the Data Lineage Pipeline!
        
        This comprehensive platform allows you to:
        
        ### 🚀 Quick Start Guide:
        1. **📊 Generate Sample Data** → Click "Generate Sample Data" in the sidebar
        2. **⚡ Initialize Spark** → Click "Initialize Spark" to start the engine  
        3. **📥 Load Datasets** → Import your data into Spark DataFrames
        4. **🔧 Build Pipeline** → Use the transformation interface to create your data pipeline
        5. **🌊 Track Lineage** → Visualize how data flows through transformations
        6. **📈 Analyze Results** → View advanced visualizations and metrics
        
        ### ✨ Key Features:
        - **15+ Transformation Types** across cleaning, aggregation, and joining operations
        - **Real-time Lineage Tracking** with interactive visualizations
        - **Data Quality Assessment** with comprehensive metrics
        - **Production-Ready Output** for Airflow deployment
        - **Interactive UI** with drag-and-drop pipeline building
        
        ### 🎛️ Get Started:
        Use the sidebar controls to begin! The system will guide you through each step.
        """)
        
        # Show sample screenshots or demo
        st.info("💡 **Tip:** Start by generating sample e-commerce data to explore all features!")
        
    else:
        # Main application tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Data Overview", 
            "🔧 Transformations", 
            "🌊 Lineage Tracking", 
            "📈 Visualizations"
        ])
        
        with tab1:
            st.header("📊 Data Overview")
            
            # Dataset selector
            selected_dataset = st.selectbox(
                "Select a dataset to explore:",
                options=list(st.session_state.loaded_datasets.keys()),
                index=0
            )
            
            if selected_dataset:
                show_dataset_preview(selected_dataset, st.session_state.loaded_datasets[selected_dataset])
        
        with tab2:
            create_transformation_interface()
        
        with tab3:
            create_lineage_visualization()
        
        with tab4:
            create_advanced_visualizations()

if __name__ == "__main__":
    main()
