"""
Main Streamlit UI for Data Lineage Pipeline
Fixed version without the weights parameter issue
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
from typing import Dict, List, Any
import plotly.graph_objects as go

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
    st.stop()

# Page configuration
st.set_page_config(
    page_title="Data Lineage Pipeline",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'spark_engine' not in st.session_state:
    st.session_state.spark_engine = None
if 'loaded_datasets' not in st.session_state:
    st.session_state.loaded_datasets = {}
if 'transformation_history' not in st.session_state:
    st.session_state.transformation_history = []
if 'lineage_data' not in st.session_state:
    st.session_state.lineage_data = None

def initialize_spark():
    """Initialize Spark engine"""
    if st.session_state.spark_engine is None:
        try:
            st.session_state.spark_engine = SparkTransformationEngine("DataLineageUI")
            return True
        except Exception as e:
            st.error(f"Failed to initialize Spark: {e}")
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
        
        # Create data directory
        data_dir = os.path.join(project_root, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # Generate customers (simplified)
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
                'is_active': np.random.choice([True, False], p=[0.8, 0.2])  # Fixed weights issue
            }
            customers_data.append(customer)
        
        customers_df = pd.DataFrame(customers_data)
        customers_df.to_csv(os.path.join(data_dir, 'customers.csv'), index=False)
        
        # Generate products (simplified)
        products_data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home']
        
        for i in range(100):
            product = {
                'product_id': f"PROD_{i+1:06d}",
                'product_name': fake.catch_phrase(),
                'category': random.choice(categories),
                'price': round(random.uniform(10, 500), 2),
                'stock_quantity': random.randint(0, 100),
                'is_discontinued': np.random.choice([True, False], p=[0.1, 0.9])  # Fixed weights issue
            }
            products_data.append(product)
        
        products_df = pd.DataFrame(products_data)
        products_df.to_csv(os.path.join(data_dir, 'products.csv'), index=False)
        
        # Generate orders (simplified)
        orders_data = []
        for i in range(500):
            order = {
                'order_id': f"ORDER_{i+1:08d}",
                'customer_id': f"CUST_{random.randint(1, 1000):06d}",
                'order_date': fake.date_between(start_date='-1y', end_date='today'),
                'total_amount': round(random.uniform(20, 1000), 2),
                'status': random.choice(['Completed', 'Pending', 'Cancelled'])
            }
            orders_data.append(order)
        
        orders_df = pd.DataFrame(orders_data)
        orders_df.to_csv(os.path.join(data_dir, 'orders.csv'), index=False)
        
        # Generate order_items (simplified)
        order_items_data = []
        for i, order in enumerate(orders_data):
            num_items = random.randint(1, 3)
            for j in range(num_items):
                item = {
                    'order_item_id': f"ITEM_{i+1:08d}_{j+1}",
                    'order_id': order['order_id'],
                    'product_id': f"PROD_{random.randint(1, 100):06d}",
                    'quantity': random.randint(1, 5),
                    'unit_price': round(random.uniform(10, 200), 2)
                }
                order_items_data.append(item)
        
        order_items_df = pd.DataFrame(order_items_data)
        order_items_df.to_csv(os.path.join(data_dir, 'order_items.csv'), index=False)
        
        # Generate inventory (simplified)
        inventory_data = []
        for i in range(100):
            inventory = {
                'product_id': f"PROD_{i+1:06d}",
                'warehouse_location': random.choice(['Warehouse_A', 'Warehouse_B', 'Warehouse_C']),
                'quantity_available': random.randint(0, 200),
                'last_updated': fake.date_between(start_date='-30d', end_date='today')
            }
            inventory_data.append(inventory)
        
        inventory_df = pd.DataFrame(inventory_data)
        inventory_df.to_csv(os.path.join(data_dir, 'inventory.csv'), index=False)
        
        return True
        
    except Exception as e:
        st.error(f"Failed to generate fallback data: {e}")
        return False

def load_sample_data():
    """Load sample datasets"""
    if not initialize_spark():
        return False
    
    data_dir = os.path.join(project_root, 'data')
    
    try:
        datasets = {}
        
        # Load customers
        customers_path = os.path.join(data_dir, 'customers.csv')
        if os.path.exists(customers_path):
            customers_df = st.session_state.spark_engine.load_data(customers_path, 'customers', 'csv')
            datasets['customers'] = customers_df
            st.success(f"Loaded customers: {customers_df.count()} records")
        
        # Load products
        products_path = os.path.join(data_dir, 'products.csv')
        if os.path.exists(products_path):
            products_df = st.session_state.spark_engine.load_data(products_path, 'products', 'csv')
            datasets['products'] = products_df
            st.success(f"Loaded products: {products_df.count()} records")
        
        # Load orders
        orders_path = os.path.join(data_dir, 'orders.csv')
        if os.path.exists(orders_path):
            orders_df = st.session_state.spark_engine.load_data(orders_path, 'orders', 'csv')
            datasets['orders'] = orders_df
            st.success(f"Loaded orders: {orders_df.count()} records")
        
        # Load order items
        order_items_path = os.path.join(data_dir, 'order_items.csv')
        if os.path.exists(order_items_path):
            order_items_df = st.session_state.spark_engine.load_data(order_items_path, 'order_items', 'csv')
            datasets['order_items'] = order_items_df
            st.success(f"Loaded order_items: {order_items_df.count()} records")
        
        # Load inventory
        inventory_path = os.path.join(data_dir, 'inventory.csv')
        if os.path.exists(inventory_path):
            inventory_df = st.session_state.spark_engine.load_data(inventory_path, 'inventory', 'csv')
            datasets['inventory'] = inventory_df
            st.success(f"Loaded inventory: {inventory_df.count()} records")
        
        st.session_state.loaded_datasets = datasets
        return True
        
    except Exception as e:
        st.error(f"Failed to load datasets: {e}")
        return False

def show_dataset_preview(dataset_name: str, df):
    """Show a preview of the dataset"""
    st.subheader(f"📊 {dataset_name.title()} Dataset")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Rows", f"{df.count():,}")
    
    with col2:
        st.metric("Total Columns", len(df.columns))
    
    # Show schema
    st.write("**Schema:**")
    schema_data = []
    for field in df.schema.fields:
        schema_data.append({
            "Column": field.name,
            "Type": str(field.dataType),
            "Nullable": field.nullable
        })
    
    st.dataframe(pd.DataFrame(schema_data), use_container_width=True)
    
    # Show sample data
    st.write("**Sample Data:**")
    sample_df = df.limit(10).toPandas()
    st.dataframe(sample_df, use_container_width=True)

def main():
    """Main Streamlit application"""
    
    # Title and header
    st.title("🔄 Data Lineage Pipeline")
    st.markdown("""
    Welcome to the **Data Lineage Pipeline** - your comprehensive solution for tracking data transformations 
    from source to destination using PySpark and Airflow!
    
    ### Features:
    - 🔧 **Interactive Data Transformations** - Choose from 15+ transformation types
    - 📊 **Real-time Lineage Tracking** - See how data flows through your pipeline
    - 🎨 **Beautiful Visualizations** - Interactive graphs and flowcharts
    - 🚀 **Production Ready** - Generate Airflow DAGs for deployment
    """)
    
    # Sidebar for data management
    st.sidebar.title("🎛️ Pipeline Control")
    
    # Data generation section in sidebar
    st.sidebar.header("Data Management")
    
    # Check if sample data exists
    data_dir = os.path.join(project_root, 'data')
    customers_file = os.path.join(data_dir, 'customers.csv')
    
    if not os.path.exists(customers_file):
        st.sidebar.warning("Sample data not found!")
        if st.sidebar.button("Generate Sample Data"):
            with st.spinner("Generating sample data..."):
                try:
                    # Try to import and run main data generator
                    from data.generate_sample_data import save_sample_data
                    save_sample_data()
                    st.sidebar.success("Sample data generated!")
                    st.rerun()
                except ImportError as e:
                    st.sidebar.warning(f"Could not import main data generator: {e}")
                    st.sidebar.info("🔧 Using fallback data generation...")
                    if generate_fallback_data():
                        st.sidebar.success("Fallback sample data generated!")
                        st.rerun()
                    else:
                        st.sidebar.error("Failed to generate any sample data")
                except TypeError as e:
                    if "weights" in str(e):
                        st.sidebar.warning("❌ Main data generator has weights parameter error")
                        st.sidebar.info("🔧 Using fallback data generation...")
                        if generate_fallback_data():
                            st.sidebar.success("Fallback sample data generated!")
                            st.rerun()
                        else:
                            st.sidebar.error("Failed to generate fallback data")
                    else:
                        st.sidebar.error(f"TypeError: {e}")
                except Exception as e:
                    st.sidebar.warning(f"Main data generator failed: {e}")
                    st.sidebar.info("🔧 Using fallback data generation...")
                    if generate_fallback_data():
                        st.sidebar.success("Fallback sample data generated!")
                        st.rerun()
                    else:
                        st.sidebar.error("All data generation methods failed")
    else:
        st.sidebar.success("Sample data available!")
        
        if st.sidebar.button("Load Datasets"):
            with st.spinner("Loading datasets..."):
                if load_sample_data():
                    st.sidebar.success("Datasets loaded successfully!")
    
    # Spark session management
    st.sidebar.header("Spark Session")
    if st.sidebar.button("Initialize Spark"):
        if initialize_spark():
            st.sidebar.success("Spark initialized!")
        else:
            st.sidebar.error("Failed to initialize Spark")
    
    # Main content area
    if len(st.session_state.loaded_datasets) == 0:
        st.info("""
        ### Getting Started:
        
        1. **Generate Sample Data** - Click the button in the sidebar to create sample e-commerce datasets
        2. **Initialize Spark** - Set up the Spark session for data processing
        3. **Load Datasets** - Import the generated data into Spark DataFrames
        4. **Choose Transformations** - Select from various data transformation options
        5. **View Lineage** - See the complete data flow visualization
        
        **Quick Start:** Use the sidebar controls to generate and load sample data!
        """)
    else:
        # Show tabs for different features
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Data Overview", "🔧 Transformations", "🌊 Lineage Tracking", "📈 Visualizations"])
        
        with tab1:
            st.header("Dataset Overview")
            
            # Dataset selector
            selected_dataset = st.selectbox(
                "Select a dataset to preview:",
                options=list(st.session_state.loaded_datasets.keys()),
                index=0
            )
            
            if selected_dataset:
                show_dataset_preview(selected_dataset, st.session_state.loaded_datasets[selected_dataset])
        
        with tab2:
            st.header("Data Transformations")
            st.info("🚧 Transformation interface coming soon! Use the Jupyter notebook for now.")
        
        with tab3:
            st.header("Data Lineage Tracking")
            st.info("🚧 Lineage visualization coming soon! Use the Jupyter notebook for now.")
        
        with tab4:
            st.header("Pipeline Visualizations")
            st.info("🚧 Advanced visualizations coming soon! Use the Jupyter notebook for now.")

if __name__ == "__main__":
    main()
