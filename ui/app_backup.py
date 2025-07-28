"""
Main Streamlit UI for Data Lineage Pipeline
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
        st.error(f"Failed to load data: {e}")
        return False

def show_dataset_preview(dataset_name: str, df):
    """Show preview of a dataset"""
    st.subheader(f"Dataset: {dataset_name}")
    
    # Show basic info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rows", df.count())
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        st.metric("Schema", "View below")
    
    # Show schema
    st.write("**Schema:**")
    schema_data = []
    for field in df.schema.fields:
        schema_data.append({
            'Column': field.name,
            'Type': str(field.dataType),
            'Nullable': field.nullable
        })
    st.dataframe(pd.DataFrame(schema_data), use_container_width=True)
    
    # Show sample data
    st.write("**Sample Data (first 10 rows):**")
    sample_data = df.limit(10).toPandas()
    st.dataframe(sample_data, use_container_width=True)

def render_transformation_ui():
    """Render transformation selection and configuration UI"""
    st.header("🔧 Configure Transformations")
    
    if not st.session_state.loaded_datasets:
        st.warning("Please load datasets first!")
        return
    
    # Select transformation category
    transformation_categories = list(AVAILABLE_TRANSFORMATIONS.keys())
    selected_category = st.selectbox("Select Transformation Category", transformation_categories)
    
    if selected_category:
        # Select specific transformation
        transformations = AVAILABLE_TRANSFORMATIONS[selected_category]
        transformation_options = {v['name']: k for k, v in transformations.items()}
        selected_transform_name = st.selectbox("Select Transformation", list(transformation_options.keys()))
        
        if selected_transform_name:
            selected_transform_key = transformation_options[selected_transform_name]
            transform_config = transformations[selected_transform_key]
            
            st.write(f"**Description:** {transform_config['description']}")
            
            # Select input dataset
            dataset_options = list(st.session_state.loaded_datasets.keys())
            selected_dataset = st.selectbox("Select Input Dataset", dataset_options)
            
            if selected_dataset:
                input_df = st.session_state.loaded_datasets[selected_dataset]
                available_columns = input_df.columns
                
                # Dynamic parameter configuration
                st.subheader("Transformation Parameters")
                
                parameters = {}
                for param_name, param_config in transform_config['parameters'].items():
                    param_type = param_config['type']
                    description = param_config.get('description', '')
                    default_value = param_config.get('default', '')
                    
                    if param_type == 'select':
                        options = param_config.get('options', available_columns)
                        parameters[param_name] = st.selectbox(f"{param_name}", options, help=description)
                    
                    elif param_type == 'multiselect':
                        options = param_config.get('options', available_columns)
                        parameters[param_name] = st.multiselect(f"{param_name}", options, help=description)
                    
                    elif param_type == 'text':
                        parameters[param_name] = st.text_input(f"{param_name}", value=str(default_value), help=description)
                    
                    elif param_type == 'number':
                        parameters[param_name] = st.number_input(f"{param_name}", value=float(default_value), help=description)
                    
                    elif param_type == 'dict':
                        json_input = st.text_area(f"{param_name}", value='{}', help=description)
                        try:
                            parameters[param_name] = json.loads(json_input)
                        except json.JSONDecodeError:
                            st.error(f"Invalid JSON for {param_name}")
                            parameters[param_name] = {}
                
                # Execute transformation button
                if st.button("Execute Transformation"):
                    try:
                        # Get transformation class
                        transform_class = transform_config['class']
                        
                        # Initialize transformation with lineage tracker
                        transformation = transform_class(st.session_state.spark_engine.lineage_tracker)
                        
                        # Execute transformation
                        with st.spinner("Executing transformation..."):
                            result_df = st.session_state.spark_engine.execute_transformation(
                                transformation, input_df, **parameters
                            )
                        
                        # Store result
                        result_name = f"{selected_dataset}_{selected_transform_key}"
                        st.session_state.loaded_datasets[result_name] = result_df
                        
                        # Update transformation history
                        st.session_state.transformation_history.append({
                            'transformation': selected_transform_name,
                            'input_dataset': selected_dataset,
                            'output_dataset': result_name,
                            'parameters': parameters
                        })
                        
                        st.success(f"Transformation completed! Result saved as '{result_name}'")
                        
                        # Update lineage data
                        st.session_state.lineage_data = st.session_state.spark_engine.get_lineage_info()
                        
                        # Show result preview
                        st.subheader("Transformation Result")
                        show_dataset_preview(result_name, result_df)
                        
                    except Exception as e:
                        st.error(f"Transformation failed: {e}")

def render_lineage_visualization():
    """Render lineage visualization"""
    st.header("📊 Data Lineage Visualization")
    
    if st.session_state.lineage_data is None:
        st.warning("No lineage data available. Execute some transformations first!")
        return
    
    try:
        # Create visualizer
        visualizer = create_lineage_visualization(st.session_state.lineage_data)
        
        # Show statistics
        stats = visualizer.get_lineage_statistics()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Nodes", stats['total_nodes'])
        with col2:
            st.metric("Total Edges", stats['total_edges'])
        with col3:
            st.metric("Data Sources", stats['data_sources'])
        with col4:
            st.metric("Transformations", stats['transformations'])
        
        # Visualization options
        viz_type = st.selectbox("Select Visualization Type", [
            "Interactive Lineage Graph",
            "Dependency Matrix", 
            "Transformation Summary"
        ])
        
        if viz_type == "Interactive Lineage Graph":
            layout_option = st.selectbox("Graph Layout", ["spring", "hierarchical"])
            fig = visualizer.create_interactive_graph(layout=layout_option)
            st.plotly_chart(fig, use_container_width=True)
        
        elif viz_type == "Dependency Matrix":
            fig = visualizer.create_dependency_matrix()
            st.plotly_chart(fig, use_container_width=True)
        
        elif viz_type == "Transformation Summary":
            fig = visualizer.create_transformation_summary()
            st.plotly_chart(fig, use_container_width=True)
        
        # Impact Analysis
        st.subheader("Impact Analysis")
        if stats['total_nodes'] > 0:
            # Get all node names for selection
            all_nodes = []
            for source in st.session_state.lineage_data.get('data_sources', []):
                all_nodes.append(source['name'])
            for sink in st.session_state.lineage_data.get('data_sinks', []):
                all_nodes.append(sink['name'])
            for transform in st.session_state.lineage_data.get('transformations', []):
                all_nodes.append(transform['output_table'])
            
            if all_nodes:
                selected_node = st.selectbox("Select node for impact analysis", all_nodes)
                if st.button("Generate Impact Analysis"):
                    try:
                        impact_fig = visualizer.create_impact_analysis(selected_node)
                        st.plotly_chart(impact_fig, use_container_width=True)
                    except Exception as e:
                        st.error(f"Failed to generate impact analysis: {e}")
        
    except Exception as e:
        st.error(f"Failed to create visualization: {e}")

def render_transformation_history():
    """Render transformation history"""
    st.header("📝 Transformation History")
    
    if not st.session_state.transformation_history:
        st.info("No transformations executed yet.")
        return
    
    # Show transformation history as a table
    history_df = pd.DataFrame(st.session_state.transformation_history)
    st.dataframe(history_df, use_container_width=True)
    
    # Export options
    if st.button("Export Lineage Data"):
        output_dir = os.path.join(project_root, 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        if st.session_state.lineage_data:
            lineage_file = os.path.join(output_dir, 'ui_lineage.json')
            with open(lineage_file, 'w') as f:
                json.dump(st.session_state.lineage_data, f, indent=2)
            st.success(f"Lineage data exported to {lineage_file}")

def main():
    """Main application"""
    
    # App title and description
    st.title("🔄 Data Lineage Pipeline")
    st.markdown("""
    Welcome to the Data Lineage Pipeline! This application allows you to:
    - Load and explore sample datasets
    - Apply various PySpark transformations
    - Track complete data lineage
    - Visualize data flow and dependencies
    """)
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Choose a page", [
        "🏠 Home",
        "📊 Data Explorer", 
        "🔧 Transformations",
        "📈 Lineage Visualization",
        "📝 History"
    ])
    
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
                    # Import and run data generator with proper path
                    from data.generate_sample_data import save_sample_data
                    save_sample_data()
                    st.sidebar.success("Sample data generated!")
                    st.rerun()
                except ImportError as e:
                    st.sidebar.warning(f"Could not import data generator: {e}")
                    st.sidebar.info("� Trying fallback data generation...")
                    if generate_fallback_data():
                        st.sidebar.success("Fallback sample data generated!")
                        st.experimental_rerun()
                    else:
                        st.sidebar.error("Failed to generate any sample data")
                except TypeError as e:
                    if "weights" in str(e):
                        st.sidebar.warning("❌ Main data generator has weights parameter error")
                        st.sidebar.info("🔧 Using fallback data generation...")
                        if generate_fallback_data():
                            st.sidebar.success("Fallback sample data generated!")
                            st.experimental_rerun()
                        else:
                            st.sidebar.error("Failed to generate fallback data")
                    else:
                        st.sidebar.error(f"TypeError: {e}")
                except Exception as e:
                    st.sidebar.warning(f"Main data generator failed: {e}")
                    st.sidebar.info("� Trying fallback data generation...")
                    if generate_fallback_data():
                        st.sidebar.success("Fallback sample data generated!")
                        st.experimental_rerun()
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
    if st.session_state.spark_engine:
        st.sidebar.success("Spark session active")
        if st.sidebar.button("Stop Spark Session"):
            st.session_state.spark_engine.stop()
            st.session_state.spark_engine = None
            st.sidebar.success("Spark session stopped")
    else:
        if st.sidebar.button("Start Spark Session"):
            if initialize_spark():
                st.sidebar.success("Spark session started")
    
    # Page routing
    if page == "🏠 Home":
        st.header("Welcome to Data Lineage Pipeline")
        
        st.markdown("""
        ### Features:
        
        1. **Sample Data Generation**: Create realistic e-commerce datasets
        2. **Data Exploration**: Preview and analyze your datasets  
        3. **Transformation Pipeline**: Apply various PySpark transformations:
           - Data Cleaning (nulls, duplicates, outliers)
           - Aggregations (group by, window functions, pivots)
           - Joins & Integration (inner, outer, lookup enrichment)
           - Statistical Analysis
        4. **Lineage Tracking**: Automatic tracking of data flow and dependencies
        5. **Interactive Visualization**: Beautiful charts and graphs showing lineage
        
        ### Getting Started:
        1. Generate sample data using the sidebar
        2. Load datasets 
        3. Navigate to the Transformations page
        4. Select and configure transformations
        5. View lineage visualization
        
        ### Sample Datasets:
        - **Customers**: Customer information with demographics
        - **Products**: Product catalog with categories and pricing
        - **Orders**: Order transactions and metadata
        - **Order Items**: Individual line items for each order
        - **Inventory**: Inventory tracking and movements
        """)
        
        # Show loaded datasets summary
        if st.session_state.loaded_datasets:
            st.subheader("Loaded Datasets")
            for name, df in st.session_state.loaded_datasets.items():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"**{name}**: {df.count()} rows, {len(df.columns)} columns")
                with col2:
                    if st.button(f"Preview {name}"):
                        show_dataset_preview(name, df)
    
    elif page == "📊 Data Explorer":
        st.header("Data Explorer")
        
        if not st.session_state.loaded_datasets:
            st.warning("No datasets loaded. Please load datasets from the sidebar.")
        else:
            selected_dataset = st.selectbox("Select Dataset to Explore", 
                                          list(st.session_state.loaded_datasets.keys()))
            
            if selected_dataset:
                df = st.session_state.loaded_datasets[selected_dataset]
                show_dataset_preview(selected_dataset, df)
    
    elif page == "🔧 Transformations":
        render_transformation_ui()
    
    elif page == "📈 Lineage Visualization":
        render_lineage_visualization()
    
    elif page == "📝 History":
        render_transformation_history()

if __name__ == "__main__":
    main()
