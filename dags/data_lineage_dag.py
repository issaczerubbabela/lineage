"""
Airflow DAG for Data Lineage Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys
import json

# Add project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(project_root)

# Import our modules
from src.transformations import SparkTransformationEngine, AVAILABLE_TRANSFORMATIONS
from src.lineage import create_lineage_visualization

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'data_lineage_pipeline',
    default_args=default_args,
    description='Data transformation pipeline with lineage tracking',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['data_engineering', 'lineage', 'pyspark'],
)

def load_data_sources(**context):
    """Load all data sources"""
    
    engine = SparkTransformationEngine("DataLineagePipeline_LoadData")
    
    try:
        data_dir = os.path.join(project_root, 'data')
        
        # Load all CSV files
        datasets = {}
        
        # Load customers
        customers_path = os.path.join(data_dir, 'customers.csv')
        if os.path.exists(customers_path):
            customers_df = engine.load_data(customers_path, 'customers', 'csv')
            datasets['customers'] = customers_df.count()
            print(f"Loaded customers: {datasets['customers']} records")
        
        # Load products
        products_path = os.path.join(data_dir, 'products.csv')
        if os.path.exists(products_path):
            products_df = engine.load_data(products_path, 'products', 'csv')
            datasets['products'] = products_df.count()
            print(f"Loaded products: {datasets['products']} records")
        
        # Load orders
        orders_path = os.path.join(data_dir, 'orders.csv')
        if os.path.exists(orders_path):
            orders_df = engine.load_data(orders_path, 'orders', 'csv')
            datasets['orders'] = orders_df.count()
            print(f"Loaded orders: {datasets['orders']} records")
        
        # Load order items
        order_items_path = os.path.join(data_dir, 'order_items.csv')
        if os.path.exists(order_items_path):
            order_items_df = engine.load_data(order_items_path, 'order_items', 'csv')
            datasets['order_items'] = order_items_df.count()
            print(f"Loaded order_items: {datasets['order_items']} records")
        
        # Load inventory
        inventory_path = os.path.join(data_dir, 'inventory.csv')
        if os.path.exists(inventory_path):
            inventory_df = engine.load_data(inventory_path, 'inventory', 'csv')
            datasets['inventory'] = inventory_df.count()
            print(f"Loaded inventory: {datasets['inventory']} records")
        
        # Export lineage
        lineage_output = os.path.join(project_root, 'output', 'load_lineage.json')
        os.makedirs(os.path.dirname(lineage_output), exist_ok=True)
        engine.export_lineage(lineage_output)
        
        # Store dataset info for next tasks
        context['task_instance'].xcom_push(key='datasets', value=datasets)
        
        return f"Successfully loaded {len(datasets)} datasets"
    
    finally:
        engine.stop()

def clean_data(**context):
    """Apply data cleaning transformations"""
    
    engine = SparkTransformationEngine("DataLineagePipeline_CleanData")
    
    try:
        data_dir = os.path.join(project_root, 'data')
        output_dir = os.path.join(project_root, 'output', 'cleaned')
        os.makedirs(output_dir, exist_ok=True)
        
        # Load datasets
        customers_df = engine.load_data(os.path.join(data_dir, 'customers.csv'), 'customers', 'csv')
        products_df = engine.load_data(os.path.join(data_dir, 'products.csv'), 'products', 'csv')
        orders_df = engine.load_data(os.path.join(data_dir, 'orders.csv'), 'orders', 'csv')
        
        # Import cleaning transformations
        from src.transformations.cleaning import (
            RemoveNullsTransformation,
            FillNullsTransformation,
            StandardizeTextTransformation
        )
        
        # Clean customers data
        remove_nulls = RemoveNullsTransformation(engine.lineage_tracker)
        customers_clean = engine.execute_transformation(
            remove_nulls, customers_df, 
            columns=['email', 'customer_id'], how='any'
        )
        
        standardize_text = StandardizeTextTransformation(engine.lineage_tracker)
        customers_clean = engine.execute_transformation(
            standardize_text, customers_clean,
            columns=['first_name', 'last_name', 'city', 'state'],
            operations=['lower', 'trim']
        )
        
        # Clean products data
        fill_nulls = FillNullsTransformation(engine.lineage_tracker)
        products_clean = engine.execute_transformation(
            fill_nulls, products_df,
            fill_values={'rating': 0.0, 'stock_quantity': 0}
        )
        
        # Clean orders data
        orders_clean = engine.execute_transformation(
            remove_nulls, orders_df,
            columns=['order_id', 'customer_id'], how='any'
        )
        
        # Save cleaned data
        engine.save_data(customers_clean, os.path.join(output_dir, 'customers_clean'), 'customers_clean', 'parquet')
        engine.save_data(products_clean, os.path.join(output_dir, 'products_clean'), 'products_clean', 'parquet')
        engine.save_data(orders_clean, os.path.join(output_dir, 'orders_clean'), 'orders_clean', 'parquet')
        
        # Export lineage
        lineage_output = os.path.join(project_root, 'output', 'clean_lineage.json')
        engine.export_lineage(lineage_output)
        
        return "Data cleaning completed successfully"
    
    finally:
        engine.stop()

def aggregate_data(**context):
    """Apply aggregation transformations"""
    
    engine = SparkTransformationEngine("DataLineagePipeline_AggregateData")
    
    try:
        cleaned_dir = os.path.join(project_root, 'output', 'cleaned')
        output_dir = os.path.join(project_root, 'output', 'aggregated')
        os.makedirs(output_dir, exist_ok=True)
        
        # Load cleaned data
        customers_df = engine.load_data(os.path.join(cleaned_dir, 'customers_clean'), 'customers_clean', 'parquet')
        orders_df = engine.load_data(os.path.join(cleaned_dir, 'orders_clean'), 'orders_clean', 'parquet')
        
        # Import aggregation transformations
        from src.transformations.aggregations import GroupByAggregationTransformation
        
        # Customer aggregations
        group_agg = GroupByAggregationTransformation(engine.lineage_tracker)
        
        customer_summary = engine.execute_transformation(
            group_agg, customers_df,
            group_by_columns=['customer_tier', 'state'],
            aggregations={'age': 'avg', 'customer_id': 'count'}
        )
        
        # Order aggregations
        order_summary = engine.execute_transformation(
            group_agg, orders_df,
            group_by_columns=['order_status', 'payment_method'],
            aggregations={'total_amount': 'sum', 'order_id': 'count', 'shipping_cost': 'avg'}
        )
        
        # Save aggregated data
        engine.save_data(customer_summary, os.path.join(output_dir, 'customer_summary'), 'customer_summary', 'parquet')
        engine.save_data(order_summary, os.path.join(output_dir, 'order_summary'), 'order_summary', 'parquet')
        
        # Export lineage
        lineage_output = os.path.join(project_root, 'output', 'aggregate_lineage.json')
        engine.export_lineage(lineage_output)
        
        return "Data aggregation completed successfully"
    
    finally:
        engine.stop()

def join_data(**context):
    """Apply join transformations"""
    
    engine = SparkTransformationEngine("DataLineagePipeline_JoinData")
    
    try:
        data_dir = os.path.join(project_root, 'data')
        cleaned_dir = os.path.join(project_root, 'output', 'cleaned')
        output_dir = os.path.join(project_root, 'output', 'joined')
        os.makedirs(output_dir, exist_ok=True)
        
        # Load data
        customers_df = engine.load_data(os.path.join(cleaned_dir, 'customers_clean'), 'customers_clean', 'parquet')
        orders_df = engine.load_data(os.path.join(cleaned_dir, 'orders_clean'), 'orders_clean', 'parquet')
        order_items_df = engine.load_data(os.path.join(data_dir, 'order_items.csv'), 'order_items', 'csv')
        products_df = engine.load_data(os.path.join(data_dir, 'products.csv'), 'products', 'csv')
        
        # Import join transformations
        from src.transformations.joins import JoinTransformation
        
        join_transform = JoinTransformation(engine.lineage_tracker)
        
        # Join orders with customers
        customer_orders = engine.execute_transformation(
            join_transform, orders_df, customers_df,
            join_keys=['customer_id'], join_type='inner'
        )
        
        # Join order items with products
        order_item_products = engine.execute_transformation(
            join_transform, order_items_df, products_df,
            join_keys=['product_id'], join_type='inner'
        )
        
        # Join customer orders with order items
        full_order_data = engine.execute_transformation(
            join_transform, customer_orders, order_item_products,
            join_keys=['order_id'], join_type='inner'
        )
        
        # Save joined data
        engine.save_data(customer_orders, os.path.join(output_dir, 'customer_orders'), 'customer_orders', 'parquet')
        engine.save_data(order_item_products, os.path.join(output_dir, 'order_item_products'), 'order_item_products', 'parquet')
        engine.save_data(full_order_data, os.path.join(output_dir, 'full_order_data'), 'full_order_data', 'parquet')
        
        # Export lineage
        lineage_output = os.path.join(project_root, 'output', 'join_lineage.json')
        engine.export_lineage(lineage_output)
        
        return "Data joining completed successfully"
    
    finally:
        engine.stop()

def generate_final_reports(**context):
    """Generate final business reports"""
    
    engine = SparkTransformationEngine("DataLineagePipeline_FinalReports")
    
    try:
        joined_dir = os.path.join(project_root, 'output', 'joined')
        output_dir = os.path.join(project_root, 'output', 'reports')
        os.makedirs(output_dir, exist_ok=True)
        
        # Load joined data
        full_order_data = engine.load_data(os.path.join(joined_dir, 'full_order_data'), 'full_order_data', 'parquet')
        
        # Import transformations
        from src.transformations.aggregations import GroupByAggregationTransformation, WindowFunctionTransformation
        
        group_agg = GroupByAggregationTransformation(engine.lineage_tracker)
        window_func = WindowFunctionTransformation(engine.lineage_tracker)
        
        # Customer revenue report
        customer_revenue = engine.execute_transformation(
            group_agg, full_order_data,
            group_by_columns=['customer_id', 'first_name', 'last_name', 'customer_tier'],
            aggregations={'line_total': 'sum', 'quantity': 'sum', 'order_id': 'count'}
        )
        
        # Product performance report
        product_performance = engine.execute_transformation(
            group_agg, full_order_data,
            group_by_columns=['product_id', 'product_name', 'category'],
            aggregations={'line_total': 'sum', 'quantity': 'sum', 'order_id': 'count'}
        )
        
        # Monthly sales trend
        monthly_sales = engine.execute_transformation(
            group_agg, full_order_data,
            group_by_columns=['order_date'],
            aggregations={'line_total': 'sum', 'quantity': 'sum', 'order_id': 'count'}
        )
        
        # Add ranking to customer revenue
        customer_revenue_ranked = engine.execute_transformation(
            window_func, customer_revenue,
            partition_columns=[],
            order_columns=['line_total_sum'],
            window_functions={'revenue_rank': 'rank', 'revenue_dense_rank': 'dense_rank'}
        )
        
        # Save reports
        engine.save_data(customer_revenue_ranked, os.path.join(output_dir, 'customer_revenue_report'), 'customer_revenue_report', 'parquet')
        engine.save_data(product_performance, os.path.join(output_dir, 'product_performance_report'), 'product_performance_report', 'parquet')
        engine.save_data(monthly_sales, os.path.join(output_dir, 'monthly_sales_report'), 'monthly_sales_report', 'parquet')
        
        # Export final lineage
        lineage_output = os.path.join(project_root, 'output', 'final_lineage.json')
        engine.export_lineage(lineage_output)
        
        return "Final reports generated successfully"
    
    finally:
        engine.stop()

def consolidate_lineage(**context):
    """Consolidate all lineage information"""
    
    try:
        output_dir = os.path.join(project_root, 'output')
        
        # Read all lineage files
        lineage_files = [
            'load_lineage.json',
            'clean_lineage.json',
            'aggregate_lineage.json',
            'join_lineage.json',
            'final_lineage.json'
        ]
        
        consolidated_lineage = {
            'transformations': [],
            'data_sources': [],
            'data_sinks': [],
            'dependencies': []
        }
        
        for filename in lineage_files:
            filepath = os.path.join(output_dir, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    lineage_data = json.load(f)
                
                # Merge lineage data
                consolidated_lineage['transformations'].extend(lineage_data.get('transformations', []))
                consolidated_lineage['data_sources'].extend(lineage_data.get('data_sources', []))
                consolidated_lineage['data_sinks'].extend(lineage_data.get('data_sinks', []))
                consolidated_lineage['dependencies'].extend(lineage_data.get('dependencies', []))
        
        # Remove duplicates and save consolidated lineage
        consolidated_path = os.path.join(output_dir, 'consolidated_lineage.json')
        with open(consolidated_path, 'w') as f:
            json.dump(consolidated_lineage, f, indent=2)
        
        # Generate visualization
        visualizer = create_lineage_visualization(consolidated_lineage)
        stats = visualizer.get_lineage_statistics()
        
        print(f"Lineage consolidation completed:")
        print(f"- Total nodes: {stats['total_nodes']}")
        print(f"- Total edges: {stats['total_edges']}")
        print(f"- Data sources: {stats['data_sources']}")
        print(f"- Data sinks: {stats['data_sinks']}")
        print(f"- Transformations: {stats['transformations']}")
        
        return "Lineage consolidation completed successfully"
    
    except Exception as e:
        print(f"Error in lineage consolidation: {str(e)}")
        raise

# Define tasks
load_data_task = PythonOperator(
    task_id='load_data_sources',
    python_callable=load_data_sources,
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

aggregate_data_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    dag=dag,
)

join_data_task = PythonOperator(
    task_id='join_data',
    python_callable=join_data,
    dag=dag,
)

generate_reports_task = PythonOperator(
    task_id='generate_final_reports',
    python_callable=generate_final_reports,
    dag=dag,
)

consolidate_lineage_task = PythonOperator(
    task_id='consolidate_lineage',
    python_callable=consolidate_lineage,
    dag=dag,
)

# Define task dependencies
load_data_task >> clean_data_task
clean_data_task >> [aggregate_data_task, join_data_task]
join_data_task >> generate_reports_task
[aggregate_data_task, generate_reports_task] >> consolidate_lineage_task
