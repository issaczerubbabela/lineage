# 🔄 Data Lineage Pipeline with PySpark and Airflow

A comprehensive, production-ready data lineage pipeline system that provides interactive data transformations, real-time lineage tracking, and beautiful visualizations using PySpark and Airflow.

## 🎯 Overview

This project implements a complete data engineering solution that allows you to:

- **Transform Data Interactively** using 15+ PySpark transformation types
- **Track Data Lineage** in real-time from source to destination  
- **Visualize Data Flows** with interactive graphs and flowcharts
- **Generate Production DAGs** for Airflow deployment
- **Assess Data Quality** with comprehensive metrics
- **Export Complete Results** for further analysis

## 🏗️ Architecture

```
📦 lineage/
├── 📊 data/                     # Sample data and generation scripts
│   ├── generate_sample_data.py  # E-commerce data generator
│   └── *.csv                    # Generated sample datasets
├── 🔧 src/                      # Core transformation engine
│   ├── transformations/         # PySpark transformation modules
│   │   ├── base.py             # Abstract base classes
│   │   ├── cleaning.py         # Data cleaning transformations
│   │   ├── aggregations.py     # Aggregation operations
│   │   ├── joins.py            # Join operations
│   │   └── __init__.py         # Engine and tracker exports
│   └── lineage/                # Lineage visualization
│       ├── visualizer.py       # Interactive graph creation
│       └── __init__.py         # Visualization exports
├── 🌐 ui/                       # Streamlit web interface
│   └── app.py                  # Complete interactive UI
├── 🚀 dags/                     # Airflow DAG templates
│   └── data_lineage_dag.py     # Production DAG template
├── 📓 notebooks/                # Jupyter exploration
│   └── data_lineage_exploration.ipynb  # Complete tutorial
├── ⚙️ config/                   # Configuration files
│   ├── environment.py          # Environment settings
│   └── transformations.json    # Transformation configs
├── 📋 requirements.txt          # Python dependencies
└── 📖 README.md                # This file
```

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd lineage

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Interactive UI

```bash
# Start the Streamlit application
streamlit run ui/app.py
```

### 3. Use the Jupyter Notebook

```bash
# Start Jupyter
jupyter notebook notebooks/data_lineage_exploration.ipynb
```

## 🎛️ Features

### 📊 Data Generation
- **Realistic E-commerce Data**: Customers, products, orders, inventory
- **Configurable Size**: Generate datasets of any size
- **Relationship Integrity**: Proper foreign key relationships
- **Quality Variations**: Includes realistic data quality issues

### 🔧 Data Transformations

#### Cleaning Operations
- **Remove Duplicates**: Eliminate duplicate records
- **Handle Missing Values**: Fill, drop, or replace nulls
- **Data Type Conversions**: Convert between data types
- **Format Validation**: Validate emails, phone numbers, etc.
- **Outlier Detection**: Identify and handle outliers

#### Aggregation Operations  
- **Group By Aggregations**: Sum, average, count operations
- **Window Functions**: Running totals, rankings
- **Time-based Aggregations**: Daily, monthly summaries
- **Statistical Summaries**: Min, max, standard deviation

#### Join Operations
- **Inner Joins**: Standard inner joins with key matching
- **Outer Joins**: Left, right, and full outer joins
- **Anti Joins**: Find records that don't match
- **Complex Joins**: Multiple keys and conditions

### 🌊 Lineage Tracking
- **Real-time Tracking**: Capture lineage as transformations execute
- **Complete Metadata**: Store transformation parameters and results
- **Dependency Mapping**: Track data dependencies across pipeline
- **Impact Analysis**: Understand downstream effects of changes

### 🎨 Visualizations
- **Interactive Network Graphs**: Node-link diagrams with zoom/pan
- **Sankey Diagrams**: Flow-based visualizations
- **Dependency Matrices**: Tabular relationship views
- **Data Volume Charts**: Track data size changes
- **Quality Metrics**: Visual data quality assessments

### 🚀 Production Features
- **Airflow DAG Generation**: Auto-create production workflows
- **Configuration Export**: Save pipeline configurations
- **Result Export**: Export data and metadata
- **Error Handling**: Comprehensive error recovery
- **Performance Monitoring**: Track execution metrics

## 📖 User Guide

### Using the Streamlit UI

1. **Data Management**
   - Generate sample data or upload your own CSV files
   - Initialize Spark session for data processing
   - Load datasets into Spark DataFrames

2. **Building Transformations**
   - Select input dataset and transformation type
   - Configure transformation parameters
   - Execute step-by-step or build complete pipelines
   - View results and lineage in real-time

3. **Lineage Visualization**
   - Interactive network graphs showing data flow
   - Detailed transformation history
   - Export visualizations as HTML files

4. **Advanced Analytics**
   - Data quality assessment across all datasets
   - Performance metrics for transformations
   - Volume tracking through pipeline stages

### Using the Jupyter Notebook

The notebook provides a comprehensive tutorial covering:

1. **Environment Setup**: Install and configure all dependencies
2. **Data Loading**: Generate and load sample datasets
3. **Transformation Building**: Create complex transformation pipelines
4. **Lineage Tracking**: Capture and visualize data lineage
5. **Quality Assessment**: Analyze data quality metrics
6. **Visualization Creation**: Build interactive charts and graphs
7. **Production Deployment**: Generate Airflow DAGs
8. **Result Export**: Export complete pipeline results

### Sample Transformations

```python
# Example: Clean customer data and join with orders
from src.transformations import SparkTransformationEngine

engine = SparkTransformationEngine("MyPipeline")

# Load data
customers = engine.load_data("data/customers.csv", "customers", "csv")
orders = engine.load_data("data/orders.csv", "orders", "csv")

# Clean customers: remove duplicates and nulls
clean_customers = engine.apply_transformation(
    "RemoveDuplicates", customers, "clean_customers"
)

# Join customers with orders
customer_orders = engine.apply_transformation(
    "InnerJoin", clean_customers, "customer_orders",
    right_dataset="orders", join_keys=["customer_id"]
)

# View lineage
lineage = engine.get_lineage()
engine.visualize_lineage(lineage)
```

## 🔧 Configuration

### Environment Settings

Edit `config/environment.py` to configure:

```python
# Spark configuration
SPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}

# Data paths
DATA_DIR = "data"
OUTPUT_DIR = "output"
EXPORT_DIR = "exports"

# Visualization settings
VIZ_CONFIG = {
    "graph_layout": "spring",
    "node_size": 20,
    "edge_width": 2
}
```

### Transformation Configuration

Edit `config/transformations.json` to customize available transformations:

```json
{
  "cleaning": {
    "enabled": ["RemoveDuplicates", "HandleMissingValues", "ValidateFormat"],
    "default_params": {
      "null_strategy": "drop",
      "duplicate_subset": null
    }
  },
  "aggregations": {
    "enabled": ["GroupByAggregation", "WindowFunction"],
    "default_params": {
      "agg_function": "sum"
    }
  }
}
```

## 🚀 Production Deployment

### Airflow DAG Generation

The system can generate production-ready Airflow DAGs:

```python
# Generate DAG from executed pipeline
dag_code = generate_airflow_dag(execution_results)

# Save to Airflow dags folder
with open("dags/generated_pipeline.py", "w") as f:
    f.write(dag_code)
```

### Deployment Checklist

- [ ] Configure Spark cluster connection
- [ ] Set up data source and sink connections
- [ ] Configure Airflow variables and connections
- [ ] Test DAG in development environment
- [ ] Deploy to production Airflow instance
- [ ] Set up monitoring and alerting
- [ ] Configure data quality checks

## 📊 Sample Datasets

The system includes realistic e-commerce sample data:

### Customers (10,000 records)
- customer_id, first_name, last_name, email, phone
- address, city, state, zip_code, country
- age, gender, registration_date, customer_tier, is_active

### Products (1,000 records)  
- product_id, product_name, category, price, cost
- weight, dimensions, color, material, stock_quantity
- supplier_id, launch_date, rating, is_discontinued

### Orders (50,000 records)
- order_id, customer_id, order_date, total_amount
- status, shipping_address, payment_method

### Order Items (150,000 records)
- order_item_id, order_id, product_id, quantity
- unit_price, discount

### Inventory (1,000 records)
- product_id, warehouse_location, quantity_available
- reserved_quantity, last_updated, reorder_level

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🐛 Troubleshooting

### Common Issues

**ImportError: No module named 'pyspark'**
```bash
pip install pyspark==3.5.0
```

**Spark session fails to start**
- Check Java installation (Java 8 or 11 required)
- Verify JAVA_HOME environment variable
- Ensure sufficient memory allocation

**Weights parameter error**
- This has been fixed in the latest version
- Restart the application if you encounter this error

**Module import errors**
- Ensure you're running from the project root directory
- Check that all dependencies are installed
- Verify Python path configuration

### Getting Help

- Check the Jupyter notebook for detailed examples
- Review the Streamlit UI tooltips and help text
- Check the console output for detailed error messages
- Open an issue on GitHub for bugs or feature requests

## 🎯 Roadmap

### Upcoming Features

- [ ] **Real-time Streaming**: Support for Kafka and streaming data
- [ ] **ML Pipeline Integration**: Integrate with MLflow and model training
- [ ] **Advanced Scheduling**: More complex Airflow scheduling options
- [ ] **Cloud Deployment**: AWS, GCP, and Azure deployment templates
- [ ] **API Interface**: REST API for programmatic access
- [ ] **Advanced Visualizations**: 3D graphs and enhanced interactivity

### Performance Improvements

- [ ] **Lazy Evaluation**: Optimize transformation execution
- [ ] **Caching**: Intelligent caching of intermediate results
- [ ] **Parallel Processing**: Enhanced parallel execution
- [ ] **Memory Optimization**: Reduce memory footprint

## 💡 Tips and Best Practices

### Performance Optimization
- Use appropriate partitioning for large datasets
- Cache intermediate results for complex pipelines
- Monitor Spark UI for performance bottlenecks
- Optimize join operations with broadcast hints

### Data Quality
- Implement data validation at ingestion points
- Use schema enforcement for critical datasets
- Set up automated data quality monitoring
- Document data quality rules and exceptions

### Lineage Management
- Use descriptive names for transformations
- Document business logic for complex operations
- Regularly export lineage metadata
- Integrate with data catalogs for enterprise use

---

Built with ❤️ for the data engineering community. Happy data pipelining! 🚀
