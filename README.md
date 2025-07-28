# Data Lineage Pipeline with PySpark and Airflow

A comprehensive data pipeline project that demonstrates data transformations using PySpark with complete lineage tracking through Apache Airflow and an interactive UI for visualization.

## Features

- **PySpark Data Transformations**: Multiple transformation options including filtering, aggregation, joins, window functions, and more
- **Data Lineage Tracking**: Complete tracking of data flow from source to destination using Airflow
- **Interactive UI**: Streamlit-based interface for selecting transformations and visualizing lineage
- **Sample Data**: Rich, realistic datasets for demonstration
- **Visualization**: Beautiful flowcharts and diagrams showing data transformation flow
- **Configurable Pipeline**: Easy-to-modify transformation pipeline

## Project Structure

```
lineage/
├── data/                    # Sample datasets
├── dags/                    # Airflow DAGs
├── src/                     # Source code
│   ├── transformations/     # PySpark transformation modules
│   ├── lineage/            # Lineage tracking utilities
│   └── ui/                 # Streamlit UI components
├── config/                 # Configuration files
├── notebooks/              # Jupyter notebooks for exploration
└── ui/                     # Main UI application
```

## Getting Started

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Initialize Airflow:
   ```bash
   airflow db init
   ```

3. Start Airflow webserver and scheduler:
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

4. Run the UI:
   ```bash
   streamlit run ui/app.py
   ```

## Data Transformations Available

- Data Cleaning (null handling, duplicates)
- Filtering and Selection
- Aggregations and Grouping
- Joins (inner, outer, left, right)
- Window Functions
- Data Type Conversions
- Custom Business Logic
- Statistical Analysis
- Time Series Operations

## Lineage Visualization

The project provides multiple visualization options:
- Interactive flowcharts
- Dependency graphs
- Data flow diagrams
- Transformation impact analysis
