"""
PySpark transformation utilities and base classes
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

class DataLineageTracker:
    """Tracks data lineage information for transformations"""
    
    def __init__(self):
        self.lineage_info = {
            'transformations': [],
            'data_sources': [],
            'data_sinks': [],
            'dependencies': []
        }
    
    def add_transformation(self, transformation_name: str, input_tables: List[str], 
                          output_table: str, transformation_logic: str,
                          metadata: Dict[str, Any] = None):
        """Add transformation information to lineage"""
        transformation = {
            'id': f"transform_{len(self.lineage_info['transformations']) + 1}",
            'name': transformation_name,
            'input_tables': input_tables,
            'output_table': output_table,
            'transformation_logic': transformation_logic,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        self.lineage_info['transformations'].append(transformation)
        
        # Add dependencies
        for input_table in input_tables:
            self.lineage_info['dependencies'].append({
                'source': input_table,
                'target': output_table,
                'transformation_id': transformation['id']
            })
    
    def add_data_source(self, source_name: str, source_type: str, location: str,
                       schema: Dict[str, str] = None):
        """Add data source information"""
        source = {
            'name': source_name,
            'type': source_type,
            'location': location,
            'schema': schema or {},
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_info['data_sources'].append(source)
    
    def add_data_sink(self, sink_name: str, sink_type: str, location: str):
        """Add data sink information"""
        sink = {
            'name': sink_name,
            'type': sink_type,
            'location': location,
            'timestamp': datetime.now().isoformat()
        }
        self.lineage_info['data_sinks'].append(sink)
    
    def export_lineage(self, output_path: str):
        """Export lineage information to JSON"""
        with open(output_path, 'w') as f:
            json.dump(self.lineage_info, f, indent=2)
    
    def get_lineage_graph(self):
        """Get lineage information as a graph structure"""
        return self.lineage_info

class BaseTransformation(ABC):
    """Base class for all data transformations"""
    
    def __init__(self, name: str, lineage_tracker: DataLineageTracker = None):
        self.name = name
        self.lineage_tracker = lineage_tracker or DataLineageTracker()
    
    @abstractmethod
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Execute the transformation"""
        pass
    
    def _track_transformation(self, input_tables: List[str], output_table: str,
                            transformation_logic: str, metadata: Dict[str, Any] = None):
        """Track transformation in lineage"""
        if self.lineage_tracker:
            self.lineage_tracker.add_transformation(
                self.name, input_tables, output_table, 
                transformation_logic, metadata
            )

class SparkTransformationEngine:
    """Main engine for executing PySpark transformations"""
    
    def __init__(self, app_name: str = "DataLineagePipeline"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.lineage_tracker = DataLineageTracker()
        self.registered_tables = {}
    
    def load_data(self, file_path: str, table_name: str, format_type: str = "csv",
                  header: bool = True, infer_schema: bool = True) -> DataFrame:
        """Load data and register as table"""
        
        if format_type.lower() == "csv":
            df = self.spark.read.option("header", header) \
                              .option("inferSchema", infer_schema) \
                              .csv(file_path)
        elif format_type.lower() == "parquet":
            df = self.spark.read.parquet(file_path)
        elif format_type.lower() == "json":
            df = self.spark.read.json(file_path)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        # Register table
        df.createOrReplaceTempView(table_name)
        self.registered_tables[table_name] = df
        
        # Track data source
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        self.lineage_tracker.add_data_source(
            table_name, format_type, file_path, schema_dict
        )
        
        return df
    
    def save_data(self, df: DataFrame, output_path: str, table_name: str,
                  format_type: str = "parquet", mode: str = "overwrite"):
        """Save data and track as sink"""
        
        if format_type.lower() == "parquet":
            df.write.mode(mode).parquet(output_path)
        elif format_type.lower() == "csv":
            df.write.mode(mode).option("header", True).csv(output_path)
        elif format_type.lower() == "json":
            df.write.mode(mode).json(output_path)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        # Track data sink
        self.lineage_tracker.add_data_sink(table_name, format_type, output_path)
    
    def execute_transformation(self, transformation: BaseTransformation,
                             input_df: DataFrame, **kwargs) -> DataFrame:
        """Execute a transformation and track lineage"""
        result_df = transformation.transform(input_df, **kwargs)
        return result_df
    
    def get_lineage_info(self):
        """Get current lineage information"""
        return self.lineage_tracker.get_lineage_graph()
    
    def export_lineage(self, output_path: str):
        """Export lineage to file"""
        self.lineage_tracker.export_lineage(output_path)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
