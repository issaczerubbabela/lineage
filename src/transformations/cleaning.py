"""
Data cleaning transformations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from .base import BaseTransformation

class RemoveNullsTransformation(BaseTransformation):
    """Remove rows with null values"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("RemoveNulls", lineage_tracker)
    
    def transform(self, df: DataFrame, columns: list = None, how: str = 'any') -> DataFrame:
        """
        Remove rows with null values
        
        Args:
            df: Input DataFrame
            columns: List of columns to check for nulls (None for all columns)
            how: 'any' to drop if any column has null, 'all' to drop if all columns are null
        """
        if columns is None:
            result_df = df.dropna(how=how)
        else:
            result_df = df.dropna(how=how, subset=columns)
        
        transformation_logic = f"DROP ROWS WHERE {how.upper()} OF {columns or 'ALL COLUMNS'} IS NULL"
        metadata = {
            'input_rows': df.count(),
            'output_rows': result_df.count(),
            'columns_checked': columns or df.columns,
            'how': how
        }
        
        self._track_transformation(
            ["input_table"], "cleaned_table", 
            transformation_logic, metadata
        )
        
        return result_df

class FillNullsTransformation(BaseTransformation):
    """Fill null values with specified values"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("FillNulls", lineage_tracker)
    
    def transform(self, df: DataFrame, fill_values: dict = None, 
                  default_value=0) -> DataFrame:
        """
        Fill null values
        
        Args:
            df: Input DataFrame
            fill_values: Dictionary mapping column names to fill values
            default_value: Default value for columns not in fill_values
        """
        if fill_values is None:
            result_df = df.fillna(default_value)
        else:
            result_df = df.fillna(fill_values)
            # Fill remaining nulls with default value
            result_df = result_df.fillna(default_value)
        
        transformation_logic = f"FILL NULL VALUES WITH {fill_values or default_value}"
        metadata = {
            'fill_values': fill_values,
            'default_value': default_value,
            'affected_columns': list(fill_values.keys()) if fill_values else df.columns
        }
        
        self._track_transformation(
            ["input_table"], "filled_table", 
            transformation_logic, metadata
        )
        
        return result_df

class RemoveDuplicatesTransformation(BaseTransformation):
    """Remove duplicate rows"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("RemoveDuplicates", lineage_tracker)
    
    def transform(self, df: DataFrame, columns: list = None) -> DataFrame:
        """
        Remove duplicate rows
        
        Args:
            df: Input DataFrame
            columns: List of columns to consider for duplicates (None for all columns)
        """
        if columns is None:
            result_df = df.distinct()
        else:
            result_df = df.dropDuplicates(columns)
        
        transformation_logic = f"REMOVE DUPLICATES BASED ON {columns or 'ALL COLUMNS'}"
        metadata = {
            'input_rows': df.count(),
            'output_rows': result_df.count(),
            'deduplication_columns': columns or df.columns
        }
        
        self._track_transformation(
            ["input_table"], "deduplicated_table", 
            transformation_logic, metadata
        )
        
        return result_df

class DataTypeConversionTransformation(BaseTransformation):
    """Convert data types of columns"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("DataTypeConversion", lineage_tracker)
    
    def transform(self, df: DataFrame, type_conversions: dict) -> DataFrame:
        """
        Convert data types
        
        Args:
            df: Input DataFrame
            type_conversions: Dictionary mapping column names to new data types
        """
        result_df = df
        
        for column, new_type in type_conversions.items():
            if column in df.columns:
                result_df = result_df.withColumn(column, col(column).cast(new_type))
        
        transformation_logic = f"CONVERT DATA TYPES: {type_conversions}"
        metadata = {
            'type_conversions': type_conversions,
            'original_schema': {field.name: str(field.dataType) for field in df.schema.fields},
            'new_schema': {field.name: str(field.dataType) for field in result_df.schema.fields}
        }
        
        self._track_transformation(
            ["input_table"], "type_converted_table", 
            transformation_logic, metadata
        )
        
        return result_df

class OutlierRemovalTransformation(BaseTransformation):
    """Remove outliers using IQR method"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("OutlierRemoval", lineage_tracker)
    
    def transform(self, df: DataFrame, columns: list, multiplier: float = 1.5) -> DataFrame:
        """
        Remove outliers using IQR method
        
        Args:
            df: Input DataFrame
            columns: List of numeric columns to check for outliers
            multiplier: IQR multiplier for outlier detection
        """
        result_df = df
        outlier_conditions = []
        
        for column in columns:
            if column in df.columns:
                # Calculate Q1 and Q3
                quantiles = df.select(
                    expr(f"percentile_approx({column}, 0.25)").alias("Q1"),
                    expr(f"percentile_approx({column}, 0.75)").alias("Q3")
                ).collect()[0]
                
                Q1, Q3 = quantiles["Q1"], quantiles["Q3"]
                IQR = Q3 - Q1
                
                lower_bound = Q1 - multiplier * IQR
                upper_bound = Q3 + multiplier * IQR
                
                # Create condition to keep non-outliers
                condition = (col(column) >= lower_bound) & (col(column) <= upper_bound)
                outlier_conditions.append(condition)
        
        # Apply all conditions
        if outlier_conditions:
            final_condition = outlier_conditions[0]
            for condition in outlier_conditions[1:]:
                final_condition = final_condition & condition
            
            result_df = result_df.filter(final_condition)
        
        transformation_logic = f"REMOVE OUTLIERS FROM {columns} USING IQR METHOD (multiplier={multiplier})"
        metadata = {
            'input_rows': df.count(),
            'output_rows': result_df.count(),
            'outlier_columns': columns,
            'iqr_multiplier': multiplier
        }
        
        self._track_transformation(
            ["input_table"], "outlier_removed_table", 
            transformation_logic, metadata
        )
        
        return result_df

class StandardizeTextTransformation(BaseTransformation):
    """Standardize text columns"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("StandardizeText", lineage_tracker)
    
    def transform(self, df: DataFrame, columns: list, operations: list = None) -> DataFrame:
        """
        Standardize text columns
        
        Args:
            df: Input DataFrame
            columns: List of text columns to standardize
            operations: List of operations ['lower', 'upper', 'trim', 'remove_special']
        """
        if operations is None:
            operations = ['lower', 'trim']
        
        result_df = df
        
        for column in columns:
            if column in df.columns:
                col_expr = col(column)
                
                if 'trim' in operations:
                    col_expr = trim(col_expr)
                if 'lower' in operations:
                    col_expr = lower(col_expr)
                elif 'upper' in operations:
                    col_expr = upper(col_expr)
                if 'remove_special' in operations:
                    col_expr = regexp_replace(col_expr, r'[^a-zA-Z0-9\s]', '')
                
                result_df = result_df.withColumn(column, col_expr)
        
        transformation_logic = f"STANDARDIZE TEXT IN {columns} WITH OPERATIONS: {operations}"
        metadata = {
            'standardized_columns': columns,
            'operations': operations
        }
        
        self._track_transformation(
            ["input_table"], "standardized_table", 
            transformation_logic, metadata
        )
        
        return result_df
