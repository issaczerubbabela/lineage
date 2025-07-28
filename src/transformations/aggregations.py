"""
Aggregation and analytical transformations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from .base import BaseTransformation

class GroupByAggregationTransformation(BaseTransformation):
    """Group by aggregation transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("GroupByAggregation", lineage_tracker)
    
    def transform(self, df: DataFrame, group_by_columns: list, 
                  aggregations: dict) -> DataFrame:
        """
        Perform group by aggregation
        
        Args:
            df: Input DataFrame
            group_by_columns: List of columns to group by
            aggregations: Dictionary mapping column names to aggregation functions
                         e.g., {'sales': 'sum', 'price': 'avg', 'orders': 'count'}
        """
        # Start with groupBy
        grouped = df.groupBy(*group_by_columns)
        
        # Build aggregation expressions
        agg_exprs = []
        for column, agg_func in aggregations.items():
            if agg_func == 'sum':
                agg_exprs.append(sum(column).alias(f"{column}_sum"))
            elif agg_func == 'avg' or agg_func == 'mean':
                agg_exprs.append(avg(column).alias(f"{column}_avg"))
            elif agg_func == 'count':
                agg_exprs.append(count(column).alias(f"{column}_count"))
            elif agg_func == 'min':
                agg_exprs.append(min(column).alias(f"{column}_min"))
            elif agg_func == 'max':
                agg_exprs.append(max(column).alias(f"{column}_max"))
            elif agg_func == 'stddev':
                agg_exprs.append(stddev(column).alias(f"{column}_stddev"))
            elif agg_func == 'var':
                agg_exprs.append(variance(column).alias(f"{column}_variance"))
        
        result_df = grouped.agg(*agg_exprs)
        
        transformation_logic = f"GROUP BY {group_by_columns} AND AGGREGATE {aggregations}"
        metadata = {
            'group_by_columns': group_by_columns,
            'aggregations': aggregations,
            'input_rows': df.count(),
            'output_rows': result_df.count()
        }
        
        self._track_transformation(
            ["input_table"], "aggregated_table", 
            transformation_logic, metadata
        )
        
        return result_df

class WindowFunctionTransformation(BaseTransformation):
    """Window function transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("WindowFunction", lineage_tracker)
    
    def transform(self, df: DataFrame, partition_columns: list = None,
                  order_columns: list = None, window_functions: dict = None) -> DataFrame:
        """
        Apply window functions
        
        Args:
            df: Input DataFrame
            partition_columns: Columns to partition by
            order_columns: Columns to order by
            window_functions: Dictionary of window functions to apply
                             e.g., {'rank': 'rank', 'running_total': 'sum(amount)'}
        """
        if window_functions is None:
            window_functions = {'row_number': 'row_number'}
        
        # Create window specification
        window_spec = Window
        
        if partition_columns:
            window_spec = window_spec.partitionBy(*partition_columns)
        
        if order_columns:
            order_exprs = []
            for col_name in order_columns:
                if isinstance(col_name, str):
                    order_exprs.append(col(col_name))
                else:
                    order_exprs.append(col_name)
            window_spec = window_spec.orderBy(*order_exprs)
        
        result_df = df
        
        # Apply window functions
        for alias_name, func_expr in window_functions.items():
            if func_expr == 'row_number':
                result_df = result_df.withColumn(alias_name, row_number().over(window_spec))
            elif func_expr == 'rank':
                result_df = result_df.withColumn(alias_name, rank().over(window_spec))
            elif func_expr == 'dense_rank':
                result_df = result_df.withColumn(alias_name, dense_rank().over(window_spec))
            elif func_expr.startswith('lag('):
                # Extract column and offset from lag(column, offset)
                import re
                match = re.match(r'lag\((\w+),?\s*(\d+)?\)', func_expr)
                if match:
                    column = match.group(1)
                    offset = int(match.group(2)) if match.group(2) else 1
                    result_df = result_df.withColumn(alias_name, lag(column, offset).over(window_spec))
            elif func_expr.startswith('lead('):
                # Extract column and offset from lead(column, offset)
                match = re.match(r'lead\((\w+),?\s*(\d+)?\)', func_expr)
                if match:
                    column = match.group(1)
                    offset = int(match.group(2)) if match.group(2) else 1
                    result_df = result_df.withColumn(alias_name, lead(column, offset).over(window_spec))
            elif func_expr.startswith('sum('):
                # Running sum
                column = func_expr[4:-1]  # Extract column name from sum(column)
                window_spec_unbounded = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
                result_df = result_df.withColumn(alias_name, sum(column).over(window_spec_unbounded))
        
        transformation_logic = f"APPLY WINDOW FUNCTIONS {window_functions} PARTITIONED BY {partition_columns} ORDERED BY {order_columns}"
        metadata = {
            'partition_columns': partition_columns,
            'order_columns': order_columns,
            'window_functions': window_functions
        }
        
        self._track_transformation(
            ["input_table"], "windowed_table", 
            transformation_logic, metadata
        )
        
        return result_df

class PivotTransformation(BaseTransformation):
    """Pivot transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("Pivot", lineage_tracker)
    
    def transform(self, df: DataFrame, group_columns: list, pivot_column: str,
                  value_column: str, agg_function: str = 'sum') -> DataFrame:
        """
        Pivot data
        
        Args:
            df: Input DataFrame
            group_columns: Columns to group by
            pivot_column: Column to pivot on
            value_column: Column containing values to aggregate
            agg_function: Aggregation function to use
        """
        # Get distinct values for pivot column
        pivot_values = [row[0] for row in df.select(pivot_column).distinct().collect()]
        
        # Create pivot
        if agg_function == 'sum':
            result_df = df.groupBy(*group_columns) \
                         .pivot(pivot_column, pivot_values) \
                         .sum(value_column)
        elif agg_function == 'avg':
            result_df = df.groupBy(*group_columns) \
                         .pivot(pivot_column, pivot_values) \
                         .avg(value_column)
        elif agg_function == 'count':
            result_df = df.groupBy(*group_columns) \
                         .pivot(pivot_column, pivot_values) \
                         .count()
        elif agg_function == 'max':
            result_df = df.groupBy(*group_columns) \
                         .pivot(pivot_column, pivot_values) \
                         .max(value_column)
        elif agg_function == 'min':
            result_df = df.groupBy(*group_columns) \
                         .pivot(pivot_column, pivot_values) \
                         .min(value_column)
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")
        
        transformation_logic = f"PIVOT {pivot_column} WITH VALUES {value_column} GROUPED BY {group_columns}"
        metadata = {
            'group_columns': group_columns,
            'pivot_column': pivot_column,
            'value_column': value_column,
            'agg_function': agg_function,
            'pivot_values': pivot_values
        }
        
        self._track_transformation(
            ["input_table"], "pivoted_table", 
            transformation_logic, metadata
        )
        
        return result_df

class StatisticalAnalysisTransformation(BaseTransformation):
    """Statistical analysis transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("StatisticalAnalysis", lineage_tracker)
    
    def transform(self, df: DataFrame, numeric_columns: list) -> DataFrame:
        """
        Perform statistical analysis on numeric columns
        
        Args:
            df: Input DataFrame
            numeric_columns: List of numeric columns to analyze
        """
        stats_data = []
        
        for column in numeric_columns:
            if column in df.columns:
                # Calculate statistics
                stats = df.select(
                    lit(column).alias("column"),
                    count(column).alias("count"),
                    mean(column).alias("mean"),
                    stddev(column).alias("stddev"),
                    min(column).alias("min"),
                    max(column).alias("max"),
                    expr(f"percentile_approx({column}, 0.25)").alias("q1"),
                    expr(f"percentile_approx({column}, 0.5)").alias("median"),
                    expr(f"percentile_approx({column}, 0.75)").alias("q3"),
                    variance(column).alias("variance"),
                    skewness(column).alias("skewness"),
                    kurtosis(column).alias("kurtosis")
                ).collect()[0]
                
                stats_data.append(stats)
        
        # Create DataFrame from statistics
        result_df = df.sql_ctx.createDataFrame(stats_data)
        
        transformation_logic = f"CALCULATE STATISTICAL SUMMARY FOR {numeric_columns}"
        metadata = {
            'analyzed_columns': numeric_columns,
            'statistics': ['count', 'mean', 'stddev', 'min', 'max', 'q1', 'median', 'q3', 'variance', 'skewness', 'kurtosis']
        }
        
        self._track_transformation(
            ["input_table"], "statistical_summary_table", 
            transformation_logic, metadata
        )
        
        return result_df

class RollingAggregationTransformation(BaseTransformation):
    """Rolling aggregation transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("RollingAggregation", lineage_tracker)
    
    def transform(self, df: DataFrame, partition_columns: list, order_column: str,
                  window_size: int, agg_column: str, agg_function: str = 'avg') -> DataFrame:
        """
        Perform rolling aggregation
        
        Args:
            df: Input DataFrame
            partition_columns: Columns to partition by
            order_column: Column to order by
            window_size: Size of rolling window
            agg_column: Column to aggregate
            agg_function: Aggregation function ('avg', 'sum', 'min', 'max')
        """
        # Create window specification
        window_spec = Window.partitionBy(*partition_columns) \
                           .orderBy(order_column) \
                           .rowsBetween(-window_size + 1, 0)
        
        # Apply rolling aggregation
        if agg_function == 'avg':
            agg_expr = avg(agg_column).over(window_spec)
        elif agg_function == 'sum':
            agg_expr = sum(agg_column).over(window_spec)
        elif agg_function == 'min':
            agg_expr = min(agg_column).over(window_spec)
        elif agg_function == 'max':
            agg_expr = max(agg_column).over(window_spec)
        elif agg_function == 'count':
            agg_expr = count(agg_column).over(window_spec)
        else:
            raise ValueError(f"Unsupported aggregation function: {agg_function}")
        
        result_df = df.withColumn(f"rolling_{agg_function}_{window_size}", agg_expr)
        
        transformation_logic = f"ROLLING {agg_function.upper()} OF {agg_column} WITH WINDOW SIZE {window_size}"
        metadata = {
            'partition_columns': partition_columns,
            'order_column': order_column,
            'window_size': window_size,
            'agg_column': agg_column,
            'agg_function': agg_function
        }
        
        self._track_transformation(
            ["input_table"], "rolling_aggregated_table", 
            transformation_logic, metadata
        )
        
        return result_df
