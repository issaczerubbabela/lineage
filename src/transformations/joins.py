"""
Join and data integration transformations
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from .base import BaseTransformation

class JoinTransformation(BaseTransformation):
    """Join transformation for combining datasets"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("Join", lineage_tracker)
    
    def transform(self, left_df: DataFrame, right_df: DataFrame, 
                  join_keys: list, join_type: str = 'inner',
                  left_suffix: str = '_left', right_suffix: str = '_right') -> DataFrame:
        """
        Join two DataFrames
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: List of column names to join on
            join_type: Type of join ('inner', 'outer', 'left', 'right', 'left_semi', 'left_anti')
            left_suffix: Suffix for duplicate columns from left DataFrame
            right_suffix: Suffix for duplicate columns from right DataFrame
        """
        # Handle duplicate column names
        left_columns = set(left_df.columns)
        right_columns = set(right_df.columns)
        duplicate_columns = left_columns.intersection(right_columns) - set(join_keys)
        
        # Rename duplicate columns
        renamed_left_df = left_df
        renamed_right_df = right_df
        
        for col_name in duplicate_columns:
            renamed_left_df = renamed_left_df.withColumnRenamed(col_name, f"{col_name}{left_suffix}")
            renamed_right_df = renamed_right_df.withColumnRenamed(col_name, f"{col_name}{right_suffix}")
        
        # Perform join
        if len(join_keys) == 1:
            join_condition = renamed_left_df[join_keys[0]] == renamed_right_df[join_keys[0]]
        else:
            join_conditions = [renamed_left_df[key] == renamed_right_df[key] for key in join_keys]
            join_condition = join_conditions[0]
            for condition in join_conditions[1:]:
                join_condition = join_condition & condition
        
        result_df = renamed_left_df.join(renamed_right_df, join_condition, join_type)
        
        # Remove duplicate join key columns from right DataFrame
        if join_type in ['inner', 'left', 'outer']:
            for key in join_keys:
                if key in renamed_right_df.columns and key in result_df.columns:
                    # Count occurrences of the column
                    key_columns = [col for col in result_df.columns if col == key]
                    if len(key_columns) > 1:
                        # Drop the duplicate from right side
                        result_df = result_df.drop(renamed_right_df[key])
        
        transformation_logic = f"{join_type.upper()} JOIN ON {join_keys}"
        metadata = {
            'join_keys': join_keys,
            'join_type': join_type,
            'left_columns': list(left_columns),
            'right_columns': list(right_columns),
            'duplicate_columns': list(duplicate_columns),
            'left_rows': left_df.count(),
            'right_rows': right_df.count(),
            'result_rows': result_df.count()
        }
        
        self._track_transformation(
            ["left_table", "right_table"], "joined_table", 
            transformation_logic, metadata
        )
        
        return result_df

class UnionTransformation(BaseTransformation):
    """Union transformation for combining datasets vertically"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("Union", lineage_tracker)
    
    def transform(self, dataframes: list, union_type: str = 'union') -> DataFrame:
        """
        Union multiple DataFrames
        
        Args:
            dataframes: List of DataFrames to union
            union_type: Type of union ('union' or 'union_all')
        """
        if len(dataframes) < 2:
            raise ValueError("At least 2 DataFrames are required for union")
        
        # Start with first DataFrame
        result_df = dataframes[0]
        
        # Union with remaining DataFrames
        for df in dataframes[1:]:
            if union_type == 'union':
                result_df = result_df.union(df)
            elif union_type == 'union_all':
                result_df = result_df.unionAll(df)
            else:
                raise ValueError(f"Unsupported union type: {union_type}")
        
        # Remove duplicates if using 'union'
        if union_type == 'union':
            result_df = result_df.distinct()
        
        transformation_logic = f"{union_type.upper()} OF {len(dataframes)} DATAFRAMES"
        metadata = {
            'union_type': union_type,
            'num_dataframes': len(dataframes),
            'input_rows': [df.count() for df in dataframes],
            'result_rows': result_df.count()
        }
        
        input_tables = [f"table_{i}" for i in range(len(dataframes))]
        self._track_transformation(
            input_tables, "unioned_table", 
            transformation_logic, metadata
        )
        
        return result_df

class LookupTransformation(BaseTransformation):
    """Lookup transformation for enriching data"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("Lookup", lineage_tracker)
    
    def transform(self, main_df: DataFrame, lookup_df: DataFrame,
                  main_key: str, lookup_key: str, 
                  lookup_columns: list = None) -> DataFrame:
        """
        Enrich main DataFrame with lookup data
        
        Args:
            main_df: Main DataFrame to enrich
            lookup_df: Lookup DataFrame containing enrichment data
            main_key: Key column in main DataFrame
            lookup_key: Key column in lookup DataFrame
            lookup_columns: Columns to add from lookup DataFrame (None for all)
        """
        # Select only required columns from lookup DataFrame
        if lookup_columns is None:
            selected_lookup_df = lookup_df
        else:
            # Include lookup key and specified columns
            columns_to_select = [lookup_key] + [col for col in lookup_columns if col != lookup_key]
            selected_lookup_df = lookup_df.select(*columns_to_select)
        
        # Rename lookup key if it's different from main key
        if main_key != lookup_key:
            selected_lookup_df = selected_lookup_df.withColumnRenamed(lookup_key, main_key)
        
        # Perform left join to enrich main data
        result_df = main_df.join(selected_lookup_df, main_key, 'left')
        
        transformation_logic = f"LOOKUP ENRICHMENT ON {main_key} = {lookup_key}"
        metadata = {
            'main_key': main_key,
            'lookup_key': lookup_key,
            'lookup_columns': lookup_columns or list(lookup_df.columns),
            'main_rows': main_df.count(),
            'lookup_rows': lookup_df.count(),
            'result_rows': result_df.count()
        }
        
        self._track_transformation(
            ["main_table", "lookup_table"], "enriched_table", 
            transformation_logic, metadata
        )
        
        return result_df

class CrossJoinTransformation(BaseTransformation):
    """Cross join transformation (Cartesian product)"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("CrossJoin", lineage_tracker)
    
    def transform(self, left_df: DataFrame, right_df: DataFrame,
                  condition: str = None) -> DataFrame:
        """
        Perform cross join (Cartesian product)
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            condition: Optional condition to filter the cross join result
        """
        # Perform cross join
        result_df = left_df.crossJoin(right_df)
        
        # Apply condition if provided
        if condition:
            result_df = result_df.filter(expr(condition))
        
        transformation_logic = f"CROSS JOIN" + (f" WHERE {condition}" if condition else "")
        metadata = {
            'left_rows': left_df.count(),
            'right_rows': right_df.count(),
            'result_rows': result_df.count(),
            'condition': condition,
            'cartesian_product_size': left_df.count() * right_df.count()
        }
        
        self._track_transformation(
            ["left_table", "right_table"], "cross_joined_table", 
            transformation_logic, metadata
        )
        
        return result_df

class DataValidationTransformation(BaseTransformation):
    """Data validation transformation"""
    
    def __init__(self, lineage_tracker=None):
        super().__init__("DataValidation", lineage_tracker)
    
    def transform(self, df: DataFrame, validation_rules: dict) -> DataFrame:
        """
        Validate data based on rules and add validation columns
        
        Args:
            df: Input DataFrame
            validation_rules: Dictionary of validation rules
                             e.g., {'age_valid': 'age >= 0 AND age <= 120',
                                   'email_valid': 'email LIKE "%@%.%"'}
        """
        result_df = df
        
        # Apply validation rules
        for rule_name, rule_condition in validation_rules.items():
            validation_column = f"is_{rule_name}"
            result_df = result_df.withColumn(validation_column, expr(rule_condition))
        
        # Add overall validation column
        if validation_rules:
            rule_columns = [f"is_{rule_name}" for rule_name in validation_rules.keys()]
            overall_condition = " AND ".join(rule_columns)
            result_df = result_df.withColumn("is_valid_record", expr(overall_condition))
        
        transformation_logic = f"VALIDATE DATA WITH RULES: {list(validation_rules.keys())}"
        metadata = {
            'validation_rules': validation_rules,
            'validation_columns_added': [f"is_{rule}" for rule in validation_rules.keys()] + ['is_valid_record']
        }
        
        self._track_transformation(
            ["input_table"], "validated_table", 
            transformation_logic, metadata
        )
        
        return result_df
