"""
Transformation modules initialization
"""

from .base import BaseTransformation, SparkTransformationEngine, DataLineageTracker
from .cleaning import (
    RemoveNullsTransformation,
    FillNullsTransformation,
    RemoveDuplicatesTransformation,
    DataTypeConversionTransformation,
    OutlierRemovalTransformation,
    StandardizeTextTransformation
)
from .aggregations import (
    GroupByAggregationTransformation,
    WindowFunctionTransformation,
    PivotTransformation,
    StatisticalAnalysisTransformation,
    RollingAggregationTransformation
)
from .joins import (
    JoinTransformation,
    UnionTransformation,
    LookupTransformation,
    CrossJoinTransformation,
    DataValidationTransformation
)

# Available transformations for the UI
AVAILABLE_TRANSFORMATIONS = {
    'Data Cleaning': {
        'remove_nulls': {
            'class': RemoveNullsTransformation,
            'name': 'Remove Null Values',
            'description': 'Remove rows containing null values',
            'parameters': {
                'columns': {'type': 'multiselect', 'description': 'Columns to check (leave empty for all)'},
                'how': {'type': 'select', 'options': ['any', 'all'], 'default': 'any', 'description': 'Remove if any/all columns are null'}
            }
        },
        'fill_nulls': {
            'class': FillNullsTransformation,
            'name': 'Fill Null Values',
            'description': 'Fill null values with specified values',
            'parameters': {
                'fill_values': {'type': 'dict', 'description': 'Column-specific fill values (JSON format)'},
                'default_value': {'type': 'text', 'default': '0', 'description': 'Default fill value'}
            }
        },
        'remove_duplicates': {
            'class': RemoveDuplicatesTransformation,
            'name': 'Remove Duplicates',
            'description': 'Remove duplicate rows',
            'parameters': {
                'columns': {'type': 'multiselect', 'description': 'Columns to consider for duplicates (leave empty for all)'}
            }
        },
        'convert_types': {
            'class': DataTypeConversionTransformation,
            'name': 'Convert Data Types',
            'description': 'Convert column data types',
            'parameters': {
                'type_conversions': {'type': 'dict', 'description': 'Column type conversions (JSON format: {"col": "string"})'}
            }
        },
        'remove_outliers': {
            'class': OutlierRemovalTransformation,
            'name': 'Remove Outliers',
            'description': 'Remove outliers using IQR method',
            'parameters': {
                'columns': {'type': 'multiselect', 'description': 'Numeric columns to check for outliers'},
                'multiplier': {'type': 'number', 'default': 1.5, 'description': 'IQR multiplier'}
            }
        },
        'standardize_text': {
            'class': StandardizeTextTransformation,
            'name': 'Standardize Text',
            'description': 'Standardize text columns',
            'parameters': {
                'columns': {'type': 'multiselect', 'description': 'Text columns to standardize'},
                'operations': {'type': 'multiselect', 'options': ['lower', 'upper', 'trim', 'remove_special'], 'default': ['lower', 'trim'], 'description': 'Standardization operations'}
            }
        }
    },
    'Aggregations': {
        'group_by': {
            'class': GroupByAggregationTransformation,
            'name': 'Group By Aggregation',
            'description': 'Group data and perform aggregations',
            'parameters': {
                'group_by_columns': {'type': 'multiselect', 'description': 'Columns to group by'},
                'aggregations': {'type': 'dict', 'description': 'Aggregations (JSON format: {"col": "sum"})'}
            }
        },
        'window_functions': {
            'class': WindowFunctionTransformation,
            'name': 'Window Functions',
            'description': 'Apply window functions',
            'parameters': {
                'partition_columns': {'type': 'multiselect', 'description': 'Columns to partition by'},
                'order_columns': {'type': 'multiselect', 'description': 'Columns to order by'},
                'window_functions': {'type': 'dict', 'description': 'Window functions (JSON format: {"alias": "function"})'}
            }
        },
        'pivot': {
            'class': PivotTransformation,
            'name': 'Pivot Data',
            'description': 'Pivot data from rows to columns',
            'parameters': {
                'group_columns': {'type': 'multiselect', 'description': 'Columns to group by'},
                'pivot_column': {'type': 'select', 'description': 'Column to pivot on'},
                'value_column': {'type': 'select', 'description': 'Column containing values'},
                'agg_function': {'type': 'select', 'options': ['sum', 'avg', 'count', 'min', 'max'], 'default': 'sum', 'description': 'Aggregation function'}
            }
        },
        'statistical_analysis': {
            'class': StatisticalAnalysisTransformation,
            'name': 'Statistical Analysis',
            'description': 'Calculate statistical summaries',
            'parameters': {
                'numeric_columns': {'type': 'multiselect', 'description': 'Numeric columns to analyze'}
            }
        },
        'rolling_aggregation': {
            'class': RollingAggregationTransformation,
            'name': 'Rolling Aggregation',
            'description': 'Perform rolling window aggregations',
            'parameters': {
                'partition_columns': {'type': 'multiselect', 'description': 'Columns to partition by'},
                'order_column': {'type': 'select', 'description': 'Column to order by'},
                'window_size': {'type': 'number', 'default': 3, 'description': 'Window size'},
                'agg_column': {'type': 'select', 'description': 'Column to aggregate'},
                'agg_function': {'type': 'select', 'options': ['avg', 'sum', 'min', 'max', 'count'], 'default': 'avg', 'description': 'Aggregation function'}
            }
        }
    },
    'Joins & Integration': {
        'join': {
            'class': JoinTransformation,
            'name': 'Join Tables',
            'description': 'Join two tables',
            'parameters': {
                'join_keys': {'type': 'multiselect', 'description': 'Columns to join on'},
                'join_type': {'type': 'select', 'options': ['inner', 'outer', 'left', 'right', 'left_semi', 'left_anti'], 'default': 'inner', 'description': 'Type of join'},
                'left_suffix': {'type': 'text', 'default': '_left', 'description': 'Suffix for duplicate columns from left table'},
                'right_suffix': {'type': 'text', 'default': '_right', 'description': 'Suffix for duplicate columns from right table'}
            }
        },
        'union': {
            'class': UnionTransformation,
            'name': 'Union Tables',
            'description': 'Union multiple tables vertically',
            'parameters': {
                'union_type': {'type': 'select', 'options': ['union', 'union_all'], 'default': 'union', 'description': 'Type of union'}
            }
        },
        'lookup': {
            'class': LookupTransformation,
            'name': 'Lookup Enrichment',
            'description': 'Enrich data with lookup table',
            'parameters': {
                'main_key': {'type': 'select', 'description': 'Key column in main table'},
                'lookup_key': {'type': 'select', 'description': 'Key column in lookup table'},
                'lookup_columns': {'type': 'multiselect', 'description': 'Columns to add from lookup table'}
            }
        },
        'data_validation': {
            'class': DataValidationTransformation,
            'name': 'Data Validation',
            'description': 'Validate data based on business rules',
            'parameters': {
                'validation_rules': {'type': 'dict', 'description': 'Validation rules (JSON format: {"rule_name": "condition"})'}
            }
        }
    }
}

__all__ = [
    'BaseTransformation',
    'SparkTransformationEngine', 
    'DataLineageTracker',
    'RemoveNullsTransformation',
    'FillNullsTransformation',
    'RemoveDuplicatesTransformation',
    'DataTypeConversionTransformation',
    'OutlierRemovalTransformation',
    'StandardizeTextTransformation',
    'GroupByAggregationTransformation',
    'WindowFunctionTransformation',
    'PivotTransformation',
    'StatisticalAnalysisTransformation',
    'RollingAggregationTransformation',
    'JoinTransformation',
    'UnionTransformation',
    'LookupTransformation',
    'CrossJoinTransformation',
    'DataValidationTransformation',
    'AVAILABLE_TRANSFORMATIONS'
]
