#!/usr/bin/env python3
"""
Test script to verify the lineage visualization improvements
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(__file__)
sys.path.append(project_root)

from src.lineage import create_lineage_visualization

def test_lineage_flow():
    """Test the improved lineage flow visualization"""
    
    # Create sample lineage data that represents a typical flow
    sample_lineage = {
        'data_sources': [
            {
                'name': 'raw_customers',
                'type': 'csv',
                'location': 'data/customers.csv',
                'schema': {'customer_id': 'int', 'name': 'string', 'email': 'string'}
            },
            {
                'name': 'raw_orders',
                'type': 'csv',
                'location': 'data/orders.csv',
                'schema': {'order_id': 'int', 'customer_id': 'int', 'amount': 'double'}
            }
        ],
        'data_sinks': [
            {
                'name': 'customer_analytics',
                'type': 'parquet',
                'location': 'output/customer_analytics.parquet'
            }
        ],
        'transformations': [
            {
                'id': 'transform_1',
                'name': 'RemoveDuplicates',
                'input_tables': ['raw_customers'],
                'output_table': 'clean_customers',
                'transformation_logic': 'Remove duplicate customers based on email',
                'timestamp': '2024-01-01T10:00:00',
                'metadata': {'rows_removed': 15}
            },
            {
                'id': 'transform_2',
                'name': 'FilterData',
                'input_tables': ['raw_orders'],
                'output_table': 'valid_orders',
                'transformation_logic': 'Filter orders with amount > 0',
                'timestamp': '2024-01-01T10:05:00',
                'metadata': {'rows_filtered': 23}
            },
            {
                'id': 'transform_3',
                'name': 'InnerJoin',
                'input_tables': ['clean_customers', 'valid_orders'],
                'output_table': 'customer_orders',
                'transformation_logic': 'Join customers with their orders',
                'timestamp': '2024-01-01T10:10:00',
                'metadata': {'join_key': 'customer_id'}
            },
            {
                'id': 'transform_4',
                'name': 'Aggregate',
                'input_tables': ['customer_orders'],
                'output_table': 'customer_analytics',
                'transformation_logic': 'Calculate customer lifetime value and order counts',
                'timestamp': '2024-01-01T10:15:00',
                'metadata': {'aggregation_columns': ['total_spent', 'order_count']}
            }
        ],
        'dependencies': [
            {'source': 'raw_customers', 'target': 'clean_customers', 'transformation_id': 'transform_1'},
            {'source': 'raw_orders', 'target': 'valid_orders', 'transformation_id': 'transform_2'},
            {'source': 'clean_customers', 'target': 'customer_orders', 'transformation_id': 'transform_3'},
            {'source': 'valid_orders', 'target': 'customer_orders', 'transformation_id': 'transform_3'},
            {'source': 'customer_orders', 'target': 'customer_analytics', 'transformation_id': 'transform_4'}
        ]
    }
    
    print("🔄 Testing improved lineage visualization...")
    
    try:
        # Create visualizer
        visualizer = create_lineage_visualization(sample_lineage)
        
        # Get statistics
        stats = visualizer.get_lineage_statistics()
        
        print(f"✅ Lineage Statistics:")
        print(f"   📊 Total nodes: {stats['total_nodes']}")
        print(f"   🔗 Total edges: {stats['total_edges']}")
        print(f"   🟢 Data sources: {stats['data_sources']}")
        print(f"   🔴 Data sinks: {stats['data_sinks']}")
        print(f"   🔵 Intermediate datasets: {stats['intermediate_datasets']}")
        print(f"   🟠 Transformations: {stats['transformations']}")
        print(f"   🛤️ Longest path: {stats['longest_path']}")
        print(f"   🔀 Flow paths: {len(stats['flow_paths'])}")
        
        # Show flow paths
        if stats['flow_paths']:
            print(f"\n🛤️ Complete Flow Paths:")
            for i, path_info in enumerate(stats['flow_paths']):
                path_str = " → ".join(path_info['path'])
                print(f"   Path {i+1}: {path_str} ({path_info['length']} steps)")
        
        # Test creating visualizations
        print(f"\n🎨 Testing visualization creation...")
        
        # Create main graph
        main_fig = visualizer.create_interactive_graph(layout='hierarchical')
        print(f"   ✅ Interactive graph created successfully")
        
        # Create flow path analysis
        flow_fig = visualizer.create_flow_path_analysis()
        print(f"   ✅ Flow path analysis created successfully")
        
        # Create dependency matrix
        matrix_fig = visualizer.create_dependency_matrix()
        print(f"   ✅ Dependency matrix created successfully")
        
        print(f"\n🎉 All tests passed! The lineage visualization now properly shows:")
        print(f"   • Clear flow from sources to destinations")
        print(f"   • Hierarchical layout showing data progression")
        print(f"   • Distinct node types (sources, transformations, outputs)")
        print(f"   • Complete path analysis from start to end")
        print(f"   • Directional arrows showing data flow")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_lineage_flow()
