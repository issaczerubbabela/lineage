"""
Lineage visualization and graph utilities
"""

import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
from typing import Dict, List, Any, Tuple
import pandas as pd

class LineageVisualizer:
    """Visualizes data lineage as interactive graphs"""
    
    def __init__(self, lineage_data: Dict[str, Any]):
        self.lineage_data = lineage_data
        self.graph = self._build_graph()
    
    def _build_graph(self) -> nx.DiGraph:
        """Build NetworkX graph from lineage data"""
        graph = nx.DiGraph()
        
        # Add data sources as nodes
        for source in self.lineage_data.get('data_sources', []):
            graph.add_node(
                source['name'],
                node_type='source',
                type=source['type'],
                location=source['location'],
                schema=source.get('schema', {}),
                color='lightblue'
            )
        
        # Add data sinks as nodes
        for sink in self.lineage_data.get('data_sinks', []):
            graph.add_node(
                sink['name'],
                node_type='sink',
                type=sink['type'],
                location=sink['location'],
                color='lightcoral'
            )
        
        # Add transformations as nodes
        for transform in self.lineage_data.get('transformations', []):
            graph.add_node(
                transform['output_table'],
                node_type='transformation',
                transformation_name=transform['name'],
                transformation_logic=transform['transformation_logic'],
                metadata=transform.get('metadata', {}),
                color='lightgreen'
            )
        
        # Add edges based on dependencies
        for dependency in self.lineage_data.get('dependencies', []):
            graph.add_edge(
                dependency['source'],
                dependency['target'],
                transformation_id=dependency.get('transformation_id', '')
            )
        
        return graph
    
    def create_interactive_graph(self, layout='spring') -> go.Figure:
        """Create interactive Plotly graph"""
        
        # Calculate positions
        if layout == 'spring':
            pos = nx.spring_layout(self.graph, k=3, iterations=50)
        elif layout == 'hierarchical':
            pos = nx.nx_agraph.graphviz_layout(self.graph, prog='dot')
        else:
            pos = nx.spring_layout(self.graph)
        
        # Extract node information
        node_x = []
        node_y = []
        node_text = []
        node_color = []
        node_size = []
        
        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            node_data = self.graph.nodes[node]
            node_type = node_data.get('node_type', 'unknown')
            
            # Create hover text
            if node_type == 'source':
                hover_text = f"<b>{node}</b><br>Type: Data Source<br>Format: {node_data.get('type', 'N/A')}<br>Location: {node_data.get('location', 'N/A')}"
            elif node_type == 'sink':
                hover_text = f"<b>{node}</b><br>Type: Data Sink<br>Format: {node_data.get('type', 'N/A')}<br>Location: {node_data.get('location', 'N/A')}"
            elif node_type == 'transformation':
                hover_text = f"<b>{node}</b><br>Type: Transformation<br>Name: {node_data.get('transformation_name', 'N/A')}<br>Logic: {node_data.get('transformation_logic', 'N/A')[:100]}..."
            else:
                hover_text = f"<b>{node}</b><br>Type: {node_type}"
            
            node_text.append(hover_text)
            node_color.append(node_data.get('color', 'lightgray'))
            
            # Size based on node type
            if node_type == 'transformation':
                node_size.append(20)
            else:
                node_size.append(15)
        
        # Extract edge information
        edge_x = []
        edge_y = []
        
        for edge in self.graph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        # Create edge trace
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=2, color='gray'),
            hoverinfo='none',
            mode='lines',
            showlegend=False
        )
        
        # Create node trace
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=[node for node in self.graph.nodes()],
            textposition="middle center",
            hovertext=node_text,
            marker=dict(
                size=node_size,
                color=node_color,
                line=dict(width=2, color='black')
            ),
            showlegend=False
        )
        
        # Create figure
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title='Data Lineage Flow',
                           titlefont_size=16,
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="Hover over nodes for details",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002,
                               xanchor="left", yanchor="bottom",
                               font=dict(color="gray", size=12)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                       ))
        
        return fig
    
    def create_dependency_matrix(self) -> go.Figure:
        """Create dependency matrix heatmap"""
        
        # Get all nodes
        all_nodes = list(self.graph.nodes())
        n_nodes = len(all_nodes)
        
        # Create adjacency matrix
        matrix = [[0 for _ in range(n_nodes)] for _ in range(n_nodes)]
        
        for i, source in enumerate(all_nodes):
            for j, target in enumerate(all_nodes):
                if self.graph.has_edge(source, target):
                    matrix[i][j] = 1
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=matrix,
            x=all_nodes,
            y=all_nodes,
            colorscale='Blues',
            showscale=True
        ))
        
        fig.update_layout(
            title='Data Dependency Matrix',
            xaxis_title='Target Tables',
            yaxis_title='Source Tables'
        )
        
        return fig
    
    def create_impact_analysis(self, node_name: str) -> go.Figure:
        """Create impact analysis for a specific node"""
        
        if node_name not in self.graph:
            raise ValueError(f"Node {node_name} not found in lineage graph")
        
        # Find downstream dependencies
        downstream = list(nx.descendants(self.graph, node_name))
        
        # Find upstream dependencies
        upstream = list(nx.ancestors(self.graph, node_name))
        
        # Create subgraph with related nodes
        related_nodes = [node_name] + upstream + downstream
        subgraph = self.graph.subgraph(related_nodes)
        
        # Calculate positions
        pos = nx.spring_layout(subgraph, k=2, iterations=50)
        
        # Color nodes based on relationship
        node_colors = []
        for node in subgraph.nodes():
            if node == node_name:
                node_colors.append('red')  # Selected node
            elif node in upstream:
                node_colors.append('lightblue')  # Upstream
            elif node in downstream:
                node_colors.append('orange')  # Downstream
            else:
                node_colors.append('lightgray')
        
        # Extract coordinates
        node_x = [pos[node][0] for node in subgraph.nodes()]
        node_y = [pos[node][1] for node in subgraph.nodes()]
        
        # Extract edge coordinates
        edge_x = []
        edge_y = []
        for edge in subgraph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        # Create traces
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=2, color='gray'),
            hoverinfo='none',
            mode='lines',
            showlegend=False
        )
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            text=list(subgraph.nodes()),
            textposition="middle center",
            marker=dict(size=15, color=node_colors, line=dict(width=2, color='black')),
            showlegend=False
        )
        
        fig = go.Figure(data=[edge_trace, node_trace])
        fig.update_layout(
            title=f'Impact Analysis for {node_name}',
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
        
        return fig
    
    def create_transformation_summary(self) -> go.Figure:
        """Create summary chart of transformations"""
        
        transformations = self.lineage_data.get('transformations', [])
        
        if not transformations:
            # Return empty figure
            fig = go.Figure()
            fig.add_annotation(
                text="No transformations found",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            return fig
        
        # Count transformations by type
        transform_types = {}
        for transform in transformations:
            transform_name = transform['name']
            if transform_name in transform_types:
                transform_types[transform_name] += 1
            else:
                transform_types[transform_name] = 1
        
        # Create bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=list(transform_types.keys()),
                y=list(transform_types.values()),
                marker_color='lightblue'
            )
        ])
        
        fig.update_layout(
            title='Transformation Types Summary',
            xaxis_title='Transformation Type',
            yaxis_title='Count'
        )
        
        return fig
    
    def get_lineage_statistics(self) -> Dict[str, Any]:
        """Get lineage statistics"""
        
        stats = {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'data_sources': len(self.lineage_data.get('data_sources', [])),
            'data_sinks': len(self.lineage_data.get('data_sinks', [])),
            'transformations': len(self.lineage_data.get('transformations', [])),
            'longest_path': 0,
            'most_connected_node': '',
            'transformation_types': {}
        }
        
        # Calculate longest path
        if self.graph.nodes():
            try:
                stats['longest_path'] = nx.dag_longest_path_length(self.graph)
            except:
                stats['longest_path'] = 0
        
        # Find most connected node
        if self.graph.nodes():
            node_connections = {node: self.graph.degree(node) for node in self.graph.nodes()}
            stats['most_connected_node'] = max(node_connections, key=node_connections.get)
        
        # Count transformation types
        for transform in self.lineage_data.get('transformations', []):
            transform_name = transform['name']
            if transform_name in stats['transformation_types']:
                stats['transformation_types'][transform_name] += 1
            else:
                stats['transformation_types'][transform_name] = 1
        
        return stats
    
    def export_graph(self, output_path: str, format: str = 'json'):
        """Export graph to file"""
        
        if format == 'json':
            graph_data = nx.node_link_data(self.graph)
            with open(output_path, 'w') as f:
                json.dump(graph_data, f, indent=2)
        elif format == 'gexf':
            nx.write_gexf(self.graph, output_path)
        elif format == 'graphml':
            nx.write_graphml(self.graph, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

def create_lineage_visualization(lineage_data: Dict[str, Any]) -> LineageVisualizer:
    """Create lineage visualizer from lineage data"""
    return LineageVisualizer(lineage_data)
