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
        """Build NetworkX graph from lineage data with proper flow structure"""
        graph = nx.DiGraph()
        
        # Add data sources as nodes
        for source in self.lineage_data.get('data_sources', []):
            graph.add_node(
                source['name'],
                node_type='source',
                type=source['type'],
                location=source['location'],
                schema=source.get('schema', {}),
                color='lightgreen',
                shape='ellipse'
            )
        
        # Add data sinks as nodes
        for sink in self.lineage_data.get('data_sinks', []):
            graph.add_node(
                sink['name'],
                node_type='sink',
                type=sink['type'],
                location=sink['location'],
                color='lightcoral',
                shape='ellipse'
            )
        
        # Add transformations and their outputs as nodes
        for transform in self.lineage_data.get('transformations', []):
            # Add the transformation as an intermediate node
            transform_node = f"transform_{transform['id']}"
            graph.add_node(
                transform_node,
                node_type='transformation',
                transformation_name=transform['name'],
                transformation_logic=transform['transformation_logic'],
                metadata=transform.get('metadata', {}),
                color='orange',
                shape='rectangle'
            )
            
            # Add the output table if it's not already added
            output_table = transform['output_table']
            if not graph.has_node(output_table):
                # Determine if this is a final sink or intermediate table
                is_sink = output_table in [sink['name'] for sink in self.lineage_data.get('data_sinks', [])]
                node_color = 'lightcoral' if is_sink else 'lightblue'
                node_type = 'sink' if is_sink else 'intermediate'
                
                graph.add_node(
                    output_table,
                    node_type=node_type,
                    color=node_color,
                    shape='ellipse'
                )
            
            # Connect transformation to its output
            graph.add_edge(transform_node, output_table, edge_type='produces')
            
            # Connect inputs to transformation
            for input_table in transform['input_tables']:
                if not graph.has_node(input_table):
                    # This might be an intermediate table from a previous transformation
                    graph.add_node(
                        input_table,
                        node_type='intermediate',
                        color='lightblue',
                        shape='ellipse'
                    )
                graph.add_edge(input_table, transform_node, edge_type='consumes')
        
        # Add explicit dependencies if they provide additional connections
        for dependency in self.lineage_data.get('dependencies', []):
            source = dependency['source']
            target = dependency['target']
            
            # Only add edge if both nodes exist and edge doesn't already exist
            if graph.has_node(source) and graph.has_node(target):
                if not graph.has_edge(source, target):
                    graph.add_edge(source, target, 
                                 edge_type='dependency',
                                 transformation_id=dependency.get('transformation_id', ''))
        
        return graph
    
    def create_interactive_graph(self, layout='hierarchical') -> go.Figure:
        """Create interactive Plotly graph with proper flow visualization"""
        
        # Calculate positions with flow-oriented layout
        if layout == 'hierarchical':
            try:
                # Use dot layout to show proper hierarchy from left to right
                pos = nx.nx_agraph.graphviz_layout(self.graph, prog='dot', args='-Grankdir=LR')
            except:
                # Fallback to spring layout with better parameters for flow
                pos = nx.spring_layout(self.graph, k=5, iterations=100, seed=42)
        elif layout == 'spring':
            pos = nx.spring_layout(self.graph, k=3, iterations=50)
        else:
            pos = nx.spring_layout(self.graph)
        
        # Separate nodes by type for different visualizations
        source_nodes = []
        sink_nodes = []
        intermediate_nodes = []
        transformation_nodes = []
        
        for node in self.graph.nodes():
            node_data = self.graph.nodes[node]
            node_type = node_data.get('node_type', 'unknown')
            
            if node_type == 'source':
                source_nodes.append(node)
            elif node_type == 'sink':
                sink_nodes.append(node)
            elif node_type == 'intermediate':
                intermediate_nodes.append(node)
            elif node_type == 'transformation':
                transformation_nodes.append(node)
        
        # Create edge traces with directional arrows
        edge_traces = []
        for edge in self.graph.edges(data=True):
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            
            # Create line with arrow
            edge_trace = go.Scatter(
                x=[x0, x1, None], 
                y=[y0, y1, None],
                mode='lines',
                line=dict(width=2, color='#666'),
                hoverinfo='text',
                hovertext=f"{edge[0]} → {edge[1]}",
                showlegend=False
            )
            edge_traces.append(edge_trace)
            
            # Add arrow head
            dx = x1 - x0
            dy = y1 - y0
            length = (dx**2 + dy**2)**0.5
            if length > 0:
                # Create arrow head at the target
                arrow_head = go.Scatter(
                    x=[x1], y=[y1],
                    mode='markers',
                    marker=dict(
                        symbol='triangle-right',
                        size=8,
                        color='#666',
                        angleref='previous'
                    ),
                    hoverinfo='none',
                    showlegend=False
                )
                edge_traces.append(arrow_head)
        
        # Helper function to create node traces
        def create_node_trace(nodes, color, symbol, name, node_type):
            if not nodes:
                return None
                
            node_x = [pos[node][0] for node in nodes]
            node_y = [pos[node][1] for node in nodes]
            node_text = []
            
            for node in nodes:
                node_data = self.graph.nodes[node]
                if node_type == 'source':
                    hover_text = f"<b>{node}</b><br>Type: Data Source<br>Format: {node_data.get('type', 'N/A')}<br>Location: {node_data.get('location', 'N/A')}"
                elif node_type == 'sink':
                    hover_text = f"<b>{node}</b><br>Type: Final Output<br>Format: {node_data.get('type', 'N/A')}<br>Location: {node_data.get('location', 'N/A')}"
                elif node_type == 'intermediate':
                    hover_text = f"<b>{node}</b><br>Type: Intermediate Dataset<br>Connections: {self.graph.in_degree(node)} in, {self.graph.out_degree(node)} out"
                elif node_type == 'transformation':
                    hover_text = f"<b>{node_data.get('transformation_name', node)}</b><br>Type: Transformation<br>Logic: {node_data.get('transformation_logic', 'N/A')[:100]}..."
                else:
                    hover_text = f"<b>{node}</b><br>Type: {node_type}"
                
                node_text.append(hover_text)
            
            return go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                hoverinfo='text',
                text=nodes,
                textposition="middle center",
                hovertext=node_text,
                marker=dict(
                    size=20 if node_type == 'transformation' else 25,
                    color=color,
                    symbol=symbol,
                    line=dict(width=2, color='black')
                ),
                name=name,
                textfont=dict(size=10)
            )
        
        # Create traces for each node type
        traces = edge_traces
        
        source_trace = create_node_trace(source_nodes, 'lightgreen', 'circle', 'Data Sources', 'source')
        if source_trace:
            traces.append(source_trace)
            
        intermediate_trace = create_node_trace(intermediate_nodes, 'lightblue', 'circle', 'Intermediate Data', 'intermediate')
        if intermediate_trace:
            traces.append(intermediate_trace)
            
        transformation_trace = create_node_trace(transformation_nodes, 'orange', 'square', 'Transformations', 'transformation')
        if transformation_trace:
            traces.append(transformation_trace)
            
        sink_trace = create_node_trace(sink_nodes, 'lightcoral', 'circle', 'Final Outputs', 'sink')
        if sink_trace:
            traces.append(sink_trace)
        
        # Create figure
        fig = go.Figure(data=traces,
                       layout=go.Layout(
                           title=dict(
                               text='🔄 Data Lineage Flow Visualization',
                               font=dict(size=16)
                           ),
                           showlegend=True,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="🟢 Sources → 🟠 Transformations → 🔴 Outputs<br>Hover over nodes for details",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002,
                               xanchor="left", yanchor="bottom",
                               font=dict(color="gray", size=12)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           plot_bgcolor='white',
                           paper_bgcolor='white'
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
        """Get enhanced lineage statistics with flow analysis"""
        
        # Separate nodes by type
        source_nodes = [node for node, data in self.graph.nodes(data=True) if data.get('node_type') == 'source']
        sink_nodes = [node for node, data in self.graph.nodes(data=True) if data.get('node_type') == 'sink']
        intermediate_nodes = [node for node, data in self.graph.nodes(data=True) if data.get('node_type') == 'intermediate']
        transformation_nodes = [node for node, data in self.graph.nodes(data=True) if data.get('node_type') == 'transformation']
        
        stats = {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'data_sources': len(source_nodes),
            'data_sinks': len(sink_nodes),
            'intermediate_datasets': len(intermediate_nodes),
            'transformations': len(transformation_nodes),
            'longest_path': 0,
            'flow_paths': [],
            'most_connected_node': '',
            'transformation_types': {}
        }
        
        # Calculate flow paths from sources to sinks
        for source in source_nodes:
            for sink in sink_nodes:
                try:
                    if nx.has_path(self.graph, source, sink):
                        path = nx.shortest_path(self.graph, source, sink)
                        path_length = len(path) - 1
                        stats['flow_paths'].append({
                            'source': source,
                            'sink': sink,
                            'path': path,
                            'length': path_length
                        })
                        stats['longest_path'] = max(stats['longest_path'], path_length)
                except:
                    continue
        
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
        
    def create_flow_path_analysis(self) -> go.Figure:
        """Create a detailed flow path analysis visualization"""
        
        stats = self.get_lineage_statistics()
        flow_paths = stats['flow_paths']
        
        if not flow_paths:
            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No complete flow paths found from sources to sinks",
                xref="paper", yref="paper",
                x=0.5, y=0.5, xanchor='center', yanchor='middle',
                font=dict(size=16, color="gray")
            )
            fig.update_layout(
                title="Flow Path Analysis",
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
            )
            return fig
        
        # Create a visualization showing all flow paths
        fig = make_subplots(
            rows=len(flow_paths), cols=1,
            subplot_titles=[f"Path {i+1}: {path['source']} → {path['sink']} ({path['length']} steps)" 
                           for i, path in enumerate(flow_paths)],
            vertical_spacing=0.1
        )
        
        for i, path_info in enumerate(flow_paths):
            path = path_info['path']
            
            # Create horizontal flow visualization
            x_positions = list(range(len(path)))
            y_position = [1] * len(path)  # All on same horizontal line
            
            # Add nodes
            colors = []
            symbols = []
            sizes = []
            hover_texts = []
            
            for j, node in enumerate(path):
                node_data = self.graph.nodes.get(node, {})
                node_type = node_data.get('node_type', 'unknown')
                
                if node_type == 'source':
                    colors.append('lightgreen')
                    symbols.append('circle')
                    sizes.append(25)
                    hover_texts.append(f"Source: {node}")
                elif node_type == 'sink':
                    colors.append('lightcoral')
                    symbols.append('circle')
                    sizes.append(25)
                    hover_texts.append(f"Final Output: {node}")
                elif node_type == 'transformation':
                    colors.append('orange')
                    symbols.append('square')
                    sizes.append(20)
                    transform_name = node_data.get('transformation_name', node)
                    hover_texts.append(f"Transformation: {transform_name}")
                else:
                    colors.append('lightblue')
                    symbols.append('circle')
                    sizes.append(20)
                    hover_texts.append(f"Intermediate: {node}")
            
            # Add nodes trace
            fig.add_trace(
                go.Scatter(
                    x=x_positions, y=y_position,
                    mode='markers+text',
                    marker=dict(size=sizes, color=colors, symbol=symbols, line=dict(width=2)),
                    text=path,
                    textposition="top center",
                    hovertext=hover_texts,
                    name=f"Path {i+1}",
                    showlegend=False
                ),
                row=i+1, col=1
            )
            
            # Add connecting lines
            for j in range(len(path) - 1):
                fig.add_trace(
                    go.Scatter(
                        x=[x_positions[j], x_positions[j+1]], 
                        y=[y_position[j], y_position[j+1]],
                        mode='lines',
                        line=dict(width=3, color='gray'),
                        hoverinfo='skip',
                        showlegend=False
                    ),
                    row=i+1, col=1
                )
                
                # Add arrow
                fig.add_annotation(
                    x=x_positions[j+1], y=y_position[j+1],
                    ax=x_positions[j], ay=y_position[j],
                    xref=f'x{i+1}', yref=f'y{i+1}',
                    axref=f'x{i+1}', ayref=f'y{i+1}',
                    arrowhead=2, arrowsize=1, arrowwidth=2, arrowcolor='gray'
                )
        
        fig.update_layout(
            title="🛤️ Complete Data Flow Paths Analysis",
            height=200 * len(flow_paths),
            showlegend=False
        )
        
        # Update all subplot axes
        for i in range(len(flow_paths)):
            fig.update_xaxes(showgrid=False, zeroline=False, showticklabels=False, row=i+1, col=1)
            fig.update_yaxes(showgrid=False, zeroline=False, showticklabels=False, row=i+1, col=1)
        
        return fig
    
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
