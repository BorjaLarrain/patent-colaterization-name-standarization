"""
Graph Generator for Entity Statistics
====================================
Functions to generate bar and percentage graphs for top entities.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from typing import Tuple, Optional
import numpy as np

def prepare_graph_data(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Prepare data for graph generation by grouping entities and calculating statistics.
    
    Args:
        df: DataFrame with columns: entity_id, standard_name, frequency
        top_n: Number of top entities to return
        
    Returns:
        DataFrame with columns: entity_id, standard_name, total_frequency, percentage
    """
    # Group by entity_id and sum frequencies
    grouped = df.groupby('entity_id').agg({
        'frequency': 'sum',
        'standard_name': 'first'  # Get the standard_name (should be same for all rows with same entity_id)
    }).reset_index()
    
    # Calculate total frequency for percentage calculation
    total_frequency = grouped['frequency'].sum()
    
    # Calculate percentage
    grouped['percentage'] = (grouped['frequency'] / total_frequency * 100) if total_frequency > 0 else 0
    
    # Sort by frequency descending and select top N
    grouped = grouped.sort_values('frequency', ascending=False).head(top_n)
    
    # Reset index
    grouped = grouped.reset_index(drop=True)
    
    return grouped


def generate_top_20_bar_graph_matplotlib(df: pd.DataFrame, entity_type: str, output_path: Path) -> None:
    """
    Generate a bar graph of top 20 entities by frequency using matplotlib.
    
    Args:
        df: DataFrame with entity data
        entity_type: Type of entity (e.g., 'financial_security')
        output_path: Path to save the PNG file
        
    Returns:
        None
    """
    # Prepare data
    graph_data = prepare_graph_data(df, top_n=20)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Truncate long names for better display
    labels = [name[:40] + '...' if len(name) > 40 else name for name in graph_data['standard_name']]
    
    # Create bar plot
    bars = ax.barh(range(len(graph_data)), graph_data['frequency'], color='steelblue')
    
    # Customize axes
    ax.set_yticks(range(len(graph_data)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel('Frequency', fontsize=12, fontweight='bold')
    ax.set_ylabel('Entity', fontsize=12, fontweight='bold')
    ax.set_title(f'Top 20 Entities by Frequency - {entity_type.replace("_", " ").title()}', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Invert y-axis to show highest frequency at top
    ax.invert_yaxis()
    
    # Add value labels on bars
    for i, (idx, row) in enumerate(graph_data.iterrows()):
        freq = row['frequency']
        ax.text(freq + max(graph_data['frequency']) * 0.01, i, 
                f'{freq:,}', va='center', fontsize=8)
    
    # Add grid for better readability
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save figure
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)  # Close figure to free memory


def generate_top_20_percentage_graph_matplotlib(df: pd.DataFrame, entity_type: str, output_path: Path) -> None:
    """
    Generate a line/point graph of top 20 entities showing percentage of total frequency.
    
    Args:
        df: DataFrame with entity data
        entity_type: Type of entity (e.g., 'financial_security')
        output_path: Path to save the PNG file
        
    Returns:
        None
    """
    # Prepare data
    graph_data = prepare_graph_data(df, top_n=20)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Truncate long names for better display
    labels = [name[:40] + '...' if len(name) > 40 else name for name in graph_data['standard_name']]
    
    # Create line plot with markers
    ax.plot(range(len(graph_data)), graph_data['percentage'], 
            marker='o', markersize=8, linewidth=2, color='crimson', label='Percentage')
    
    # Customize axes
    ax.set_xticks(range(len(graph_data)))
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('Percentage of Total (%)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Entity', fontsize=12, fontweight='bold')
    ax.set_title(f'Top 20 Entities by Percentage of Total - {entity_type.replace("_", " ").title()}', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on points
    for i, (idx, row) in enumerate(graph_data.iterrows()):
        pct = row['percentage']
        ax.text(i, pct + max(graph_data['percentage']) * 0.02, 
                f'{pct:.2f}%', ha='center', fontsize=8, rotation=0)
    
    # Add grid for better readability
    ax.grid(alpha=0.3, linestyle='--')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save figure
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)  # Close figure to free memory


def generate_top_20_bar_graph_plotly(df: pd.DataFrame, entity_type: str) -> go.Figure:
    """
    Generate an interactive bar graph of top 20 entities by frequency using Plotly.
    
    Args:
        df: DataFrame with entity data
        entity_type: Type of entity (e.g., 'financial_security')
        
    Returns:
        plotly.graph_objects.Figure object
    """
    # Prepare data
    graph_data = prepare_graph_data(df, top_n=20)
    
    # Truncate long names for better display
    labels = [name[:50] + '...' if len(name) > 50 else name for name in graph_data['standard_name']]
    
    # Create bar chart
    fig = go.Figure(data=[
        go.Bar(
            x=graph_data['frequency'],
            y=labels,
            orientation='h',
            marker=dict(color='steelblue'),
            text=[f'{freq:,}' for freq in graph_data['frequency']],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Frequency: %{x:,}<extra></extra>'
        )
    ])
    
    # Customize layout
    fig.update_layout(
        title=f'Top 20 Entities by Frequency - {entity_type.replace("_", " ").title()}',
        xaxis_title='Frequency',
        yaxis_title='Entity',
        height=600,
        showlegend=False,
        yaxis=dict(autorange='reversed'),  # Invert y-axis
        margin=dict(l=200, r=50, t=80, b=50)
    )
    
    return fig


def generate_top_20_percentage_graph_plotly(df: pd.DataFrame, entity_type: str) -> go.Figure:
    """
    Generate an interactive line/point graph of top 20 entities showing percentage using Plotly.
    
    Args:
        df: DataFrame with entity data
        entity_type: Type of entity (e.g., 'financial_security')
        
    Returns:
        plotly.graph_objects.Figure object
    """
    # Prepare data
    graph_data = prepare_graph_data(df, top_n=20)
    
    # Truncate long names for better display
    labels = [name[:50] + '...' if len(name) > 50 else name for name in graph_data['standard_name']]
    
    # Create line chart with markers
    fig = go.Figure(data=[
        go.Scatter(
            x=labels,
            y=graph_data['percentage'],
            mode='lines+markers',
            marker=dict(size=10, color='crimson'),
            line=dict(width=2, color='crimson'),
            text=[f'{pct:.2f}%' for pct in graph_data['percentage']],
            textposition='top center',
            hovertemplate='<b>%{x}</b><br>Percentage: %{y:.2f}%<extra></extra>'
        )
    ])
    
    # Customize layout
    fig.update_layout(
        title=f'Top 20 Entities by Percentage of Total - {entity_type.replace("_", " ").title()}',
        xaxis_title='Entity',
        yaxis_title='Percentage of Total (%)',
        height=600,
        showlegend=False,
        xaxis=dict(tickangle=-45),
        margin=dict(l=100, r=50, t=80, b=150)
    )
    
    return fig

