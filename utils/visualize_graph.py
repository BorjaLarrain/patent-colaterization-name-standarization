"""
Script para visualizar el grafo de matches
==========================================
Este script crea visualizaciones del grafo de matches encontrados.
Genera archivos de texto con representaciones del grafo.
"""

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
OUTPUT_DIR = BASE_DIR / "results" / "visualization"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def create_match_graph(matches_df):
    """
    Crea un grafo de matches a partir del DataFrame de matches
    
    Args:
        matches_df: DataFrame con columnas idx1, idx2, similarity
        
    Returns:
        dict: Grafo representado como diccionario de adyacencia
    """
    graph = defaultdict(set)
    
    for _, row in matches_df.iterrows():
        idx1, idx2 = int(row['idx1']), int(row['idx2'])
        graph[idx1].add(idx2)
        graph[idx2].add(idx1)
    
    return dict(graph)

def find_connected_components(graph):
    """
    Encuentra componentes conectados usando DFS
    
    Args:
        graph: Grafo como diccionario de adyacencia
        
    Returns:
        list: Lista de componentes (cada uno es un set de índices)
    """
    visited = set()
    components = []
    
    def dfs(node, component):
        visited.add(node)
        component.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                dfs(neighbor, component)
    
    for node in graph:
        if node not in visited:
            component = set()
            dfs(node, component)
            components.append(component)
    
    # Agregar nodos aislados
    all_nodes = set(graph.keys())
    for neighbors in graph.values():
        all_nodes.update(neighbors)
    
    isolated = all_nodes - visited
    for node in isolated:
        components.append({node})
    
    return components

def visualize_component(component, graph, df, matches_df, component_id, output_lines):
    """
    Visualiza un componente del grafo en formato texto
    
    Args:
        component: Set de índices del componente
        graph: Grafo completo
        df: DataFrame con los nombres
        matches_df: DataFrame con los matches
        component_id: ID del componente
        output_lines: Lista donde agregar las líneas de salida
    """
    output_lines.append("=" * 80)
    output_lines.append(f"COMPONENTE {component_id} - {len(component)} nombres relacionados")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    # Listar todos los nombres del componente
    output_lines.append("Nombres en este componente:")
    output_lines.append("-" * 80)
    sorted_indices = sorted(component)
    for idx in sorted_indices:
        name = df.loc[idx, 'normalized_name']
        freq = df.loc[idx, 'frequency'] if 'frequency' in df.columns else 'N/A'
        output_lines.append(f"  [{idx:5d}] {name} (freq: {freq})")
    output_lines.append("")
    
    # Mostrar todas las conexiones (matches) en este componente
    component_matches = matches_df[
        (matches_df['idx1'].isin(component)) & 
        (matches_df['idx2'].isin(component))
    ].sort_values('similarity', ascending=False)
    
    output_lines.append(f"Conexiones (matches) en este componente ({len(component_matches)} pares):")
    output_lines.append("-" * 80)
    
    for _, row in component_matches.iterrows():
        idx1, idx2, sim = int(row['idx1']), int(row['idx2']), row['similarity']
        name1 = df.loc[idx1, 'normalized_name']
        name2 = df.loc[idx2, 'normalized_name']
        output_lines.append(f"  [{idx1:5d}] ↔ [{idx2:5d}]  Similitud: {sim:.1f}%")
        output_lines.append(f"    {name1}")
        output_lines.append(f"    {name2}")
        output_lines.append("")
    
    # Visualización ASCII del grafo
    output_lines.append("Representación del grafo (conexiones):")
    output_lines.append("-" * 80)
    
    # Crear lista de nodos ordenados
    nodes = sorted(component)
    
    # Mostrar conexiones de forma más clara
    shown_edges = set()
    for node1 in nodes:
        connections = [node2 for node2 in sorted(graph.get(node1, [])) 
                      if node2 in component and node2 > node1]
        if connections:
            for node2 in connections:
                edge = tuple(sorted([node1, node2]))
                if edge not in shown_edges:
                    shown_edges.add(edge)
                    # Obtener similitud
                    match_row = component_matches[
                        ((component_matches['idx1'] == node1) & (component_matches['idx2'] == node2)) |
                        ((component_matches['idx1'] == node2) & (component_matches['idx2'] == node1))
                    ]
                    sim = match_row['similarity'].iloc[0] if len(match_row) > 0 else 0
                    output_lines.append(f"  [{node1:5d}] ────({sim:.0f}%)──── [{node2:5d}]")
    
    output_lines.append("")
    output_lines.append("")

def create_graph_visualization(entity_type='financial', max_components=20, min_component_size=2, max_component_size=15):
    """
    Crea visualización del grafo de matches
    
    Args:
        entity_type: 'financial' o 'non_financial'
        max_components: Máximo número de componentes a visualizar
        min_component_size: Tamaño mínimo del componente para visualizar
        max_component_size: Tamaño máximo del componente para visualizar
    """
    print(f"Creando visualización del grafo para {entity_type} entities...")
    
    # Cargar datos
    df_file = RESULTS_DIR / f"{entity_type}_with_blocking_keys.csv"
    matches_file = RESULTS_DIR / f"{entity_type}_matches.csv"
    
    if not df_file.exists() or not matches_file.exists():
        print(f"   ✗ Error: No se encontraron los archivos para {entity_type}")
        return
    
    df = pd.read_csv(df_file)
    matches_df = pd.read_csv(matches_file)
    
    print(f"   ✓ Cargados {len(df):,} nombres y {len(matches_df):,} matches")
    
    # Crear grafo
    print("   Creando grafo...")
    graph = create_match_graph(matches_df)
    
    # Encontrar componentes
    print("   Encontrando componentes conectados...")
    components = find_connected_components(graph)
    
    # Filtrar componentes por tamaño
    filtered_components = [
        comp for comp in components 
        if min_component_size <= len(comp) <= max_component_size
    ]
    
    # Ordenar por tamaño (más grandes primero)
    filtered_components.sort(key=len, reverse=True)
    
    # Tomar solo los primeros max_components
    components_to_visualize = filtered_components[:max_components]
    
    print(f"   ✓ Encontrados {len(components)} componentes totales")
    print(f"   ✓ Visualizando {len(components_to_visualize)} componentes (tamaño {min_component_size}-{max_component_size})")
    
    # Crear visualización
    output_lines = []
    output_lines.append("=" * 80)
    output_lines.append(f"VISUALIZACIÓN DEL GRAFO DE MATCHES - {entity_type.upper()} ENTITIES")
    output_lines.append("=" * 80)
    output_lines.append("")
    output_lines.append(f"Total de componentes: {len(components)}")
    output_lines.append(f"Componentes visualizados: {len(components_to_visualize)}")
    output_lines.append(f"Tamaño de componentes visualizados: {min_component_size}-{max_component_size} nombres")
    output_lines.append("")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    # Visualizar cada componente
    for i, component in enumerate(components_to_visualize, 1):
        visualize_component(component, graph, df, matches_df, i, output_lines)
    
    # Estadísticas adicionales
    output_lines.append("=" * 80)
    output_lines.append("ESTADÍSTICAS ADICIONALES")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    component_sizes = [len(comp) for comp in components]
    output_lines.append(f"Distribución de tamaños de componentes:")
    output_lines.append(f"  - Componentes con 1 nombre: {sum(1 for s in component_sizes if s == 1):,}")
    output_lines.append(f"  - Componentes con 2-5 nombres: {sum(1 for s in component_sizes if 2 <= s <= 5):,}")
    output_lines.append(f"  - Componentes con 6-10 nombres: {sum(1 for s in component_sizes if 6 <= s <= 10):,}")
    output_lines.append(f"  - Componentes con 11-20 nombres: {sum(1 for s in component_sizes if 11 <= s <= 20):,}")
    output_lines.append(f"  - Componentes con >20 nombres: {sum(1 for s in component_sizes if s > 20):,}")
    output_lines.append(f"  - Tamaño máximo: {max(component_sizes) if component_sizes else 0}")
    
    # Guardar archivo
    output_file = OUTPUT_DIR / f"{entity_type}_graph_visualization.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    print(f"   ✓ Visualización guardada en: {output_file}")
    
    return output_file

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("VISUALIZACIÓN DEL GRAFO DE MATCHES")
    print("=" * 80)
    print()
    
    # Visualizar financial entities
    create_graph_visualization('financial', max_components=15, min_component_size=2, max_component_size=12)
    
    print()
    
    # Visualizar non-financial entities
    create_graph_visualization('non_financial', max_components=15, min_component_size=2, max_component_size=12)
    
    print()
    print("=" * 80)
    print("✓ Visualizaciones completadas")
    print(f"✓ Archivos guardados en: {OUTPUT_DIR}")
    print("=" * 80)

