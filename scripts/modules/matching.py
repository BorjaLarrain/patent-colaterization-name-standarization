"""
Módulo de Fuzzy Matching
========================
Implementa fuzzy matching usando WRatio de rapidfuzz.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from rapidfuzz import fuzz
import itertools

# Configuración de matching
SIMILARITY_THRESHOLD = 88  # Threshold de similitud (0-100)
MIN_BLOCK_SIZE_FOR_MATCHING = 2  # Solo hacer matching en bloques con al menos 2 nombres


def run_matching(financial_df, non_financial_df, financial_blocks, non_financial_blocks, base_dir=None):
    """
    Ejecuta fuzzy matching en los bloques.
    
    Args:
        financial_df: DataFrame con nombres normalizados
        non_financial_df: DataFrame con nombres normalizados
        financial_blocks: Diccionario de bloques financieros
        non_financial_blocks: Diccionario de bloques no financieros
        base_dir: Directorio base del proyecto
        
    Returns:
        tuple: (financial_components, non_financial_components, financial_matches_df, non_financial_matches_df)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    results_dir = base_dir / "results" / "intermediate"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("FASE 4: FUZZY MATCHING")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Threshold de similitud: {SIMILARITY_THRESHOLD}%")
    print()
    
    # Resetear índices
    financial_df = financial_df.reset_index(drop=True)
    non_financial_df = non_financial_df.reset_index(drop=True)
    
    # Encontrar matches
    print("1. Buscando matches con fuzzy matching...")
    print("\n   Financial entities:")
    financial_matches = process_all_blocks(financial_df, financial_blocks, 'normalized_name', SIMILARITY_THRESHOLD)
    
    print("\n   Non-financial entities:")
    non_financial_matches = process_all_blocks(non_financial_df, non_financial_blocks, 'normalized_name', SIMILARITY_THRESHOLD)
    
    print(f"\n   ✓ Total matches encontrados:")
    print(f"     - Financial: {len(financial_matches):,} pares de matches")
    print(f"     - Non-financial: {len(non_financial_matches):,} pares de matches")
    
    # Crear grafo de matches y encontrar componentes conectados
    print("\n2. Creando grupos de nombres relacionados...")
    print("   Financial entities:")
    financial_graph = create_match_graph(financial_matches)
    financial_components = find_connected_components(financial_graph, financial_df)
    print(f"   ✓ {len(financial_components):,} grupos encontrados")
    
    print("   Non-financial entities:")
    non_financial_graph = create_match_graph(non_financial_matches)
    non_financial_components = find_connected_components(non_financial_graph, non_financial_df)
    print(f"   ✓ {len(non_financial_components):,} grupos encontrados")
    
    # Guardar resultados
    print("\n3. Guardando resultados de matching...")
    
    # Guardar matches como DataFrame
    financial_matches_df = pd.DataFrame(financial_matches, columns=['idx1', 'idx2', 'similarity'])
    financial_matches_df['name1'] = financial_matches_df['idx1'].apply(lambda x: financial_df.loc[x, 'normalized_name'])
    financial_matches_df['name2'] = financial_matches_df['idx2'].apply(lambda x: financial_df.loc[x, 'normalized_name'])
    
    non_financial_matches_df = pd.DataFrame(non_financial_matches, columns=['idx1', 'idx2', 'similarity'])
    non_financial_matches_df['name1'] = non_financial_matches_df['idx1'].apply(lambda x: non_financial_df.loc[x, 'normalized_name'])
    non_financial_matches_df['name2'] = non_financial_matches_df['idx2'].apply(lambda x: non_financial_df.loc[x, 'normalized_name'])
    
    output_file_financial_matches = results_dir / "financial_matches.csv"
    output_file_non_financial_matches = results_dir / "non_financial_matches.csv"
    
    financial_matches_df.to_csv(output_file_financial_matches, index=False)
    non_financial_matches_df.to_csv(output_file_non_financial_matches, index=False)
    
    print(f"   ✓ Matches guardados:")
    print(f"     - {output_file_financial_matches}")
    print(f"     - {output_file_non_financial_matches}")
    
    # Guardar componentes (grupos)
    financial_components_json = {str(i): [int(idx) for idx in comp] for i, comp in enumerate(financial_components)}
    non_financial_components_json = {str(i): [int(idx) for idx in comp] for i, comp in enumerate(non_financial_components)}
    
    output_file_financial_components = results_dir / "financial_components.json"
    output_file_non_financial_components = results_dir / "non_financial_components.json"
    
    with open(output_file_financial_components, 'w', encoding='utf-8') as f:
        json.dump(financial_components_json, f, indent=2)
    
    with open(output_file_non_financial_components, 'w', encoding='utf-8') as f:
        json.dump(non_financial_components_json, f, indent=2)
    
    print(f"   ✓ Componentes (grupos) guardados:")
    print(f"     - {output_file_financial_components}")
    print(f"     - {output_file_non_financial_components}")
    
    # Estadísticas
    print("\n4. Estadísticas de matching:")
    print("\n   Financial entities:")
    print(f"     - Total matches: {len(financial_matches):,}")
    print(f"     - Grupos encontrados: {len(financial_components):,}")
    print(f"     - Grupos con múltiples nombres: {sum(1 for c in financial_components if len(c) > 1):,}")
    print(f"     - Nombres únicos (sin matches): {sum(1 for c in financial_components if len(c) == 1):,}")
    
    print("\n   Non-financial entities:")
    print(f"     - Total matches: {len(non_financial_matches):,}")
    print(f"     - Grupos encontrados: {len(non_financial_components):,}")
    print(f"     - Grupos con múltiples nombres: {sum(1 for c in non_financial_components if len(c) > 1):,}")
    print(f"     - Nombres únicos (sin matches): {sum(1 for c in non_financial_components if len(c) == 1):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Fuzzy matching completado con threshold: {SIMILARITY_THRESHOLD}%")
    print(f"✓ Financial entities: {len(financial_matches):,} matches, {len(financial_components):,} grupos")
    print(f"✓ Non-financial entities: {len(non_financial_matches):,} matches, {len(non_financial_components):,} grupos")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return financial_components, non_financial_components, financial_matches_df, non_financial_matches_df


def calculate_similarity(name1, name2):
    """Calcula similitud entre dos nombres usando WRatio."""
    if pd.isna(name1) or pd.isna(name2):
        return 0.0
    
    return fuzz.WRatio(str(name1), str(name2))


def find_matches_in_block(df, block_indices, name_column='normalized_name', threshold=SIMILARITY_THRESHOLD):
    """Encuentra matches dentro de un bloque."""
    matches = []
    
    if len(block_indices) < MIN_BLOCK_SIZE_FOR_MATCHING:
        return matches
    
    for idx1, idx2 in itertools.combinations(block_indices, 2):
        name1 = df.loc[idx1, name_column]
        name2 = df.loc[idx2, name_column]
        
        similarity = calculate_similarity(name1, name2)
        
        if similarity >= threshold:
            matches.append((idx1, idx2, similarity))
    
    return matches


def process_all_blocks(df, blocks, name_column='normalized_name', threshold=SIMILARITY_THRESHOLD):
    """Procesa todos los bloques y encuentra matches."""
    all_matches = []
    blocks_processed = 0
    blocks_with_matches = 0
    
    total_blocks = len(blocks)
    
    print(f"   Procesando {total_blocks:,} bloques...")
    
    for blocking_key, block_indices in blocks.items():
        blocks_processed += 1
        
        if blocks_processed % 500 == 0:
            print(f"     Procesados: {blocks_processed:,}/{total_blocks:,} bloques ({100*blocks_processed/total_blocks:.1f}%)")
        
        matches = find_matches_in_block(df, block_indices, name_column, threshold)
        
        if matches:
            blocks_with_matches += 1
            all_matches.extend(matches)
    
    print(f"   ✓ Procesados {blocks_processed:,} bloques")
    print(f"   ✓ {blocks_with_matches:,} bloques con matches encontrados")
    
    return all_matches


def create_match_graph(matches):
    """Crea un grafo de matches para encontrar componentes conectados."""
    graph = defaultdict(set)
    
    for idx1, idx2, similarity in matches:
        graph[idx1].add(idx2)
        graph[idx2].add(idx1)
    
    return dict(graph)


def find_connected_components(graph, df):
    """
    Encuentra componentes conectados en el grafo usando DFS.
    
    También incluye nodos aislados (sin matches).
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
    
    # Agregar nodos aislados (sin matches)
    all_nodes_in_graph = set(graph.keys())
    for neighbors in graph.values():
        all_nodes_in_graph.update(neighbors)
    
    # Todos los índices del DataFrame que no están en el grafo son singletons
    all_df_indices = set(range(len(df)))
    isolated = all_df_indices - all_nodes_in_graph
    
    for node in isolated:
        components.append({node})
    
    return components


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results" / "intermediate"
    
    financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
    non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
    
    with open(results_dir / "financial_blocks.json", 'r', encoding='utf-8') as f:
        financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
    
    with open(results_dir / "non_financial_blocks.json", 'r', encoding='utf-8') as f:
        non_financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
    
    run_matching(financial_df, non_financial_df, financial_blocks, non_financial_blocks, base_dir)

