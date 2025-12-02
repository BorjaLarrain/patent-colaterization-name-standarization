"""
Fase 4: Fuzzy Matching
======================
Este script implementa fuzzy matching usando WRatio de rapidfuzz.

- Carga datos normalizados y bloques optimizados
- Compara nombres dentro de cada bloque usando WRatio
- Identifica matches potenciales (similitud >= threshold)
- Guarda resultados de matching
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from rapidfuzz import fuzz
import itertools

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Configuración de matching
SIMILARITY_THRESHOLD = 88  # Threshold de similitud (0-100) - Aumentado de 85 a 88 para reducir falsos positivos
MIN_BLOCK_SIZE_FOR_MATCHING = 2  # Solo hacer matching en bloques con al menos 2 nombres

def calculate_similarity(name1, name2):
    """
    Calcula similitud entre dos nombres usando WRatio
    
    Args:
        name1: Primer nombre
        name2: Segundo nombre
        
    Returns:
        float: Score de similitud (0-100)
    """
    if pd.isna(name1) or pd.isna(name2):
        return 0.0
    
    return fuzz.WRatio(str(name1), str(name2))

def find_matches_in_block(df, block_indices, name_column='normalized_name', threshold=SIMILARITY_THRESHOLD):
    """
    Encuentra matches dentro de un bloque
    
    Args:
        df: DataFrame con los datos
        block_indices: Lista de índices que pertenecen al bloque
        name_column: Nombre de la columna con nombres normalizados
        threshold: Threshold de similitud
        
    Returns:
        list: Lista de tuplas (idx1, idx2, similarity_score)
    """
    matches = []
    
    # Solo procesar si hay al menos 2 nombres en el bloque
    if len(block_indices) < MIN_BLOCK_SIZE_FOR_MATCHING:
        return matches
    
    # Comparar cada par de nombres en el bloque
    for idx1, idx2 in itertools.combinations(block_indices, 2):
        name1 = df.loc[idx1, name_column]
        name2 = df.loc[idx2, name_column]
        
        similarity = calculate_similarity(name1, name2)
        
        if similarity >= threshold:
            matches.append((idx1, idx2, similarity))
    
    return matches

def process_all_blocks(df, blocks, name_column='normalized_name', threshold=SIMILARITY_THRESHOLD):
    """
    Procesa todos los bloques y encuentra matches
    
    Args:
        df: DataFrame con los datos
        blocks: Diccionario de bloques
        name_column: Nombre de la columna con nombres normalizados
        threshold: Threshold de similitud
        
    Returns:
        list: Lista de todos los matches encontrados
    """
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
    """
    Crea un grafo de matches para encontrar componentes conectados
    
    Args:
        matches: Lista de tuplas (idx1, idx2, similarity)
        
    Returns:
        dict: Grafo representado como diccionario de adyacencia
    """
    graph = defaultdict(set)
    
    for idx1, idx2, similarity in matches:
        graph[idx1].add(idx2)
        graph[idx2].add(idx1)
    
    return dict(graph)

def find_connected_components(graph):
    """
    Encuentra componentes conectados en el grafo usando DFS
    
    Args:
        graph: Grafo representado como diccionario de adyacencia
        
    Returns:
        list: Lista de componentes (cada componente es un set de índices)
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
    all_nodes = set(graph.keys())
    for node in graph.values():
        all_nodes.update(node)
    
    isolated = all_nodes - visited
    for node in isolated:
        components.append({node})
    
    return components

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 4: FUZZY MATCHING")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Threshold de similitud: {SIMILARITY_THRESHOLD}%")
print()

# 1. CARGAR datos y bloques optimizados
print("1. Cargando datos y bloques optimizados...")
input_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"
blocks_file_financial = RESULTS_DIR / "financial_blocks_optimized.json"
blocks_file_non_financial = RESULTS_DIR / "non_financial_blocks_optimized.json"

if not all(f.exists() for f in [input_file_financial, input_file_non_financial, 
                                 blocks_file_financial, blocks_file_non_financial]):
    print("   ✗ Error: No se encontraron los archivos necesarios.")
    print(f"   Por favor ejecuta primero: 10_blocking_step3_3.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

# Resetear índices
financial_df = financial_df.reset_index(drop=True)
non_financial_df = non_financial_df.reset_index(drop=True)

# Cargar bloques optimizados
with open(blocks_file_financial, 'r', encoding='utf-8') as f:
    financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}

with open(blocks_file_non_financial, 'r', encoding='utf-8') as f:
    non_financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}

print(f"   ✓ Financial entities: {len(financial_df):,} registros, {len(financial_blocks):,} bloques")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros, {len(non_financial_blocks):,} bloques")

# 2. ENCONTRAR matches en bloques
print("\n2. Buscando matches con fuzzy matching...")
print("\n   Financial entities:")
financial_matches = process_all_blocks(financial_df, financial_blocks, 'normalized_name', SIMILARITY_THRESHOLD)

print("\n   Non-financial entities:")
non_financial_matches = process_all_blocks(non_financial_df, non_financial_blocks, 'normalized_name', SIMILARITY_THRESHOLD)

print(f"\n   ✓ Total matches encontrados:")
print(f"     - Financial: {len(financial_matches):,} pares de matches")
print(f"     - Non-financial: {len(non_financial_matches):,} pares de matches")

# 3. CREAR grafo de matches y encontrar componentes conectados
print("\n3. Creando grupos de nombres relacionados...")
print("   Financial entities:")
financial_graph = create_match_graph(financial_matches)
financial_components = find_connected_components(financial_graph)
print(f"   ✓ {len(financial_components):,} grupos encontrados")

print("   Non-financial entities:")
non_financial_graph = create_match_graph(non_financial_matches)
non_financial_components = find_connected_components(non_financial_graph)
print(f"   ✓ {len(non_financial_components):,} grupos encontrados")

# 4. GUARDAR resultados de matching
print("\n4. Guardando resultados de matching...")

# Guardar matches como DataFrame
financial_matches_df = pd.DataFrame(financial_matches, columns=['idx1', 'idx2', 'similarity'])
financial_matches_df['name1'] = financial_matches_df['idx1'].apply(lambda x: financial_df.loc[x, 'normalized_name'])
financial_matches_df['name2'] = financial_matches_df['idx2'].apply(lambda x: financial_df.loc[x, 'normalized_name'])

non_financial_matches_df = pd.DataFrame(non_financial_matches, columns=['idx1', 'idx2', 'similarity'])
non_financial_matches_df['name1'] = non_financial_matches_df['idx1'].apply(lambda x: non_financial_df.loc[x, 'normalized_name'])
non_financial_matches_df['name2'] = non_financial_matches_df['idx2'].apply(lambda x: non_financial_df.loc[x, 'normalized_name'])

output_file_financial_matches = RESULTS_DIR / "financial_matches.csv"
output_file_non_financial_matches = RESULTS_DIR / "non_financial_matches.csv"

financial_matches_df.to_csv(output_file_financial_matches, index=False)
non_financial_matches_df.to_csv(output_file_non_financial_matches, index=False)

print(f"   ✓ Matches guardados:")
print(f"     - {output_file_financial_matches}")
print(f"     - {output_file_non_financial_matches}")

# Guardar componentes (grupos)
financial_components_json = {str(i): [int(idx) for idx in comp] for i, comp in enumerate(financial_components)}
non_financial_components_json = {str(i): [int(idx) for idx in comp] for i, comp in enumerate(non_financial_components)}

output_file_financial_components = RESULTS_DIR / "financial_components.json"
output_file_non_financial_components = RESULTS_DIR / "non_financial_components.json"

with open(output_file_financial_components, 'w', encoding='utf-8') as f:
    json.dump(financial_components_json, f, indent=2)

with open(output_file_non_financial_components, 'w', encoding='utf-8') as f:
    json.dump(non_financial_components_json, f, indent=2)

print(f"   ✓ Componentes (grupos) guardados:")
print(f"     - {output_file_financial_components}")
print(f"     - {output_file_non_financial_components}")

# 5. Estadísticas
print("\n5. Estadísticas de matching:")
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

# 6. Mostrar ejemplos de matches
print("\n6. Ejemplos de matches encontrados:")
print("\n   Financial entities (top 10 por similitud):")
if len(financial_matches) > 0:
    financial_matches_sorted = sorted(financial_matches, key=lambda x: x[2], reverse=True)
    for i, (idx1, idx2, sim) in enumerate(financial_matches_sorted[:10], 1):
        name1 = financial_df.loc[idx1, 'normalized_name']
        name2 = financial_df.loc[idx2, 'normalized_name']
        print(f"     {i}. Similitud: {sim:.1f}%")
        print(f"        {name1}")
        print(f"        {name2}")
        print()

# 7. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Fuzzy matching completado con threshold: {SIMILARITY_THRESHOLD}%")
print(f"✓ Financial entities: {len(financial_matches):,} matches, {len(financial_components):,} grupos")
print(f"✓ Non-financial entities: {len(non_financial_matches):,} matches, {len(non_financial_components):,} grupos")
print(f"✓ Resultados guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 5 - Agrupación y Asignación de IDs")
print("=" * 80)

