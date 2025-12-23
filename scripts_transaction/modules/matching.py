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


def run_matching_single(entity_df, entity_blocks, entity_type, base_dir=None):
    """
    Ejecuta fuzzy matching en los bloques para un solo tipo de entidad.
    
    Args:
        entity_df: DataFrame con nombres normalizados
        entity_blocks: Diccionario de bloques
        entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
        base_dir: Directorio base del proyecto
        
    Returns:
        tuple: (components, matches_df)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    results_dir = base_dir / "results_transaction" / "intermediate"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print(f"FASE 4: FUZZY MATCHING ({entity_type.upper()})")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Threshold de similitud: {SIMILARITY_THRESHOLD}%")
    print()
    
    # Resetear índices
    entity_df = entity_df.reset_index(drop=True)
    
    # Encontrar matches
    print("1. Buscando matches con fuzzy matching...")
    matches = process_all_blocks(entity_df, entity_blocks, 'normalized_name', SIMILARITY_THRESHOLD)
    print(f"\n   ✓ Total matches encontrados: {len(matches):,} pares de matches")
    
    # Crear grafo de matches y encontrar componentes conectados
    print("\n2. Creando grupos de nombres relacionados...")
    graph = create_match_graph(matches)
    components = find_connected_components(graph, entity_df)
    print(f"   ✓ {len(components):,} grupos encontrados")
    
    # Validar y dividir componentes con conexiones transitivas débiles
    print("\n2.0. Validando componentes (eliminando conexiones transitivas débiles)...")
    min_sim = 88 if 'financial' in entity_type else 85
    components = validate_and_split_components(
        components, entity_df, matches, 'normalized_name', 
        min_pairwise_similarity=min_sim
    )
    print(f"   ✓ {len(components):,} grupos después de validación")
    
    # Post-procesamiento: fusionar entidades relacionadas que comparten las primeras dos palabras
    print("\n2.1. Fusionando entidades relacionadas (mismo nombre base)...")
    components_merged = merge_related_entities_by_first_two_words(
        components, entity_df, 'normalized_name', similarity_threshold=80
    )
    print(f"   ✓ {len(components_merged):,} grupos después de fusión")
    components = components_merged
    
    # Guardar resultados
    print("\n3. Guardando resultados de matching...")
    
    # Guardar matches como DataFrame
    matches_df = pd.DataFrame(matches, columns=['idx1', 'idx2', 'similarity'])
    matches_df['name1'] = matches_df['idx1'].apply(lambda x: entity_df.loc[x, 'normalized_name'])
    matches_df['name2'] = matches_df['idx2'].apply(lambda x: entity_df.loc[x, 'normalized_name'])
    
    output_file_matches = results_dir / f"{entity_type}_matches.csv"
    matches_df.to_csv(output_file_matches, index=False)
    print(f"   ✓ Matches guardados: {output_file_matches}")
    
    # Guardar componentes (grupos)
    components_json = {str(i): [int(idx) for idx in comp] for i, comp in enumerate(components)}
    output_file_components = results_dir / f"{entity_type}_components.json"
    
    with open(output_file_components, 'w', encoding='utf-8') as f:
        json.dump(components_json, f, indent=2)
    
    print(f"   ✓ Componentes (grupos) guardados: {output_file_components}")
    
    # Estadísticas
    print("\n4. Estadísticas de matching:")
    print(f"     - Total matches: {len(matches):,}")
    print(f"     - Grupos encontrados: {len(components):,}")
    print(f"     - Grupos con múltiples nombres: {sum(1 for c in components if len(c) > 1):,}")
    print(f"     - Nombres únicos (sin matches): {sum(1 for c in components if len(c) == 1):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Fuzzy matching completado con threshold: {SIMILARITY_THRESHOLD}%")
    print(f"✓ {entity_type}: {len(matches):,} matches, {len(components):,} grupos")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return components, matches_df


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


def validate_and_split_components(components, df, matches_list, name_column='normalized_name', 
                                  min_pairwise_similarity=85):
    """
    Valida componentes y los divide si contienen pares con similitud muy baja.
    
    Esto previene falsos positivos causados por cierre transitivo débil:
    Si A→B (88%) y B→C (88%), pero A↔C solo tiene 83%, el componente
    se divide para separar A-C.
    
    Args:
        components: Lista de componentes (sets de índices)
        df: DataFrame con nombres normalizados
        matches_list: Lista de matches como (idx1, idx2, similarity)
        name_column: Columna con nombres normalizados
        min_pairwise_similarity: Similitud mínima entre cualquier par en un componente
        
    Returns:
        Lista de componentes validados (potencialmente divididos)
    """
    # Crear diccionario de similitudes para acceso rápido
    similarity_dict = {}
    for idx1, idx2, sim in matches_list:
        similarity_dict[(idx1, idx2)] = sim
        similarity_dict[(idx2, idx1)] = sim
    
    validated_components = []
    split_count = 0
    
    for component in components:
        if len(component) <= 1:
            # Singletons no necesitan validación
            validated_components.append(component)
            continue
        
        # Calcular similitud entre todos los pares en el componente
        component_list = list(component)
        min_similarity = 100.0
        weak_pairs = []
        
        for i, idx1 in enumerate(component_list):
            for idx2 in component_list[i+1:]:
                # Buscar similitud en matches directos
                sim = similarity_dict.get((idx1, idx2), None)
                
                if sim is None:
                    # Si no hay match directo, calcularlo (puede ser conexión transitiva)
                    name1 = df.loc[idx1, name_column]
                    name2 = df.loc[idx2, name_column]
                    sim = calculate_similarity(name1, name2)
                
                if sim < min_similarity:
                    min_similarity = sim
                
                # Guardar pares débiles (por debajo del umbral)
                if sim < min_pairwise_similarity:
                    weak_pairs.append((idx1, idx2, sim))
        
        # Si todos los pares tienen buena similitud, mantener el componente intacto
        if min_similarity >= min_pairwise_similarity:
            validated_components.append(component)
        else:
            # Hay pares débiles: necesitamos dividir el componente
            # Usar el grafo de matches pero excluyendo edges débiles
            split_count += 1
            
            # Crear subgrafo solo con edges fuertes (>= min_pairwise_similarity)
            strong_graph = defaultdict(set)
            for idx1, idx2, sim in matches_list:
                if idx1 in component and idx2 in component and sim >= min_pairwise_similarity:
                    strong_graph[idx1].add(idx2)
                    strong_graph[idx2].add(idx1)
            
            # Encontrar componentes conectados en el subgrafo fuerte
            visited = set()
            
            def dfs(node, sub_component):
                visited.add(node)
                sub_component.add(node)
                for neighbor in strong_graph.get(node, []):
                    if neighbor not in visited and neighbor in component:
                        dfs(neighbor, sub_component)
            
            for idx in component:
                if idx not in visited:
                    sub_component = set()
                    dfs(idx, sub_component)
                    if sub_component:
                        validated_components.append(sub_component)
            
            # Agregar nodos que quedaron aislados (sin conexiones fuertes)
            isolated = component - visited
            for idx in isolated:
                validated_components.append({idx})
    
    if split_count > 0:
        print(f"   → {split_count} componentes divididos por similitud mínima baja")
    
    return validated_components


def merge_related_entities_by_first_two_words(components, df, name_column='normalized_name', 
                                             similarity_threshold=80):
    """
    Post-procesamiento: Fusiona componentes que comparten las primeras dos palabras
    y tienen similitud >= threshold.
    
    Esto ayuda a agrupar entidades relacionadas como:
    - "WELLS FARGO BANK NA" y "WELLS FARGO TRUST CO NA"
    - "CREDIT SUISSE" y "CREDIT SUISSE AG"
    
    Args:
        components: Lista de componentes (sets de índices)
        df: DataFrame con nombres normalizados
        name_column: Columna con nombres normalizados
        similarity_threshold: Threshold de similitud para fusionar (más bajo que el threshold principal)
    
    Returns:
        Lista de componentes fusionados
    """
    from rapidfuzz import fuzz
    import pandas as pd
    
    def extract_first_two_words(name):
        """Extrae las primeras dos palabras significativas."""
        if pd.isna(name):
            return None
        words = str(name).upper().split()
        if len(words) < 2:
            return None
        # Saltar palabras genéricas
        skip_words = {'THE', 'OF', 'AND', '&', 'A', 'AN', 'AS'}
        significant = [w for w in words[:4] if w not in skip_words]
        if len(significant) >= 2:
            return f"{significant[0]}_{significant[1]}"
        elif len(significant) == 1:
            return significant[0]
        return None
    
    # Crear índice: first_two_words -> lista de componentes
    word_to_components = defaultdict(list)
    component_to_words = {}
    
    for comp_idx, component in enumerate(components):
        # Obtener las primeras dos palabras del nombre estándar del componente
        # (usar el nombre con mayor frecuencia o más corto)
        component_names = [df.loc[idx, name_column] for idx in component]
        if component_names:
            # Seleccionar nombre representativo (más corto o más frecuente)
            representative_name = min(component_names, key=len)
            two_words = extract_first_two_words(representative_name)
            if two_words:
                word_to_components[two_words].append(comp_idx)
                component_to_words[comp_idx] = two_words
    
    # Fusionar componentes que comparten las primeras dos palabras
    merged_components = []
    merged_indices = set()
    
    for two_words, comp_indices in word_to_components.items():
        if len(comp_indices) > 1:
            # Hay múltiples componentes con las mismas dos primeras palabras
            # Verificar similitud entre sus nombres representativos
            comp_groups = []
            for comp_idx in comp_indices:
                if comp_idx not in merged_indices:
                    comp_groups.append(comp_idx)
            
            if len(comp_groups) > 1:
                # Intentar fusionar grupos que son suficientemente similares
                merged_group = set()
                for comp_idx in comp_groups:
                    merged_group.update(components[comp_idx])
                    merged_indices.add(comp_idx)
                
                # Calcular similitud promedio entre nombres del grupo fusionado
                group_names = [df.loc[idx, name_column] for idx in merged_group]
                if len(group_names) > 1:
                    # Verificar que la similitud promedio sea razonable
                    similarities = []
                    for i, name1 in enumerate(group_names):
                        for name2 in group_names[i+1:]:
                            sim = fuzz.WRatio(name1, name2)
                            similarities.append(sim)
                    
                    if similarities and sum(similarities) / len(similarities) >= similarity_threshold:
                        merged_components.append(merged_group)
                        continue
            
            # Si no se fusionó, agregar componentes individuales
            for comp_idx in comp_groups:
                if comp_idx not in merged_indices:
                    merged_components.append(components[comp_idx])
                    merged_indices.add(comp_idx)
        else:
            # Solo un componente con estas dos palabras
            comp_idx = comp_indices[0]
            if comp_idx not in merged_indices:
                merged_components.append(components[comp_idx])
                merged_indices.add(comp_idx)
    
    # Agregar componentes que no fueron procesados
    for comp_idx, component in enumerate(components):
        if comp_idx not in merged_indices:
            merged_components.append(component)
    
    return merged_components


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results_transaction" / "intermediate"
    
    entity_df = pd.read_csv(results_dir / "financial_security_normalized.csv")
    
    with open(results_dir / "financial_security_blocks.json", 'r', encoding='utf-8') as f:
        entity_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
    
    run_matching_single(entity_df, entity_blocks, "financial_security", base_dir)

