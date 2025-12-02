"""
Fase 5: Agrupación y Asignación de IDs
=======================================
Este script implementa la Fase 5 del pipeline:
- Asigna IDs únicos a cada grupo/cluster
- Selecciona nombre estándar para cada grupo
- Crea tabla de mapeo final
- Identifica casos problemáticos para revisión
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
FINAL_RESULTS_DIR = BASE_DIR / "results" / "final"
FINAL_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def select_standard_name(df, component_indices, name_column='normalized_name', freq_column='frequency'):
    """
    Selecciona el nombre estándar para un componente
    
    Estrategia:
    1. Preferir el nombre con mayor frecuencia
    2. Si hay empate, preferir el más corto
    3. Si hay empate, preferir el que no tiene palabras genéricas al final
    
    Args:
        df: DataFrame con los datos
        component_indices: Lista de índices del componente
        name_column: Columna con nombres normalizados
        freq_column: Columna con frecuencias
        
    Returns:
        tuple: (índice del nombre estándar, nombre estándar)
    """
    if len(component_indices) == 1:
        idx = component_indices[0]
        return idx, df.loc[idx, name_column]
    
    # Obtener datos del componente
    component_data = []
    for idx in component_indices:
        name = df.loc[idx, name_column]
        freq = df.loc[idx, freq_column]
        length = len(name)
        component_data.append({
            'idx': idx,
            'name': name,
            'freq': freq,
            'length': length
        })
    
    # Ordenar por frecuencia (desc), luego por longitud (asc)
    component_data.sort(key=lambda x: (-x['freq'], x['length']))
    
    # Retornar el primero (mayor frecuencia, más corto)
    return component_data[0]['idx'], component_data[0]['name']

def calculate_component_stats(df, component_indices, matches_df, name_column='normalized_name'):
    """
    Calcula estadísticas de un componente para identificar problemas
    
    Args:
        df: DataFrame con los datos
        component_indices: Lista de índices del componente
        matches_df: DataFrame con todos los matches
        name_column: Columna con nombres normalizados
        
    Returns:
        dict: Estadísticas del componente
    """
    if len(component_indices) == 1:
        return {
            'avg_similarity': None,
            'min_similarity': None,
            'max_similarity': None,
            'size': 1,
            'needs_review': False
        }
    
    # Obtener matches dentro de este componente
    component_matches = matches_df[
        (matches_df['idx1'].isin(component_indices)) & 
        (matches_df['idx2'].isin(component_indices))
    ]
    
    if len(component_matches) == 0:
        return {
            'avg_similarity': None,
            'min_similarity': None,
            'max_similarity': None,
            'size': len(component_indices),
            'needs_review': True  # Componente sin matches es sospechoso
        }
    
    similarities = component_matches['similarity'].tolist()
    avg_sim = sum(similarities) / len(similarities)
    min_sim = min(similarities)
    max_sim = max(similarities)
    
    # Marcar para revisión si:
    # - Similitud promedio baja (< 90%)
    # - Similitud mínima muy baja (< 87%)
    # - Componente muy grande (> 20 nombres)
    needs_review = (
        avg_sim < 90 or 
        min_sim < 87 or 
        len(component_indices) > 20
    )
    
    return {
        'avg_similarity': avg_sim,
        'min_similarity': min_sim,
        'max_similarity': max_sim,
        'size': len(component_indices),
        'needs_review': needs_review
    }

def process_components(df, components, matches_df, name_column='normalized_name', 
                      freq_column='frequency', entity_type='financial'):
    """
    Procesa todos los componentes y asigna IDs y nombres estándar
    
    Args:
        df: DataFrame con los datos
        components: Lista de componentes (cada uno es un set de índices)
        matches_df: DataFrame con todos los matches
        name_column: Columna con nombres normalizados
        freq_column: Columna con frecuencias
        entity_type: Tipo de entidad ('financial' o 'non_financial')
        
    Returns:
        tuple: (DataFrame con mapeo, DataFrame con casos para revisión)
    """
    mapping_data = []
    review_cases = []
    
    print(f"   Procesando {len(components):,} componentes...")
    
    for component_id, component_indices in enumerate(components):
        # Seleccionar nombre estándar
        std_idx, std_name = select_standard_name(df, list(component_indices), name_column, freq_column)
        
        # Calcular estadísticas
        stats = calculate_component_stats(df, list(component_indices), matches_df, name_column)
        
        # Si necesita revisión, agregar a casos problemáticos
        if stats['needs_review']:
            component_names = [df.loc[idx, name_column] for idx in component_indices]
            review_cases.append({
                'component_id': component_id,
                'size': stats['size'],
                'avg_similarity': stats['avg_similarity'],
                'min_similarity': stats['min_similarity'],
                'standard_name': std_name,
                'all_names': component_names
            })
        
        # Crear entrada de mapeo para cada nombre en el componente
        for idx in component_indices:
            original_name = df.loc[idx, 'original_name']
            normalized_name = df.loc[idx, name_column]
            freq = df.loc[idx, freq_column]
            
            mapping_data.append({
                'entity_id': f"{entity_type}_{component_id}",
                'original_name': original_name,
                'normalized_name': normalized_name,
                'standard_name': std_name,
                'frequency': freq,
                'component_size': stats['size'],
                'avg_similarity': stats['avg_similarity'],
                'min_similarity': stats['min_similarity'],
                'needs_review': stats['needs_review']
            })
    
    mapping_df = pd.DataFrame(mapping_data)
    review_df = pd.DataFrame(review_cases) if review_cases else pd.DataFrame()
    
    return mapping_df, review_df

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos, componentes y matches
print("1. Cargando datos, componentes y matches...")
input_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"
components_file_financial = RESULTS_DIR / "financial_components.json"
components_file_non_financial = RESULTS_DIR / "non_financial_components.json"
matches_file_financial = RESULTS_DIR / "financial_matches.csv"
matches_file_non_financial = RESULTS_DIR / "non_financial_matches.csv"

if not all(f.exists() for f in [input_file_financial, input_file_non_financial,
                                 components_file_financial, components_file_non_financial,
                                 matches_file_financial, matches_file_non_financial]):
    print("   ✗ Error: No se encontraron los archivos necesarios.")
    print(f"   Por favor ejecuta primero: 11_fuzzy_matching_step4.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)
financial_matches_df = pd.read_csv(matches_file_financial)
non_financial_matches_df = pd.read_csv(matches_file_non_financial)

# Resetear índices
financial_df = financial_df.reset_index(drop=True)
non_financial_df = non_financial_df.reset_index(drop=True)
financial_matches_df = financial_matches_df.reset_index(drop=True)
non_financial_matches_df = non_financial_matches_df.reset_index(drop=True)

# Cargar componentes
with open(components_file_financial, 'r', encoding='utf-8') as f:
    financial_components_json = json.load(f)
    financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]

with open(components_file_non_financial, 'r', encoding='utf-8') as f:
    non_financial_components_json = json.load(f)
    non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]

print(f"   ✓ Financial entities: {len(financial_df):,} registros, {len(financial_components):,} componentes")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros, {len(non_financial_components):,} componentes")
print(f"   ✓ Financial matches: {len(financial_matches_df):,} pares")
print(f"   ✓ Non-financial matches: {len(non_financial_matches_df):,} pares")

# 2. PROCESAR componentes y asignar IDs
print("\n2. Procesando componentes y asignando IDs...")
print("   Financial entities:")
financial_mapping, financial_review = process_components(
    financial_df, financial_components, financial_matches_df,
    'normalized_name', 'frequency', 'financial'
)

print("   Non-financial entities:")
non_financial_mapping, non_financial_review = process_components(
    non_financial_df, non_financial_components, non_financial_matches_df,
    'normalized_name', 'frequency', 'non_financial'
)

print(f"\n   ✓ Componentes procesados:")
print(f"     - Financial: {len(financial_components):,} componentes")
print(f"     - Non-financial: {len(non_financial_components):,} componentes")
print(f"\n   ✓ Casos para revisión identificados:")
print(f"     - Financial: {len(financial_review):,} componentes")
print(f"     - Non-financial: {len(non_financial_review):,} componentes")

# 3. GUARDAR resultados
print("\n3. Guardando resultados...")

# Guardar mapeos finales
output_file_financial_mapping = FINAL_RESULTS_DIR / "financial_entity_mapping.csv"
output_file_non_financial_mapping = FINAL_RESULTS_DIR / "non_financial_entity_mapping.csv"

financial_mapping.to_csv(output_file_financial_mapping, index=False)
non_financial_mapping.to_csv(output_file_non_financial_mapping, index=False)

print(f"   ✓ Mapeos guardados:")
print(f"     - {output_file_financial_mapping}")
print(f"     - {output_file_non_financial_mapping}")

# Guardar casos para revisión
if len(financial_review) > 0:
    output_file_financial_review = FINAL_RESULTS_DIR / "financial_review_cases.csv"
    financial_review.to_csv(output_file_financial_review, index=False)
    print(f"     - {output_file_financial_review}")

if len(non_financial_review) > 0:
    output_file_non_financial_review = FINAL_RESULTS_DIR / "non_financial_review_cases.csv"
    non_financial_review.to_csv(output_file_non_financial_review, index=False)
    print(f"     - {output_file_non_financial_review}")

# 4. Estadísticas finales
print("\n4. Estadísticas finales:")
print("\n   Financial entities:")
print(f"     - Total registros: {len(financial_mapping):,}")
print(f"     - Entidades únicas: {financial_mapping['entity_id'].nunique():,}")
print(f"     - Componentes con múltiples nombres: {sum(1 for c in financial_components if len(c) > 1):,}")
print(f"     - Componentes únicos: {sum(1 for c in financial_components if len(c) == 1):,}")
print(f"     - Componentes para revisión: {len(financial_review):,}")

print("\n   Non-financial entities:")
print(f"     - Total registros: {len(non_financial_mapping):,}")
print(f"     - Entidades únicas: {non_financial_mapping['entity_id'].nunique():,}")
print(f"     - Componentes con múltiples nombres: {sum(1 for c in non_financial_components if len(c) > 1):,}")
print(f"     - Componentes únicos: {sum(1 for c in non_financial_components if len(c) == 1):,}")
print(f"     - Componentes para revisión: {len(non_financial_review):,}")

# 5. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Fase 5 completada")
print(f"✓ Financial entities: {financial_mapping['entity_id'].nunique():,} entidades únicas")
print(f"✓ Non-financial entities: {non_financial_mapping['entity_id'].nunique():,} entidades únicas")
print(f"✓ Resultados guardados en: {FINAL_RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 6 - Validación y Refinamiento")
print("=" * 80)

