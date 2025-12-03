"""
Módulo de Agrupación y Asignación de IDs
=========================================
Asigna IDs únicos y selecciona nombres estándar para cada grupo.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime


def run_grouping(financial_df, non_financial_df, financial_components, non_financial_components,
                 financial_matches_df, non_financial_matches_df, base_dir=None, transaction_type='pledge'):
    """
    Ejecuta agrupación y asignación de IDs.
    
    Args:
        financial_df: DataFrame con nombres normalizados
        non_financial_df: DataFrame con nombres normalizados
        financial_components: Lista de componentes financieros (sets de índices)
        non_financial_components: Lista de componentes no financieros
        financial_matches_df: DataFrame con matches financieros
        non_financial_matches_df: DataFrame con matches no financieros
        base_dir: Directorio base del proyecto
        transaction_type: Tipo de transacción ('pledge' o 'release')
        
    Returns:
        tuple: (financial_mapping, non_financial_mapping, financial_review, non_financial_review)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    final_results_dir = base_dir / "results" / "final"
    final_results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    if transaction_type:
        print(f"FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs ({transaction_type.upper()})")
    else:
        print("FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Resetear índices
    financial_df = financial_df.reset_index(drop=True)
    non_financial_df = non_financial_df.reset_index(drop=True)
    financial_matches_df = financial_matches_df.reset_index(drop=True)
    non_financial_matches_df = non_financial_matches_df.reset_index(drop=True)
    
    # Procesar componentes
    print("1. Procesando componentes y asignando IDs...")
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
    
    # Nota: No guardamos archivos intermedios aquí porque:
    # - *_entity_mapping.csv (sin _complete) es redundante
    # - *_review_cases.csv es redundante (Streamlit filtra por needs_review)
    # El archivo final *_entity_mapping_complete.csv se genera en complete_mapping.py
    print("\n2. Resultados procesados (archivos finales se generarán en complete_mapping.py)")
    print("   ℹ️  No se guardan archivos intermedios redundantes")
    print("   ℹ️  Streamlit puede filtrar casos para revisión dinámicamente")
    
    # Estadísticas
    print("\n3. Estadísticas finales:")
    print("\n   Financial entities:")
    print(f"     - Total registros: {len(financial_mapping):,}")
    print(f"     - Entidades únicas: {financial_mapping['entity_id'].nunique():,}")
    print(f"     - Componentes para revisión: {len(financial_review):,}")
    
    print("\n   Non-financial entities:")
    print(f"     - Total registros: {len(non_financial_mapping):,}")
    print(f"     - Entidades únicas: {non_financial_mapping['entity_id'].nunique():,}")
    print(f"     - Componentes para revisión: {len(non_financial_review):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Fase 5 completada")
    print(f"✓ Financial entities: {financial_mapping['entity_id'].nunique():,} entidades únicas")
    print(f"✓ Non-financial entities: {non_financial_mapping['entity_id'].nunique():,} entidades únicas")
    print(f"✓ Resultados guardados en: {final_results_dir}")
    print("=" * 80)
    
    return financial_mapping, non_financial_mapping, financial_review, non_financial_review


def select_standard_name(df, component_indices, name_column='normalized_name', freq_column='frequency'):
    """
    Selecciona el nombre estándar para un componente.
    
    Estrategia:
    1. Preferir el nombre con mayor frecuencia
    2. Si hay empate, preferir el más corto
    """
    if len(component_indices) == 1:
        idx = component_indices[0]
        return idx, df.loc[idx, name_column]
    
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
    
    return component_data[0]['idx'], component_data[0]['name']


def calculate_component_stats(df, component_indices, matches_df, name_column='normalized_name'):
    """Calcula estadísticas de un componente para identificar problemas."""
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
            'needs_review': True
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
    Procesa todos los componentes y asigna IDs y nombres estándar.
    
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


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    
    financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
    non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
    financial_matches_df = pd.read_csv(results_dir / "financial_matches.csv")
    non_financial_matches_df = pd.read_csv(results_dir / "non_financial_matches.csv")
    
    with open(results_dir / "financial_components.json", 'r', encoding='utf-8') as f:
        financial_components_json = json.load(f)
        financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]
    
    with open(results_dir / "non_financial_components.json", 'r', encoding='utf-8') as f:
        non_financial_components_json = json.load(f)
        non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]
    
    run_grouping(financial_df, non_financial_df, financial_components, non_financial_components,
                 financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None)

