"""
Módulo de Validación
====================
Implementa validación automática para identificar componentes problemáticos.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Umbrales para validación
LOW_SIMILARITY_THRESHOLD = 90.0
MIN_SIMILARITY_THRESHOLD = 87.0
LARGE_GROUP_SIZE = 20
HIGH_FREQUENCY_THRESHOLD = 1000


def run_validation_single(mapping, components, matches_df, entity_type, base_dir=None):
    """
    Ejecuta validación automática para un solo tipo de entidad.
    
    Args:
        mapping: DataFrame con mapeo
        components: Lista de componentes
        matches_df: DataFrame con matches
        entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
        base_dir: Directorio base del proyecto
        
    Returns:
        DataFrame de validación
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    validation_dir = base_dir / "results_transaction" / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print(f"FASE 6: VALIDACIÓN AUTOMÁTICA ({entity_type.upper()})")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Validar componentes
    print("1. Validando componentes...")
    validation, problematic = validate_all_components(
        mapping, components, matches_df, 'normalized_name', 'frequency', entity_type
    )
    
    # Guardar resultados
    print("\n2. Guardando resultados de validación...")
    output_file = validation_dir / f"{entity_type}_validation_report.csv"
    output_file_problematic = validation_dir / f"{entity_type}_problematic_components.csv"
    
    validation.to_csv(output_file, index=False)
    print(f"   ✓ {output_file}")
    
    if len(problematic) > 0:
        problematic.to_csv(output_file_problematic, index=False)
        print(f"   ✓ {output_file_problematic}")
    
    # Estadísticas
    print("\n3. Estadísticas de validación:")
    print(f"     - Componentes válidos: {len(validation[validation['is_valid'] == True]):,}")
    print(f"     - Componentes problemáticos: {len(problematic):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Validación completada para {entity_type}")
    print(f"✓ Resultados guardados en: {validation_dir}")
    print("=" * 80)
    
    return validation


def validate_component_quality(df, component_indices, matches_df, name_column='normalized_name'):
    """Valida la calidad de un componente e identifica problemas potenciales."""
    if len(component_indices) == 1:
        return {
            'is_valid': True,
            'issues': []
        }
    
    component_matches = matches_df[
        (matches_df['idx1'].isin(component_indices)) & 
        (matches_df['idx2'].isin(component_indices))
    ]
    
    if len(component_matches) == 0:
        return {
            'is_valid': False,
            'issues': ['No hay matches dentro del componente']
        }
    
    similarities = component_matches['similarity'].tolist()
    avg_sim = sum(similarities) / len(similarities)
    min_sim = min(similarities)
    
    issues = []
    
    if avg_sim < LOW_SIMILARITY_THRESHOLD:
        issues.append(f'Similitud promedio baja: {avg_sim:.1f}%')
    
    if min_sim < MIN_SIMILARITY_THRESHOLD:
        issues.append(f'Similitud mínima muy baja: {min_sim:.1f}%')
    
    if len(component_indices) > LARGE_GROUP_SIZE:
        issues.append(f'Grupo muy grande: {len(component_indices)} nombres')
    
    is_valid = len(issues) == 0
    
    return {
        'is_valid': is_valid,
        'issues': issues
    }


def validate_all_components(mapping_df, components, matches_df, name_column='normalized_name',
                           freq_column='frequency', entity_type='financial'):
    """
    Valida todos los componentes.
    
    Returns:
        tuple: (validation_report, problematic_components)
    """
    validation_results = []
    problematic = []
    
    print(f"   Validando {len(components):,} componentes...")
    
    for component_id, component_indices in enumerate(components):
        # Obtener índices del DataFrame desde el mapping
        component_mapping = mapping_df[mapping_df['entity_id'] == f"{entity_type}_{component_id}"]
        component_df_indices = component_mapping.index.tolist()
        
        if len(component_df_indices) == 0:
            continue
        
        validation = validate_component_quality(
            mapping_df, component_df_indices, matches_df, name_column
        )
        
        validation_results.append({
            'component_id': component_id,
            'size': len(component_indices),
            'is_valid': validation['is_valid'],
            'issues': '; '.join(validation['issues']) if validation['issues'] else None
        })
        
        if not validation['is_valid']:
            standard_name = component_mapping['standard_name'].iloc[0] if len(component_mapping) > 0 else None
            problematic.append({
                'component_id': component_id,
                'size': len(component_indices),
                'standard_name': standard_name,
                'issues': '; '.join(validation['issues'])
            })
    
    validation_df = pd.DataFrame(validation_results)
    problematic_df = pd.DataFrame(problematic) if problematic else pd.DataFrame()
    
    return validation_df, problematic_df


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    
    financial_mapping = pd.read_csv(final_results_dir / "financial_entity_mapping.csv")
    non_financial_mapping = pd.read_csv(final_results_dir / "non_financial_entity_mapping.csv")
    financial_matches_df = pd.read_csv(results_dir / "financial_matches.csv")
    non_financial_matches_df = pd.read_csv(results_dir / "non_financial_matches.csv")
    
    with open(results_dir / "financial_components.json", 'r', encoding='utf-8') as f:
        financial_components_json = json.load(f)
        financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]
    
    with open(results_dir / "non_financial_components.json", 'r', encoding='utf-8') as f:
        non_financial_components_json = json.load(f)
        non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]
    
    run_validation(financial_mapping, non_financial_mapping, financial_components, non_financial_components,
                  financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None)

