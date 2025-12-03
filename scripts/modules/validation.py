"""
Módulo de Validación
====================
Implementa validación automática para identificar componentes problemáticos.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from rapidfuzz import fuzz

# Nombres de referencia de la Figura 10
REFERENCE_NAMES = [
    "BANK OF AMERICA", "SILICON VALLEY BANK", "WELLS FARGO", "JPMORGAN",
    "CITI", "GENERAL ELECTRIC CAPITAL", "COMERICA", "CREDIT SUISSE",
    "BANK OF NEW YORK", "FLEET", "PNC BANK", "WILMINGTON TRUST",
    "DEUTSCHE BANK", "US BANK", "WACHOVIA"
]

# Umbrales para validación
LOW_SIMILARITY_THRESHOLD = 90.0
MIN_SIMILARITY_THRESHOLD = 87.0
LARGE_GROUP_SIZE = 20
HIGH_FREQUENCY_THRESHOLD = 1000


def run_validation(financial_mapping, non_financial_mapping, financial_components, non_financial_components,
                  financial_matches_df, non_financial_matches_df, base_dir=None, transaction_type='pledge'):
    """
    Ejecuta validación automática.
    
    Args:
        financial_mapping: DataFrame con mapeo financiero
        non_financial_mapping: DataFrame con mapeo no financiero
        financial_components: Lista de componentes financieros
        non_financial_components: Lista de componentes no financieros
        financial_matches_df: DataFrame con matches financieros
        non_financial_matches_df: DataFrame con matches no financieros
        base_dir: Directorio base del proyecto
        transaction_type: Tipo de transacción ('pledge' o 'release')
        
    Returns:
        tuple: (financial_validation, non_financial_validation)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    validation_dir = base_dir / "results" / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("FASE 6.1: VALIDACIÓN AUTOMÁTICA")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Validar componentes financieros
    print("1. Validando componentes financieros...")
    financial_validation, financial_problematic = validate_all_components(
        financial_mapping, financial_components, financial_matches_df, 'normalized_name', 'frequency', 'financial'
    )
    
    # Validar componentes no financieros
    print("2. Validando componentes no financieros...")
    non_financial_validation, non_financial_problematic = validate_all_components(
        non_financial_mapping, non_financial_components, non_financial_matches_df, 'normalized_name', 'frequency', 'non_financial'
    )
    
    # Guardar resultados
    print("\n3. Guardando resultados de validación...")
    
    suffix = f"_{transaction_type}"
    output_file_financial = validation_dir / f"financial_validation_report{suffix}.csv"
    output_file_non_financial = validation_dir / f"non_financial_validation_report{suffix}.csv"
    output_file_financial_problematic = validation_dir / f"financial_problematic_components{suffix}.csv"
    output_file_non_financial_problematic = validation_dir / f"non_financial_problematic_components{suffix}.csv"
    
    financial_validation.to_csv(output_file_financial, index=False)
    non_financial_validation.to_csv(output_file_non_financial, index=False)
    
    if len(financial_problematic) > 0:
        financial_problematic.to_csv(output_file_financial_problematic, index=False)
        print(f"   ✓ {output_file_financial_problematic}")
    
    if len(non_financial_problematic) > 0:
        non_financial_problematic.to_csv(output_file_non_financial_problematic, index=False)
        print(f"   ✓ {output_file_non_financial_problematic}")
    
    print(f"   ✓ {output_file_financial}")
    print(f"   ✓ {output_file_non_financial}")
    
    # Estadísticas
    print("\n4. Estadísticas de validación:")
    print("\n   Financial entities:")
    print(f"     - Componentes válidos: {len(financial_validation[financial_validation['is_valid'] == True]):,}")
    print(f"     - Componentes problemáticos: {len(financial_problematic):,}")
    
    print("\n   Non-financial entities:")
    print(f"     - Componentes válidos: {len(non_financial_validation[non_financial_validation['is_valid'] == True]):,}")
    print(f"     - Componentes problemáticos: {len(non_financial_problematic):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Validación completada")
    print(f"✓ Resultados guardados en: {validation_dir}")
    print("=" * 80)
    
    return financial_validation, non_financial_validation


def validate_component_quality(df, component_indices, matches_df, name_column='normalized_name'):
    """Valida la calidad de un componente y identifica problemas potenciales."""
    if len(component_indices) == 1:
        return {
            'is_valid': True,
            'issues': [],
            'reference_match': None
        }
    
    component_matches = matches_df[
        (matches_df['idx1'].isin(component_indices)) & 
        (matches_df['idx2'].isin(component_indices))
    ]
    
    if len(component_matches) == 0:
        return {
            'is_valid': False,
            'issues': ['No hay matches dentro del componente'],
            'reference_match': None
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
    
    # Verificar nombres de referencia
    reference_match = None
    component_names = [df.loc[idx, name_column] for idx in component_indices]
    for name in component_names:
        for ref in REFERENCE_NAMES:
            similarity = fuzz.WRatio(name.upper(), ref.upper())
            if similarity >= 80:
                reference_match = ref
                break
        if reference_match:
            break
    
    is_valid = len(issues) == 0
    
    return {
        'is_valid': is_valid,
        'issues': issues,
        'reference_match': reference_match
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
            'issues': '; '.join(validation['issues']) if validation['issues'] else None,
            'reference_match': validation['reference_match']
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
    
    financial_mapping = pd.read_csv(final_results_dir / "financial_entity_mapping_pledge.csv")
    non_financial_mapping = pd.read_csv(final_results_dir / "non_financial_entity_mapping_pledge.csv")
    financial_matches_df = pd.read_csv(results_dir / "financial_matches_pledge.csv")
    non_financial_matches_df = pd.read_csv(results_dir / "non_financial_matches_pledge.csv")
    
    with open(results_dir / "financial_components_pledge.json", 'r', encoding='utf-8') as f:
        financial_components_json = json.load(f)
        financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]
    
    with open(results_dir / "non_financial_components_pledge.json", 'r', encoding='utf-8') as f:
        non_financial_components_json = json.load(f)
        non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]
    
    run_validation(financial_mapping, non_financial_mapping, financial_components, non_financial_components,
                  financial_matches_df, non_financial_matches_df, base_dir, transaction_type='pledge')

