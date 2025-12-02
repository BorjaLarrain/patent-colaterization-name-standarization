"""
Fase 6.1: Validación Automática
================================
Este script implementa validación automática para identificar:
- Grupos con baja similitud promedio
- Posibles falsos positivos
- Verificación de nombres conocidos (Figura 10)
- Nombres de alta frecuencia sin matches
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from rapidfuzz import fuzz

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
FINAL_RESULTS_DIR = BASE_DIR / "results" / "final"
VALIDATION_DIR = BASE_DIR / "results" / "validation"
VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

# Nombres de referencia de la Figura 10
REFERENCE_NAMES = [
    "BANK OF AMERICA",
    "SILICON VALLEY BANK",
    "WELLS FARGO",
    "JPMORGAN",
    "CITI",
    "GENERAL ELECTRIC CAPITAL",
    "COMERICA",
    "CREDIT SUISSE",
    "BANK OF NEW YORK",
    "FLEET",
    "PNC BANK",
    "WILMINGTON TRUST",
    "DEUTSCHE BANK",
    "US BANK",
    "WACHOVIA"
]

# Umbrales para validación
LOW_SIMILARITY_THRESHOLD = 90.0  # Similitud promedio baja
MIN_SIMILARITY_THRESHOLD = 87.0  # Similitud mínima sospechosa
LARGE_GROUP_SIZE = 20  # Grupos grandes para revisar
HIGH_FREQUENCY_THRESHOLD = 1000  # Frecuencia alta para nombres únicos

def find_reference_name_matches(normalized_name, reference_names):
    """
    Encuentra si un nombre normalizado coincide con algún nombre de referencia
    """
    matches = []
    normalized_upper = normalized_name.upper()
    
    for ref in reference_names:
        ref_upper = ref.upper()
        # Buscar si el nombre de referencia está contenido en el nombre normalizado
        if ref_upper in normalized_upper or normalized_upper in ref_upper:
            similarity = fuzz.WRatio(normalized_upper, ref_upper)
            if similarity >= 80:  # Threshold más bajo para referencia
                matches.append((ref, similarity))
    
    return matches

def validate_component_quality(df, component_indices, matches_df, name_column='normalized_name'):
    """
    Valida la calidad de un componente y identifica problemas potenciales
    """
    if len(component_indices) == 1:
        return {
            'is_valid': True,
            'issues': [],
            'reference_match': None
        }
    
    # Obtener matches dentro del componente
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
    max_sim = max(similarities)
    
    issues = []
    
    # Verificar similitud promedio
    if avg_sim < LOW_SIMILARITY_THRESHOLD:
        issues.append(f'Similitud promedio baja: {avg_sim:.1f}%')
    
    # Verificar similitud mínima
    if min_sim < MIN_SIMILARITY_THRESHOLD:
        issues.append(f'Similitud mínima muy baja: {min_sim:.1f}%')
    
    # Verificar tamaño del grupo
    if len(component_indices) > LARGE_GROUP_SIZE:
        issues.append(f'Grupo muy grande: {len(component_indices)} nombres')
    
    # Verificar nombres de referencia
    reference_matches = []
    for idx in component_indices:
        name = df.loc[idx, name_column]
        matches = find_reference_name_matches(name, REFERENCE_NAMES)
        if matches:
            reference_matches.extend(matches)
    
    reference_match = reference_matches[0][0] if reference_matches else None
    
    is_valid = len(issues) == 0
    
    return {
        'is_valid': is_valid,
        'issues': issues,
        'avg_similarity': avg_sim,
        'min_similarity': min_sim,
        'max_similarity': max_sim,
        'reference_match': reference_match,
        'size': len(component_indices)
    }

def find_high_frequency_singletons(df, freq_column='frequency', threshold=HIGH_FREQUENCY_THRESHOLD):
    """
    Encuentra nombres de alta frecuencia que no tienen matches (singletons)
    """
    # Esto requiere verificar qué nombres NO están en ningún componente
    # Por ahora, retornamos nombres de alta frecuencia
    high_freq = df[df[freq_column] >= threshold].copy()
    high_freq = high_freq.sort_values(freq_column, ascending=False)
    return high_freq

def validate_all_components(df, components, matches_df, name_column='normalized_name', 
                            freq_column='frequency', entity_type='financial'):
    """
    Valida todos los componentes y genera reporte
    """
    validation_results = []
    problematic_components = []
    
    print(f"   Validando {len(components):,} componentes...")
    
    for component_id, component_indices in enumerate(components):
        validation = validate_component_quality(
            df, list(component_indices), matches_df, name_column
        )
        
        # Obtener información del componente
        std_idx = min(component_indices)  # Índice del nombre estándar (simplificado)
        standard_name = df.loc[std_idx, name_column]
        total_freq = df.loc[list(component_indices), freq_column].sum()
        
        validation_results.append({
            'component_id': component_id,
            'entity_id': f"{entity_type}_{component_id}",
            'standard_name': standard_name,
            'size': validation['size'],
            'total_frequency': total_freq,
            'avg_similarity': validation.get('avg_similarity'),
            'min_similarity': validation.get('min_similarity'),
            'max_similarity': validation.get('max_similarity'),
            'is_valid': validation['is_valid'],
            'issues': '; '.join(validation['issues']) if validation['issues'] else None,
            'reference_match': validation.get('reference_match'),
            'num_issues': len(validation['issues'])
        })
        
        if not validation['is_valid'] or validation['issues']:
            problematic_components.append({
                'component_id': component_id,
                'standard_name': standard_name,
                'size': validation['size'],
                'issues': validation['issues'],
                'avg_similarity': validation.get('avg_similarity'),
                'min_similarity': validation.get('min_similarity'),
                'reference_match': validation.get('reference_match')
            })
    
    return pd.DataFrame(validation_results), problematic_components

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 6.1: VALIDACIÓN AUTOMÁTICA")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos
print("1. Cargando datos...")
mapping_file_financial = FINAL_RESULTS_DIR / "financial_entity_mapping.csv"
mapping_file_non_financial = FINAL_RESULTS_DIR / "non_financial_entity_mapping.csv"
components_file_financial = RESULTS_DIR / "financial_components.json"
components_file_non_financial = RESULTS_DIR / "non_financial_components.json"
matches_file_financial = RESULTS_DIR / "financial_matches.csv"
matches_file_non_financial = RESULTS_DIR / "non_financial_matches.csv"
data_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
data_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"

if not all(f.exists() for f in [mapping_file_financial, components_file_financial,
                                matches_file_financial, data_file_financial]):
    print("   ✗ Error: No se encontraron los archivos necesarios.")
    print(f"   Por favor ejecuta primero: 12_grouping_and_ids_step5.py")
    exit(1)

financial_df = pd.read_csv(data_file_financial)
non_financial_df = pd.read_csv(data_file_non_financial)
financial_matches_df = pd.read_csv(matches_file_financial)
non_financial_matches_df = pd.read_csv(matches_file_non_financial)

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

# 2. VALIDAR componentes
print("\n2. Validando componentes...")
print("   Financial entities:")
financial_validation, financial_problematic = validate_all_components(
    financial_df, financial_components, financial_matches_df,
    'normalized_name', 'frequency', 'financial'
)

print("   Non-financial entities:")
non_financial_validation, non_financial_problematic = validate_all_components(
    non_financial_df, non_financial_components, non_financial_matches_df,
    'normalized_name', 'frequency', 'non_financial'
)

print(f"\n   ✓ Componentes validados:")
print(f"     - Financial: {len(financial_validation):,} componentes")
print(f"     - Non-financial: {len(non_financial_validation):,} componentes")
print(f"\n   ✓ Componentes problemáticos identificados:")
print(f"     - Financial: {len(financial_problematic):,} componentes")
print(f"     - Non-financial: {len(non_financial_problematic):,} componentes")

# 3. ENCONTRAR nombres de alta frecuencia sin matches
print("\n3. Buscando nombres de alta frecuencia sin matches...")
financial_high_freq = find_high_frequency_singletons(financial_df, 'frequency', HIGH_FREQUENCY_THRESHOLD)
non_financial_high_freq = find_high_frequency_singletons(non_financial_df, 'frequency', HIGH_FREQUENCY_THRESHOLD)

print(f"   ✓ Nombres de alta frecuencia encontrados:")
print(f"     - Financial: {len(financial_high_freq):,} nombres (freq >= {HIGH_FREQUENCY_THRESHOLD:,})")
print(f"     - Non-financial: {len(non_financial_high_freq):,} nombres (freq >= {HIGH_FREQUENCY_THRESHOLD:,})")

# 4. GUARDAR resultados
print("\n4. Guardando resultados de validación...")

# Guardar validación completa
output_file_financial_validation = VALIDATION_DIR / "financial_validation_report.csv"
output_file_non_financial_validation = VALIDATION_DIR / "non_financial_validation_report.csv"

financial_validation.to_csv(output_file_financial_validation, index=False)
non_financial_validation.to_csv(output_file_non_financial_validation, index=False)

print(f"   ✓ Reportes de validación guardados:")
print(f"     - {output_file_financial_validation}")
print(f"     - {output_file_non_financial_validation}")

# Guardar componentes problemáticos
if financial_problematic:
    problematic_df = pd.DataFrame(financial_problematic)
    output_file = VALIDATION_DIR / "financial_problematic_components.csv"
    problematic_df.to_csv(output_file, index=False)
    print(f"     - {output_file}")

if non_financial_problematic:
    problematic_df = pd.DataFrame(non_financial_problematic)
    output_file = VALIDATION_DIR / "non_financial_problematic_components.csv"
    problematic_df.to_csv(output_file, index=False)
    print(f"     - {output_file}")

# Guardar nombres de alta frecuencia
if len(financial_high_freq) > 0:
    output_file = VALIDATION_DIR / "financial_high_frequency_names.csv"
    financial_high_freq.to_csv(output_file, index=False)
    print(f"     - {output_file}")

if len(non_financial_high_freq) > 0:
    output_file = VALIDATION_DIR / "non_financial_high_frequency_names.csv"
    non_financial_high_freq.to_csv(output_file, index=False)
    print(f"     - {output_file}")

# 5. Estadísticas
print("\n5. Estadísticas de validación:")
print("\n   Financial entities:")
valid_count = financial_validation['is_valid'].sum()
print(f"     - Componentes válidos: {valid_count:,} ({100*valid_count/len(financial_validation):.1f}%)")
print(f"     - Componentes problemáticos: {len(financial_problematic):,}")
print(f"     - Con similitud promedio < {LOW_SIMILARITY_THRESHOLD}%: {len(financial_validation[financial_validation['avg_similarity'] < LOW_SIMILARITY_THRESHOLD]):,}")
print(f"     - Con similitud mínima < {MIN_SIMILARITY_THRESHOLD}%: {len(financial_validation[financial_validation['min_similarity'] < MIN_SIMILARITY_THRESHOLD]):,}")
print(f"     - Grupos grandes (> {LARGE_GROUP_SIZE} nombres): {len(financial_validation[financial_validation['size'] > LARGE_GROUP_SIZE]):,}")

print("\n   Non-financial entities:")
valid_count = non_financial_validation['is_valid'].sum()
print(f"     - Componentes válidos: {valid_count:,} ({100*valid_count/len(non_financial_validation):.1f}%)")
print(f"     - Componentes problemáticos: {len(non_financial_problematic):,}")
print(f"     - Con similitud promedio < {LOW_SIMILARITY_THRESHOLD}%: {len(non_financial_validation[non_financial_validation['avg_similarity'] < LOW_SIMILARITY_THRESHOLD]):,}")
print(f"     - Con similitud mínima < {MIN_SIMILARITY_THRESHOLD}%: {len(non_financial_validation[non_financial_validation['min_similarity'] < MIN_SIMILARITY_THRESHOLD]):,}")
print(f"     - Grupos grandes (> {LARGE_GROUP_SIZE} nombres): {len(non_financial_validation[non_financial_validation['size'] > LARGE_GROUP_SIZE]):,}")

# 6. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Validación automática completada")
print(f"✓ Financial: {len(financial_problematic):,} componentes problemáticos identificados")
print(f"✓ Non-financial: {len(non_financial_problematic):,} componentes problemáticos identificados")
print(f"✓ Resultados guardados en: {VALIDATION_DIR}")
print(f"✓ Próximo paso: Fase 6.2 - Revisión Manual")
print("=" * 80)

