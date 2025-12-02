"""
Fase 2.5: Normalización Final
=============================
Este script aplica la normalización final a los nombres de entidades.
- Carga datos del paso anterior (step2_4)
- Elimina espacios múltiples
- Trim de espacios al inicio/final
- Crea versión normalizada final para comparación
- Guarda resultados en results/intermediate/

Esta es la versión final normalizada que se usará para el fuzzy matching.
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def final_normalization(name):
    """
    Aplica normalización final a un nombre
    
    Args:
        name: String con el nombre a normalizar
        
    Returns:
        String con normalización final aplicada
    """
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # 1. Eliminar espacios múltiples
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # 2. Eliminar espacios al inicio y final (trim)
    cleaned = cleaned.strip()
    
    # 3. Asegurar que no queden espacios al inicio/final después de trim
    cleaned = cleaned.strip()
    
    return cleaned

def apply_final_normalization(df, name_column='name_cleaned_common'):
    """
    Aplica normalización final a una columna de nombres
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres
        
    Returns:
        DataFrame con nueva columna 'name_normalized_final'
    """
    df_processed = df.copy()
    df_processed['name_normalized_final'] = df_processed[name_column].apply(final_normalization)
    return df_processed

def show_examples(df, name_column, name_final_column, num_examples=10):
    """
    Muestra ejemplos de transformaciones aplicadas
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE TRANSFORMACIONES")
    print("=" * 80)
    
    # Filtrar solo los que cambiaron
    changed = df[df[name_column] != df[name_final_column]]
    
    if len(changed) == 0:
        print("No se encontraron transformaciones (todos los nombres ya estaban normalizados).")
        return
    
    # Ordenar por frecuencia descendente para mostrar los más relevantes
    if 'freq' in changed.columns:
        changed = changed.sort_values('freq', ascending=False)
    
    # Mostrar solo los primeros num_examples
    examples_to_show = changed.head(num_examples)
    
    for idx, (i, row) in enumerate(examples_to_show.iterrows(), 1):
        original = row[name_column]
        final = row[name_final_column]
        
        print(f"\nEjemplo {idx}:")
        print(f"  Antes:        '{original}'")
        print(f"  Después:      '{final}'")
        if 'freq' in row:
            print(f"  Frecuencia:   {row['freq']:,}")

def create_summary_statistics(df, name_final_column):
    """
    Crea estadísticas resumen de la normalización final
    """
    stats = {}
    
    # Longitud promedio
    df['name_length'] = df[name_final_column].str.len()
    stats['avg_length'] = df['name_length'].mean()
    stats['median_length'] = df['name_length'].median()
    stats['min_length'] = df['name_length'].min()
    stats['max_length'] = df['name_length'].max()
    
    # Nombres únicos
    stats['unique_names'] = df[name_final_column].nunique()
    stats['total_names'] = len(df)
    stats['reduction_ratio'] = 1 - (stats['unique_names'] / stats['total_names'])
    
    return stats

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 2.5: NORMALIZACIÓN FINAL")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos del paso anterior
print("1. Cargando datos del paso anterior (step2_4)...")
input_file_financial = RESULTS_DIR / "financial_normalized_step2_4.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_4.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos del paso anterior.")
    print(f"   Por favor ejecuta primero: 06_normalization_step2_4.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. APLICAR normalización final
print("\n2. Aplicando normalización final...")
financial_df = apply_final_normalization(financial_df, 'name_cleaned_common')
non_financial_df = apply_final_normalization(non_financial_df, 'name_cleaned_common')
print("   ✓ Normalización final aplicada")

# 3. Estadísticas de transformaciones
print("\n3. Estadísticas de transformaciones:")
financial_changed = (financial_df['name_cleaned_common'] != financial_df['name_normalized_final']).sum()
non_financial_changed = (non_financial_df['name_cleaned_common'] != non_financial_df['name_normalized_final']).sum()

print(f"   - Financial entities modificadas: {financial_changed:,} de {len(financial_df):,} "
      f"({100*financial_changed/len(financial_df):.1f}%)")
print(f"   - Non-financial entities modificadas: {non_financial_changed:,} de {len(non_financial_df):,} "
      f"({100*non_financial_changed/len(non_financial_df):.1f}%)")

# 4. Estadísticas resumen
print("\n4. Estadísticas resumen de nombres normalizados:")
financial_stats = create_summary_statistics(financial_df, 'name_normalized_final')
non_financial_stats = create_summary_statistics(non_financial_df, 'name_normalized_final')

print("\n   Financial entities:")
print(f"     - Nombres únicos: {financial_stats['unique_names']:,} de {financial_stats['total_names']:,}")
print(f"     - Reducción: {100*financial_stats['reduction_ratio']:.1f}%")
print(f"     - Longitud promedio: {financial_stats['avg_length']:.1f} caracteres")
print(f"     - Longitud mediana: {financial_stats['median_length']:.1f} caracteres")

print("\n   Non-financial entities:")
print(f"     - Nombres únicos: {non_financial_stats['unique_names']:,} de {non_financial_stats['total_names']:,}")
print(f"     - Reducción: {100*non_financial_stats['reduction_ratio']:.1f}%")
print(f"     - Longitud promedio: {non_financial_stats['avg_length']:.1f} caracteres")
print(f"     - Longitud mediana: {non_financial_stats['median_length']:.1f} caracteres")

# 5. Mostrar ejemplos
show_examples(financial_df, 'name_cleaned_common', 'name_normalized_final', num_examples=5)

# 6. GUARDAR resultados intermedios
print("\n5. Guardando resultados intermedios...")
output_file_financial = RESULTS_DIR / "financial_normalized_final.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_normalized_final.csv"

financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 7. Crear versión simplificada solo con columnas esenciales para matching
print("\n6. Creando versión simplificada para matching...")
financial_simple = financial_df[['ee_name', 'freq', 'name_normalized_final']].copy()
financial_simple.columns = ['original_name', 'frequency', 'normalized_name']

non_financial_simple = non_financial_df[['or_name', 'freq', 'name_normalized_final']].copy()
non_financial_simple.columns = ['original_name', 'frequency', 'normalized_name']

output_file_financial_simple = RESULTS_DIR / "financial_for_matching.csv"
output_file_non_financial_simple = RESULTS_DIR / "non_financial_for_matching.csv"

financial_simple.to_csv(output_file_financial_simple, index=False)
non_financial_simple.to_csv(output_file_non_financial_simple, index=False)

print(f"   ✓ Versiones simplificadas guardadas:")
print(f"     - {output_file_financial_simple}")
print(f"     - {output_file_non_financial_simple}")

# 8. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Normalización final completada")
print(f"✓ Archivos guardados en: {RESULTS_DIR}")
print(f"✓ Versiones simplificadas creadas para matching")
print(f"\n✓ FASE 2 COMPLETADA - Nombres normalizados listos para:")
print(f"  - Fase 3: Blocking")
print(f"  - Fase 4: Fuzzy Matching")
print("=" * 80)

