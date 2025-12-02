"""
Fase 2.4: Limpieza de Elementos Comunes
=======================================
Este script limpia elementos comunes de los nombres de entidades.
- Carga datos del paso anterior (step2_3)
- Elimina "THE" al inicio/final
- Normaliza "AND" → "&"
- Elimina información geográfica redundante (con cuidado)
- Normaliza abreviaciones comunes
- Guarda resultados en results/intermediate/
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def clean_common_elements(name):
    """
    Limpia elementos comunes de un nombre
    
    Args:
        name: String con el nombre a limpiar
        
    Returns:
        String con elementos comunes limpiados
    """
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # 1. Normalizar "AND" → "&"
    # Solo cuando "AND" está entre palabras (no al inicio/final)
    cleaned = re.sub(r'\bAND\b', '&', cleaned, flags=re.IGNORECASE)
    
    # 2. Eliminar "THE" al inicio y final
    cleaned = re.sub(r'^THE\s+', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+THE$', '', cleaned, flags=re.IGNORECASE)
    
    # 3. Normalizar abreviaciones comunes (solo las más relevantes y seguras)
    # Estas son abreviaciones que aparecen frecuentemente en nombres de entidades
    
    # UNITED STATES → US
    cleaned = re.sub(r'\bUNITED\s+STATES\b', 'US', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bU\s*\.\s*S\s*\.\b', 'US', cleaned, flags=re.IGNORECASE)
    
    # UNITED KINGDOM → UK
    cleaned = re.sub(r'\bUNITED\s+KINGDOM\b', 'UK', cleaned, flags=re.IGNORECASE)
    
    # NOTA: NO normalizamos "AMERICA" → "AMER" porque:
    # - "BANK OF AMERICA" es un nombre core conocido
    # - Cambiarlo puede crear confusión y empeorar el matching
    # - El do-file está diseñado para otro contexto (patentes)
    # - Para entidades financieras, es mejor ser conservador
    
    # 4. Eliminar información geográfica redundante al final
    # Solo si aparece después de un sufijo legal y no es parte del nombre core
    # Ejemplo: "BANK INC CA" → "BANK INC" (si CA es California, pero esto es riesgoso)
    # Por ahora, ser conservador y NO eliminar estados/países automáticamente
    # ya que pueden ser parte del nombre legal
    
    # 5. Normalizar espacios alrededor de "&"
    cleaned = re.sub(r'\s*&\s*', ' & ', cleaned)
    
    # 6. Eliminar espacios múltiples resultantes
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # 7. Eliminar espacios al inicio y final
    cleaned = cleaned.strip()
    
    return cleaned

def apply_common_elements_cleaning(df, name_column='name_normalized_suffixes'):
    """
    Aplica limpieza de elementos comunes a una columna de nombres
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres
        
    Returns:
        DataFrame con nueva columna 'name_cleaned_common'
    """
    df_processed = df.copy()
    df_processed['name_cleaned_common'] = df_processed[name_column].apply(clean_common_elements)
    return df_processed

def show_examples(df, name_column, name_cleaned_column, num_examples=10):
    """
    Muestra ejemplos de transformaciones aplicadas
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE TRANSFORMACIONES")
    print("=" * 80)
    
    # Filtrar solo los que cambiaron
    changed = df[df[name_column] != df[name_cleaned_column]]
    
    if len(changed) == 0:
        print("No se encontraron transformaciones.")
        return
    
    # Ordenar por frecuencia descendente para mostrar los más relevantes
    if 'freq' in changed.columns:
        changed = changed.sort_values('freq', ascending=False)
    
    # Mostrar solo los primeros num_examples
    examples_to_show = changed.head(num_examples)
    
    for idx, (i, row) in enumerate(examples_to_show.iterrows(), 1):
        original = row[name_column]
        cleaned = row[name_cleaned_column]
        
        print(f"\nEjemplo {idx}:")
        print(f"  Antes:        {original}")
        print(f"  Después:      {cleaned}")
        if 'freq' in row:
            print(f"  Frecuencia:   {row['freq']:,}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 2.4: LIMPIEZA DE ELEMENTOS COMUNES")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos del paso anterior
print("1. Cargando datos del paso anterior (step2_3)...")
input_file_financial = RESULTS_DIR / "financial_normalized_step2_3.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_3.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos del paso anterior.")
    print(f"   Por favor ejecuta primero: 05_normalization_step2_3.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. APLICAR limpieza de elementos comunes
print("\n2. Aplicando limpieza de elementos comunes...")
financial_df = apply_common_elements_cleaning(financial_df, 'name_normalized_suffixes')
non_financial_df = apply_common_elements_cleaning(non_financial_df, 'name_normalized_suffixes')
print("   ✓ Limpieza de elementos comunes aplicada")

# 3. Estadísticas de transformaciones
print("\n3. Estadísticas de transformaciones:")
financial_changed = (financial_df['name_normalized_suffixes'] != financial_df['name_cleaned_common']).sum()
non_financial_changed = (non_financial_df['name_normalized_suffixes'] != non_financial_df['name_cleaned_common']).sum()

print(f"   - Financial entities modificadas: {financial_changed:,} de {len(financial_df):,} "
      f"({100*financial_changed/len(financial_df):.1f}%)")
print(f"   - Non-financial entities modificadas: {non_financial_changed:,} de {len(non_financial_df):,} "
      f"({100*non_financial_changed/len(non_financial_df):.1f}%)")

# 4. Mostrar ejemplos
show_examples(financial_df, 'name_normalized_suffixes', 'name_cleaned_common', num_examples=8)

# 5. GUARDAR resultados intermedios
print("\n4. Guardando resultados intermedios...")
output_file_financial = RESULTS_DIR / "financial_normalized_step2_4.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_4.csv"

financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 6. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Elementos comunes limpiados en {financial_changed + non_financial_changed:,} nombres")
print(f"✓ Resultados intermedios guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 2.5 - Normalización Final")
print("=" * 80)

