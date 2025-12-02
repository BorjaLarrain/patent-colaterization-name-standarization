"""
Fase 2.1: Limpieza Básica
=========================
Este script aplica limpieza básica a los nombres de entidades.
- Carga datos originales (sin modificar)
- Crea copias para trabajar
- Aplica transformaciones de limpieza básica
- Guarda resultados en results/intermediate/

Transformaciones aplicadas:
- Convertir todo a mayúsculas
- Eliminar espacios múltiples y normalizar espacios
- Eliminar caracteres especiales problemáticos
- Normalizar puntuación (N.A. → NA, U.S. → US, etc.)
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "Original_data"
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def basic_cleaning(name):
    """
    Aplica limpieza básica a un nombre según Fase 2.1
    
    Args:
        name: String con el nombre a limpiar
        
    Returns:
        String con el nombre limpiado
    """
    if pd.isna(name):
        return name
    
    # Convertir a string
    cleaned = str(name)
    
    # 1. Convertir todo a mayúsculas
    cleaned = cleaned.upper()
    
    # 2. Normalizar puntuación común ANTES de eliminar caracteres especiales
    # Esto permite preservar información importante
    cleaned = cleaned.replace('N.A.', 'NA')
    cleaned = cleaned.replace('N. A.', 'NA')
    cleaned = cleaned.replace('N.A', 'NA')
    cleaned = cleaned.replace('N A', 'NA')
    cleaned = cleaned.replace('U.S.', 'US')
    cleaned = cleaned.replace('U. S.', 'US')
    cleaned = cleaned.replace('U.S', 'US')
    cleaned = cleaned.replace('U S', 'US')
    
    # 3. Eliminar caracteres especiales problemáticos
    # Mantener solo: letras, números, espacios, y algunos caracteres básicos (&, -)
    # Primero normalizar algunos caracteres comunes
    cleaned = cleaned.replace('&AMP;', '&')  # HTML entities
    cleaned = cleaned.replace('&AMP', '&')
    
    # Eliminar caracteres especiales problemáticos pero mantener algunos útiles
    # Mantener: letras, números, espacios, &, -
    # Eliminar: comillas, paréntesis, corchetes, etc.
    cleaned = re.sub(r'[^\w\s&\-]', ' ', cleaned)  # \w = letras, números, _
    
    # 4. Normalizar espacios múltiples a un solo espacio
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # 5. Eliminar espacios al inicio y final (trim)
    cleaned = cleaned.strip()
    
    return cleaned

def apply_basic_cleaning(df, name_column):
    """
    Aplica limpieza básica a una columna de nombres en un DataFrame
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres
        
    Returns:
        DataFrame con nueva columna 'name_cleaned'
    """
    df_cleaned = df.copy()
    df_cleaned['name_cleaned'] = df_cleaned[name_column].apply(basic_cleaning)
    return df_cleaned

def show_examples(df_original, df_cleaned, name_column, num_examples=10):
    """
    Muestra ejemplos de transformaciones aplicadas
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE TRANSFORMACIONES")
    print("=" * 80)
    
    for i in range(min(num_examples, len(df_original))):
        original = df_original[name_column].iloc[i]
        cleaned = df_cleaned['name_cleaned'].iloc[i]
        
        if original != cleaned:
            print(f"\nEjemplo {i+1}:")
            print(f"  Original: {original}")
            print(f"  Limpiado: {cleaned}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 2.1: LIMPIEZA BÁSICA")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos originales (solo lectura, NO se modifican)
print("1. Cargando datos originales...")
financial_df_original = pd.read_csv(DATA_DIR / 'financial_entity_freq.csv')
non_financial_df_original = pd.read_csv(DATA_DIR / 'Non_financial_entity_freq.csv')
print(f"   ✓ Financial entities: {len(financial_df_original):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df_original):,} registros")
print("   ✓ Datos originales cargados (NO modificados)")

# 2. CREAR COPIA para trabajar (sin modificar originales)
print("\n2. Creando copias para procesamiento...")
financial_df = financial_df_original.copy()
non_financial_df = non_financial_df_original.copy()
print("   ✓ Copias creadas")

# 3. APLICAR limpieza básica
print("\n3. Aplicando limpieza básica...")
financial_df = apply_basic_cleaning(financial_df, 'ee_name')
non_financial_df = apply_basic_cleaning(non_financial_df, 'or_name')
print("   ✓ Limpieza básica aplicada")

# 4. Estadísticas de transformaciones
print("\n4. Estadísticas de transformaciones:")
financial_changed = (financial_df['ee_name'] != financial_df['name_cleaned']).sum()
non_financial_changed = (non_financial_df['or_name'] != non_financial_df['name_cleaned']).sum()

print(f"   - Financial entities modificadas: {financial_changed:,} de {len(financial_df):,} "
      f"({100*financial_changed/len(financial_df):.1f}%)")
print(f"   - Non-financial entities modificadas: {non_financial_changed:,} de {len(non_financial_df):,} "
      f"({100*non_financial_changed/len(non_financial_df):.1f}%)")

# 5. Mostrar ejemplos
show_examples(financial_df_original, financial_df, 'ee_name', num_examples=5)

# 6. GUARDAR resultados intermedios
print("\n5. Guardando resultados intermedios...")
output_file_financial = RESULTS_DIR / "financial_normalized_step2_1.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_1.csv"

# Guardar con todas las columnas originales + la nueva columna name_cleaned
financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 7. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Datos originales preservados en: {DATA_DIR}")
print(f"✓ Resultados intermedios guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 2.2 - Eliminación de Roles Funcionales")
print("=" * 80)

