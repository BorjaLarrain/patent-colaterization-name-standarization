"""
Fase 3.1: Extracción de Primera Palabra Normalizada
===================================================
Este script extrae la primera palabra significativa de cada nombre normalizado
para crear bloques para el fuzzy matching.

- Carga datos normalizados finales
- Extrae primera palabra significativa
- Maneja casos especiales (THE, palabras genéricas, etc.)
- Guarda resultados en results/intermediate/
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from collections import Counter

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Palabras genéricas que no son distintivas (comunes en nombres de entidades)
GENERIC_WORDS = {
    'THE', 'BANK', 'COMPANY', 'CO', 'CORP', 'INC', 'LTD', 'LLC', 'LP', 
    'PLC', 'NA', 'TRUST', 'TRUSTEE', 'AGENT', 'ASSOCIATION', 'ASSOC',
    'GROUP', 'GRP', 'HOLDINGS', 'HLDG', 'INTERNATIONAL', 'INTL', 'US',
    'USA', 'NATIONAL', 'NATL'
}

# Preposiciones y artículos que no son distintivos
PREPOSITIONS = {
    'OF', 'AND', '&', 'FOR', 'IN', 'ON', 'AT', 'TO', 'BY', 'WITH', 'FROM'
}

def extract_first_significant_word(name):
    """
    Extrae la primera palabra significativa de un nombre normalizado
    
    Args:
        name: String con el nombre normalizado
        
    Returns:
        String con la primera palabra significativa (en mayúsculas)
    """
    if pd.isna(name) or not str(name).strip():
        return None
    
    # Convertir a string y limpiar
    name_str = str(name).strip().upper()
    
    # Dividir en palabras
    words = name_str.split()
    
    if not words:
        return None
    
    # Caso 1: Si empieza con "THE", tomar la segunda palabra
    if words[0] == 'THE' and len(words) > 1:
        first_word = words[1]
    else:
        first_word = words[0]
    
    # Caso 2: Si la primera palabra es muy genérica, buscar palabra significativa
    if first_word in GENERIC_WORDS or first_word in PREPOSITIONS:
        # Patrón común: "BANK OF X" o "COMPANY OF X" → tomar X (tercera palabra)
        if len(words) >= 3 and words[0] in GENERIC_WORDS and words[1] == 'OF':
            first_word = words[2]
        # Si segunda palabra no es genérica ni preposición, usarla
        elif len(words) > 1 and words[1] not in GENERIC_WORDS and words[1] not in PREPOSITIONS:
            first_word = words[1]
        # Si segunda es preposición, intentar tercera
        elif len(words) > 2 and words[1] in PREPOSITIONS and words[2] not in GENERIC_WORDS and words[2] not in PREPOSITIONS:
            first_word = words[2]
        # Si tercera también es genérica, intentar cuarta
        elif len(words) > 3 and words[3] not in GENERIC_WORDS and words[3] not in PREPOSITIONS:
            first_word = words[3]
        # Si todas son genéricas/preposiciones, usar la primera disponible que no sea preposición
        else:
            for word in words[1:]:
                if word not in PREPOSITIONS:
                    first_word = word
                    break
    
    # Limpiar la palabra (eliminar puntuación residual)
    first_word = re.sub(r'[^\w]', '', first_word)
    
    return first_word if first_word else None

def apply_blocking_key_extraction(df, name_column='normalized_name'):
    """
    Extrae la clave de blocking (primera palabra significativa) para cada nombre
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres normalizados
        
    Returns:
        DataFrame con nueva columna 'blocking_key'
    """
    df_processed = df.copy()
    df_processed['blocking_key'] = df_processed[name_column].apply(extract_first_significant_word)
    return df_processed

def analyze_blocking_keys(df, blocking_key_column='blocking_key'):
    """
    Analiza las claves de blocking para entender la distribución
    """
    print("\n" + "=" * 80)
    print("ANÁLISIS DE CLAVES DE BLOCKING")
    print("=" * 80)
    
    # Contar frecuencia de cada clave
    key_counts = df[blocking_key_column].value_counts()
    
    print(f"\nTotal de claves únicas: {len(key_counts):,}")
    print(f"\nTop 20 claves más frecuentes:")
    print("-" * 80)
    for key, count in key_counts.head(20).items():
        print(f"  {key:30s} : {count:>6,} nombres")
    
    # Identificar bloques grandes
    large_blocks = key_counts[key_counts > 100]
    if len(large_blocks) > 0:
        print(f"\n⚠️  Bloques grandes (>100 nombres): {len(large_blocks)}")
        print("   Estos bloques pueden necesitar sub-bloqueo:")
        for key, count in large_blocks.head(10).items():
            print(f"     {key:30s} : {count:>6,} nombres")
    
    # Identificar bloques muy grandes
    very_large_blocks = key_counts[key_counts > 1000]
    if len(very_large_blocks) > 0:
        print(f"\n⚠️  ⚠️  Bloques muy grandes (>1000 nombres): {len(very_large_blocks)}")
        for key, count in very_large_blocks.items():
            print(f"     {key:30s} : {count:>6,} nombres")
    
    return key_counts

def show_examples(df, name_column, blocking_key_column, num_examples=15):
    """
    Muestra ejemplos de extracción de claves de blocking
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE EXTRACCIÓN DE CLAVES DE BLOCKING")
    print("=" * 80)
    
    # Mostrar ejemplos diversos
    examples = df[[name_column, blocking_key_column]].drop_duplicates().head(num_examples)
    
    for idx, (i, row) in enumerate(examples.iterrows(), 1):
        name = row[name_column]
        key = row[blocking_key_column]
        
        print(f"\nEjemplo {idx}:")
        print(f"  Nombre:       {name}")
        print(f"  Clave bloque: {key}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 3.1: EXTRACCIÓN DE PRIMERA PALABRA NORMALIZADA")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos normalizados finales
print("1. Cargando datos normalizados finales...")
input_file_financial = RESULTS_DIR / "financial_for_matching.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_for_matching.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos de datos normalizados.")
    print(f"   Por favor ejecuta primero: 07_normalization_step2_5.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. APLICAR extracción de claves de blocking
print("\n2. Extrayendo claves de blocking...")
financial_df = apply_blocking_key_extraction(financial_df, 'normalized_name')
non_financial_df = apply_blocking_key_extraction(non_financial_df, 'normalized_name')
print("   ✓ Claves de blocking extraídas")

# 3. Verificar que no haya valores nulos
financial_nulls = financial_df['blocking_key'].isna().sum()
non_financial_nulls = non_financial_df['blocking_key'].isna().sum()

if financial_nulls > 0 or non_financial_nulls > 0:
    print(f"\n   ⚠️  Advertencia: {financial_nulls + non_financial_nulls} nombres sin clave de blocking")
    print(f"      (Financial: {financial_nulls}, Non-financial: {non_financial_nulls})")

# 4. Analizar distribución de bloques
print("\n3. Analizando distribución de bloques...")
print("\n" + "=" * 80)
print("FINANCIAL ENTITIES")
print("=" * 80)
financial_key_counts = analyze_blocking_keys(financial_df, 'blocking_key')

print("\n" + "=" * 80)
print("NON-FINANCIAL ENTITIES")
print("=" * 80)
non_financial_key_counts = analyze_blocking_keys(non_financial_df, 'blocking_key')

# 5. Mostrar ejemplos
print("\n4. Ejemplos de extracción:")
show_examples(financial_df, 'normalized_name', 'blocking_key', num_examples=10)

# 6. GUARDAR resultados
print("\n5. Guardando resultados...")
output_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"

financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 7. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Claves de blocking extraídas para {len(financial_df) + len(non_financial_df):,} nombres")
print(f"✓ Financial entities: {financial_df['blocking_key'].nunique():,} bloques únicos")
print(f"✓ Non-financial entities: {non_financial_df['blocking_key'].nunique():,} bloques únicos")
print(f"✓ Resultados guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 3.2 - Creación de Bloques")
print("=" * 80)

