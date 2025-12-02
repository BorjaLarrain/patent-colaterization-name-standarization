"""
Fase 2.3: Normalización de Sufijos Legales
==========================================
Este script normaliza los sufijos legales de los nombres de entidades.
- Carga datos del paso anterior (step2_2)
- Normaliza sufijos legales según el do-file de referencia
- Guarda resultados en results/intermediate/

Sufijos a normalizar:
- NATIONAL ASSOCIATION → NA
- COMPANY → CO
- CORPORATION → CORP
- INCORPORATED → INC
- LIMITED → LTD
- Y variaciones con/sin puntos y espacios
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def normalize_legal_suffixes(name):
    """
    Normaliza sufijos legales en un nombre
    
    Args:
        name: String con el nombre a normalizar
        
    Returns:
        String con los sufijos legales normalizados
    """
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # Lista de normalizaciones de sufijos legales
    # Ordenados de más específicos a más generales
    
    # 1. NATIONAL ASSOCIATION → NA (manejar variaciones)
    cleaned = re.sub(r'\bNATIONAL\s+ASSOCIATION\b', 'NA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s*\.\s*A\s*\.\b', 'NA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s+A\b', 'NA', cleaned, flags=re.IGNORECASE)
    
    # 2. CORPORATION → CORP (manejar variaciones)
    cleaned = re.sub(r'\bCORPORATION\b', 'CORP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCORP\s*\.\b', 'CORP', cleaned, flags=re.IGNORECASE)
    
    # 3. INCORPORATED → INC (manejar variaciones)
    cleaned = re.sub(r'\bINCORPORATED\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINCORP\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINCORPORATION\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINC\s*\.\b', 'INC', cleaned, flags=re.IGNORECASE)
    
    # 4. COMPANY → CO (manejar variaciones)
    cleaned = re.sub(r'\bCOMPANIES\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOMPANY\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOMPN\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOS\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCO\s*\.\b', 'CO', cleaned, flags=re.IGNORECASE)
    
    # 5. LIMITED → LTD (manejar variaciones)
    cleaned = re.sub(r'\bUNLIMITED\b', 'UNLTD', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bLIMITED\b', 'LTD', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bLTD\s*\.\b', 'LTD', cleaned, flags=re.IGNORECASE)
    
    # 6. Otros sufijos comunes del do-file
    # LLC y variaciones
    cleaned = re.sub(r'\bL\s*\.\s*L\s*\.\s*C\s*\.\b', 'LLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+L\s+C\b', 'LLC', cleaned, flags=re.IGNORECASE)
    
    # LP y variaciones
    cleaned = re.sub(r'\bL\s*\.\s*P\s*\.\b', 'LP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+P\b', 'LP', cleaned, flags=re.IGNORECASE)
    
    # LLP y variaciones
    cleaned = re.sub(r'\bL\s*\.\s*L\s*\.\s*P\s*\.\b', 'LLP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+L\s+P\b', 'LLP', cleaned, flags=re.IGNORECASE)
    
    # PLC y variaciones
    cleaned = re.sub(r'\bPUB\s+LTD\s+CO\b', 'PLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bP\s*\.\s*L\s*\.\s*C\s*\.\b', 'PLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bP\s+L\s+C\b', 'PLC', cleaned, flags=re.IGNORECASE)
    
    # BANCORPORATION → BANCORP
    cleaned = re.sub(r'\bBANCORPORATION\b', 'BANCORP', cleaned, flags=re.IGNORECASE)
    
    # Sufijos internacionales comunes
    # AG (Aktiengesellschaft)
    cleaned = re.sub(r'\bAKTIENGESELLSCHAFT\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bAKTIENGESELL\s+SCHAFT\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bA\s*\.\s*G\s*\.\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bA\s+G\b', 'AG', cleaned, flags=re.IGNORECASE)
    
    # GMBH
    cleaned = re.sub(r'\bGESELLSCHAFT\s+MIT\s+BESCHRAENKTER\s+HAFTUNG\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bGESELLSCHAFT\s+MBH\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bGESELLSCHAFT\s+M\s+B\s+H\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bG\s*\.\s*M\s*\.\s*B\s*\.\s*H\s*\.\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bG\s+M\s+B\s+H\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    
    # NV (Naamloze Vennootschap)
    cleaned = re.sub(r'\bN\s*\.\s*V\s*\.\b', 'NV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s+V\b', 'NV', cleaned, flags=re.IGNORECASE)
    
    # BV (Besloten Vennootschap)
    cleaned = re.sub(r'\bB\s*\.\s*V\s*\.\b', 'BV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bB\s+V\b', 'BV', cleaned, flags=re.IGNORECASE)
    
    # SA (Société Anonyme)
    cleaned = re.sub(r'\bS\s*\.\s*A\s*\.\b', 'SA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+A\b', 'SA', cleaned, flags=re.IGNORECASE)
    
    # SRL (Società a Responsabilità Limitata)
    cleaned = re.sub(r'\bS\s*\.\s*R\s*\.\s*L\s*\.\b', 'SRL', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+R\s+L\b', 'SRL', cleaned, flags=re.IGNORECASE)
    
    # SARL (Société à Responsabilité Limitée)
    cleaned = re.sub(r'\bS\s*\.\s*A\s*\.\s*R\s*\.\s*L\s*\.\b', 'SARL', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+A\s+R\s+L\b', 'SARL', cleaned, flags=re.IGNORECASE)
    
    # Eliminar espacios múltiples resultantes
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Eliminar espacios al inicio y final
    cleaned = cleaned.strip()
    
    return cleaned

def apply_legal_suffix_normalization(df, name_column='name_no_roles'):
    """
    Aplica normalización de sufijos legales a una columna de nombres
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres
        
    Returns:
        DataFrame con nueva columna 'name_normalized_suffixes'
    """
    df_processed = df.copy()
    df_processed['name_normalized_suffixes'] = df_processed[name_column].apply(normalize_legal_suffixes)
    return df_processed

def show_examples(df, name_column, name_normalized_column, num_examples=10):
    """
    Muestra ejemplos de transformaciones aplicadas
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE TRANSFORMACIONES")
    print("=" * 80)
    
    # Filtrar solo los que cambiaron
    changed = df[df[name_column] != df[name_normalized_column]]
    
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
        normalized = row[name_normalized_column]
        
        print(f"\nEjemplo {idx}:")
        print(f"  Antes:        {original}")
        print(f"  Después:      {normalized}")
        if 'freq' in row:
            print(f"  Frecuencia:   {row['freq']:,}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 2.3: NORMALIZACIÓN DE SUFIJOS LEGALES")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos del paso anterior
print("1. Cargando datos del paso anterior (step2_2)...")
input_file_financial = RESULTS_DIR / "financial_normalized_step2_2.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_2.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos del paso anterior.")
    print(f"   Por favor ejecuta primero: 04_normalization_step2_2.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. APLICAR normalización de sufijos legales
print("\n2. Aplicando normalización de sufijos legales...")
financial_df = apply_legal_suffix_normalization(financial_df, 'name_no_roles')
non_financial_df = apply_legal_suffix_normalization(non_financial_df, 'name_no_roles')
print("   ✓ Normalización de sufijos aplicada")

# 3. Estadísticas de transformaciones
print("\n3. Estadísticas de transformaciones:")
financial_changed = (financial_df['name_no_roles'] != financial_df['name_normalized_suffixes']).sum()
non_financial_changed = (non_financial_df['name_no_roles'] != non_financial_df['name_normalized_suffixes']).sum()

print(f"   - Financial entities modificadas: {financial_changed:,} de {len(financial_df):,} "
      f"({100*financial_changed/len(financial_df):.1f}%)")
print(f"   - Non-financial entities modificadas: {non_financial_changed:,} de {len(non_financial_df):,} "
      f"({100*non_financial_changed/len(non_financial_df):.1f}%)")

# 4. Mostrar ejemplos
show_examples(financial_df, 'name_no_roles', 'name_normalized_suffixes', num_examples=8)

# 5. GUARDAR resultados intermedios
print("\n4. Guardando resultados intermedios...")
output_file_financial = RESULTS_DIR / "financial_normalized_step2_3.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_3.csv"

financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 6. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Sufijos legales normalizados en {financial_changed + non_financial_changed:,} nombres")
print(f"✓ Resultados intermedios guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 2.4 - Limpieza de Elementos Comunes")
print("=" * 80)

