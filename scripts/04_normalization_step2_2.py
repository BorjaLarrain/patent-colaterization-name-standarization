"""
Fase 2.2: Eliminación de Roles Funcionales
==========================================
Este script elimina roles funcionales de los nombres de entidades.
- Carga datos del paso anterior (step2_1)
- Elimina patrones de roles funcionales
- Guarda resultados en results/intermediate/

Roles funcionales a eliminar:
- AS COLLATERAL AGENT
- AS ADMINISTRATIVE AGENT
- AS NOTES COLLATERAL AGENT
- AS TRUSTEE
- AS AGENT
- Y variaciones con/sin comas y espacios
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

def remove_functional_roles(name):
    """
    Elimina roles funcionales de un nombre
    
    Args:
        name: String con el nombre a limpiar
        
    Returns:
        String con los roles funcionales eliminados
    """
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # Lista de patrones de roles funcionales a eliminar
    # Ordenados de más específicos a más generales para evitar eliminar partes incorrectas
    
    role_patterns = [
        # Patrones compuestos (más específicos primero)
        r'\s+AS\s+ADMINISTRATIVE\s+AND\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+COLLATERAL\s+AND\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+NOTES\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+FIRST\s+LIEN\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+SECOND\s+LIEN\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+TERM\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+ABL\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+COLLATERAL\s+TRUSTEE\s*',
        r'\s+AS\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+TRUSTEE\s*',
        r'\s+AS\s+AGENT\s*',
        # Variaciones con "THE"
        r'\s+AS\s+THE\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+THE\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+THE\s+TRUSTEE\s*',
        # Variaciones con información adicional
        r'\s+AS\s+AGENT\s+FOR\s+[^,]*',
        r'\s+AS\s+COLLATERAL\s+AGENT\s+FOR\s+[^,]*',
        r'\s+AS\s+ADMINISTRATIVE\s+AGENT\s+FOR\s+[^,]*',
        # Otros roles menos comunes
        r'\s+AS\s+SERVICING\s+AGENT\s*',
        r'\s+AS\s+SUCCESSOR\s+AGENT\s*',
        r'\s+AS\s+SUCCESSOR\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+SUCCESSOR\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+NEW\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+DOMESTIC\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+CANADIAN\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+U\.S\.\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+US\s+COLLATERAL\s+AGENT\s*',
    ]
    
    # Aplicar cada patrón
    for pattern in role_patterns:
        cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)
    
    # Limpiar espacios múltiples resultantes
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Eliminar espacios al inicio y final
    cleaned = cleaned.strip()
    
    return cleaned

def apply_role_removal(df, name_column='name_cleaned'):
    """
    Aplica eliminación de roles funcionales a una columna de nombres
    
    Args:
        df: DataFrame con los datos
        name_column: Nombre de la columna que contiene los nombres limpiados
        
    Returns:
        DataFrame con nueva columna 'name_no_roles'
    """
    df_processed = df.copy()
    df_processed['name_no_roles'] = df_processed[name_column].apply(remove_functional_roles)
    return df_processed

def show_examples(df, name_column, name_no_roles_column, num_examples=10):
    """
    Muestra ejemplos de transformaciones aplicadas
    """
    print("\n" + "=" * 80)
    print("EJEMPLOS DE TRANSFORMACIONES")
    print("=" * 80)
    
    # Filtrar solo los que cambiaron
    changed = df[df[name_column] != df[name_no_roles_column]]
    
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
        cleaned = row[name_no_roles_column]
        
        print(f"\nEjemplo {idx}:")
        print(f"  Con roles:    {original}")
        print(f"  Sin roles:    {cleaned}")
        if 'freq' in row:
            print(f"  Frecuencia:   {row['freq']:,}")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 2.2: ELIMINACIÓN DE ROLES FUNCIONALES")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos del paso anterior
print("1. Cargando datos del paso anterior (step2_1)...")
input_file_financial = RESULTS_DIR / "financial_normalized_step2_1.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_1.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos del paso anterior.")
    print(f"   Por favor ejecuta primero: 03_normalization_step2_1.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. APLICAR eliminación de roles funcionales
print("\n2. Aplicando eliminación de roles funcionales...")
financial_df = apply_role_removal(financial_df, 'name_cleaned')
non_financial_df = apply_role_removal(non_financial_df, 'name_cleaned')
print("   ✓ Eliminación de roles aplicada")

# 3. Estadísticas de transformaciones
print("\n3. Estadísticas de transformaciones:")
financial_changed = (financial_df['name_cleaned'] != financial_df['name_no_roles']).sum()
non_financial_changed = (non_financial_df['name_cleaned'] != non_financial_df['name_no_roles']).sum()

print(f"   - Financial entities modificadas: {financial_changed:,} de {len(financial_df):,} "
      f"({100*financial_changed/len(financial_df):.1f}%)")
print(f"   - Non-financial entities modificadas: {non_financial_changed:,} de {len(non_financial_df):,} "
      f"({100*non_financial_changed/len(non_financial_df):.1f}%)")

# 4. Mostrar ejemplos
show_examples(financial_df, 'name_cleaned', 'name_no_roles', num_examples=8)

# 5. GUARDAR resultados intermedios
print("\n4. Guardando resultados intermedios...")
output_file_financial = RESULTS_DIR / "financial_normalized_step2_2.csv"
output_file_non_financial = RESULTS_DIR / "non_financial_normalized_step2_2.csv"

financial_df.to_csv(output_file_financial, index=False)
non_financial_df.to_csv(output_file_non_financial, index=False)

print(f"   ✓ Resultados guardados:")
print(f"     - {output_file_financial}")
print(f"     - {output_file_non_financial}")

# 6. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Roles funcionales eliminados de {financial_changed + non_financial_changed:,} nombres")
print(f"✓ Resultados intermedios guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 2.3 - Normalización de Sufijos Legales")
print("=" * 80)

