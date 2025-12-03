"""
Módulo de Normalización
=======================
Consolida todos los pasos de normalización (scripts 03-07) en un solo módulo.
Procesa en memoria y guarda solo el resultado final.
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime


def normalize_names(financial_df, non_financial_df, base_dir=None, transaction_type='pledge'):
    """
    Normaliza nombres aplicando todos los pasos de normalización.
    
    Args:
        financial_df: DataFrame con entidades financieras (columna 'ee_name')
        non_financial_df: DataFrame con entidades no financieras (columna 'or_name')
        base_dir: Directorio base del proyecto
        transaction_type: Tipo de transacción ('pledge' o 'release')
        
    Returns:
        tuple: (financial_normalized, non_financial_normalized) - DataFrames normalizados
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    results_dir = base_dir / "results" / "intermediate"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    if transaction_type:
        print(f"FASE 2: NORMALIZACIÓN ({transaction_type.upper()})")
    else:
        print("FASE 2: NORMALIZACIÓN")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Crear copias para trabajar
    financial_work = financial_df.copy()
    non_financial_work = non_financial_df.copy()
    
    # Paso 2.1: Limpieza básica
    print("2.1. Aplicando limpieza básica...")
    financial_work['name_cleaned'] = financial_work['ee_name'].apply(basic_cleaning)
    non_financial_work['name_cleaned'] = non_financial_work['or_name'].apply(basic_cleaning)
    print(f"   ✓ Limpieza básica aplicada")
    
    # Paso 2.2: Eliminación de roles funcionales
    print("2.2. Eliminando roles funcionales...")
    financial_work['name_no_roles'] = financial_work['name_cleaned'].apply(remove_functional_roles)
    non_financial_work['name_no_roles'] = non_financial_work['name_cleaned'].apply(remove_functional_roles)
    print(f"   ✓ Roles funcionales eliminados")
    
    # Paso 2.3: Normalización de sufijos legales
    print("2.3. Normalizando sufijos legales...")
    financial_work['name_normalized_suffixes'] = financial_work['name_no_roles'].apply(normalize_legal_suffixes)
    non_financial_work['name_normalized_suffixes'] = non_financial_work['name_no_roles'].apply(normalize_legal_suffixes)
    print(f"   ✓ Sufijos legales normalizados")
    
    # Paso 2.4: Limpieza de elementos comunes
    print("2.4. Limpiando elementos comunes...")
    financial_work['name_cleaned_common'] = financial_work['name_normalized_suffixes'].apply(clean_common_elements)
    non_financial_work['name_cleaned_common'] = non_financial_work['name_normalized_suffixes'].apply(clean_common_elements)
    print(f"   ✓ Elementos comunes limpiados")
    
    # Paso 2.5: Normalización final
    print("2.5. Aplicando normalización final...")
    financial_work['name_normalized_final'] = financial_work['name_cleaned_common'].apply(final_normalization)
    non_financial_work['name_normalized_final'] = non_financial_work['name_cleaned_common'].apply(final_normalization)
    print(f"   ✓ Normalización final aplicada")
    
    # Crear versión simplificada para matching
    print("\n3. Creando versión simplificada para matching...")
    financial_normalized = financial_work[['ee_name', 'freq', 'name_normalized_final']].copy()
    financial_normalized.columns = ['original_name', 'frequency', 'normalized_name']
    
    non_financial_normalized = non_financial_work[['or_name', 'freq', 'name_normalized_final']].copy()
    non_financial_normalized.columns = ['original_name', 'frequency', 'normalized_name']
    
    # Guardar solo el resultado final con sufijo del tipo de transacción (si existe)
    print("\n4. Guardando resultados finales...")
    suffix = f"_{transaction_type}" if transaction_type else ""
    output_file_financial = results_dir / f"financial_normalized{suffix}.csv"
    output_file_non_financial = results_dir / f"non_financial_normalized{suffix}.csv"
    
    financial_normalized.to_csv(output_file_financial, index=False)
    non_financial_normalized.to_csv(output_file_non_financial, index=False)
    
    print(f"   ✓ {output_file_financial}")
    print(f"   ✓ {output_file_non_financial}")
    
    # Estadísticas
    print("\n5. Estadísticas:")
    print(f"   - Financial: {len(financial_normalized):,} nombres normalizados")
    print(f"   - Non-financial: {len(non_financial_normalized):,} nombres normalizados")
    print(f"   - Financial únicos: {financial_normalized['normalized_name'].nunique():,}")
    print(f"   - Non-financial únicos: {non_financial_normalized['normalized_name'].nunique():,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    if transaction_type:
        print(f"✓ Normalización completada para {transaction_type}")
    else:
        print("✓ Normalización completada")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return financial_normalized, non_financial_normalized


def basic_cleaning(name):
    """Aplica limpieza básica (Paso 2.1)."""
    if pd.isna(name):
        return name
    
    cleaned = str(name).upper()
    
    # Normalizar puntuación común
    cleaned = cleaned.replace('N.A.', 'NA')
    cleaned = cleaned.replace('N. A.', 'NA')
    cleaned = cleaned.replace('N.A', 'NA')
    cleaned = cleaned.replace('N A', 'NA')
    cleaned = cleaned.replace('U.S.', 'US')
    cleaned = cleaned.replace('U. S.', 'US')
    cleaned = cleaned.replace('U.S', 'US')
    cleaned = cleaned.replace('U S', 'US')
    
    # Normalizar nombres compuestos comunes (sin espacios)
    # Esto ayuda con casos como "WELLSFARGO" -> "WELLS FARGO"
    common_compound_names = {
        'WELLSFARGO': 'WELLS FARGO',
        'JPMORGAN': 'JP MORGAN',
        'BANKOFAMERICA': 'BANK OF AMERICA',
        'BANKOFAMER': 'BANK OF AMER',
    }
    
    for compound, expanded in common_compound_names.items():
        # Reemplazar solo si es una palabra completa (no parte de otra)
        cleaned = re.sub(r'\b' + compound + r'\b', expanded, cleaned)
    
    # Eliminar caracteres especiales problemáticos
    cleaned = cleaned.replace('&AMP;', '&')
    cleaned = cleaned.replace('&AMP', '&')
    cleaned = re.sub(r'[^\w\s&\-]', ' ', cleaned)
    
    # Normalizar espacios
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


def remove_functional_roles(name):
    """Elimina roles funcionales (Paso 2.2)."""
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    role_patterns = [
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
        r'\s+AS\s+THE\s+ADMINISTRATIVE\s+AGENT\s*',
        r'\s+AS\s+THE\s+COLLATERAL\s+AGENT\s*',
        r'\s+AS\s+THE\s+TRUSTEE\s*',
        r'\s+AS\s+AGENT\s+FOR\s+[^,]*',
        r'\s+AS\s+COLLATERAL\s+AGENT\s+FOR\s+[^,]*',
        r'\s+AS\s+ADMINISTRATIVE\s+AGENT\s+FOR\s+[^,]*',
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
    
    for pattern in role_patterns:
        cleaned = re.sub(pattern, ' ', cleaned, flags=re.IGNORECASE)
    
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


def normalize_legal_suffixes(name):
    """Normaliza sufijos legales (Paso 2.3)."""
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # NATIONAL ASSOCIATION → NA
    cleaned = re.sub(r'\bNATIONAL\s+ASSOCIATION\b', 'NA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s*\.\s*A\s*\.\b', 'NA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s+A\b', 'NA', cleaned, flags=re.IGNORECASE)
    
    # CORPORATION → CORP
    cleaned = re.sub(r'\bCORPORATION\b', 'CORP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCORP\s*\.\b', 'CORP', cleaned, flags=re.IGNORECASE)
    
    # INCORPORATED → INC
    cleaned = re.sub(r'\bINCORPORATED\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINCORP\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINCORPORATION\b', 'INC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bINC\s*\.\b', 'INC', cleaned, flags=re.IGNORECASE)
    
    # COMPANY → CO
    cleaned = re.sub(r'\bCOMPANIES\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOMPANY\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOMPN\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCOS\b', 'CO', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bCO\s*\.\b', 'CO', cleaned, flags=re.IGNORECASE)
    
    # LIMITED → LTD
    cleaned = re.sub(r'\bUNLIMITED\b', 'UNLTD', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bLIMITED\b', 'LTD', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bLTD\s*\.\b', 'LTD', cleaned, flags=re.IGNORECASE)
    
    # LLC, LP, LLP, PLC
    cleaned = re.sub(r'\bL\s*\.\s*L\s*\.\s*C\s*\.\b', 'LLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+L\s+C\b', 'LLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s*\.\s*P\s*\.\b', 'LP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+P\b', 'LP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s*\.\s*L\s*\.\s*P\s*\.\b', 'LLP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bL\s+L\s+P\b', 'LLP', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bPUB\s+LTD\s+CO\b', 'PLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bP\s*\.\s*L\s*\.\s*C\s*\.\b', 'PLC', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bP\s+L\s+C\b', 'PLC', cleaned, flags=re.IGNORECASE)
    
    # BANCORPORATION → BANCORP
    cleaned = re.sub(r'\bBANCORPORATION\b', 'BANCORP', cleaned, flags=re.IGNORECASE)
    
    # Sufijos internacionales
    cleaned = re.sub(r'\bAKTIENGESELLSCHAFT\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bAKTIENGESELL\s+SCHAFT\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bA\s*\.\s*G\s*\.\b', 'AG', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bA\s+G\b', 'AG', cleaned, flags=re.IGNORECASE)
    
    cleaned = re.sub(r'\bGESELLSCHAFT\s+MIT\s+BESCHRAENKTER\s+HAFTUNG\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bGESELLSCHAFT\s+MBH\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bGESELLSCHAFT\s+M\s+B\s+H\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bG\s*\.\s*M\s*\.\s*B\s*\.\s*H\s*\.\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bG\s+M\s+B\s+H\b', 'GMBH', cleaned, flags=re.IGNORECASE)
    
    cleaned = re.sub(r'\bN\s*\.\s*V\s*\.\b', 'NV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bN\s+V\b', 'NV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bB\s*\.\s*V\s*\.\b', 'BV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bB\s+V\b', 'BV', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s*\.\s*A\s*\.\b', 'SA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+A\b', 'SA', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s*\.\s*R\s*\.\s*L\s*\.\b', 'SRL', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+R\s+L\b', 'SRL', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s*\.\s*A\s*\.\s*R\s*\.\s*L\s*\.\b', 'SARL', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bS\s+A\s+R\s+L\b', 'SARL', cleaned, flags=re.IGNORECASE)
    
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


def clean_common_elements(name):
    """Limpia elementos comunes (Paso 2.4)."""
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    
    # Normalizar "AND" → "&"
    cleaned = re.sub(r'\bAND\b', '&', cleaned, flags=re.IGNORECASE)
    
    # Eliminar "THE" al inicio y final
    cleaned = re.sub(r'^THE\s+', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+THE$', '', cleaned, flags=re.IGNORECASE)
    
    # Normalizar abreviaciones comunes
    cleaned = re.sub(r'\bUNITED\s+STATES\b', 'US', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bU\s*\.\s*S\s*\.\b', 'US', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bUNITED\s+KINGDOM\b', 'UK', cleaned, flags=re.IGNORECASE)
    
    # Normalizar espacios alrededor de "&"
    cleaned = re.sub(r'\s*&\s*', ' & ', cleaned)
    
    # Eliminar espacios múltiples
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


def final_normalization(name):
    """Aplica normalización final (Paso 2.5)."""
    if pd.isna(name):
        return name
    
    cleaned = str(name)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


if __name__ == "__main__":
    # Para ejecución independiente
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from scripts.pipeline import merge_csv_files
    
    base_dir = Path(__file__).parent.parent.parent
    
    # Merge CSV files first
    merged_financial, merged_non_financial = merge_csv_files(base_dir)
    
    normalize_names(merged_financial, merged_non_financial, base_dir, transaction_type=None)

