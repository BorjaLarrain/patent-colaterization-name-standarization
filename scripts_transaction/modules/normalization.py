"""
Módulo de Normalización (Transaction Pipeline)
==============================================
Normaliza nombres para un tipo de entidad específico.
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime


def normalize_names_single(entity_df, entity_type, base_dir=None):
    """
    Normaliza nombres para un solo tipo de entidad.
    
    Args:
        entity_df: DataFrame con entidades (columna 'ee_name' o 'or_name')
        entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
        base_dir: Directorio base del proyecto
        
    Returns:
        DataFrame normalizado
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    results_dir = base_dir / "results_transaction" / "intermediate"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print(f"FASE 2: NORMALIZACIÓN ({entity_type.upper()})")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Crear copia para trabajar
    work_df = entity_df.copy()
    
    # Detectar columna de nombre desde el DataFrame
    if 'ee_name' in work_df.columns:
        name_column = 'ee_name'
    elif 'or_name' in work_df.columns:
        name_column = 'or_name'
    else:
        raise ValueError(f"No se encontró columna 'ee_name' ni 'or_name' en el DataFrame. Columnas disponibles: {list(work_df.columns)}")
    
    print(f"   ℹ️  Usando columna: {name_column}")
    
    # Paso 2.1: Limpieza básica
    print("2.1. Aplicando limpieza básica...")
    work_df['name_cleaned'] = work_df[name_column].apply(basic_cleaning)
    print(f"   ✓ Limpieza básica aplicada")
    
    # Paso 2.2: Eliminación de roles funcionales
    print("2.2. Eliminando roles funcionales...")
    work_df['name_no_roles'] = work_df['name_cleaned'].apply(remove_functional_roles)
    print(f"   ✓ Roles funcionales eliminados")
    
    # Paso 2.3: Normalización de sufijos legales
    print("2.3. Normalizando sufijos legales...")
    work_df['name_normalized_suffixes'] = work_df['name_no_roles'].apply(normalize_legal_suffixes)
    print(f"   ✓ Sufijos legales normalizados")
    
    # Paso 2.4: Limpieza de elementos comunes
    print("2.4. Limpiando elementos comunes...")
    work_df['name_cleaned_common'] = work_df['name_normalized_suffixes'].apply(clean_common_elements)
    print(f"   ✓ Elementos comunes limpiados")
    
    # Paso 2.5: Normalización final
    print("2.5. Aplicando normalización final...")
    work_df['name_normalized_final'] = work_df['name_cleaned_common'].apply(final_normalization)
    print(f"   ✓ Normalización final aplicada")
    
    # Crear versión simplificada para matching
    print("\n3. Creando versión simplificada para matching...")
    # Asegurar que tenemos la columna 'freq'
    if 'freq' not in work_df.columns:
        # Buscar columna de frecuencia con nombre alternativo
        freq_col = None
        for col in ['freq', 'frequency', 'Frequency', 'Freq']:
            if col in work_df.columns:
                freq_col = col
                break
        if freq_col is None:
            raise ValueError(f"No se encontró columna de frecuencia. Columnas disponibles: {list(work_df.columns)}")
        work_df['freq'] = work_df[freq_col]
    
    normalized = work_df[[name_column, 'freq', 'name_normalized_final']].copy()
    normalized.columns = ['original_name', 'frequency', 'normalized_name']
    
    # Guardar resultado final
    print("\n4. Guardando resultados finales...")
    output_file = results_dir / f"{entity_type}_normalized.csv"
    normalized.to_csv(output_file, index=False)
    print(f"   ✓ {output_file}")
    
    # Estadísticas
    print("\n5. Estadísticas:")
    print(f"   - Total nombres: {len(normalized):,}")
    print(f"   - Nombres únicos: {normalized['normalized_name'].nunique():,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Normalización completada para {entity_type}")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return normalized


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
    common_compound_names = {
        'WELLSFARGO': 'WELLS FARGO',
        'JPMORGAN': 'JP MORGAN',
        'BANKOFAMERICA': 'BANK OF AMERICA',
        'BANKOFAMER': 'BANK OF AMER',
    }
    
    for compound, expanded in common_compound_names.items():
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

