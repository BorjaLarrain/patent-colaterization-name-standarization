"""
Módulo de Blocking
=================
Consolida los scripts de blocking (08-10) en un solo módulo.
Crea bloques optimizados y guarda solo el resultado final.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Palabras genéricas que no son distintivas
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

# Umbral para considerar un bloque como "grande"
LARGE_BLOCK_THRESHOLD = 100


def create_blocks(financial_df, non_financial_df, base_dir=None):
    """
    Crea bloques optimizados para fuzzy matching.
    
    Args:
        financial_df: DataFrame con nombres normalizados (columna 'normalized_name')
        non_financial_df: DataFrame con nombres normalizados (columna 'normalized_name')
        base_dir: Directorio base del proyecto
        
    Returns:
        tuple: (financial_blocks, non_financial_blocks) - Diccionarios de bloques optimizados
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    results_dir = base_dir / "results" / "intermediate"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("FASE 3: BLOCKING")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Resetear índices
    financial_df = financial_df.reset_index(drop=True)
    non_financial_df = non_financial_df.reset_index(drop=True)
    
    # Paso 3.1: Extraer claves de blocking
    print("3.1. Extrayendo claves de blocking...")
    financial_df['blocking_key'] = financial_df['normalized_name'].apply(extract_first_significant_word)
    non_financial_df['blocking_key'] = non_financial_df['normalized_name'].apply(extract_first_significant_word)
    print(f"   ✓ Claves de blocking extraídas")
    
    # Paso 3.2: Crear bloques iniciales
    print("3.2. Creando bloques iniciales...")
    financial_blocks = create_blocks_dict(financial_df, 'blocking_key')
    non_financial_blocks = create_blocks_dict(non_financial_df, 'blocking_key')
    print(f"   ✓ Financial: {len(financial_blocks):,} bloques")
    print(f"   ✓ Non-financial: {len(non_financial_blocks):,} bloques")
    
    # Paso 3.3: Optimizar bloques grandes
    print("3.3. Optimizando bloques grandes...")
    financial_blocks_opt, financial_sub_blocked = optimize_blocks(
        financial_df, financial_blocks, 'normalized_name', LARGE_BLOCK_THRESHOLD
    )
    non_financial_blocks_opt, non_financial_sub_blocked = optimize_blocks(
        non_financial_df, non_financial_blocks, 'normalized_name', LARGE_BLOCK_THRESHOLD
    )
    print(f"   ✓ Financial: {financial_sub_blocked} bloques sub-bloqueados")
    print(f"   ✓ Non-financial: {non_financial_sub_blocked} bloques sub-bloqueados")
    
    # Guardar solo bloques optimizados finales
    print("\n4. Guardando bloques optimizados...")
    output_file_financial = results_dir / "financial_blocks.json"
    output_file_non_financial = results_dir / "non_financial_blocks.json"
    
    financial_blocks_json = {str(k): [int(i) for i in v] for k, v in financial_blocks_opt.items()}
    non_financial_blocks_json = {str(k): [int(i) for i in v] for k, v in non_financial_blocks_opt.items()}
    
    with open(output_file_financial, 'w', encoding='utf-8') as f:
        json.dump(financial_blocks_json, f, indent=2)
    
    with open(output_file_non_financial, 'w', encoding='utf-8') as f:
        json.dump(non_financial_blocks_json, f, indent=2)
    
    print(f"   ✓ {output_file_financial}")
    print(f"   ✓ {output_file_non_financial}")
    
    # Estadísticas
    print("\n5. Estadísticas:")
    print(f"   - Financial bloques finales: {len(financial_blocks_opt):,}")
    print(f"   - Non-financial bloques finales: {len(non_financial_blocks_opt):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Blocking completado")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return financial_blocks_opt, non_financial_blocks_opt


def extract_first_significant_word(name):
    """Extrae la primera palabra significativa de un nombre normalizado."""
    if pd.isna(name) or not str(name).strip():
        return None
    
    name_str = str(name).strip().upper()
    words = name_str.split()
    
    if not words:
        return None
    
    # Si empieza con "THE", tomar la segunda palabra
    if words[0] == 'THE' and len(words) > 1:
        first_word = words[1]
    else:
        first_word = words[0]
    
    # Si la primera palabra es muy genérica, buscar palabra significativa
    if first_word in GENERIC_WORDS or first_word in PREPOSITIONS:
        if len(words) >= 3 and words[0] in GENERIC_WORDS and words[1] == 'OF':
            first_word = words[2]
        elif len(words) > 1 and words[1] not in GENERIC_WORDS and words[1] not in PREPOSITIONS:
            first_word = words[1]
        elif len(words) > 2 and words[1] in PREPOSITIONS and words[2] not in GENERIC_WORDS and words[2] not in PREPOSITIONS:
            first_word = words[2]
        elif len(words) > 3 and words[3] not in GENERIC_WORDS and words[3] not in PREPOSITIONS:
            first_word = words[3]
        else:
            for word in words[1:]:
                if word not in PREPOSITIONS:
                    first_word = word
                    break
    
    # Limpiar la palabra
    import re
    first_word = re.sub(r'[^\w]', '', first_word)
    
    return first_word if first_word else None


def create_blocks_dict(df, blocking_key_column='blocking_key'):
    """Crea bloques agrupando nombres por su clave de blocking."""
    blocks = defaultdict(list)
    
    for idx, row in df.iterrows():
        blocking_key = row[blocking_key_column]
        if pd.notna(blocking_key):
            blocks[blocking_key].append(idx)
    
    return dict(blocks)


def extract_second_word(name):
    """Extrae la segunda palabra de un nombre normalizado."""
    if pd.isna(name) or not str(name).strip():
        return None
    
    words = str(name).strip().upper().split()
    
    if len(words) < 2:
        return None
    
    return words[1] if words[1] else None


def extract_name_length_category(name):
    """Categoriza un nombre por su longitud."""
    if pd.isna(name):
        return "UNKNOWN"
    
    length = len(str(name))
    
    if length <= 15:
        return "SHORT"
    elif length <= 30:
        return "MEDIUM"
    elif length <= 50:
        return "LONG"
    else:
        return "VERY_LONG"


def sub_block_by_second_word(df, block_indices, name_column='normalized_name'):
    """Aplica sub-bloqueo por segunda palabra."""
    sub_blocks = defaultdict(list)
    
    for idx in block_indices:
        name = df.loc[idx, name_column]
        second_word = extract_second_word(name)
        
        if second_word:
            sub_blocks[second_word].append(idx)
        else:
            sub_blocks["_NO_SECOND_WORD"].append(idx)
    
    return dict(sub_blocks)


def sub_block_by_length(df, block_indices, name_column='normalized_name'):
    """Aplica sub-bloqueo por longitud del nombre."""
    sub_blocks = defaultdict(list)
    
    for idx in block_indices:
        name = df.loc[idx, name_column]
        length_cat = extract_name_length_category(name)
        sub_blocks[length_cat].append(idx)
    
    return dict(sub_blocks)


def optimize_blocks(df, blocks, name_column='normalized_name', threshold=LARGE_BLOCK_THRESHOLD):
    """
    Optimiza bloques grandes aplicando sub-bloqueo.
    
    Returns:
        tuple: (optimized_blocks, sub_blocked_count)
    """
    optimized_blocks = {}
    sub_blocked_count = 0
    
    for blocking_key, block_indices in blocks.items():
        block_size = len(block_indices)
        
        # Si el bloque es pequeño, mantenerlo como está
        if block_size <= threshold:
            optimized_blocks[blocking_key] = block_indices
        else:
            # Bloque grande: aplicar sub-bloqueo
            sub_blocked_count += 1
            
            # Intentar sub-bloqueo por segunda palabra primero
            sub_blocks = sub_block_by_second_word(df, block_indices, name_column)
            
            # Si el sub-bloqueo por segunda palabra no ayuda mucho, intentar por longitud
            if len(sub_blocks) < block_size * 0.3:
                sub_blocks = sub_block_by_length(df, block_indices, name_column)
            
            # Crear nuevas claves de bloque con formato: "ORIGINAL_KEY_SUBKEY"
            for sub_key, sub_indices in sub_blocks.items():
                new_key = f"{blocking_key}_{sub_key}"
                optimized_blocks[new_key] = sub_indices
    
    return optimized_blocks, sub_blocked_count


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results" / "intermediate"
    
    financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
    non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
    
    create_blocks(financial_df, non_financial_df, base_dir)

