"""
Fase 3.3: Optimización de Bloques
==================================
Este script optimiza bloques grandes aplicando sub-bloqueo.

- Identifica bloques grandes (>100 elementos)
- Aplica sub-bloqueo por segunda palabra o longitud
- Actualiza estructura de bloques
- Guarda resultados optimizados
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Umbral para considerar un bloque como "grande" y necesitar sub-bloqueo
LARGE_BLOCK_THRESHOLD = 100

def extract_second_word(name):
    """
    Extrae la segunda palabra de un nombre normalizado
    
    Args:
        name: String con el nombre normalizado
        
    Returns:
        String con la segunda palabra o None
    """
    if pd.isna(name) or not str(name).strip():
        return None
    
    words = str(name).strip().upper().split()
    
    if len(words) < 2:
        return None
    
    return words[1] if words[1] else None

def extract_name_length_category(name):
    """
    Categoriza un nombre por su longitud
    
    Args:
        name: String con el nombre normalizado
        
    Returns:
        String con categoría de longitud
    """
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
    """
    Aplica sub-bloqueo por segunda palabra
    
    Args:
        df: DataFrame con los datos
        block_indices: Lista de índices que pertenecen al bloque
        name_column: Nombre de la columna con nombres normalizados
        
    Returns:
        dict: Diccionario con segunda palabra como clave y lista de índices como valor
    """
    sub_blocks = defaultdict(list)
    
    for idx in block_indices:
        name = df.loc[idx, name_column]
        second_word = extract_second_word(name)
        
        if second_word:
            sub_blocks[second_word].append(idx)
        else:
            # Si no hay segunda palabra, poner en bloque especial
            sub_blocks["_NO_SECOND_WORD"].append(idx)
    
    return dict(sub_blocks)

def sub_block_by_length(df, block_indices, name_column='normalized_name'):
    """
    Aplica sub-bloqueo por longitud del nombre
    
    Args:
        df: DataFrame con los datos
        block_indices: Lista de índices que pertenecen al bloque
        name_column: Nombre de la columna con nombres normalizados
        
    Returns:
        dict: Diccionario con categoría de longitud como clave y lista de índices como valor
    """
    sub_blocks = defaultdict(list)
    
    for idx in block_indices:
        name = df.loc[idx, name_column]
        length_cat = extract_name_length_category(name)
        sub_blocks[length_cat].append(idx)
    
    return dict(sub_blocks)

def optimize_blocks(df, blocks, name_column='normalized_name', threshold=LARGE_BLOCK_THRESHOLD):
    """
    Optimiza bloques grandes aplicando sub-bloqueo
    
    Args:
        df: DataFrame con los datos
        blocks: Diccionario de bloques originales
        name_column: Nombre de la columna con nombres normalizados
        threshold: Umbral para considerar un bloque como grande
        
    Returns:
        dict: Diccionario de bloques optimizados
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
            
            # Si el sub-bloqueo por segunda palabra no ayuda mucho (muchos sub-bloques de 1),
            # intentar por longitud
            if len(sub_blocks) < block_size * 0.3:  # Menos del 30% de sub-bloques útiles
                sub_blocks = sub_block_by_length(df, block_indices, name_column)
            
            # Crear nuevas claves de bloque con formato: "ORIGINAL_KEY_SUBKEY"
            for sub_key, sub_indices in sub_blocks.items():
                new_key = f"{blocking_key}_{sub_key}"
                optimized_blocks[new_key] = sub_indices
    
    return optimized_blocks, sub_blocked_count

def analyze_optimization(original_blocks, optimized_blocks):
    """
    Analiza el impacto de la optimización
    
    Args:
        original_blocks: Diccionario de bloques originales
        optimized_blocks: Diccionario de bloques optimizados
        
    Returns:
        dict: Estadísticas de optimización
    """
    original_large = sum(1 for indices in original_blocks.values() if len(indices) > LARGE_BLOCK_THRESHOLD)
    optimized_large = sum(1 for indices in optimized_blocks.values() if len(indices) > LARGE_BLOCK_THRESHOLD)
    
    original_max = max(len(indices) for indices in original_blocks.values()) if original_blocks else 0
    optimized_max = max(len(indices) for indices in optimized_blocks.values()) if optimized_blocks else 0
    
    original_avg = sum(len(indices) for indices in original_blocks.values()) / len(original_blocks) if original_blocks else 0
    optimized_avg = sum(len(indices) for indices in optimized_blocks.values()) / len(optimized_blocks) if optimized_blocks else 0
    
    return {
        'original_blocks': len(original_blocks),
        'optimized_blocks': len(optimized_blocks),
        'original_large_blocks': original_large,
        'optimized_large_blocks': optimized_large,
        'original_max_size': original_max,
        'optimized_max_size': optimized_max,
        'original_avg_size': original_avg,
        'optimized_avg_size': optimized_avg,
        'blocks_added': len(optimized_blocks) - len(original_blocks)
    }

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 3.3: OPTIMIZACIÓN DE BLOQUES")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos y bloques originales
print("1. Cargando datos y bloques originales...")
input_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"
blocks_file_financial = RESULTS_DIR / "financial_blocks.json"
blocks_file_non_financial = RESULTS_DIR / "non_financial_blocks.json"

if not all(f.exists() for f in [input_file_financial, input_file_non_financial, 
                                 blocks_file_financial, blocks_file_non_financial]):
    print("   ✗ Error: No se encontraron los archivos necesarios.")
    print(f"   Por favor ejecuta primero: 09_blocking_step3_2.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

# Resetear índices
financial_df = financial_df.reset_index(drop=True)
non_financial_df = non_financial_df.reset_index(drop=True)

# Cargar bloques originales
with open(blocks_file_financial, 'r', encoding='utf-8') as f:
    financial_blocks_original = {k: [int(i) for i in v] for k, v in json.load(f).items()}

with open(blocks_file_non_financial, 'r', encoding='utf-8') as f:
    non_financial_blocks_original = {k: [int(i) for i in v] for k, v in json.load(f).items()}

print(f"   ✓ Financial entities: {len(financial_df):,} registros, {len(financial_blocks_original):,} bloques")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros, {len(non_financial_blocks_original):,} bloques")

# 2. IDENTIFICAR bloques grandes
print(f"\n2. Identificando bloques grandes (>{LARGE_BLOCK_THRESHOLD} nombres)...")
financial_large = {k: v for k, v in financial_blocks_original.items() if len(v) > LARGE_BLOCK_THRESHOLD}
non_financial_large = {k: v for k, v in non_financial_blocks_original.items() if len(v) > LARGE_BLOCK_THRESHOLD}

print(f"   - Financial entities: {len(financial_large)} bloques grandes")
for key, indices in sorted(financial_large.items(), key=lambda x: len(x[1]), reverse=True):
    print(f"     {key:30s} : {len(indices):>4,} nombres")

print(f"   - Non-financial entities: {len(non_financial_large)} bloques grandes")
for key, indices in sorted(non_financial_large.items(), key=lambda x: len(x[1]), reverse=True):
    print(f"     {key:30s} : {len(indices):>4,} nombres")

if len(financial_large) == 0 and len(non_financial_large) == 0:
    print("\n   ✓ No hay bloques grandes que requieran optimización.")
    print("   Los bloques actuales son suficientemente pequeños para fuzzy matching eficiente.")
    exit(0)

# 3. OPTIMIZAR bloques grandes
print(f"\n3. Aplicando sub-bloqueo a bloques grandes...")
financial_blocks_optimized, financial_sub_blocked = optimize_blocks(
    financial_df, financial_blocks_original, 'normalized_name', LARGE_BLOCK_THRESHOLD
)
non_financial_blocks_optimized, non_financial_sub_blocked = optimize_blocks(
    non_financial_df, non_financial_blocks_original, 'normalized_name', LARGE_BLOCK_THRESHOLD
)

print(f"   ✓ Financial entities: {financial_sub_blocked} bloques sub-bloqueados")
print(f"   ✓ Non-financial entities: {non_financial_sub_blocked} bloques sub-bloqueados")

# 4. ANALIZAR impacto de optimización
print("\n4. Analizando impacto de optimización...")
financial_stats = analyze_optimization(financial_blocks_original, financial_blocks_optimized)
non_financial_stats = analyze_optimization(non_financial_blocks_original, non_financial_blocks_optimized)

print("\n" + "=" * 80)
print("RESULTADOS DE OPTIMIZACIÓN - FINANCIAL ENTITIES")
print("=" * 80)
print(f"  Bloques originales:     {financial_stats['original_blocks']:,}")
print(f"  Bloques optimizados:    {financial_stats['optimized_blocks']:,}")
print(f"  Bloques agregados:      {financial_stats['blocks_added']:,}")
print(f"  Bloques grandes antes: {financial_stats['original_large_blocks']}")
print(f"  Bloques grandes después: {financial_stats['optimized_large_blocks']}")
print(f"  Tamaño máximo antes:    {financial_stats['original_max_size']:,}")
print(f"  Tamaño máximo después:  {financial_stats['optimized_max_size']:,}")
print(f"  Tamaño promedio antes:  {financial_stats['original_avg_size']:.1f}")
print(f"  Tamaño promedio después: {financial_stats['optimized_avg_size']:.1f}")

print("\n" + "=" * 80)
print("RESULTADOS DE OPTIMIZACIÓN - NON-FINANCIAL ENTITIES")
print("=" * 80)
print(f"  Bloques originales:     {non_financial_stats['original_blocks']:,}")
print(f"  Bloques optimizados:    {non_financial_stats['optimized_blocks']:,}")
print(f"  Bloques agregados:      {non_financial_stats['blocks_added']:,}")
print(f"  Bloques grandes antes: {non_financial_stats['original_large_blocks']}")
print(f"  Bloques grandes después: {non_financial_stats['optimized_large_blocks']}")
print(f"  Tamaño máximo antes:    {non_financial_stats['original_max_size']:,}")
print(f"  Tamaño máximo después:  {non_financial_stats['optimized_max_size']:,}")
print(f"  Tamaño promedio antes:  {non_financial_stats['original_avg_size']:.1f}")
print(f"  Tamaño promedio después: {non_financial_stats['optimized_avg_size']:.1f}")

# 5. GUARDAR bloques optimizados
print("\n5. Guardando bloques optimizados...")
output_file_financial_optimized = RESULTS_DIR / "financial_blocks_optimized.json"
output_file_non_financial_optimized = RESULTS_DIR / "non_financial_blocks_optimized.json"

# Convertir índices a strings para JSON
financial_optimized_json = {str(k): [int(i) for i in v] for k, v in financial_blocks_optimized.items()}
non_financial_optimized_json = {str(k): [int(i) for i in v] for k, v in non_financial_blocks_optimized.items()}

with open(output_file_financial_optimized, 'w', encoding='utf-8') as f:
    json.dump(financial_optimized_json, f, indent=2)

with open(output_file_non_financial_optimized, 'w', encoding='utf-8') as f:
    json.dump(non_financial_optimized_json, f, indent=2)

print(f"   ✓ Bloques optimizados guardados:")
print(f"     - {output_file_financial_optimized}")
print(f"     - {output_file_non_financial_optimized}")

# 6. Crear resumen de bloques optimizados
print("\n6. Creando resumen de bloques optimizados...")
financial_summary_opt = pd.DataFrame([
    {
        'blocking_key': key,
        'block_size': len(indices),
        'sample_names': ', '.join([financial_df.loc[i, 'normalized_name'] for i in indices[:3]])
    }
    for key, indices in sorted(financial_blocks_optimized.items(), key=lambda x: len(x[1]), reverse=True)
])

non_financial_summary_opt = pd.DataFrame([
    {
        'blocking_key': key,
        'block_size': len(indices),
        'sample_names': ', '.join([non_financial_df.loc[i, 'normalized_name'] for i in indices[:3]])
    }
    for key, indices in sorted(non_financial_blocks_optimized.items(), key=lambda x: len(x[1]), reverse=True)
])

output_file_financial_summary_opt = RESULTS_DIR / "financial_blocks_optimized_summary.csv"
output_file_non_financial_summary_opt = RESULTS_DIR / "non_financial_blocks_optimized_summary.csv"

financial_summary_opt.to_csv(output_file_financial_summary_opt, index=False)
non_financial_summary_opt.to_csv(output_file_non_financial_summary_opt, index=False)

print(f"   ✓ Resúmenes guardados:")
print(f"     - {output_file_financial_summary_opt}")
print(f"     - {output_file_non_financial_summary_opt}")

# 7. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Optimización completada")
print(f"✓ Financial entities: {financial_sub_blocked} bloques optimizados")
print(f"✓ Non-financial entities: {non_financial_sub_blocked} bloques optimizados")
print(f"✓ Bloques optimizados guardados en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 4 - Fuzzy Matching")
print("=" * 80)

