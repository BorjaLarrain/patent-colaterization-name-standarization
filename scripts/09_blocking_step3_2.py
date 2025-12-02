"""
Fase 3.2: Creación de Bloques
=============================
Este script agrupa los nombres por su clave de blocking y crea índices
para facilitar las búsquedas durante el fuzzy matching.

- Carga datos con claves de blocking
- Agrupa nombres por blocking_key
- Crea índice de bloques
- Documenta tamaño de cada bloque
- Guarda resultados en results/intermediate/
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

def create_blocks(df, blocking_key_column='blocking_key', name_column='normalized_name'):
    """
    Crea bloques agrupando nombres por su clave de blocking
    
    Args:
        df: DataFrame con los datos
        blocking_key_column: Nombre de la columna con las claves de blocking
        name_column: Nombre de la columna con los nombres normalizados
        
    Returns:
        dict: Diccionario con blocking_key como clave y lista de índices como valor
    """
    blocks = defaultdict(list)
    
    for idx, row in df.iterrows():
        blocking_key = row[blocking_key_column]
        if pd.notna(blocking_key):
            blocks[blocking_key].append(idx)
    
    return dict(blocks)

def create_block_index(blocks):
    """
    Crea un índice inverso: de índice de fila a blocking_key
    
    Args:
        blocks: Diccionario de bloques
        
    Returns:
        dict: Diccionario con índice de fila como clave y blocking_key como valor
    """
    index = {}
    for blocking_key, indices in blocks.items():
        for idx in indices:
            index[idx] = blocking_key
    return index

def analyze_blocks(blocks, df, blocking_key_column='blocking_key'):
    """
    Analiza los bloques y genera estadísticas
    
    Args:
        blocks: Diccionario de bloques
        df: DataFrame original
        blocking_key_column: Nombre de la columna con las claves de blocking
        
    Returns:
        dict: Estadísticas de los bloques
    """
    stats = {
        'total_blocks': len(blocks),
        'total_names': sum(len(indices) for indices in blocks.values()),
        'block_sizes': [len(indices) for indices in blocks.values()],
        'large_blocks': {},  # >100 nombres
        'very_large_blocks': {},  # >1000 nombres
        'small_blocks': 0,  # 1 nombre
        'medium_blocks': 0  # 2-10 nombres
    }
    
    for blocking_key, indices in blocks.items():
        size = len(indices)
        
        if size == 1:
            stats['small_blocks'] += 1
        elif 2 <= size <= 10:
            stats['medium_blocks'] += 1
        elif size > 100:
            stats['large_blocks'][blocking_key] = size
            if size > 1000:
                stats['very_large_blocks'][blocking_key] = size
    
    stats['avg_block_size'] = sum(stats['block_sizes']) / len(stats['block_sizes']) if stats['block_sizes'] else 0
    stats['median_block_size'] = sorted(stats['block_sizes'])[len(stats['block_sizes']) // 2] if stats['block_sizes'] else 0
    stats['max_block_size'] = max(stats['block_sizes']) if stats['block_sizes'] else 0
    
    return stats

def save_blocks_structure(blocks, output_file):
    """
    Guarda la estructura de bloques en formato JSON
    
    Args:
        blocks: Diccionario de bloques
        output_file: Path donde guardar el archivo
    """
    # Convertir índices a strings para JSON
    blocks_json = {str(k): [int(i) for i in v] for k, v in blocks.items()}
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(blocks_json, f, indent=2)

def print_block_statistics(stats, entity_type):
    """
    Imprime estadísticas de los bloques
    """
    print("\n" + "=" * 80)
    print(f"ESTADÍSTICAS DE BLOQUES - {entity_type}")
    print("=" * 80)
    
    print(f"\nResumen general:")
    print(f"  - Total de bloques: {stats['total_blocks']:,}")
    print(f"  - Total de nombres: {stats['total_names']:,}")
    print(f"  - Tamaño promedio de bloque: {stats['avg_block_size']:.1f} nombres")
    print(f"  - Tamaño mediano de bloque: {stats['median_block_size']:.0f} nombres")
    print(f"  - Tamaño máximo de bloque: {stats['max_block_size']:,} nombres")
    
    print(f"\nDistribución por tamaño:")
    print(f"  - Bloques pequeños (1 nombre): {stats['small_blocks']:,} ({100*stats['small_blocks']/stats['total_blocks']:.1f}%)")
    print(f"  - Bloques medianos (2-10 nombres): {stats['medium_blocks']:,} ({100*stats['medium_blocks']/stats['total_blocks']:.1f}%)")
    print(f"  - Bloques grandes (>100 nombres): {len(stats['large_blocks']):,}")
    print(f"  - Bloques muy grandes (>1000 nombres): {len(stats['very_large_blocks']):,}")
    
    if stats['large_blocks']:
        print(f"\n⚠️  Bloques grandes (>100 nombres) que pueden necesitar sub-bloqueo:")
        sorted_large = sorted(stats['large_blocks'].items(), key=lambda x: x[1], reverse=True)
        for key, size in sorted_large[:10]:
            print(f"     {key:30s} : {size:>6,} nombres")
    
    if stats['very_large_blocks']:
        print(f"\n⚠️  ⚠️  Bloques muy grandes (>1000 nombres) que REQUIEREN sub-bloqueo:")
        sorted_very_large = sorted(stats['very_large_blocks'].items(), key=lambda x: x[1], reverse=True)
        for key, size in sorted_very_large:
            print(f"     {key:30s} : {size:>6,} nombres")

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 3.2: CREACIÓN DE BLOQUES")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos con claves de blocking
print("1. Cargando datos con claves de blocking...")
input_file_financial = RESULTS_DIR / "financial_with_blocking_keys.csv"
input_file_non_financial = RESULTS_DIR / "non_financial_with_blocking_keys.csv"

if not input_file_financial.exists() or not input_file_non_financial.exists():
    print("   ✗ Error: No se encontraron los archivos con claves de blocking.")
    print(f"   Por favor ejecuta primero: 08_blocking_step3_1.py")
    exit(1)

financial_df = pd.read_csv(input_file_financial)
non_financial_df = pd.read_csv(input_file_non_financial)

# Resetear índices para que sean consistentes
financial_df = financial_df.reset_index(drop=True)
non_financial_df = non_financial_df.reset_index(drop=True)

print(f"   ✓ Financial entities: {len(financial_df):,} registros")
print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")

# 2. CREAR bloques
print("\n2. Creando bloques...")
financial_blocks = create_blocks(financial_df, 'blocking_key', 'normalized_name')
non_financial_blocks = create_blocks(non_financial_df, 'blocking_key', 'normalized_name')
print("   ✓ Bloques creados")

# 3. CREAR índices
print("\n3. Creando índices de bloques...")
financial_index = create_block_index(financial_blocks)
non_financial_index = create_block_index(non_financial_blocks)
print("   ✓ Índices creados")

# 4. ANALIZAR bloques
print("\n4. Analizando bloques...")
financial_stats = analyze_blocks(financial_blocks, financial_df, 'blocking_key')
non_financial_stats = analyze_blocks(non_financial_blocks, non_financial_df, 'blocking_key')

print_block_statistics(financial_stats, "FINANCIAL ENTITIES")
print_block_statistics(non_financial_stats, "NON-FINANCIAL ENTITIES")

# 5. GUARDAR estructura de bloques
print("\n5. Guardando estructura de bloques...")
output_file_financial_blocks = RESULTS_DIR / "financial_blocks.json"
output_file_non_financial_blocks = RESULTS_DIR / "non_financial_blocks.json"

save_blocks_structure(financial_blocks, output_file_financial_blocks)
save_blocks_structure(non_financial_blocks, output_file_non_financial_blocks)

print(f"   ✓ Estructura de bloques guardada:")
print(f"     - {output_file_financial_blocks}")
print(f"     - {output_file_non_financial_blocks}")

# 6. GUARDAR índices
print("\n6. Guardando índices de bloques...")
output_file_financial_index = RESULTS_DIR / "financial_block_index.json"
output_file_non_financial_index = RESULTS_DIR / "non_financial_block_index.json"

# Convertir índices a strings para JSON
financial_index_json = {str(k): str(v) for k, v in financial_index.items()}
non_financial_index_json = {str(k): str(v) for k, v in non_financial_index.items()}

with open(output_file_financial_index, 'w', encoding='utf-8') as f:
    json.dump(financial_index_json, f, indent=2)

with open(output_file_non_financial_index, 'w', encoding='utf-8') as f:
    json.dump(non_financial_index_json, f, indent=2)

print(f"   ✓ Índices guardados:")
print(f"     - {output_file_financial_index}")
print(f"     - {output_file_non_financial_index}")

# 7. Crear archivo de resumen de bloques
print("\n7. Creando resumen de bloques...")
financial_summary = pd.DataFrame([
    {
        'blocking_key': key,
        'block_size': len(indices),
        'sample_names': ', '.join([financial_df.loc[i, 'normalized_name'] for i in indices[:3]])
    }
    for key, indices in sorted(financial_blocks.items(), key=lambda x: len(x[1]), reverse=True)
])

non_financial_summary = pd.DataFrame([
    {
        'blocking_key': key,
        'block_size': len(indices),
        'sample_names': ', '.join([non_financial_df.loc[i, 'normalized_name'] for i in indices[:3]])
    }
    for key, indices in sorted(non_financial_blocks.items(), key=lambda x: len(x[1]), reverse=True)
])

output_file_financial_summary = RESULTS_DIR / "financial_blocks_summary.csv"
output_file_non_financial_summary = RESULTS_DIR / "non_financial_blocks_summary.csv"

financial_summary.to_csv(output_file_financial_summary, index=False)
non_financial_summary.to_csv(output_file_non_financial_summary, index=False)

print(f"   ✓ Resúmenes guardados:")
print(f"     - {output_file_financial_summary}")
print(f"     - {output_file_non_financial_summary}")

# 8. Resumen final
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Bloques creados para {len(financial_df) + len(non_financial_df):,} nombres")
print(f"✓ Financial entities: {len(financial_blocks):,} bloques")
print(f"✓ Non-financial entities: {len(non_financial_blocks):,} bloques")
print(f"✓ Estructuras guardadas en: {RESULTS_DIR}")
print(f"✓ Próximo paso: Fase 3.3 - Optimización de Bloques (si es necesario)")
print("=" * 80)

