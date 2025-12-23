"""
Módulo de Completar Mapeo (Transaction Pipeline)
=================================================
Asegura que TODOS los nombres del archivo original estén incluidos en el mapeo final,
incluyendo los que no tienen matches (singletons).
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from . import grouping


def run_complete_mapping_single(entity_mapping=None, entity_type=None, base_dir=None):
    """
    Completa el mapeo agregando nombres faltantes (singletons) para un tipo de entidad.
    
    Args:
        entity_mapping: DataFrame con mapeo (si None, intenta cargar desde archivo)
        entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
        base_dir: Directorio base del proyecto
        
    Returns:
        DataFrame con mapeo completo
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    if entity_type is None:
        raise ValueError("entity_type must be specified")
    
    backup_dir = base_dir / "original-data" / "backup"
    results_dir = base_dir / "results_transaction" / "intermediate"
    final_results_dir = base_dir / "results_transaction" / "final"
    final_results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print(f"COMPLETAR MAPEO FINAL ({entity_type.upper()})")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Determinar archivo original según tipo
    print("1. Cargando datos...")
    if entity_type == 'financial_security':
        original_file = backup_dir / "financial_entity_freq_pledge.csv.backup"
        name_column = 'ee_name'
    elif entity_type == 'financial_release':
        original_file = backup_dir / "financial_entity_freq_release.csv.backup"
        name_column = 'or_name'  # Release files have or_name
    elif entity_type == 'non_financial_security':
        original_file = backup_dir / "non_financial_entity_freq_pledge.csv.backup"
        name_column = 'or_name'
    elif entity_type == 'non_financial_release':
        original_file = backup_dir / "nonfinancial_entity_freq_release.csv.backup"
        name_column = 'ee_name'  # Release files have ee_name
    else:
        raise ValueError(f"Unknown entity_type: {entity_type}")
    
    normalized_file = results_dir / f"{entity_type}_normalized.csv"
    
    if not original_file.exists():
        print(f"   ✗ Error: No se encontró el archivo original: {original_file}")
        return None
    
    if not normalized_file.exists():
        print(f"   ✗ Error: No se encontró el archivo normalizado: {normalized_file}")
        return None
    
    # Cargar datos
    original_df = pd.read_csv(original_file)
    normalized_df = pd.read_csv(normalized_file)
    
    # Fix column names if needed
    if name_column == 'or_name' and 'or_name' not in original_df.columns:
        if 'ee_name' in original_df.columns:
            original_df = original_df.rename(columns={'ee_name': 'or_name'})
    elif name_column == 'ee_name' and 'ee_name' not in original_df.columns:
        if 'or_name' in original_df.columns:
            original_df = original_df.rename(columns={'or_name': 'ee_name'})
    
    print(f"   ✓ Original: {len(original_df):,} nombres")
    print(f"   ✓ Normalized: {len(normalized_df):,} nombres")
    
    # Si no se pasó el mapeo, intentar cargarlo o generarlo
    if entity_mapping is None:
        mapping_file = final_results_dir / f"{entity_type}_entity_mapping.csv"
        if mapping_file.exists():
            entity_mapping = pd.read_csv(mapping_file)
            print("   ℹ️  Mapeo cargado desde archivo")
        else:
            # Intentar generar desde componentes
            print("   ℹ️  Generando mapeo desde componentes...")
            components_file = results_dir / f"{entity_type}_components.json"
            matches_file = results_dir / f"{entity_type}_matches.csv"
            
            if components_file.exists() and matches_file.exists():
                with open(components_file, 'r', encoding='utf-8') as f:
                    components_json = json.load(f)
                    components = [set(int(idx) for idx in comp) for comp in components_json.values()]
                
                matches_df = pd.read_csv(matches_file)
                entity_mapping, _ = grouping.process_components(
                    normalized_df, components, matches_df,
                    'normalized_name', 'frequency', entity_type
                )
                print("   ✓ Mapeo generado desde componentes")
            else:
                print("   ✗ Error: No se encontró el mapeo ni los componentes necesarios.")
                return None
    else:
        print("   ✓ Mapeo recibido como parámetro")
    
    print(f"   ✓ Mapping actual: {len(entity_mapping):,} nombres")
    
    # Identificar nombres faltantes
    print("\n2. Identificando nombres faltantes...")
    mapped_original_names = set(entity_mapping['original_name'].str.upper().str.strip())
    original_df['name_upper'] = original_df[name_column].str.upper().str.strip()
    missing_df = original_df[~original_df['name_upper'].isin(mapped_original_names)].copy()
    
    print(f"   ✓ Nombres faltantes: {len(missing_df):,}")
    
    # Agregar nombres faltantes como singletons
    print("\n3. Agregando nombres faltantes como singletons...")
    max_entity_id = entity_mapping['entity_id'].str.extract(rf'{entity_type}_(\d+)')[0].astype(float).max()
    next_entity_id = int(max_entity_id) + 1 if not pd.isna(max_entity_id) else 0
    
    singleton_mappings = []
    for idx, row in missing_df.iterrows():
        original_name = row[name_column]
        freq = row['freq']
        
        normalized_row = normalized_df[normalized_df['original_name'].str.upper().str.strip() == original_name.upper().strip()]
        normalized_name = normalized_row.iloc[0]['normalized_name'] if len(normalized_row) > 0 else original_name.upper().strip()
        standard_name = normalized_name
        
        singleton_mappings.append({
            'entity_id': f'{entity_type}_{next_entity_id}',
            'original_name': original_name,
            'normalized_name': normalized_name,
            'standard_name': standard_name,
            'frequency': freq,
            'component_size': 1,
            'avg_similarity': None,
            'min_similarity': None,
            'needs_review': False
        })
        
        next_entity_id += 1
    
    print(f"   ✓ {len(singleton_mappings):,} singletons agregados")
    
    # Combinar mapeos
    print("\n4. Combinando mapeos...")
    complete_mapping = pd.concat([
        entity_mapping,
        pd.DataFrame(singleton_mappings)
    ], ignore_index=True)
    
    print(f"   ✓ Mapeo completo: {len(complete_mapping):,} nombres")
    
    # Guardar mapeo completo
    print("\n5. Guardando mapeo completo...")
    output_file = final_results_dir / f"{entity_type}_entity_mapping_complete.csv"
    complete_mapping.to_csv(output_file, index=False)
    print(f"   ✓ {output_file}")
    print(f"     - Total nombres: {len(complete_mapping):,}")
    print(f"     - Entidades únicas: {complete_mapping['entity_id'].nunique():,}")
    print(f"     - Singletons: {len(complete_mapping[complete_mapping['component_size'] == 1]):,}")
    
    # Estadísticas finales
    print("\n6. Estadísticas finales:")
    print(f"     - Total nombres en original: {len(original_df):,}")
    print(f"     - Total nombres en mapeo completo: {len(complete_mapping):,}")
    print(f"     - Entidades únicas: {complete_mapping['entity_id'].nunique():,}")
    print(f"     - Nombres agrupados: {len(complete_mapping[complete_mapping['component_size'] > 1]):,}")
    print(f"     - Singletons: {len(complete_mapping[complete_mapping['component_size'] == 1]):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Mapeo completo creado para {entity_type}")
    print(f"✓ Todos los nombres del archivo original están incluidos")
    print(f"✓ Archivo guardado: {output_file}")
    print("=" * 80)
    
    return complete_mapping


def update_database(base_dir=None, overwrite=True):
    """
    Actualiza la base de datos con los mapeos completos de todas las entidades.
    
    Args:
        base_dir: Directorio base del proyecto
        overwrite: Si True, sobrescribe datos existentes
    """
    import sys
    
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    # Agregar el directorio base al path para importar database_manager
    if str(base_dir) not in sys.path:
        sys.path.insert(0, str(base_dir))
    
    from database_manager_transaction import EntityDatabaseTransaction
    
    final_results_dir = base_dir / "results_transaction" / "final"
    database_dir = base_dir / "database"
    database_dir.mkdir(parents=True, exist_ok=True)
    db_path = database_dir / "entities_by_transaction.db"
    
    print("\n" + "=" * 80)
    print("ACTUALIZANDO BASE DE DATOS")
    print("=" * 80)
    
    db = EntityDatabaseTransaction(db_path)
    
    # Actualizar cada tipo de entidad
    entity_types = ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']
    
    for entity_type in entity_types:
        csv_file = final_results_dir / f"{entity_type}_entity_mapping_complete.csv"
        if csv_file.exists():
            print(f"\n{entity_type}:")
            try:
                db.import_from_csv(csv_file, entity_type, clear_existing=overwrite)
                stats = db.get_statistics(entity_type)
                print(f"   ✓ {stats['total_names']:,} nombres, {stats['unique_entities']:,} entidades")
            except Exception as e:
                print(f"   ✗ Error: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"\n{entity_type}: ⚠️ Archivo no encontrado: {csv_file}")
    
    print("\n" + "=" * 80)
    print("✓ Base de datos actualizada")
    print(f"✓ Ubicación: {db_path}")
    print("=" * 80)


if __name__ == "__main__":
    run_complete_mapping_single(entity_type="financial_security")

