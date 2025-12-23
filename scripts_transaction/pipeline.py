#!/usr/bin/env python3
"""
Pipeline Principal de Estandarización de Nombres por Transacción
=================================================================
Ejecuta el pipeline completo procesando 4 grupos separados:
- Financial Security (pledge)
- Financial Release
- Non-Financial Security (pledge)
- Non-Financial Release

Uso:
    python scripts_transaction/pipeline.py                    # Ejecuta todo
    python scripts_transaction/pipeline.py --phase exploration  # Solo exploración
    python scripts_transaction/pipeline.py --phase normalization  # Solo normalización
    python scripts_transaction/pipeline.py --phase blocking    # Solo blocking
    python scripts_transaction/pipeline.py --phase matching   # Solo matching
    python scripts_transaction/pipeline.py --phase grouping   # Solo agrupación
    python scripts_transaction/pipeline.py --phase validation # Solo validación
    python scripts_transaction/pipeline.py --phase complete   # Solo completar mapeo
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import json

# Agregar el directorio scripts_transaction al path
sys.path.insert(0, str(Path(__file__).parent))

from modules import exploration, normalization, blocking, matching, grouping, validation, complete_mapping


def load_csv_files(base_dir=None):
    """
    Carga los 4 archivos CSV desde el directorio backup.
    
    Args:
        base_dir: Directorio base del proyecto
        
    Returns:
        dict: Diccionario con los 4 DataFrames
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    backup_dir = base_dir / "original-data" / "backup"
    
    print("\n" + "=" * 80)
    print("CARGANDO ARCHIVOS CSV")
    print("=" * 80)
    
    files = {
        'financial_security': backup_dir / 'financial_entity_freq_pledge.csv.backup',
        'financial_release': backup_dir / 'financial_entity_freq_release.csv.backup',
        'non_financial_security': backup_dir / 'non_financial_entity_freq_pledge.csv.backup',
        'non_financial_release': backup_dir / 'nonfinancial_entity_freq_release.csv.backup',
    }
    
    dataframes = {}
    
    for entity_type, file_path in files.items():
        if file_path.exists():
            df = pd.read_csv(file_path)
            # Column names are already correct in the files:
            # - financial_security: ee_name
            # - financial_release: or_name
            # - non_financial_security: or_name
            # - non_financial_release: ee_name
            # The normalization module will detect the column automatically
            dataframes[entity_type] = df
            print(f"   ✓ {entity_type}: {len(df):,} registros (columns: {list(df.columns)})")
        else:
            print(f"   ⚠️ Archivo no encontrado: {file_path}")
            dataframes[entity_type] = None
    
    print("=" * 80)
    
    return dataframes


def run_pipeline_for_entity_type(entity_type, entity_df, base_dir=None, skip_validation=True):
    """
    Ejecuta el pipeline completo para un tipo de entidad.
    
    Args:
        entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
        entity_df: DataFrame con los datos
        base_dir: Directorio base del proyecto
        skip_validation: Si True, omite la fase de validación
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    if entity_df is None:
        print(f"\n⚠️ No hay datos para {entity_type}, saltando...")
        return
    
    print("\n" + "=" * 80)
    print(f"PROCESANDO: {entity_type.upper()}")
    print("=" * 80)
    
    # Fase 2: Normalización
    print("\n" + "=" * 80)
    print(f"FASE 2: NORMALIZACIÓN ({entity_type.upper()})")
    print("=" * 80)
    entity_normalized = normalization.normalize_names_single(entity_df, entity_type, base_dir)
    
    # Fase 3: Blocking
    print("\n" + "=" * 80)
    print(f"FASE 3: BLOCKING ({entity_type.upper()})")
    print("=" * 80)
    entity_blocks = blocking.create_blocks_single(entity_normalized, entity_type, base_dir)
    
    # Fase 4: Matching
    print("\n" + "=" * 80)
    print(f"FASE 4: FUZZY MATCHING ({entity_type.upper()})")
    print("=" * 80)
    entity_components, entity_matches_df = matching.run_matching_single(
        entity_normalized, entity_blocks, entity_type, base_dir
    )
    
    # Fase 5: Grouping
    print("\n" + "=" * 80)
    print(f"FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs ({entity_type.upper()})")
    print("=" * 80)
    entity_mapping, entity_review = grouping.run_grouping_single(
        entity_normalized, entity_components, entity_matches_df, entity_type, base_dir
    )
    
    # Fase 6: Validation (Opcional)
    if not skip_validation:
        print("\n" + "=" * 80)
        print(f"FASE 6: VALIDACIÓN ({entity_type.upper()})")
        print("=" * 80)
        validation.run_validation_single(
            entity_mapping, entity_components, entity_matches_df, entity_type, base_dir
        )
    
    # Completar mapeo
    print("\n" + "=" * 80)
    print(f"COMPLETAR MAPEO ({entity_type.upper()})")
    print("=" * 80)
    complete_mapping.run_complete_mapping_single(
        entity_mapping=entity_mapping,
        entity_type=entity_type,
        base_dir=base_dir
    )
    
    print(f"\n✓ Pipeline completado para {entity_type}")


def run_full_pipeline(base_dir=None, skip_validation=True):
    """
    Ejecuta todo el pipeline completo para los 4 tipos de entidad.
    
    Args:
        base_dir: Directorio base del proyecto
        skip_validation: Si True, omite la fase de validación
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    print("=" * 80)
    print("PIPELINE COMPLETO DE ESTANDARIZACIÓN DE NOMBRES POR TRANSACCIÓN")
    print("=" * 80)
    print()
    
    # Cargar archivos CSV
    dataframes = load_csv_files(base_dir)
    
    # Fase 1: Exploración (opcional, puede ejecutarse por separado)
    # print("\n" + "=" * 80)
    # print("FASE 1: EXPLORACIÓN")
    # print("=" * 80)
    # exploration.run_exploration(base_dir)  # Si se implementa
    
    # Procesar cada tipo de entidad
    entity_types = ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']
    
    for entity_type in entity_types:
        entity_df = dataframes.get(entity_type)
        run_pipeline_for_entity_type(entity_type, entity_df, base_dir, skip_validation=skip_validation)
    
    # Actualizar base de datos
    print("\n" + "=" * 80)
    print("ACTUALIZANDO BASE DE DATOS")
    print("=" * 80)
    complete_mapping.update_database(base_dir, overwrite=True)
    
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETADO EXITOSAMENTE")
    print("=" * 80)
    print("✓ Todas las fases se ejecutaron correctamente")
    print("✓ Resultados finales en: results_transaction/final/")
    print("✓ Base de datos actualizada en: database/entities_by_transaction.db")
    print("=" * 80)


def run_phase(phase_name, base_dir=None):
    """Ejecuta una fase específica del pipeline."""
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    results_dir = base_dir / "results_transaction" / "intermediate"
    final_results_dir = base_dir / "results_transaction" / "final"
    
    if phase_name == "exploration":
        # exploration.run_exploration(base_dir)  # Si se implementa
        print("Exploración no implementada aún para transaction pipeline")
    
    elif phase_name == "normalization":
        dataframes = load_csv_files(base_dir)
        for entity_type, entity_df in dataframes.items():
            if entity_df is not None:
                normalization.normalize_names_single(entity_df, entity_type, base_dir)
    
    elif phase_name == "blocking":
        dataframes = load_csv_files(base_dir)
        for entity_type in dataframes.keys():
            normalized_file = results_dir / f"{entity_type}_normalized.csv"
            if normalized_file.exists():
                entity_df = pd.read_csv(normalized_file)
                blocking.create_blocks_single(entity_df, entity_type, base_dir)
    
    elif phase_name == "matching":
        for entity_type in ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']:
            normalized_file = results_dir / f"{entity_type}_normalized.csv"
            blocks_file = results_dir / f"{entity_type}_blocks.json"
            if normalized_file.exists() and blocks_file.exists():
                entity_df = pd.read_csv(normalized_file)
                with open(blocks_file, 'r', encoding='utf-8') as f:
                    blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
                matching.run_matching_single(entity_df, blocks, entity_type, base_dir)
    
    elif phase_name == "grouping":
        for entity_type in ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']:
            normalized_file = results_dir / f"{entity_type}_normalized.csv"
            matches_file = results_dir / f"{entity_type}_matches.csv"
            components_file = results_dir / f"{entity_type}_components.json"
            if normalized_file.exists() and matches_file.exists() and components_file.exists():
                entity_df = pd.read_csv(normalized_file)
                matches_df = pd.read_csv(matches_file)
                with open(components_file, 'r', encoding='utf-8') as f:
                    components_json = json.load(f)
                    components = [set(int(idx) for idx in comp) for comp in components_json.values()]
                grouping.run_grouping_single(entity_df, components, matches_df, entity_type, base_dir)
    
    elif phase_name == "validation":
        for entity_type in ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']:
            mapping_file = final_results_dir / f"{entity_type}_entity_mapping.csv"
            matches_file = results_dir / f"{entity_type}_matches.csv"
            components_file = results_dir / f"{entity_type}_components.json"
            if mapping_file.exists() and matches_file.exists() and components_file.exists():
                mapping = pd.read_csv(mapping_file)
                matches_df = pd.read_csv(matches_file)
                with open(components_file, 'r', encoding='utf-8') as f:
                    components_json = json.load(f)
                    components = [set(int(idx) for idx in comp) for comp in components_json.values()]
                validation.run_validation_single(mapping, components, matches_df, entity_type, base_dir)
    
    elif phase_name == "complete":
        for entity_type in ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']:
            complete_mapping.run_complete_mapping_single(entity_type=entity_type, base_dir=base_dir)
    
    else:
        print(f"Error: Fase desconocida: {phase_name}")
        print("Fases disponibles: exploration, normalization, blocking, matching, grouping, validation, complete")
        sys.exit(1)


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description="Pipeline de Estandarización de Nombres por Transacción",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python scripts_transaction/pipeline.py                    # Ejecuta todo el pipeline
  python scripts_transaction/pipeline.py --phase exploration  # Solo exploración
  python scripts_transaction/pipeline.py --phase normalization  # Solo normalización
  python scripts_transaction/pipeline.py --phase blocking    # Solo blocking
  python scripts_transaction/pipeline.py --phase matching   # Solo matching
  python scripts_transaction/pipeline.py --phase grouping   # Solo agrupación
  python scripts_transaction/pipeline.py --phase validation # Solo validación
  python scripts_transaction/pipeline.py --phase complete   # Solo completar mapeo
        """
    )
    
    parser.add_argument(
        '--phase',
        choices=['exploration', 'normalization', 'blocking', 'matching', 'grouping', 'validation', 'complete'],
        help='Ejecutar solo una fase específica del pipeline'
    )
    
    parser.add_argument(
        '--with-validation',
        action='store_true',
        help='Incluir fase de validación (por defecto se omite)'
    )
    
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Ejecutar sin confirmación manual'
    )
    
    args = parser.parse_args()
    
    base_dir = Path(__file__).parent.parent
    
    skip_val = not args.with_validation
    
    # Solicitar confirmación manual antes de ejecutar
    if not args.yes:
        if args.phase:
            print(f"\n⚠️  Se ejecutará la fase: {args.phase}")
        else:
            print("\n⚠️  Se ejecutará el PIPELINE COMPLETO")
            if skip_val:
                print("   (Omitiendo validación - usa --with-validation para incluirla)")
        
        print("\n¿Desea continuar? (yes/no): ", end='', flush=True)
        try:
            response = input().strip().lower()
            if response not in ['yes', 'y', 'sí', 'si']:
                print("\n❌ Ejecución cancelada por el usuario.")
                sys.exit(0)
        except (KeyboardInterrupt, EOFError):
            print("\n\n❌ Ejecución cancelada por el usuario.")
            sys.exit(0)
        print()
    
    if args.phase:
        print(f"Ejecutando fase: {args.phase}")
        run_phase(args.phase, base_dir)
    else:
        print("Ejecutando pipeline completo...")
        if skip_val:
            print("(Omitiendo validación - usa --with-validation para incluirla)")
        run_full_pipeline(base_dir, skip_validation=skip_val)


if __name__ == "__main__":
    main()

