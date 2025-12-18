#!/usr/bin/env python3
"""
Pipeline Principal de Estandarización de Nombres
================================================
Ejecuta todo el pipeline de estandarización o fases individuales.

Uso:
    python scripts/pipeline.py                    # Ejecuta todo
    python scripts/pipeline.py --phase exploration  # Solo exploración
    python scripts/pipeline.py --phase normalization  # Solo normalización
    python scripts/pipeline.py --phase blocking    # Solo blocking
    python scripts/pipeline.py --phase matching   # Solo matching
    python scripts/pipeline.py --phase grouping   # Solo agrupación
    python scripts/pipeline.py --phase validation # Solo validación
    python scripts/pipeline.py --phase complete   # Solo completar mapeo
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import json

# Agregar el directorio scripts al path
sys.path.insert(0, str(Path(__file__).parent))

from modules import exploration, normalization, blocking, matching, grouping, validation, complete_mapping


def merge_csv_files(base_dir=None):
    """
    Merge CSV files: financial pledge+release, non-financial pledge+release
    
    Args:
        base_dir: Directorio base del proyecto
        
    Returns:
        tuple: (merged_financial_df, merged_non_financial_df)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    data_dir = base_dir / "original-data"
    
    print("\n" + "=" * 80)
    print("MERGIENDO ARCHIVOS CSV")
    print("=" * 80)
    
    # Merge financial files
    financial_pledge_file = data_dir / 'financial_entity_freq_pledge.csv'
    financial_release_file = data_dir / 'financial_entity_freq_release.csv'
    
    financial_dfs = []
    if financial_pledge_file.exists():
        df_pledge = pd.read_csv(financial_pledge_file)
        financial_dfs.append(df_pledge)
        print(f"   ✓ Financial pledge: {len(df_pledge):,} registros")
    else:
        print(f"   ⚠️ Archivo no encontrado: {financial_pledge_file}")
    
    if financial_release_file.exists():
        df_release = pd.read_csv(financial_release_file)
        # Fix column names for release files
        if 'or_name' in df_release.columns and 'ee_name' not in df_release.columns:
            df_release = df_release.rename(columns={'or_name': 'ee_name'})
        financial_dfs.append(df_release)
        print(f"   ✓ Financial release: {len(df_release):,} registros")
    else:
        print(f"   ⚠️ Archivo no encontrado: {financial_release_file}")
    
    if not financial_dfs:
        raise FileNotFoundError("No se encontraron archivos financieros")
    
    # Merge financial dataframes, combining frequencies for same names
    merged_financial = pd.concat(financial_dfs, ignore_index=True)
    # Group by name and sum frequencies
    merged_financial = merged_financial.groupby('ee_name', as_index=False)['freq'].sum()
    print(f"   ✓ Financial merged: {len(merged_financial):,} registros únicos")
    
    # Merge non-financial files
    non_financial_pledge_file = data_dir / 'non_financial_entity_freq_pledge.csv'
    non_financial_release_file = data_dir / 'nonfinancial_entity_freq_release.csv'
    
    non_financial_dfs = []
    if non_financial_pledge_file.exists():
        df_pledge = pd.read_csv(non_financial_pledge_file)
        non_financial_dfs.append(df_pledge)
        print(f"   ✓ Non-financial pledge: {len(df_pledge):,} registros")
    else:
        print(f"   ⚠️ Archivo no encontrado: {non_financial_pledge_file}")
    
    if non_financial_release_file.exists():
        df_release = pd.read_csv(non_financial_release_file)
        # Fix column names for release files
        if 'ee_name' in df_release.columns and 'or_name' not in df_release.columns:
            df_release = df_release.rename(columns={'ee_name': 'or_name'})
        non_financial_dfs.append(df_release)
        print(f"   ✓ Non-financial release: {len(df_release):,} registros")
    else:
        print(f"   ⚠️ Archivo no encontrado: {non_financial_release_file}")
    
    if not non_financial_dfs:
        raise FileNotFoundError("No se encontraron archivos no financieros")
    
    # Merge non-financial dataframes, combining frequencies for same names
    merged_non_financial = pd.concat(non_financial_dfs, ignore_index=True)
    # Group by name and sum frequencies
    merged_non_financial = merged_non_financial.groupby('or_name', as_index=False)['freq'].sum()
    print(f"   ✓ Non-financial merged: {len(merged_non_financial):,} registros únicos")
    
    print("=" * 80)
    
    return merged_financial, merged_non_financial


def run_pipeline_for_entity_type(entity_type, base_dir=None, skip_validation=True):
    """
    Ejecuta el pipeline completo para un tipo de entidad (financial o non_financial).
    
    Args:
        entity_type: 'financial' o 'non_financial'
        base_dir: Directorio base del proyecto
        skip_validation: Si True, omite la fase de validación (útil si usas Streamlit)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    print("\n" + "=" * 80)
    print(f"PROCESANDO ENTIDADES: {entity_type.upper()}")
    print("=" * 80)
    
    # Merge CSV files at the beginning
    merged_financial, merged_non_financial = merge_csv_files(base_dir)
    
    # Select the appropriate dataframe
    if entity_type == 'financial':
        entity_df = merged_financial
        other_df = merged_non_financial
    else:
        entity_df = merged_non_financial
        other_df = merged_financial
    
    # Fase 2: Normalización
    print("\n" + "=" * 80)
    print(f"FASE 2: NORMALIZACIÓN ({entity_type.upper()})")
    print("=" * 80)
    if entity_type == 'financial':
        entity_normalized, other_normalized = normalization.normalize_names(
            entity_df, other_df, base_dir, transaction_type=None
        )
    else:
        other_normalized, entity_normalized = normalization.normalize_names(
            other_df, entity_df, base_dir, transaction_type=None
        )
    
    # Fase 3: Blocking
    print("\n" + "=" * 80)
    print(f"FASE 3: BLOCKING ({entity_type.upper()})")
    print("=" * 80)
    if entity_type == 'financial':
        entity_blocks, other_blocks = blocking.create_blocks(
            entity_normalized, other_normalized, base_dir, transaction_type=None
        )
    else:
        other_blocks, entity_blocks = blocking.create_blocks(
            other_normalized, entity_normalized, base_dir, transaction_type=None
        )
    
    # Fase 4: Matching
    print("\n" + "=" * 80)
    print(f"FASE 4: FUZZY MATCHING ({entity_type.upper()})")
    print("=" * 80)
    if entity_type == 'financial':
        entity_components, other_components, entity_matches_df, other_matches_df = matching.run_matching(
            entity_normalized, other_normalized, entity_blocks, other_blocks, 
            base_dir, transaction_type=None
        )
    else:
        other_components, entity_components, other_matches_df, entity_matches_df = matching.run_matching(
            other_normalized, entity_normalized, other_blocks, entity_blocks, 
            base_dir, transaction_type=None
        )
    
    # Fase 5: Grouping
    print("\n" + "=" * 80)
    print(f"FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs ({entity_type.upper()})")
    print("=" * 80)
    if entity_type == 'financial':
        entity_mapping, other_mapping, entity_review, other_review = grouping.run_grouping(
            entity_normalized, other_normalized, entity_components, other_components,
            entity_matches_df, other_matches_df, base_dir, transaction_type=None
        )
    else:
        other_mapping, entity_mapping, other_review, entity_review = grouping.run_grouping(
            other_normalized, entity_normalized, other_components, entity_components,
            other_matches_df, entity_matches_df, base_dir, transaction_type=None
        )
    
    # Fase 6: Validation (Opcional - puede hacerse dinámicamente en Streamlit)
    if not skip_validation:
        print("\n" + "=" * 80)
        print(f"FASE 6: VALIDACIÓN ({entity_type.upper()})")
        print("=" * 80)
        if entity_type == 'financial':
            validation.run_validation(
                entity_mapping, other_mapping, entity_components, other_components,
                entity_matches_df, other_matches_df, base_dir, transaction_type=None
            )
        else:
            validation.run_validation(
                other_mapping, entity_mapping, other_components, entity_components,
                other_matches_df, entity_matches_df, base_dir, transaction_type=None
            )
    
    # Completar mapeo
    print("\n" + "=" * 80)
    print(f"COMPLETAR MAPEO ({entity_type.upper()})")
    print("=" * 80)
    if entity_type == 'financial':
        complete_mapping.run_complete_mapping(
            financial_mapping=entity_mapping,
            non_financial_mapping=other_mapping,
            base_dir=base_dir,
            transaction_type=None
        )
    else:
        complete_mapping.run_complete_mapping(
            financial_mapping=other_mapping,
            non_financial_mapping=entity_mapping,
            base_dir=base_dir,
            transaction_type=None
        )
    
    print(f"\n✓ Pipeline completado para {entity_type}")


def run_full_pipeline(base_dir=None, skip_validation=True):
    """
    Ejecuta todo el pipeline completo para ambos tipos de entidad (financial y non_financial).
    Los datos de pledge y release se fusionan al inicio.
    
    Args:
        base_dir: Directorio base del proyecto
        skip_validation: Si True, omite la fase de validación (útil si usas Streamlit)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    print("=" * 80)
    print("PIPELINE COMPLETO DE ESTANDARIZACIÓN DE NOMBRES")
    print("=" * 80)
    print()
    
    # Merge CSV files at the beginning
    merged_financial, merged_non_financial = merge_csv_files(base_dir)
    
    # Fase 1: Exploración (usando datos fusionados)
    print("\n" + "=" * 80)
    print("FASE 1: EXPLORACIÓN")
    print("=" * 80)
    exploration.run_exploration(base_dir)
    
    # Process both entity types together
    print("\n" + "=" * 80)
    print("PROCESANDO AMBOS TIPOS DE ENTIDADES")
    print("=" * 80)
    
    # Fase 2: Normalización
    print("\n" + "=" * 80)
    print("FASE 2: NORMALIZACIÓN")
    print("=" * 80)
    financial_normalized, non_financial_normalized = normalization.normalize_names(
        merged_financial, merged_non_financial, base_dir, transaction_type=None
    )
    
    # Fase 3: Blocking
    print("\n" + "=" * 80)
    print("FASE 3: BLOCKING")
    print("=" * 80)
    financial_blocks, non_financial_blocks = blocking.create_blocks(
        financial_normalized, non_financial_normalized, base_dir, transaction_type=None
    )
    
    # Fase 4: Matching
    print("\n" + "=" * 80)
    print("FASE 4: FUZZY MATCHING")
    print("=" * 80)
    financial_components, non_financial_components, financial_matches_df, non_financial_matches_df = matching.run_matching(
        financial_normalized, non_financial_normalized, financial_blocks, non_financial_blocks, 
        base_dir, transaction_type=None
    )
    
    # Fase 5: Grouping
    print("\n" + "=" * 80)
    print("FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs")
    print("=" * 80)
    financial_mapping, non_financial_mapping, financial_review, non_financial_review = grouping.run_grouping(
        financial_normalized, non_financial_normalized, financial_components, non_financial_components,
        financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None
    )
    
    # Fase 6: Validation (Opcional)
    if not skip_validation:
        print("\n" + "=" * 80)
        print("FASE 6: VALIDACIÓN")
        print("=" * 80)
        validation.run_validation(
            financial_mapping, non_financial_mapping, financial_components, non_financial_components,
            financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None
        )
    
    # Completar mapeo
    print("\n" + "=" * 80)
    print("COMPLETAR MAPEO")
    print("=" * 80)
    complete_mapping.run_complete_mapping(
        financial_mapping=financial_mapping,
        non_financial_mapping=non_financial_mapping,
        base_dir=base_dir,
        transaction_type=None
    )
    
    # Actualizar base de datos
    print("\n" + "=" * 80)
    print("ACTUALIZANDO BASE DE DATOS")
    print("=" * 80)
    complete_mapping.update_database(base_dir, overwrite=True)
    
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETADO EXITOSAMENTE")
    print("=" * 80)
    print("✓ Todas las fases se ejecutaron correctamente")
    print("✓ Resultados finales en: results/final/")
    print("✓ Base de datos actualizada en: database/entities.db")
    print("=" * 80)


def run_phase(phase_name, base_dir=None):
    """Ejecuta una fase específica del pipeline usando datos fusionados."""
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    data_dir = base_dir / "original-data"
    
    if phase_name == "exploration":
        exploration.run_exploration(base_dir)
    
    elif phase_name == "normalization":
        # Merge CSV files first
        merged_financial, merged_non_financial = merge_csv_files(base_dir)
        normalization.normalize_names(merged_financial, merged_non_financial, base_dir, transaction_type=None)
    
    elif phase_name == "blocking":
        financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
        non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
        blocking.create_blocks(financial_df, non_financial_df, base_dir, transaction_type=None)
    
    elif phase_name == "matching":
        financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
        non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
        
        with open(results_dir / "financial_blocks.json", 'r', encoding='utf-8') as f:
            financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
        
        with open(results_dir / "non_financial_blocks.json", 'r', encoding='utf-8') as f:
            non_financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
        
        matching.run_matching(financial_df, non_financial_df, financial_blocks, non_financial_blocks, base_dir, transaction_type=None)
    
    elif phase_name == "grouping":
        financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
        non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
        financial_matches_df = pd.read_csv(results_dir / "financial_matches.csv")
        non_financial_matches_df = pd.read_csv(results_dir / "non_financial_matches.csv")
        
        with open(results_dir / "financial_components.json", 'r', encoding='utf-8') as f:
            financial_components_json = json.load(f)
            financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]
        
        with open(results_dir / "non_financial_components.json", 'r', encoding='utf-8') as f:
            non_financial_components_json = json.load(f)
            non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]
        
        grouping.run_grouping(
            financial_df, non_financial_df, financial_components, non_financial_components,
            financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None
        )
    
    elif phase_name == "validation":
        financial_mapping = pd.read_csv(final_results_dir / "financial_entity_mapping.csv")
        non_financial_mapping = pd.read_csv(final_results_dir / "non_financial_entity_mapping.csv")
        financial_matches_df = pd.read_csv(results_dir / "financial_matches.csv")
        non_financial_matches_df = pd.read_csv(results_dir / "non_financial_matches.csv")
        
        with open(results_dir / "financial_components.json", 'r', encoding='utf-8') as f:
            financial_components_json = json.load(f)
            financial_components = [set(int(idx) for idx in comp) for comp in financial_components_json.values()]
        
        with open(results_dir / "non_financial_components.json", 'r', encoding='utf-8') as f:
            non_financial_components_json = json.load(f)
            non_financial_components = [set(int(idx) for idx in comp) for comp in non_financial_components_json.values()]
        
        validation.run_validation(
            financial_mapping, non_financial_mapping, financial_components, non_financial_components,
            financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None
        )
    
    elif phase_name == "complete":
        # Try to load mappings from files if they exist
        financial_mapping = None
        non_financial_mapping = None
        
        mapping_file_financial = final_results_dir / "financial_entity_mapping.csv"
        mapping_file_non_financial = final_results_dir / "non_financial_entity_mapping.csv"
        
        if mapping_file_financial.exists():
            financial_mapping = pd.read_csv(mapping_file_financial)
        
        if mapping_file_non_financial.exists():
            non_financial_mapping = pd.read_csv(mapping_file_non_financial)
        
        complete_mapping.run_complete_mapping(
            financial_mapping=financial_mapping,
            non_financial_mapping=non_financial_mapping,
            base_dir=base_dir,
            transaction_type=None
        )
    
    else:
        print(f"Error: Fase desconocida: {phase_name}")
        print("Fases disponibles: exploration, normalization, blocking, matching, grouping, validation, complete")
        sys.exit(1)


def main():
    """Función principal."""
    parser = argparse.ArgumentParser(
        description="Pipeline de Estandarización de Nombres",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python scripts/pipeline.py                    # Ejecuta todo el pipeline
  python scripts/pipeline.py --phase exploration  # Solo exploración
  python scripts/pipeline.py --phase normalization  # Solo normalización
  python scripts/pipeline.py --phase blocking    # Solo blocking
  python scripts/pipeline.py --phase matching   # Solo matching
  python scripts/pipeline.py --phase grouping   # Solo agrupación
  python scripts/pipeline.py --phase validation # Solo validación
  python scripts/pipeline.py --phase complete   # Solo completar mapeo
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
        help='Incluir fase de validación (por defecto se omite, útil si NO usas Streamlit)'
    )
    
    parser.add_argument(
        '--yes',
        action='store_true',
        help='Ejecutar sin confirmación manual (útil para scripts automatizados)'
    )
    
    args = parser.parse_args()
    
    base_dir = Path(__file__).parent.parent
    
    # Por defecto, omitir validación (útil si usas Streamlit)
    # Usar --with-validation para incluirla
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

