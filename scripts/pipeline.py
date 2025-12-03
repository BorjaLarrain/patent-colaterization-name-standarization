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


def run_pipeline_for_type(transaction_type, base_dir=None, skip_validation=True):
    """
    Ejecuta el pipeline completo para un tipo de transacción (pledge o release).
    
    Args:
        transaction_type: 'pledge' o 'release'
        base_dir: Directorio base del proyecto
        skip_validation: Si True, omite la fase de validación (útil si usas Streamlit)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    data_dir = base_dir / "original-data"
    
    print("\n" + "=" * 80)
    print(f"PROCESANDO TRANSACCIONES: {transaction_type.upper()}")
    print("=" * 80)
    
    # Cargar datos según el tipo
    financial_file = data_dir / f'financial_entity_freq_{transaction_type}.csv'
    # Note: release file uses 'nonfinancial' (no underscore), pledge uses 'non_financial'
    if transaction_type == 'release':
        non_financial_file = data_dir / f'nonfinancial_entity_freq_{transaction_type}.csv'
    else:
        non_financial_file = data_dir / f'non_financial_entity_freq_{transaction_type}.csv'
    
    # Verificar que los archivos existen
    if not financial_file.exists():
        print(f"⚠️ Archivo no encontrado: {financial_file}")
        print(f"   Saltando procesamiento de {transaction_type}")
        return
    
    if not non_financial_file.exists():
        print(f"⚠️ Archivo no encontrado: {non_financial_file}")
        print(f"   Saltando procesamiento de {transaction_type}")
        return
    
    financial_df = pd.read_csv(financial_file)
    non_financial_df = pd.read_csv(non_financial_file)
    
    # Fix column names for release files (they have swapped column names)
    if transaction_type == 'release':
        # Financial release file has 'or_name' instead of 'ee_name'
        if 'or_name' in financial_df.columns and 'ee_name' not in financial_df.columns:
            financial_df = financial_df.rename(columns={'or_name': 'ee_name'})
        # Non-financial release file has 'ee_name' instead of 'or_name'
        if 'ee_name' in non_financial_df.columns and 'or_name' not in non_financial_df.columns:
            non_financial_df = non_financial_df.rename(columns={'ee_name': 'or_name'})
    
    # Fase 2: Normalización
    print("\n" + "=" * 80)
    print(f"FASE 2: NORMALIZACIÓN ({transaction_type.upper()})")
    print("=" * 80)
    financial_normalized, non_financial_normalized = normalization.normalize_names(
        financial_df, non_financial_df, base_dir, transaction_type=transaction_type
    )
    
    # Fase 3: Blocking
    print("\n" + "=" * 80)
    print(f"FASE 3: BLOCKING ({transaction_type.upper()})")
    print("=" * 80)
    financial_blocks, non_financial_blocks = blocking.create_blocks(
        financial_normalized, non_financial_normalized, base_dir, transaction_type=transaction_type
    )
    
    # Fase 4: Matching
    print("\n" + "=" * 80)
    print(f"FASE 4: FUZZY MATCHING ({transaction_type.upper()})")
    print("=" * 80)
    financial_components, non_financial_components, financial_matches_df, non_financial_matches_df = matching.run_matching(
        financial_normalized, non_financial_normalized, financial_blocks, non_financial_blocks, 
        base_dir, transaction_type=transaction_type
    )
    
    # Fase 5: Grouping
    print("\n" + "=" * 80)
    print(f"FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs ({transaction_type.upper()})")
    print("=" * 80)
    financial_mapping, non_financial_mapping, financial_review, non_financial_review = grouping.run_grouping(
        financial_normalized, non_financial_normalized, financial_components, non_financial_components,
        financial_matches_df, non_financial_matches_df, base_dir, transaction_type=transaction_type
    )
    
    # Fase 6: Validation (Opcional - puede hacerse dinámicamente en Streamlit)
    if not skip_validation:
        print("\n" + "=" * 80)
        print(f"FASE 6: VALIDACIÓN ({transaction_type.upper()})")
        print("=" * 80)
        validation.run_validation(
            financial_mapping, non_financial_mapping, financial_components, non_financial_components,
            financial_matches_df, non_financial_matches_df, base_dir, transaction_type=transaction_type
        )
    
    # Completar mapeo
    print("\n" + "=" * 80)
    print(f"COMPLETAR MAPEO ({transaction_type.upper()})")
    print("=" * 80)
    complete_mapping.run_complete_mapping(
        financial_mapping=financial_mapping,
        non_financial_mapping=non_financial_mapping,
        base_dir=base_dir,
        transaction_type=transaction_type
    )
    
    print(f"\n✓ Pipeline completado para {transaction_type}")


def run_full_pipeline(base_dir=None, skip_validation=True):
    """
    Ejecuta todo el pipeline completo para ambos tipos de transacción (pledge y release).
    
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
    
    # Fase 1: Exploración (solo para pledge por ahora)
    print("\n" + "=" * 80)
    print("FASE 1: EXPLORACIÓN")
    print("=" * 80)
    financial_df, non_financial_df = exploration.run_exploration(base_dir)
    
    # Procesar pledge
    run_pipeline_for_type('pledge', base_dir, skip_validation)
    
    # Procesar release
    run_pipeline_for_type('release', base_dir, skip_validation)
    
    # Actualizar base de datos (para ambos tipos)
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
    """Ejecuta una fase específica del pipeline."""
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    data_dir = base_dir / "original-data"
    
    if phase_name == "exploration":
        financial_df = pd.read_csv(data_dir / 'financial_entity_freq_pledge.csv')
        non_financial_df = pd.read_csv(data_dir / 'non_financial_entity_freq_pledge.csv')
        exploration.run_exploration(base_dir)
    
    elif phase_name == "normalization":
        financial_df = pd.read_csv(data_dir / 'financial_entity_freq_pledge.csv')
        non_financial_df = pd.read_csv(data_dir / 'non_financial_entity_freq_pledge.csv')
        normalization.normalize_names(financial_df, non_financial_df, base_dir)
    
    elif phase_name == "blocking":
        financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
        non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
        blocking.create_blocks(financial_df, non_financial_df, base_dir)
    
    elif phase_name == "matching":
        financial_df = pd.read_csv(results_dir / "financial_normalized.csv")
        non_financial_df = pd.read_csv(results_dir / "non_financial_normalized.csv")
        
        with open(results_dir / "financial_blocks.json", 'r', encoding='utf-8') as f:
            financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
        
        with open(results_dir / "non_financial_blocks.json", 'r', encoding='utf-8') as f:
            non_financial_blocks = {k: [int(i) for i in v] for k, v in json.load(f).items()}
        
        matching.run_matching(financial_df, non_financial_df, financial_blocks, non_financial_blocks, base_dir)
    
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
            financial_matches_df, non_financial_matches_df, base_dir
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
            financial_matches_df, non_financial_matches_df, base_dir
        )
    
    elif phase_name == "complete":
        # Intentar cargar mapeos desde archivos si existen (legacy)
        # Si no existen, complete_mapping.py intentará cargarlos o mostrará error
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
            base_dir=base_dir
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
    
    args = parser.parse_args()
    
    base_dir = Path(__file__).parent.parent
    
    # Por defecto, omitir validación (útil si usas Streamlit)
    # Usar --with-validation para incluirla
    skip_val = not args.with_validation
    
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

