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


def run_full_pipeline(base_dir=None, skip_validation=True):
    """
    Ejecuta todo el pipeline completo.
    
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
    
    # Fase 1: Exploración
    print("\n" + "=" * 80)
    print("FASE 1: EXPLORACIÓN")
    print("=" * 80)
    financial_df, non_financial_df = exploration.run_exploration(base_dir)
    
    # Fase 2: Normalización
    print("\n" + "=" * 80)
    print("FASE 2: NORMALIZACIÓN")
    print("=" * 80)
    financial_normalized, non_financial_normalized = normalization.normalize_names(
        financial_df, non_financial_df, base_dir
    )
    
    # Fase 3: Blocking
    print("\n" + "=" * 80)
    print("FASE 3: BLOCKING")
    print("=" * 80)
    financial_blocks, non_financial_blocks = blocking.create_blocks(
        financial_normalized, non_financial_normalized, base_dir
    )
    
    # Fase 4: Matching
    print("\n" + "=" * 80)
    print("FASE 4: FUZZY MATCHING")
    print("=" * 80)
    financial_components, non_financial_components, financial_matches_df, non_financial_matches_df = matching.run_matching(
        financial_normalized, non_financial_normalized, financial_blocks, non_financial_blocks, base_dir
    )
    
    # Fase 5: Grouping
    print("\n" + "=" * 80)
    print("FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs")
    print("=" * 80)
    financial_mapping, non_financial_mapping, financial_review, non_financial_review = grouping.run_grouping(
        financial_normalized, non_financial_normalized, financial_components, non_financial_components,
        financial_matches_df, non_financial_matches_df, base_dir
    )
    
    # Fase 6: Validation (Opcional - puede hacerse dinámicamente en Streamlit)
    if not skip_validation:
        print("\n" + "=" * 80)
        print("FASE 6: VALIDACIÓN")
        print("=" * 80)
        validation.run_validation(
            financial_mapping, non_financial_mapping, financial_components, non_financial_components,
            financial_matches_df, non_financial_matches_df, base_dir
        )
    
    # Completar mapeo
    print("\n" + "=" * 80)
    print("COMPLETAR MAPEO")
    print("=" * 80)
    complete_mapping.run_complete_mapping(
        financial_mapping=financial_mapping,
        non_financial_mapping=non_financial_mapping,
        base_dir=base_dir
    )
    
    # Actualizar base de datos
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
        financial_df = pd.read_csv(data_dir / 'financial_entity_freq.csv')
        non_financial_df = pd.read_csv(data_dir / 'Non_financial_entity_freq.csv')
        exploration.run_exploration(base_dir)
    
    elif phase_name == "normalization":
        financial_df = pd.read_csv(data_dir / 'financial_entity_freq.csv')
        non_financial_df = pd.read_csv(data_dir / 'Non_financial_entity_freq.csv')
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

