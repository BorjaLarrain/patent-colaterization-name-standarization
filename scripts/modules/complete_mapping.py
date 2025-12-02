"""
Módulo de Completar Mapeo
=========================
Asegura que TODOS los nombres del archivo original estén incluidos en el mapeo final,
incluyendo los que no tienen matches (singletons).
"""

import pandas as pd
from pathlib import Path
from datetime import datetime


def run_complete_mapping(base_dir=None):
    """
    Completa el mapeo agregando nombres faltantes (singletons).
    
    Args:
        base_dir: Directorio base del proyecto
        
    Returns:
        tuple: (complete_financial_mapping, complete_non_financial_mapping)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    data_dir = base_dir / "original-data"
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    final_results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("COMPLETAR MAPEO FINAL")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Cargar datos
    print("1. Cargando datos...")
    original_file_financial = data_dir / "financial_entity_freq.csv"
    original_file_non_financial = data_dir / "Non_financial_entity_freq.csv"
    mapping_file_financial = final_results_dir / "financial_entity_mapping.csv"
    mapping_file_non_financial = final_results_dir / "non_financial_entity_mapping.csv"
    normalized_file_financial = results_dir / "financial_normalized.csv"
    normalized_file_non_financial = results_dir / "non_financial_normalized.csv"
    
    if not all(f.exists() for f in [original_file_financial, mapping_file_financial, normalized_file_financial]):
        print("   ✗ Error: No se encontraron los archivos necesarios.")
        return None, None
    
    original_financial = pd.read_csv(original_file_financial)
    original_non_financial = pd.read_csv(original_file_non_financial) if original_file_non_financial.exists() else None
    
    mapping_financial = pd.read_csv(mapping_file_financial)
    mapping_non_financial = pd.read_csv(mapping_file_non_financial) if mapping_file_non_financial.exists() else None
    
    normalized_financial = pd.read_csv(normalized_file_financial)
    normalized_non_financial = pd.read_csv(normalized_file_non_financial) if normalized_file_non_financial.exists() else None
    
    print(f"   ✓ Original financial: {len(original_financial):,} nombres")
    print(f"   ✓ Mapping financial actual: {len(mapping_financial):,} nombres")
    print(f"   ✓ Faltantes: {len(original_financial) - len(mapping_financial):,} nombres")
    
    # Identificar nombres faltantes
    print("\n2. Identificando nombres faltantes...")
    mapped_original_names = set(mapping_financial['original_name'].str.upper().str.strip())
    original_financial['name_upper'] = original_financial['ee_name'].str.upper().str.strip()
    missing_financial = original_financial[~original_financial['name_upper'].isin(mapped_original_names)].copy()
    
    print(f"   ✓ Nombres faltantes en financial: {len(missing_financial):,}")
    
    # Agregar nombres faltantes como singletons
    print("\n3. Agregando nombres faltantes como singletons...")
    max_entity_id = mapping_financial['entity_id'].str.extract(r'financial_(\d+)')[0].astype(int).max()
    next_entity_id = int(max_entity_id) + 1 if not pd.isna(max_entity_id) else 0
    
    singleton_mappings = []
    for idx, row in missing_financial.iterrows():
        original_name = row['ee_name']
        freq = row['freq']
        
        normalized_row = normalized_financial[normalized_financial['original_name'].str.upper().str.strip() == original_name.upper().strip()]
        normalized_name = normalized_row.iloc[0]['normalized_name'] if len(normalized_row) > 0 else original_name.upper().strip()
        standard_name = normalized_name
        
        singleton_mappings.append({
            'entity_id': f'financial_{next_entity_id}',
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
    complete_mapping_financial = pd.concat([
        mapping_financial,
        pd.DataFrame(singleton_mappings)
    ], ignore_index=True)
    
    print(f"   ✓ Mapeo completo financial: {len(complete_mapping_financial):,} nombres")
    
    # Hacer lo mismo para non_financial si existe
    complete_mapping_non_financial = None
    if original_non_financial is not None and mapping_non_financial is not None:
        print("\n5. Procesando non-financial entities...")
        mapped_original_names_nf = set(mapping_non_financial['original_name'].str.upper().str.strip())
        original_non_financial['name_upper'] = original_non_financial['or_name'].str.upper().str.strip()
        missing_non_financial = original_non_financial[~original_non_financial['name_upper'].isin(mapped_original_names_nf)].copy()
        
        print(f"   ✓ Nombres faltantes en non-financial: {len(missing_non_financial):,}")
        
        max_entity_id_nf = mapping_non_financial['entity_id'].str.extract(r'non_financial_(\d+)')[0].astype(int).max()
        next_entity_id_nf = int(max_entity_id_nf) + 1 if not pd.isna(max_entity_id_nf) else 0
        
        singleton_mappings_nf = []
        for idx, row in missing_non_financial.iterrows():
            original_name = row['or_name']
            freq = row['freq']
            
            normalized_row = normalized_non_financial[normalized_non_financial['original_name'].str.upper().str.strip() == original_name.upper().strip()]
            normalized_name = normalized_row.iloc[0]['normalized_name'] if len(normalized_row) > 0 else original_name.upper().strip()
            
            singleton_mappings_nf.append({
                'entity_id': f'non_financial_{next_entity_id_nf}',
                'original_name': original_name,
                'normalized_name': normalized_name,
                'standard_name': normalized_name,
                'frequency': freq,
                'component_size': 1,
                'avg_similarity': None,
                'min_similarity': None,
                'needs_review': False
            })
            
            next_entity_id_nf += 1
        
        complete_mapping_non_financial = pd.concat([
            mapping_non_financial,
            pd.DataFrame(singleton_mappings_nf)
        ], ignore_index=True)
        
        print(f"   ✓ Mapeo completo non-financial: {len(complete_mapping_non_financial):,} nombres")
    
    # Guardar mapeos completos
    print("\n6. Guardando mapeos completos...")
    output_file_financial = final_results_dir / "financial_entity_mapping_complete.csv"
    complete_mapping_financial.to_csv(output_file_financial, index=False)
    print(f"   ✓ {output_file_financial}")
    print(f"     - Total nombres: {len(complete_mapping_financial):,}")
    print(f"     - Entidades únicas: {complete_mapping_financial['entity_id'].nunique():,}")
    print(f"     - Singletons: {len(complete_mapping_financial[complete_mapping_financial['component_size'] == 1]):,}")
    
    if complete_mapping_non_financial is not None:
        output_file_non_financial = final_results_dir / "non_financial_entity_mapping_complete.csv"
        complete_mapping_non_financial.to_csv(output_file_non_financial, index=False)
        print(f"   ✓ {output_file_non_financial}")
        print(f"     - Total nombres: {len(complete_mapping_non_financial):,}")
        print(f"     - Entidades únicas: {complete_mapping_non_financial['entity_id'].nunique():,}")
        print(f"     - Singletons: {len(complete_mapping_non_financial[complete_mapping_non_financial['component_size'] == 1]):,}")
    
    # Estadísticas finales
    print("\n7. Estadísticas finales:")
    print("\n   Financial entities:")
    print(f"     - Total nombres en original: {len(original_financial):,}")
    print(f"     - Total nombres en mapeo completo: {len(complete_mapping_financial):,}")
    print(f"     - Entidades únicas: {complete_mapping_financial['entity_id'].nunique():,}")
    print(f"     - Nombres agrupados: {len(complete_mapping_financial[complete_mapping_financial['component_size'] > 1]):,}")
    print(f"     - Singletons: {len(complete_mapping_financial[complete_mapping_financial['component_size'] == 1]):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Mapeo completo creado")
    print(f"✓ Todos los nombres del archivo original están incluidos")
    print(f"✓ Archivo guardado: {output_file_financial}")
    print(f"✓ El nombre definitivo es: standard_name")
    print("=" * 80)
    
    return complete_mapping_financial, complete_mapping_non_financial


if __name__ == "__main__":
    run_complete_mapping()

