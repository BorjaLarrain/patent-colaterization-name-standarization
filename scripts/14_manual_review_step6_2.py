"""
Fase 6.2: Herramientas para Revisión Manual
============================================
Este script genera herramientas y formatos para facilitar la revisión manual:
- HTML interactivo para revisión
- CSV formateado para Excel
- Script para documentar decisiones manuales
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import ast

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"
FINAL_RESULTS_DIR = BASE_DIR / "results" / "final"
VALIDATION_DIR = BASE_DIR / "results" / "validation"
MANUAL_REVIEW_DIR = BASE_DIR / "results" / "manual_review"
MANUAL_REVIEW_DIR.mkdir(parents=True, exist_ok=True)

def create_html_review_file(problematic_df, mapping_df, components_json, entity_type='financial'):
    """
    Crea un archivo HTML interactivo para revisión manual
    """
    html_content = []
    html_template = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Revisión Manual - Componentes Problemáticos</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .component {{ background: white; margin: 20px 0; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .component-header {{ background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; }}
        .component-id {{ font-size: 18px; font-weight: bold; color: #1976d2; }}
        .issues {{ color: #d32f2f; margin: 10px 0; }}
        .stats {{ display: flex; gap: 20px; margin: 10px 0; }}
        .stat {{ padding: 5px 10px; background: #f0f0f0; border-radius: 3px; }}
        .names-list {{ margin-top: 15px; }}
        .name-item {{ padding: 8px; margin: 5px 0; background: #fafafa; border-left: 3px solid #2196f3; }}
        .name-item:hover {{ background: #e3f2fd; }}
        .standard-name {{ font-weight: bold; color: #1976d2; }}
        .actions {{ margin-top: 15px; padding: 10px; background: #fff3cd; border-radius: 5px; }}
        .action-btn {{ padding: 8px 15px; margin: 5px; border: none; border-radius: 4px; cursor: pointer; }}
        .btn-valid {{ background: #4caf50; color: white; }}
        .btn-split {{ background: #ff9800; color: white; }}
        .btn-invalid {{ background: #f44336; color: white; }}
        .notes {{ width: 100%; min-height: 60px; margin-top: 10px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; }}
    </style>
</head>
<body>
    <h1>Revisión Manual de Componentes Problemáticos</h1>
    <p><strong>Tipo:</strong> {entity_type}</p>
    <p><strong>Fecha:</strong> {date}</p>
    <p><strong>Total componentes a revisar:</strong> {total}</p>
    <hr>
"""
    html_content.append(html_template.format(
        entity_type=entity_type.upper(),
        date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        total=len(problematic_df)
    ))
    
    for idx, row in problematic_df.iterrows():
        component_id = row['component_id']
        standard_name = row['standard_name']
        size = row['size']
        issues = row['issues']
        avg_sim = row.get('avg_similarity', 'N/A')
        min_sim = row.get('min_similarity', 'N/A')
        
        # Obtener todos los nombres del componente
        component_data = mapping_df[mapping_df['entity_id'] == f"{entity_type}_{component_id}"]
        all_names = component_data['normalized_name'].unique().tolist()
        
        html_content.append(f"""
    <div class="component" id="component-{component_id}">
        <div class="component-header">
            <div class="component-id">Componente {component_id}</div>
            <div class="stats">
                <span class="stat">Tamaño: {size} nombres</span>
                <span class="stat">Similitud promedio: {avg_sim:.1f}%</span>
                <span class="stat">Similitud mínima: {min_sim:.1f}%</span>
            </div>
            <div class="issues"><strong>Problemas identificados:</strong> {', '.join(issues) if isinstance(issues, list) else issues}</div>
        </div>
        
        <div class="names-list">
            <h3>Nombre estándar: <span class="standard-name">{standard_name}</span></h3>
            <h4>Todos los nombres en este componente ({len(all_names)}):</h4>
            <div>
""")
        
        for name in all_names[:50]:  # Limitar a 50 para no hacer el HTML muy grande
            html_content.append(f'                <div class="name-item">{name}</div>\n')
        
        if len(all_names) > 50:
            html_content.append(f'                <div class="name-item"><em>... y {len(all_names) - 50} nombres más</em></div>\n')
        
        html_content.append("""
            </div>
        </div>
        
        <div class="actions">
            <h4>Acción:</h4>
            <button class="action-btn btn-valid" onclick="markValid(this)">✓ Válido (mantener grupo)</button>
            <button class="action-btn btn-split" onclick="markSplit(this)">✂ Dividir grupo</button>
            <button class="action-btn btn-invalid" onclick="markInvalid(this)">✗ Inválido (separar nombres)</button>
            <textarea class="notes" placeholder="Notas sobre esta decisión..."></textarea>
        </div>
    </div>
""")
    
    html_content.append("""
    <script>
        function markValid(btn) {
            const component = btn.closest('.component');
            component.style.border = '3px solid #4caf50';
            btn.style.background = '#2e7d32';
        }
        function markSplit(btn) {
            const component = btn.closest('.component');
            component.style.border = '3px solid #ff9800';
            btn.style.background = '#f57c00';
        }
        function markInvalid(btn) {
            const component = btn.closest('.component');
            component.style.border = '3px solid #f44336';
            btn.style.background = '#c62828';
        }
    </script>
</body>
</html>
""")
    
    return ''.join(html_content)

def create_excel_review_file(problematic_df, mapping_df, entity_type='financial'):
    """
    Crea un archivo CSV/Excel formateado para revisión manual
    """
    review_data = []
    
    for idx, row in problematic_df.iterrows():
        component_id = row['component_id']
        standard_name = row['standard_name']
        
        # Obtener todos los nombres del componente
        component_data = mapping_df[mapping_df['entity_id'] == f"{entity_type}_{component_id}"]
        
        for _, name_row in component_data.iterrows():
            review_data.append({
                'component_id': component_id,
                'entity_id': name_row['entity_id'],
                'standard_name': standard_name,
                'normalized_name': name_row['normalized_name'],
                'original_name': name_row['original_name'],
                'frequency': name_row['frequency'],
                'avg_similarity': row.get('avg_similarity', ''),
                'min_similarity': row.get('min_similarity', ''),
                'issues': '; '.join(row['issues']) if isinstance(row['issues'], list) else row['issues'],
                'decision': '',  # Para llenar manualmente
                'notes': ''  # Para llenar manualmente
            })
    
    return pd.DataFrame(review_data)

def create_sample_review_file(mapping_df, entity_type='financial', sample_size=50):
    """
    Crea un archivo con muestra aleatoria para validar calidad general
    """
    # Seleccionar muestra aleatoria
    sample = mapping_df.sample(n=min(sample_size, len(mapping_df)), random_state=42)
    
    review_data = []
    for _, row in sample.iterrows():
        review_data.append({
            'entity_id': row['entity_id'],
            'standard_name': row['standard_name'],
            'normalized_name': row['normalized_name'],
            'original_name': row['original_name'],
            'frequency': row['frequency'],
            'component_size': row['component_size'],
            'avg_similarity': row.get('avg_similarity', ''),
            'is_correct': '',  # Para marcar manualmente
            'notes': ''  # Para notas
        })
    
    return pd.DataFrame(review_data)

# ============================================================================
# EJECUCIÓN PRINCIPAL
# ============================================================================

print("=" * 80)
print("FASE 6.2: HERRAMIENTAS PARA REVISIÓN MANUAL")
print("=" * 80)
print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# 1. CARGAR datos
print("1. Cargando datos...")
mapping_file_financial = FINAL_RESULTS_DIR / "financial_entity_mapping.csv"
mapping_file_non_financial = FINAL_RESULTS_DIR / "non_financial_entity_mapping.csv"
problematic_file_financial = VALIDATION_DIR / "financial_problematic_components.csv"
problematic_file_non_financial = VALIDATION_DIR / "non_financial_problematic_components.csv"

if not all(f.exists() for f in [mapping_file_financial, problematic_file_financial]):
    print("   ✗ Error: No se encontraron los archivos necesarios.")
    print(f"   Por favor ejecuta primero: 13_validation_step6_1.py")
    exit(1)

financial_mapping = pd.read_csv(mapping_file_financial)
non_financial_mapping = pd.read_csv(mapping_file_non_financial) if mapping_file_non_financial.exists() else None
financial_problematic = pd.read_csv(problematic_file_financial)
non_financial_problematic = pd.read_csv(problematic_file_non_financial) if problematic_file_non_financial.exists() else None

print(f"   ✓ Financial: {len(financial_mapping):,} registros, {len(financial_problematic):,} componentes problemáticos")
if non_financial_mapping is not None:
    print(f"   ✓ Non-financial: {len(non_financial_mapping):,} registros, {len(non_financial_problematic):,} componentes problemáticos")

# 2. CREAR archivos HTML para revisión
print("\n2. Creando archivos HTML para revisión...")
html_content = create_html_review_file(financial_problematic, financial_mapping, None, 'financial')
html_file = MANUAL_REVIEW_DIR / "financial_review.html"
with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html_content)
print(f"   ✓ {html_file}")

if non_financial_problematic is not None and len(non_financial_problematic) > 0:
    html_content = create_html_review_file(non_financial_problematic, non_financial_mapping, None, 'non_financial')
    html_file = MANUAL_REVIEW_DIR / "non_financial_review.html"
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"   ✓ {html_file}")

# 3. CREAR archivos CSV/Excel para revisión
print("\n3. Creando archivos CSV para revisión en Excel...")
excel_review = create_excel_review_file(financial_problematic, financial_mapping, 'financial')
excel_file = MANUAL_REVIEW_DIR / "financial_review_excel.csv"
excel_review.to_csv(excel_file, index=False)
print(f"   ✓ {excel_file} ({len(excel_review):,} filas)")

if non_financial_problematic is not None and len(non_financial_problematic) > 0:
    excel_review = create_excel_review_file(non_financial_problematic, non_financial_mapping, 'non_financial')
    excel_file = MANUAL_REVIEW_DIR / "non_financial_review_excel.csv"
    excel_review.to_csv(excel_file, index=False)
    print(f"   ✓ {excel_file} ({len(excel_review):,} filas)")

# 4. CREAR muestra aleatoria para validación
print("\n4. Creando muestras aleatorias para validación...")
sample = create_sample_review_file(financial_mapping, 'financial', 50)
sample_file = MANUAL_REVIEW_DIR / "financial_sample_review.csv"
sample.to_csv(sample_file, index=False)
print(f"   ✓ {sample_file} ({len(sample):,} registros)")

if non_financial_mapping is not None:
    sample = create_sample_review_file(non_financial_mapping, 'non_financial', 50)
    sample_file = MANUAL_REVIEW_DIR / "non_financial_sample_review.csv"
    sample.to_csv(sample_file, index=False)
    print(f"   ✓ {sample_file} ({len(sample):,} registros)")

# 5. Resumen
print("\n" + "=" * 80)
print("RESUMEN")
print("=" * 80)
print(f"✓ Herramientas de revisión manual creadas")
print(f"✓ Archivos guardados en: {MANUAL_REVIEW_DIR}")
print(f"\nArchivos generados:")
print(f"  - HTML interactivo: financial_review.html")
print(f"  - CSV para Excel: financial_review_excel.csv")
print(f"  - Muestra aleatoria: financial_sample_review.csv")
print(f"\nInstrucciones:")
print(f"  1. Abre el archivo HTML en tu navegador para revisión interactiva")
print(f"  2. O abre el CSV en Excel para revisión estructurada")
print(f"  3. Marca tus decisiones en las columnas 'decision' y 'notes'")
print(f"  4. Usa el script 15_apply_manual_decisions.py para aplicar cambios")
print("=" * 80)

