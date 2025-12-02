"""
Parte 1.2: Análisis de Variaciones
===================================
Este script analiza variaciones de nombres conocidos y crea diccionarios de patrones.
Los resultados se guardan en results/exploration/variations_analysis.txt
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
from collections import Counter

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "Original_data"
RESULTS_DIR = BASE_DIR / "results" / "exploration"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Archivo de salida
OUTPUT_FILE = RESULTS_DIR / "variations_analysis.txt"

print("Iniciando análisis de variaciones...")
print(f"Resultados se guardarán en: {OUTPUT_FILE}")

# 1. Cargar las bases de datos
print("\nCargando datos...")
financial_df = pd.read_csv(DATA_DIR / 'financial_entity_freq.csv')
non_financial_df = pd.read_csv(DATA_DIR / 'Non_financial_entity_freq.csv')
print("✓ Datos cargados correctamente")

# Preparar string para guardar resultados
output_lines = []
output_lines.append("=" * 80)
output_lines.append("ANÁLISIS DE VARIACIONES DE NOMBRES")
output_lines.append(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
output_lines.append("=" * 80)
output_lines.append("")

# 2. Nombres de referencia (de la Figura 10 del paper)
reference_names = [
    "BANK OF AMERICA",
    "SILICON VALLEY BANK",
    "WELLS FARGO",
    "JPMORGAN",
    "CITI",
    "GENERAL ELECTRIC CAPITAL",
    "COMERICA",
    "CREDIT SUISSE",
    "BANK OF NEW YORK",
    "FLEET",
    "PNC BANK",
    "WILMINGTON TRUST",
    "DEUTSCHE BANK",
    "US BANK",
    "WACHOVIA"
]

def find_variations(base_name, df, name_column='ee_name'):
    """
    Encuentra todas las variaciones de un nombre base
    """
    # Buscar nombres que contengan el base_name (case insensitive)
    pattern = re.compile(re.escape(base_name), re.IGNORECASE)
    matches = df[df[name_column].str.contains(pattern, na=False, regex=True)]
    
    return matches.sort_values('freq', ascending=False)

# 3. Analizar variaciones de nombres conocidos
print("\nAnalizando variaciones de nombres de referencia...")
output_lines.append("=" * 80)
output_lines.append("VARIACIONES DE NOMBRES DE REFERENCIA")
output_lines.append("=" * 80)
output_lines.append("")

variations_dict = {}

for ref_name in reference_names:
    print(f"  Analizando: {ref_name}...")
    variations = find_variations(ref_name, financial_df)
    
    if len(variations) > 0:
        variations_dict[ref_name] = variations
        
        output_lines.append("-" * 80)
        output_lines.append(f"REFERENCIA: {ref_name}")
        output_lines.append("-" * 80)
        output_lines.append(f"Total variaciones encontradas: {len(variations)}")
        output_lines.append(f"Frecuencia total: {variations['freq'].sum():,}")
        output_lines.append("")
        output_lines.append("Top 20 variaciones por frecuencia:")
        output_lines.append("")
        
        top_variations = variations.head(20)
        for idx, row in top_variations.iterrows():
            output_lines.append(f"  {row['freq']:>8,} | {row['ee_name']}")
        
        output_lines.append("")

print(f"✓ Analizadas {len([k for k in variations_dict.keys() if len(variations_dict[k]) > 0])} referencias con variaciones")

# 4. Crear diccionario de patrones comunes observados
print("\nCreando diccionario de patrones...")
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("DICCIONARIO DE PATRONES COMUNES")
output_lines.append("=" * 80)
output_lines.append("")

financial_names = financial_df['ee_name'].astype(str)

# Extraer todos los roles funcionales únicos
all_roles = set()
for name in financial_names:
    # Buscar patrones "AS ..."
    roles = re.findall(r'AS\s+[A-Z\s]+(?:AGENT|TRUSTEE|ADMINISTRATIVE|COLLATERAL|NOTES)', str(name).upper())
    all_roles.update(roles)

output_lines.append("Roles funcionales encontrados:")
output_lines.append("-" * 80)
for role in sorted(all_roles):
    count = financial_names.str.contains(re.escape(role), case=False, na=False).sum()
    output_lines.append(f"  {role:50s} : {count:>6,} ocurrencias")
output_lines.append("")

# 5. Análisis de variaciones de formato
print("Analizando variaciones de formato...")
output_lines.append("=" * 80)
output_lines.append("VARIACIONES DE FORMATO")
output_lines.append("=" * 80)
output_lines.append("")

# Analizar variaciones de puntuación en "N.A."
output_lines.append("Variaciones de 'N.A.' encontradas:")
output_lines.append("-" * 80)
na_patterns = [
    r'\bN\.A\.',
    r'\bN\.A\b',
    r'\bN\. A\.',
    r'\bNA\b',
    r'\bN A\b'
]

for pattern in na_patterns:
    matches = financial_names.str.contains(pattern, case=False, na=False).sum()
    if matches > 0:
        output_lines.append(f"  {pattern:20s} : {matches:>6,} ocurrencias")
output_lines.append("")

# 6. Categorización de variaciones (ejemplo con BANK OF AMERICA)
print("Categorizando variaciones de ejemplo...")
if "BANK OF AMERICA" in variations_dict:
    boa_variations = variations_dict["BANK OF AMERICA"]
    
    output_lines.append("=" * 80)
    output_lines.append("ANÁLISIS DETALLADO: BANK OF AMERICA")
    output_lines.append("=" * 80)
    output_lines.append(f"Total variaciones: {len(boa_variations)}")
    output_lines.append(f"Frecuencia total: {boa_variations['freq'].sum():,}")
    output_lines.append("")
    
    # Categorizar variaciones
    categories = {
        'Con roles funcionales': [],
        'Sin roles funcionales': [],
        'Variaciones de N.A.': [],
        'Posibles typos': []
    }
    
    for idx, row in boa_variations.iterrows():
        name = str(row['ee_name']).upper()
        
        if 'AS ' in name:
            categories['Con roles funcionales'].append((row['ee_name'], row['freq']))
        else:
            categories['Sin roles funcionales'].append((row['ee_name'], row['freq']))
        
        # Detectar variaciones de N.A.
        if re.search(r'N\.?\s*A\.?', name):
            categories['Variaciones de N.A.'].append((row['ee_name'], row['freq']))
        
        # Detectar posibles typos
        if 'AMERICAN' in name and 'AMERICA' not in name.replace('AMERICAN', ''):
            categories['Posibles typos'].append((row['ee_name'], row['freq']))
    
    output_lines.append("Categorías de variaciones:")
    output_lines.append("-" * 80)
    for cat, items in categories.items():
        if items:
            output_lines.append(f"\n{cat}: {len(items)} variaciones")
            # Mostrar top 5 de cada categoría
            sorted_items = sorted(items, key=lambda x: x[1], reverse=True)[:5]
            for name, freq in sorted_items:
                output_lines.append(f"  {freq:>8,} | {name}")

# 7. Palabras más comunes (excluyendo stop words)
print("Analizando palabras más comunes...")
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("PALABRAS MÁS COMUNES (excluyendo stop words)")
output_lines.append("=" * 80)
output_lines.append("")

all_words = []
stop_words = {'AS', 'THE', 'AND', 'OF', 'NA', 'INC', 'CORP', 'CO', 'LTD', 'LLC', 'PLC', 
              'BANK', 'TRUST', 'COMPANY', 'ASSOCIATION', 'NATIONAL'}

for name in financial_names:
    # Extraer palabras de al menos 3 caracteres
    words = re.findall(r'\b[A-Z]{3,}\b', str(name).upper())
    all_words.extend([w for w in words if w not in stop_words])

word_freq = Counter(all_words)
output_lines.append("Top 30 palabras más comunes:")
output_lines.append("-" * 80)
for word, count in word_freq.most_common(30):
    output_lines.append(f"  {word:30s} : {count:>6,} ocurrencias")

# Guardar resultados en archivo
print("\nGuardando resultados...")
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))

print(f"✓ Resultados guardados exitosamente en: {OUTPUT_FILE}")
print(f"\nResumen:")
print(f"  - Referencias analizadas: {len(reference_names)}")
print(f"  - Referencias con variaciones encontradas: {len([k for k in variations_dict.keys() if len(variations_dict[k]) > 0])}")
print(f"  - Roles funcionales únicos: {len(all_roles)}")
print(f"  - Palabras comunes analizadas: {len(word_freq)}")

