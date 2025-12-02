"""
Parte 1.1: Carga y Exploración Inicial
======================================
Este script realiza la carga y análisis exploratorio inicial de los datos.
Los resultados se guardan en results/exploration/basic_stats.txt
"""

import pandas as pd
import numpy as np
from collections import Counter
import re
from pathlib import Path
from datetime import datetime

# Configuración de paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "Original_data"
RESULTS_DIR = BASE_DIR / "results" / "exploration"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Archivo de salida
OUTPUT_FILE = RESULTS_DIR / "basic_stats.txt"

print("Iniciando exploración de datos...")
print(f"Resultados se guardarán en: {OUTPUT_FILE}")

# 1. Cargar las bases de datos
print("\nCargando datos...")
financial_df = pd.read_csv(DATA_DIR / 'financial_entity_freq.csv')
non_financial_df = pd.read_csv(DATA_DIR / 'Non_financial_entity_freq.csv')
print("✓ Datos cargados correctamente")

# Preparar string para guardar resultados
output_lines = []
output_lines.append("=" * 80)
output_lines.append("EXPLORACIÓN INICIAL DE DATOS")
output_lines.append(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
output_lines.append("=" * 80)
output_lines.append("")

# 2. Estadísticas descriptivas básicas
output_lines.append("=" * 80)
output_lines.append("FINANCIAL ENTITIES")
output_lines.append("=" * 80)
output_lines.append(f"Total registros: {len(financial_df):,}")
output_lines.append(f"Nombres únicos: {financial_df['ee_name'].nunique():,}")
output_lines.append(f"Frecuencia total: {financial_df['freq'].sum():,}")
output_lines.append("")
output_lines.append("Top 10 por frecuencia:")
output_lines.append("-" * 80)

# Formatear top 10 para mejor legibilidad
top_10_financial = financial_df.head(10)[['ee_name', 'freq']]
for idx, row in top_10_financial.iterrows():
    output_lines.append(f"{row['freq']:>10,} | {row['ee_name']}")

output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("NON-FINANCIAL ENTITIES")
output_lines.append("=" * 80)
output_lines.append(f"Total registros: {len(non_financial_df):,}")
output_lines.append(f"Nombres únicos: {non_financial_df['or_name'].nunique():,}")
output_lines.append(f"Frecuencia total: {non_financial_df['freq'].sum():,}")
output_lines.append("")
output_lines.append("Top 10 por frecuencia:")
output_lines.append("-" * 80)

top_10_non_financial = non_financial_df.head(10)[['or_name', 'freq']]
for idx, row in top_10_non_financial.iterrows():
    output_lines.append(f"{row['freq']:>10,} | {row['or_name']}")

# 3. Distribución de frecuencias
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("DISTRIBUCIÓN DE FRECUENCIAS (Financial Entities)")
output_lines.append("=" * 80)

freq_stats = financial_df['freq'].describe()
output_lines.append(f"Media:        {freq_stats['mean']:,.2f}")
output_lines.append(f"Desv. Est.:   {freq_stats['std']:,.2f}")
output_lines.append(f"Mínimo:       {freq_stats['min']:,.0f}")
output_lines.append(f"25% (Q1):     {freq_stats['25%']:,.0f}")
output_lines.append(f"50% (Mediana): {freq_stats['50%']:,.0f}")
output_lines.append(f"75% (Q3):     {freq_stats['75%']:,.0f}")
output_lines.append(f"Máximo:       {freq_stats['max']:,.0f}")

# 4. Identificar patrones comunes - Roles funcionales
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("PATRONES: ROLES FUNCIONALES")
output_lines.append("=" * 80)
financial_names = financial_df['ee_name'].astype(str)

# Buscar patrones de roles funcionales
role_patterns = [
    r'AS COLLATERAL AGENT',
    r'AS ADMINISTRATIVE AGENT',
    r'AS TRUSTEE',
    r'AS AGENT',
    r'AS NOTES COLLATERAL AGENT',
    r'AS COLLATERAL TRUSTEE'
]

role_results = []
for pattern in role_patterns:
    matches = financial_names.str.contains(pattern, case=False, na=False).sum()
    if matches > 0:
        role_results.append((pattern, matches))

# Ordenar por frecuencia descendente
role_results.sort(key=lambda x: x[1], reverse=True)
for pattern, count in role_results:
    output_lines.append(f"{pattern:40s} : {count:>6,} ocurrencias")

# 5. Identificar sufijos legales comunes
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("PATRONES: SUFIJOS LEGALES")
output_lines.append("=" * 80)

legal_suffixes = [
    r'\bN\.A\.',
    r'\bNATIONAL ASSOCIATION',
    r'\bINC\.',
    r'\bINCORPORATED',
    r'\bCORP\.',
    r'\bCORPORATION',
    r'\bLLC',
    r'\bL\.L\.C\.',
    r'\bL\.P\.',
    r'\bPLC',
    r'\bCO\.',
    r'\bCOMPANY'
]

legal_results = []
for suffix in legal_suffixes:
    matches = financial_names.str.contains(suffix, case=False, na=False).sum()
    if matches > 0:
        legal_results.append((suffix, matches))

# Ordenar por frecuencia descendente
legal_results.sort(key=lambda x: x[1], reverse=True)
for suffix, count in legal_results:
    output_lines.append(f"{suffix:40s} : {count:>6,} ocurrencias")

# 6. Longitud de nombres
output_lines.append("")
output_lines.append("=" * 80)
output_lines.append("ESTADÍSTICAS DE LONGITUD DE NOMBRES (Financial)")
output_lines.append("=" * 80)

financial_df['name_length'] = financial_df['ee_name'].str.len()
output_lines.append(f"Longitud promedio: {financial_df['name_length'].mean():.1f} caracteres")
output_lines.append(f"Longitud mediana:  {financial_df['name_length'].median():.1f} caracteres")
output_lines.append(f"Longitud mínima:   {financial_df['name_length'].min()} caracteres")
output_lines.append(f"Longitud máxima:   {financial_df['name_length'].max()} caracteres")

# Guardar resultados en archivo
print("\nGuardando resultados...")
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))

print(f"✓ Resultados guardados exitosamente en: {OUTPUT_FILE}")
print(f"\nResumen:")
print(f"  - Total registros financieros: {len(financial_df):,}")
print(f"  - Total registros no financieros: {len(non_financial_df):,}")
print(f"  - Roles funcionales encontrados: {len(role_results)}")
print(f"  - Sufijos legales encontrados: {len(legal_results)}")