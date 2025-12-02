"""
Módulo de Exploración
=====================
Consolida los scripts de exploración inicial y análisis de variaciones.
"""

import pandas as pd
import numpy as np
from collections import Counter
import re
from pathlib import Path
from datetime import datetime


def run_exploration(base_dir=None):
    """
    Ejecuta la exploración completa de datos.
    
    Args:
        base_dir: Directorio base del proyecto. Si es None, se infiere desde el archivo.
        
    Returns:
        tuple: (financial_df, non_financial_df) - DataFrames cargados
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    data_dir = base_dir / "original-data"
    results_dir = base_dir / "results" / "exploration"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("FASE 1: EXPLORACIÓN DE DATOS")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Cargar datos
    print("1. Cargando datos...")
    financial_df = pd.read_csv(data_dir / 'financial_entity_freq.csv')
    non_financial_df = pd.read_csv(data_dir / 'Non_financial_entity_freq.csv')
    print(f"   ✓ Financial entities: {len(financial_df):,} registros")
    print(f"   ✓ Non-financial entities: {len(non_financial_df):,} registros")
    
    # Ejecutar análisis básico
    print("\n2. Ejecutando análisis básico...")
    run_basic_exploration(financial_df, non_financial_df, results_dir)
    
    # Ejecutar análisis de variaciones
    print("\n3. Ejecutando análisis de variaciones...")
    run_variation_analysis(financial_df, non_financial_df, results_dir)
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Exploración completada")
    print(f"✓ Resultados guardados en: {results_dir}")
    print("=" * 80)
    
    return financial_df, non_financial_df


def run_basic_exploration(financial_df, non_financial_df, results_dir):
    """Ejecuta análisis exploratorio básico."""
    output_file = results_dir / "basic_stats.txt"
    output_lines = []
    
    output_lines.append("=" * 80)
    output_lines.append("EXPLORACIÓN INICIAL DE DATOS")
    output_lines.append(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    # Estadísticas descriptivas básicas
    output_lines.append("=" * 80)
    output_lines.append("FINANCIAL ENTITIES")
    output_lines.append("=" * 80)
    output_lines.append(f"Total registros: {len(financial_df):,}")
    output_lines.append(f"Nombres únicos: {financial_df['ee_name'].nunique():,}")
    output_lines.append(f"Frecuencia total: {financial_df['freq'].sum():,}")
    output_lines.append("")
    output_lines.append("Top 10 por frecuencia:")
    output_lines.append("-" * 80)
    
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
    
    # Distribución de frecuencias
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
    
    # Patrones comunes - Roles funcionales
    output_lines.append("")
    output_lines.append("=" * 80)
    output_lines.append("PATRONES: ROLES FUNCIONALES")
    output_lines.append("=" * 80)
    financial_names = financial_df['ee_name'].astype(str)
    
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
    
    role_results.sort(key=lambda x: x[1], reverse=True)
    for pattern, count in role_results:
        output_lines.append(f"{pattern:40s} : {count:>6,} ocurrencias")
    
    # Sufijos legales comunes
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
    
    legal_results.sort(key=lambda x: x[1], reverse=True)
    for suffix, count in legal_results:
        output_lines.append(f"{suffix:40s} : {count:>6,} ocurrencias")
    
    # Longitud de nombres
    output_lines.append("")
    output_lines.append("=" * 80)
    output_lines.append("ESTADÍSTICAS DE LONGITUD DE NOMBRES (Financial)")
    output_lines.append("=" * 80)
    
    financial_df['name_length'] = financial_df['ee_name'].str.len()
    output_lines.append(f"Longitud promedio: {financial_df['name_length'].mean():.1f} caracteres")
    output_lines.append(f"Longitud mediana:  {financial_df['name_length'].median():.1f} caracteres")
    output_lines.append(f"Longitud mínima:   {financial_df['name_length'].min()} caracteres")
    output_lines.append(f"Longitud máxima:   {financial_df['name_length'].max()} caracteres")
    
    # Guardar
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    print(f"   ✓ Resultados guardados en: {output_file}")


def run_variation_analysis(financial_df, non_financial_df, results_dir):
    """Ejecuta análisis de variaciones."""
    output_file = results_dir / "variations_analysis.txt"
    output_lines = []
    
    output_lines.append("=" * 80)
    output_lines.append("ANÁLISIS DE VARIACIONES DE NOMBRES")
    output_lines.append(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    # Nombres de referencia
    reference_names = [
        "BANK OF AMERICA", "SILICON VALLEY BANK", "WELLS FARGO", "JPMORGAN",
        "CITI", "GENERAL ELECTRIC CAPITAL", "COMERICA", "CREDIT SUISSE",
        "BANK OF NEW YORK", "FLEET", "PNC BANK", "WILMINGTON TRUST",
        "DEUTSCHE BANK", "US BANK", "WACHOVIA"
    ]
    
    def find_variations(base_name, df, name_column='ee_name'):
        pattern = re.compile(re.escape(base_name), re.IGNORECASE)
        matches = df[df[name_column].str.contains(pattern, na=False, regex=True)]
        return matches.sort_values('freq', ascending=False)
    
    # Analizar variaciones
    output_lines.append("=" * 80)
    output_lines.append("VARIACIONES DE NOMBRES DE REFERENCIA")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    variations_dict = {}
    for ref_name in reference_names:
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
    
    # Diccionario de patrones comunes
    output_lines.append("")
    output_lines.append("=" * 80)
    output_lines.append("DICCIONARIO DE PATRONES COMUNES")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    financial_names = financial_df['ee_name'].astype(str)
    all_roles = set()
    for name in financial_names:
        roles = re.findall(r'AS\s+[A-Z\s]+(?:AGENT|TRUSTEE|ADMINISTRATIVE|COLLATERAL|NOTES)', str(name).upper())
        all_roles.update(roles)
    
    output_lines.append("Roles funcionales encontrados:")
    output_lines.append("-" * 80)
    for role in sorted(all_roles):
        count = financial_names.str.contains(re.escape(role), case=False, na=False).sum()
        output_lines.append(f"  {role:50s} : {count:>6,} ocurrencias")
    output_lines.append("")
    
    # Variaciones de formato
    output_lines.append("=" * 80)
    output_lines.append("VARIACIONES DE FORMATO")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    output_lines.append("Variaciones de 'N.A.' encontradas:")
    output_lines.append("-" * 80)
    na_patterns = [
        r'\bN\.A\.', r'\bN\.A\b', r'\bN\. A\.', r'\bNA\b', r'\bN A\b'
    ]
    
    for pattern in na_patterns:
        matches = financial_names.str.contains(pattern, case=False, na=False).sum()
        if matches > 0:
            output_lines.append(f"  {pattern:20s} : {matches:>6,} ocurrencias")
    output_lines.append("")
    
    # Palabras más comunes
    output_lines.append("")
    output_lines.append("=" * 80)
    output_lines.append("PALABRAS MÁS COMUNES (excluyendo stop words)")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    all_words = []
    stop_words = {'AS', 'THE', 'AND', 'OF', 'NA', 'INC', 'CORP', 'CO', 'LTD', 'LLC', 'PLC',
                  'BANK', 'TRUST', 'COMPANY', 'ASSOCIATION', 'NATIONAL'}
    
    for name in financial_names:
        words = re.findall(r'\b[A-Z]{3,}\b', str(name).upper())
        all_words.extend([w for w in words if w not in stop_words])
    
    word_freq = Counter(all_words)
    output_lines.append("Top 30 palabras más comunes:")
    output_lines.append("-" * 80)
    for word, count in word_freq.most_common(30):
        output_lines.append(f"  {word:30s} : {count:>6,} ocurrencias")
    
    # Guardar
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    print(f"   ✓ Resultados guardados en: {output_file}")


if __name__ == "__main__":
    run_exploration()

