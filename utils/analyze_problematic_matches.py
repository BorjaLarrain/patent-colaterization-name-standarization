"""
Análisis de Matches Problemáticos
==================================
Este script analiza casos donde WRatio da scores altos pero los nombres
claramente NO deberían ser la misma entidad (falsos positivos).
"""

import pandas as pd
from rapidfuzz import fuzz
from pathlib import Path

# Configuración
BASE_DIR = Path(__file__).parent.parent
RESULTS_DIR = BASE_DIR / "results" / "intermediate"

print("=" * 80)
print("ANÁLISIS DE MATCHES PROBLEMÁTICOS")
print("=" * 80)
print()

# Casos problemáticos identificados
problematic_cases = [
    {
        "name1": "BAKER BEN",
        "name2": "BAKER HUGHES PIPELINE MANAGEMENT GROUP INC",
        "reason": "Nombre de persona vs empresa - solo comparten 'BAKER'"
    },
    {
        "name1": "BANQUE INDOSUEZ",
        "name2": "BANQUE PARIBAS NEW YORK BRANCH",
        "reason": "Bancos diferentes - solo comparten 'BANQUE'"
    },
    {
        "name1": "BANQUE NATIONALE DE PARIS",
        "name2": "BANQUE PARIBAS",
        "reason": "Bancos relacionados pero diferentes - BNP Paribas es fusión"
    },
    {
        "name1": "COOPERATIEVE CENTRALE RAIFFEISEN-BOERENLEENBANK B A RABOBANK NEDERLAND NEW YORK BRANCH",
        "name2": "COOPERATIEVE RABOBANK U A NEW YORK BRANCH",
        "reason": "Nombres muy largos con muchas palabras comunes"
    },
    {
        "name1": "NATIONAL BANK OF CANADA A CANADIAN CHARTERED BANK",
        "name2": "CANADA FINANCE HOLDING CO",
        "reason": "Entidades diferentes - solo comparten 'CANADA'"
    }
]

print("CASOS PROBLEMÁTICOS IDENTIFICADOS:")
print("-" * 80)
print()

for i, case in enumerate(problematic_cases, 1):
    name1 = case["name1"]
    name2 = case["name2"]
    
    # Calcular diferentes métricas
    ratio = fuzz.ratio(name1, name2)
    partial_ratio = fuzz.partial_ratio(name1, name2)
    token_sort_ratio = fuzz.token_sort_ratio(name1, name2)
    token_set_ratio = fuzz.token_set_ratio(name1, name2)
    wratio = fuzz.WRatio(name1, name2)
    
    print(f"Caso {i}: {case['reason']}")
    print(f"  Nombre 1: {name1}")
    print(f"  Nombre 2: {name2}")
    print()
    print(f"  Métricas de similitud:")
    print(f"    - ratio:              {ratio:.1f}%")
    print(f"    - partial_ratio:     {partial_ratio:.1f}%")
    print(f"    - token_sort_ratio:   {token_sort_ratio:.1f}%")
    print(f"    - token_set_ratio:   {token_set_ratio:.1f}%")
    print(f"    - WRatio:            {wratio:.1f}%  ← Este es el que usa el script")
    print()
    
    # Análisis de palabras comunes
    words1 = set(name1.split())
    words2 = set(name2.split())
    common_words = words1.intersection(words2)
    all_words = words1.union(words2)
    
    print(f"  Análisis de palabras:")
    print(f"    - Palabras en nombre 1: {len(words1)}")
    print(f"    - Palabras en nombre 2: {len(words2)}")
    print(f"    - Palabras comunes: {len(common_words)} ({', '.join(sorted(common_words))})")
    print(f"    - % palabras comunes: {100*len(common_words)/len(all_words):.1f}%")
    print()
    print("-" * 80)
    print()

print("=" * 80)
print("EXPLICACIÓN: ¿CÓMO FUNCIONA WRatio?")
print("=" * 80)
print()
print("WRatio (Weighted Ratio) es una métrica híbrida que combina:")
print()
print("1. ratio: Comparación carácter por carácter")
print("   - Mide cuántos caracteres son iguales")
print("   - Sensible a orden y posición")
print()
print("2. partial_ratio: Busca la subcadena más similar")
print("   - Útil cuando un nombre está contenido en otro")
print("   - Ejemplo: 'BANK' vs 'BANK OF AMERICA'")
print()
print("3. token_sort_ratio: Compara tokens ordenados")
print("   - Divide en palabras, ordena, y compara")
print("   - Ignora orden de palabras")
print("   - Ejemplo: 'BANK OF AMERICA' vs 'AMERICA BANK OF'")
print()
print("4. token_set_ratio: Compara conjuntos de tokens")
print("   - Compara conjuntos de palabras únicas")
print("   - Más tolerante a palabras repetidas")
print()
print("WRatio selecciona automáticamente el mejor método según:")
print("  - Longitud de los strings")
print("  - Tipo de similitud (caracteres vs tokens)")
print("  - Y toma el MÁXIMO de todas las métricas")
print()
print("PROBLEMA: WRatio puede dar scores altos cuando:")
print("  ✓ Hay muchas palabras comunes (aunque sean entidades diferentes)")
print("  ✓ Un nombre es muy corto y está contenido en otro muy largo")
print("  ✓ Los nombres comparten palabras genéricas ('BANK', 'COMPANY', etc.)")
print()

print("=" * 80)
print("SOLUCIONES PROPUESTAS")
print("=" * 80)
print()
print("1. AUMENTAR THRESHOLD")
print("   - Actual: 85%")
print("   - Sugerencia: 88-90% para matches más seguros")
print("   - Trade-off: Puede perder algunos matches válidos")
print()
print("2. AGREGAR FILTROS ADICIONALES")
print("   - Rechazar matches donde la diferencia de longitud es muy grande")
print("   - Rechazar matches donde solo comparten palabras genéricas")
print("   - Requerir que compartan al menos N palabras significativas")
print()
print("3. POST-PROCESAMIENTO (Fase 6)")
print("   - Revisar grupos con baja similitud promedio")
print("   - Identificar y separar falsos positivos manualmente")
print("   - Crear reglas específicas para casos conocidos")
print()
print("4. MEJORAR NORMALIZACIÓN")
print("   - Eliminar palabras genéricas antes de matching")
print("   - Normalizar mejor nombres de personas vs empresas")
print()

print("=" * 80)

