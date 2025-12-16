"""
Módulo de Agrupación y Asignación de IDs
=========================================
Asigna IDs únicos y selecciona nombres estándar para cada grupo.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import re

# Legal suffixes to remove when extracting root names
LEGAL_SUFFIXES = {
    'AG', 'INC', 'LLC', 'LTD', 'SA', 'PLC', 'NV', 'BV', 'LLP', 'LP', 
    'CORP', 'CORPORATION', 'INCORPORATED', 'LIMITED', 'CO', 'COMPANY',
    'S.A.', 'S.A', 'SRL', 'SPA', 'SPZ', 'GMBH', 'KG', 'SE', 'AB', 'OY',
    'ASA', 'AS', 'PTE', 'PTY', 'BHD', 'SDN', 'BHD', 'PVT', 'PRIVATE',
    'NA', 'N.A', 'N.A.'  # National Association suffix
}

# Suffixes to keep in standard names (not removed during root name extraction)
KEEP_SUFFIXES = {'LLC', 'NA', 'N.A', 'N.A.', 'CORP', 'CO'}

# Branch, location, and geographic tokens to always strip
# NOTE: "TRUST" and "COMPANY" are kept here - they'll be stripped from non-exception names
# but exceptions are checked FIRST, so they won't be stripped from exception names
# NOTE: "NEW" and "YORK" removed - they should only be stripped as part of "OF NEW YORK" pattern,
# not when part of exception names like "BANK OF NEW YORK MELLON"
BRANCH_GEO_TOKENS = {
    'BRANCH', 'TRUST', 'TRUSTEE', 'COMPANY', 'AMERICAS', 'EUROPE', 'ASIA', 'AFRICA',
    'CAYMAN', 'ISLANDS', 'LUXEMBOURG', 'LONDON', 'PARIS',
    'FRANKFURT', 'TOKYO', 'HONG', 'KONG', 'SINGAPORE', 'ZURICH', 'GENEVA',
    'MADRID', 'MILAN', 'ROME', 'AMSTERDAM', 'BRUSSELS', 'DUBLIN', 'OSLO',
    'STOCKHOLM', 'COPENHAGEN', 'VIENNA', 'LISBON', 'ATHENS', 'WARSAW',
    'PRAGUE', 'BUDAPEST', 'MOSCOW', 'ISTANBUL', 'DUBAI', 'RIYADH', 'DOHA',
    'BAHRAIN', 'KUWAIT', 'JEDDAH', 'MUMBAI', 'DELHI', 'BANGALORE',
    'SHANGHAI', 'BEIJING', 'SEOUL', 'TAIPEI', 'BANGKOK', 'KUALA', 'LUMPUR',
    'JAKARTA', 'MANILA', 'SYDNEY', 'MELBOURNE', 'AUCKLAND', 'TORONTO',
    'VANCOUVER', 'MONTREAL', 'MEXICO', 'CITY', 'SAO', 'PAULO', 'RIO',
    'JANEIRO', 'BUENOS', 'AIRES', 'SANTIAGO', 'LIMA', 'BOGOTA', 'CARACAS'
}

# Multi-word phrases to check before token removal
MULTI_WORD_PATTERNS = [
    r'CAYMAN\s+ISLANDS',
    r'TRUST\s+CO',
    r'HONG\s+KONG',
    r'SAO\s+PAULO',
    r'RIO\s+DE\s+JANEIRO',
    r'BUENOS\s+AIRES',
    r'KUALA\s+LUMPUR'
]

# Location patterns to strip "OF [LOCATION]" phrases (applied after exception checking)
# These patterns handle location qualifiers like "OF NEW YORK", "OF TEXAS", etc.
LOCATION_PATTERNS = [
    r'\s+OF\s+NEW\s+YORK\s*$',  # "OF NEW YORK" at the end
    r'\s+OF\s+TEXAS\s*$',
    r'\s+OF\s+CALIFORNIA\s*$',
    r'\s+OF\s+FLORIDA\s*$',
    r'\s+OF\s+ILLINOIS\s*$',
    r'\s+OF\s+DELAWARE\s*$',
    # Add more US states as needed
]

# Exception institutions that should not be stripped (protected names)
# Include variations with and without "THE" since normalization removes "THE" from the beginning
# Also include normalized versions (e.g., "US TRUST" for "UNITED STATES TRUST COMPANY")
EXCEPTION_INSTITUTIONS = {
    'THE BANK OF NEW YORK MELLON',
    'BANK OF NEW YORK MELLON',  # Without "THE" (normalized version)
    'GLAS AMERICAS',
    'BANKERS TRUST COMPANY',
    'BANK OF MONTREAL',
    'UNITED STATES TRUST COMPANY',
    'US TRUST'  # Normalized version: "UNITED STATES" -> "US", "COMPANY" -> "CO" (CO gets stripped, leaving "US TRUST")
}

# Role/descriptor phrases to remove (regex patterns)
# Order matters: more specific patterns should come first
ROLE_DESCRIPTOR_PATTERNS = [
    r'\s+FORMERLY\s+KNOWN\s+AS\s+[^,]*',  # "FORMERLY KNOWN AS ..."
    r'\s+FORMERLY\s+KNOWNAS\s+[^,]*',  # Typo variant "FORMERLY KNOWNAS ..."
    r'\s+ACTING\s+THROUGH\s+ITS\s+[^,]*',  # "ACTING THROUGH ITS ..."
    r'\s+A\s+DIVISION\s+OF\s+[^,]*',  # "A DIVISION OF ..."
    r'\s+A\s+NATIONAL\s+BANKING\s+ASSOCIATION\s*',  # "A NATIONAL BANKING ASSOCIATION" (specific, before "THE A")
    r'\s+COLLATERAL\s+AGENT\s+[^,]*',  # "COLLATERAL AGENT ..."
    r'\s+A\s+DELAWARE\s+PARTNERSHIP\s*',  # "A DELAWARE PARTNERSHIP"
    r'\s+THE\s+A\s+[^,]*',  # "THE A ..." patterns (general, after specific patterns)
    r'\s+NY\s+\d{5}\s*$',  # "NY 10010" patterns (before general zip code pattern)
    r'\s+\d{5}(?:-\d{4})?\s*$',  # Trailing zip codes like "10010"
    r'\s+&\s*$',  # Trailing "&"
]


def run_grouping(financial_df, non_financial_df, financial_components, non_financial_components,
                 financial_matches_df, non_financial_matches_df, base_dir=None, transaction_type='pledge'):
    """
    Ejecuta agrupación y asignación de IDs.
    
    Args:
        financial_df: DataFrame con nombres normalizados
        non_financial_df: DataFrame con nombres normalizados
        financial_components: Lista de componentes financieros (sets de índices)
        non_financial_components: Lista de componentes no financieros
        financial_matches_df: DataFrame con matches financieros
        non_financial_matches_df: DataFrame con matches no financieros
        base_dir: Directorio base del proyecto
        transaction_type: Tipo de transacción ('pledge' o 'release')
        
    Returns:
        tuple: (financial_mapping, non_financial_mapping, financial_review, non_financial_review)
    """
    if base_dir is None:
        base_dir = Path(__file__).parent.parent.parent
    
    final_results_dir = base_dir / "results" / "final"
    final_results_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    if transaction_type:
        print(f"FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs ({transaction_type.upper()})")
    else:
        print("FASE 5: AGRUPACIÓN Y ASIGNACIÓN DE IDs")
    print("=" * 80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Resetear índices
    financial_df = financial_df.reset_index(drop=True)
    non_financial_df = non_financial_df.reset_index(drop=True)
    financial_matches_df = financial_matches_df.reset_index(drop=True)
    non_financial_matches_df = non_financial_matches_df.reset_index(drop=True)
    
    # Procesar componentes
    print("1. Procesando componentes y asignando IDs...")
    print("   Financial entities:")
    financial_mapping, financial_review = process_components(
        financial_df, financial_components, financial_matches_df,
        'normalized_name', 'frequency', 'financial'
    )
    
    print("   Non-financial entities:")
    non_financial_mapping, non_financial_review = process_components(
        non_financial_df, non_financial_components, non_financial_matches_df,
        'normalized_name', 'frequency', 'non_financial'
    )
    
    print(f"\n   ✓ Componentes procesados:")
    print(f"     - Financial: {len(financial_components):,} componentes")
    print(f"     - Non-financial: {len(non_financial_components):,} componentes")
    print(f"\n   ✓ Casos para revisión identificados:")
    print(f"     - Financial: {len(financial_review):,} componentes")
    print(f"     - Non-financial: {len(non_financial_review):,} componentes")
    
    # Nota: No guardamos archivos intermedios aquí porque:
    # - *_entity_mapping.csv (sin _complete) es redundante
    # - *_review_cases.csv es redundante (Streamlit filtra por needs_review)
    # El archivo final *_entity_mapping_complete.csv se genera en complete_mapping.py
    print("\n2. Resultados procesados (archivos finales se generarán en complete_mapping.py)")
    print("   ℹ️  No se guardan archivos intermedios redundantes")
    print("   ℹ️  Streamlit puede filtrar casos para revisión dinámicamente")
    
    # Estadísticas
    print("\n3. Estadísticas finales:")
    print("\n   Financial entities:")
    print(f"     - Total registros: {len(financial_mapping):,}")
    print(f"     - Entidades únicas: {financial_mapping['entity_id'].nunique():,}")
    print(f"     - Componentes para revisión: {len(financial_review):,}")
    
    print("\n   Non-financial entities:")
    print(f"     - Total registros: {len(non_financial_mapping):,}")
    print(f"     - Entidades únicas: {non_financial_mapping['entity_id'].nunique():,}")
    print(f"     - Componentes para revisión: {len(non_financial_review):,}")
    
    print("\n" + "=" * 80)
    print("RESUMEN")
    print("=" * 80)
    print(f"✓ Fase 5 completada")
    print(f"✓ Financial entities: {financial_mapping['entity_id'].nunique():,} entidades únicas")
    print(f"✓ Non-financial entities: {non_financial_mapping['entity_id'].nunique():,} entidades únicas")
    print(f"✓ Resultados guardados en: {final_results_dir}")
    print("=" * 80)
    
    return financial_mapping, non_financial_mapping, financial_review, non_financial_review


def extract_root_name(name: str) -> str:
    """
    Extrae el nombre raíz (root name) de un nombre normalizado.
    
    Proceso:
    1. Verifica si el nombre coincide con instituciones excepcionales (protegidas)
    2. Elimina frases de roles/descriptores (FORMERLY KNOWN AS, ACTING THROUGH ITS, etc.)
    3. Elimina patrones multi-palabra (CAYMAN ISLANDS, TRUST CO, etc.)
    4. Elimina sufijos legales y tokens geográficos/sucursales
    
    Args:
        name: Nombre normalizado (ya en mayúsculas y sin puntuación extra)
        
    Returns:
        Nombre raíz simplificado (ej: 'CREDIT SUISSE' de 'CREDIT SUISSE AG CAYMAN ISLANDS BRANCH')
    """
    if not name or not name.strip():
        return name
    
    # Normalizar espacios y convertir a mayúsculas
    name_upper = name.upper().strip()
    
    # Paso 1: Verificar si el nombre contiene alguna institución excepcional
    # (antes de cualquier stripping)
    # Ordenar excepciones por longitud (más largas primero) para encontrar la mejor coincidencia
    sorted_exceptions = sorted(EXCEPTION_INSTITUTIONS, key=len, reverse=True)
    name_words = name_upper.split()
    
    for exception in sorted_exceptions:
        exception_upper = exception.upper().strip()
        exception_words = exception_upper.split()
        
        # Si el nombre coincide exactamente
        if name_upper == exception_upper:
            return exception_upper
        
        # Si el nombre comienza con la excepción (puede tener más palabras después)
        if name_upper.startswith(exception_upper + ' '):
            return exception_upper
        
        # Verificar si las primeras palabras del nombre coinciden exactamente con la excepción
        if len(name_words) >= len(exception_words):
            if name_words[:len(exception_words)] == exception_words:
                return exception_upper
    
    # Paso 2: Eliminar frases de roles/descriptores usando patrones regex
    for pattern in ROLE_DESCRIPTOR_PATTERNS:
        name_upper = re.sub(pattern, '', name_upper, flags=re.IGNORECASE)
    
    # Normalizar espacios después de eliminar patrones
    name_upper = ' '.join(name_upper.split())
    
    if not name_upper:
        return name
    
    # Verificar excepciones nuevamente después de eliminar roles/descriptores
    for exception in sorted_exceptions:
        exception_upper = exception.upper().strip()
        exception_words = exception_upper.split()
        name_words = name_upper.split()
        
        if name_upper == exception_upper or name_upper.startswith(exception_upper + ' '):
            return exception_upper
        
        # Verificar si las primeras palabras coinciden
        if len(name_words) >= len(exception_words) and name_words[:len(exception_words)] == exception_words:
            return exception_upper
    
    # Paso 3: Eliminar patrones multi-palabra (ej: "CAYMAN ISLANDS", "TRUST CO")
    for pattern in MULTI_WORD_PATTERNS:
        name_upper = re.sub(pattern, '', name_upper, flags=re.IGNORECASE)
    
    # Re-normalizar espacios después de eliminar patrones multi-palabra
    name_upper = ' '.join(name_upper.split())
    
    # Verificar excepciones nuevamente después de eliminar patrones multi-palabra
    for exception in sorted_exceptions:
        exception_upper = exception.upper().strip()
        exception_words = exception_upper.split()
        name_words = name_upper.split()
        
        if name_upper == exception_upper or name_upper.startswith(exception_upper + ' '):
            return exception_upper
        
        # Verificar si las primeras palabras coinciden
        if len(name_words) >= len(exception_words) and name_words[:len(exception_words)] == exception_words:
            return exception_upper
    
    # Paso 3.5: Eliminar patrones de ubicación "OF [LOCATION]" (después de verificar excepciones)
    # Esto maneja casos como "UNITED STATES TRUST COMPANY OF NEW YORK"
    for pattern in LOCATION_PATTERNS:
        name_upper = re.sub(pattern, '', name_upper, flags=re.IGNORECASE)
    
    # Re-normalizar espacios después de eliminar patrones de ubicación
    name_upper = ' '.join(name_upper.split())
    
    # Verificar excepciones nuevamente después de eliminar patrones de ubicación
    for exception in sorted_exceptions:
        exception_upper = exception.upper().strip()
        exception_words = exception_upper.split()
        name_words = name_upper.split()
        
        if name_upper == exception_upper or name_upper.startswith(exception_upper + ' '):
            return exception_upper
        
        # Verificar si las primeras palabras coinciden
        if len(name_words) >= len(exception_words) and name_words[:len(exception_words)] == exception_words:
            return exception_upper
    
    tokens = name_upper.split()
    
    # Filtrar tokens vacíos
    tokens = [t for t in tokens if t]
    
    if not tokens:
        return name
    
    # Paso 4: Eliminar tokens desde el final hacia el principio (donde suelen aparecer)
    # PRIMERO verificar si el nombre actual (antes de stripping) coincide con una excepción
    current_name = ' '.join(tokens)
    for exception in sorted_exceptions:
        exception_upper = exception.upper().strip()
        exception_words = exception_upper.split()
        current_words = current_name.split()
        
        if current_name == exception_upper or current_name.startswith(exception_upper + ' '):
            return exception_upper
        
        # Verificar si las primeras palabras coinciden
        if len(current_words) >= len(exception_words) and current_words[:len(exception_words)] == exception_words:
            return exception_upper
    
    # Intentar reconstruir el nombre eliminando tokens finales que están en nuestras listas
    # y verificar excepciones en cada paso
    for i in range(len(tokens), 0, -1):
        candidate = ' '.join(tokens[:i])
        candidate_words = candidate.split()
        
        for exception in sorted_exceptions:
            exception_upper = exception.upper().strip()
            exception_words = exception_upper.split()
            
            # Verificar coincidencia exacta
            if candidate == exception_upper:
                return exception_upper
            
            # Verificar si el candidato comienza con la excepción
            if candidate.startswith(exception_upper + ' ') or candidate.startswith(exception_upper):
                return exception_upper
            
            # Verificar si las primeras palabras del candidato coinciden con la excepción
            if len(candidate_words) >= len(exception_words) and candidate_words[:len(exception_words)] == exception_words:
                return exception_upper
        
        # Solo continuar eliminando si el token actual está en nuestras listas de stripping
        # PERO preservar los sufijos en KEEP_SUFFIXES
        if i > 0 and (tokens[i-1] in LEGAL_SUFFIXES or tokens[i-1] in BRANCH_GEO_TOKENS) and tokens[i-1] not in KEEP_SUFFIXES:
            continue
        else:
            # Si llegamos a un token que no está en las listas, no tiene sentido seguir
            break
    
    # Si no se encontró excepción, proceder con el stripping normal
    filtered_tokens = []
    i = len(tokens) - 1
    
    # Eliminar desde el final hasta encontrar tokens que no sean sufijos/geográficos
    while i >= 0:
        token = tokens[i]
        # Si es un sufijo legal o token geográfico/sucursal, saltarlo
        # PERO preservar los sufijos en KEEP_SUFFIXES
        if (token in LEGAL_SUFFIXES or token in BRANCH_GEO_TOKENS) and token not in KEEP_SUFFIXES:
            i -= 1
            continue
        # Si llegamos aquí, es un token que queremos mantener
        filtered_tokens.insert(0, token)
        i -= 1
    
    # También eliminar cualquier token restante que sea sufijo/geográfico en el medio
    # (por si acaso quedó alguno)
    # PERO preservar los sufijos en KEEP_SUFFIXES
    final_tokens = [t for t in filtered_tokens if (t not in LEGAL_SUFFIXES and t not in BRANCH_GEO_TOKENS) or t in KEEP_SUFFIXES]
    
    # Asegurar que al menos quede un token
    if not final_tokens:
        return name
    
    root_name = ' '.join(final_tokens).strip()
    
    # Si el root_name resultante está vacío o es muy corto, usar el nombre original
    if len(root_name) < 2:
        return name
    
    return root_name


def select_standard_name(df, component_indices, name_column='normalized_name', freq_column='frequency'):
    """
    Selecciona el nombre estándar para un componente usando nombres raíz (root names).
    
    Estrategia:
    1. Para componentes con múltiples elementos:
       - Extrae el nombre raíz de cada candidato (eliminando sufijos legales y tokens geográficos/sucursales)
       - Agrega frecuencias por nombre raíz (suma de todas las variantes que comparten el mismo root)
       - Selecciona el nombre raíz con mayor frecuencia total
       - En caso de empate, prefiere el root derivado del nombre subyacente más corto
    2. Para singletons: usa el nombre normalizado directamente
    """
    if len(component_indices) == 1:
        idx = component_indices[0]
        return idx, df.loc[idx, name_column]
    
    # Construir datos de cada candidato con su root_name
    component_data = []
    for idx in component_indices:
        name = df.loc[idx, name_column]
        freq = df.loc[idx, freq_column]
        length = len(name)
        root_name = extract_root_name(name)
        component_data.append({
            'idx': idx,
            'name': name,
            'freq': freq,
            'length': length,
            'root_name': root_name
        })
    
    # Agrupar por root_name y calcular estadísticas agregadas
    root_groups = {}
    for item in component_data:
        root = item['root_name']
        if root not in root_groups:
            root_groups[root] = {
                'root_name': root,
                'total_freq': 0,
                'best_length': float('inf'),
                'best_idx': None
            }
        root_groups[root]['total_freq'] += item['freq']
        if item['length'] < root_groups[root]['best_length']:
            root_groups[root]['best_length'] = item['length']
            root_groups[root]['best_idx'] = item['idx']
    
    # Seleccionar el mejor root_name:
    # 1. Mayor frecuencia total
    # 2. Si hay empate, menor longitud del nombre subyacente
    # 3. Si sigue habiendo empate, orden alfabético del root_name
    root_list = list(root_groups.values())
    root_list.sort(key=lambda x: (-x['total_freq'], x['best_length'], x['root_name']))
    
    best_root = root_list[0]
    best_idx = best_root['best_idx']
    standard_name = best_root['root_name']
    
    return best_idx, standard_name


def calculate_component_stats(df, component_indices, matches_df, name_column='normalized_name'):
    """Calcula estadísticas de un componente para identificar problemas."""
    if len(component_indices) == 1:
        return {
            'avg_similarity': None,
            'min_similarity': None,
            'max_similarity': None,
            'size': 1,
            'needs_review': False
        }
    
    # Obtener matches dentro de este componente
    component_matches = matches_df[
        (matches_df['idx1'].isin(component_indices)) & 
        (matches_df['idx2'].isin(component_indices))
    ]
    
    if len(component_matches) == 0:
        return {
            'avg_similarity': None,
            'min_similarity': None,
            'max_similarity': None,
            'size': len(component_indices),
            'needs_review': True
        }
    
    similarities = component_matches['similarity'].tolist()
    avg_sim = sum(similarities) / len(similarities)
    min_sim = min(similarities)
    max_sim = max(similarities)
    
    # Marcar para revisión si:
    # - Similitud promedio baja (< 90%)
    # - Similitud mínima muy baja (< 87%)
    # - Componente muy grande (> 20 nombres)
    needs_review = (
        avg_sim < 90 or 
        min_sim < 87 or 
        len(component_indices) > 20
    )
    
    return {
        'avg_similarity': avg_sim,
        'min_similarity': min_sim,
        'max_similarity': max_sim,
        'size': len(component_indices),
        'needs_review': needs_review
    }


def process_components(df, components, matches_df, name_column='normalized_name', 
                      freq_column='frequency', entity_type='financial'):
    """
    Procesa todos los componentes y asigna IDs y nombres estándar.
    
    Returns:
        tuple: (DataFrame con mapeo, DataFrame con casos para revisión)
    """
    mapping_data = []
    review_cases = []
    
    print(f"   Procesando {len(components):,} componentes...")
    
    for component_id, component_indices in enumerate(components):
        # Seleccionar nombre estándar
        std_idx, std_name = select_standard_name(df, list(component_indices), name_column, freq_column)
        
        # Calcular estadísticas
        stats = calculate_component_stats(df, list(component_indices), matches_df, name_column)
        
        # Si necesita revisión, agregar a casos problemáticos
        if stats['needs_review']:
            component_names = [df.loc[idx, name_column] for idx in component_indices]
            review_cases.append({
                'component_id': component_id,
                'size': stats['size'],
                'avg_similarity': stats['avg_similarity'],
                'min_similarity': stats['min_similarity'],
                'standard_name': std_name,
                'all_names': component_names
            })
        
        # Crear entrada de mapeo para cada nombre en el componente
        for idx in component_indices:
            original_name = df.loc[idx, 'original_name']
            normalized_name = df.loc[idx, name_column]
            freq = df.loc[idx, freq_column]
            
            mapping_data.append({
                'entity_id': f"{entity_type}_{component_id}",
                'original_name': original_name,
                'normalized_name': normalized_name,
                'standard_name': std_name,
                'frequency': freq,
                'component_size': stats['size'],
                'avg_similarity': stats['avg_similarity'],
                'min_similarity': stats['min_similarity'],
                'needs_review': stats['needs_review']
            })
    
    mapping_df = pd.DataFrame(mapping_data)
    review_df = pd.DataFrame(review_cases) if review_cases else pd.DataFrame()
    
    return mapping_df, review_df


if __name__ == "__main__":
    # Para ejecución independiente
    base_dir = Path(__file__).parent.parent.parent
    results_dir = base_dir / "results" / "intermediate"
    final_results_dir = base_dir / "results" / "final"
    
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
    
    run_grouping(financial_df, non_financial_df, financial_components, non_financial_components,
                 financial_matches_df, non_financial_matches_df, base_dir, transaction_type=None)

