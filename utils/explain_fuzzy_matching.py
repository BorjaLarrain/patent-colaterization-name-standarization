"""
Script explicativo: Cómo funciona find_matches_in_block()
=========================================================
Este script demuestra paso a paso cómo funciona find_matches_in_block()
con un ejemplo concreto.
"""

import itertools

print("=" * 80)
print("EXPLICACIÓN: find_matches_in_block()")
print("=" * 80)
print()

# Ejemplo: Bloque con 4 nombres
print("EJEMPLO: Bloque con 4 nombres")
print("-" * 80)
print()

block_indices = [10, 20, 30, 40]
print(f"Bloque contiene índices: {block_indices}")
print(f"Total nombres en el bloque: {len(block_indices)}")
print()

# Mostrar qué hace itertools.combinations
print("itertools.combinations(block_indices, 2) genera TODOS los pares posibles:")
print("-" * 80)

pairs = list(itertools.combinations(block_indices, 2))
for i, (idx1, idx2) in enumerate(pairs, 1):
    print(f"  Par {i}: ({idx1}, {idx2})")

print()
print(f"Total de pares generados: {len(pairs)}")
print()

# Fórmula
print("FÓRMULA:")
print("-" * 80)
n = len(block_indices)
total_pairs = n * (n - 1) / 2
print(f"Con {n} nombres: {n} × ({n}-1) / 2 = {total_pairs} pares")
print()

# Comparación con otros enfoques
print("COMPARACIÓN CON OTROS ENFOQUES:")
print("-" * 80)
print()
print("❌ Si comparáramos solo pares consecutivos:")
print("   (10, 20), (20, 30), (30, 40)")
print("   Total: 3 pares")
print("   Problema: No detectaría que 10 y 40 son similares")
print()
print("✅ Con itertools.combinations (lo que hace el código):")
print("   (10, 20), (10, 30), (10, 40), (20, 30), (20, 40), (30, 40)")
print("   Total: 6 pares")
print("   Ventaja: Compara TODOS los pares posibles")
print()

# Ejemplo con más nombres
print("EJEMPLO CON MÁS NOMBRES:")
print("-" * 80)
for n in [5, 10, 50, 100]:
    pairs_count = n * (n - 1) / 2
    print(f"  {n:3d} nombres → {pairs_count:6.0f} pares a comparar")
print()

# Por qué es importante
print("¿POR QUÉ ES IMPORTANTE COMPARAR TODOS LOS PARES?")
print("-" * 80)
print()
print("Ejemplo real:")
print("  Nombre A: 'BANK OF AMERICA NA'")
print("  Nombre B: 'BANK OF AMERICA'")
print("  Nombre C: 'BANK OF AMERIC NA'")
print("  Nombre D: 'BANK OF AMERICA NATIONAL'")
print()
print("Si solo comparáramos consecutivos:")
print("  A ↔ B (95%) ✅")
print("  B ↔ C (97%) ✅")
print("  C ↔ D (92%) ✅")
print("  Problema: No sabemos si A ↔ D son similares")
print()
print("Comparando TODOS los pares:")
print("  A ↔ B (95%) ✅")
print("  A ↔ C (98%) ✅")
print("  A ↔ D (93%) ✅")
print("  B ↔ C (97%) ✅")
print("  B ↔ D (94%) ✅")
print("  C ↔ D (92%) ✅")
print("  Resultado: Todos están conectados → mismo componente")
print()

print("=" * 80)
print("CONCLUSIÓN")
print("=" * 80)
print("find_matches_in_block() genera n × (n-1) / 2 pares")
print("Esto permite encontrar TODAS las conexiones posibles")
print("y crear grupos completos de nombres relacionados")
print("=" * 80)

