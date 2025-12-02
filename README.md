# Resumen del Pipeline de Estandarización de Nombres

## Resumen Ejecutivo

Este pipeline estandariza nombres de entidades financieras y no financieras, agrupando variaciones del mismo nombre bajo un identificador único y un nombre estándar.

**Resultado final:**
- **Financial entities:** 8,459 nombres → 5,231 entidades únicas
- **Non-financial entities:** 21,453 nombres → 17,726 entidades únicas
- **Threshold de similitud:** 88% (ajustado desde 85% para reducir falsos positivos)

---

## Estructura del Proyecto (Reorganizada)

El pipeline ha sido reorganizado en módulos consolidados para simplificar la ejecución y mantenimiento:

```
scripts/
├── pipeline.py                    # Script principal - ejecuta todo el pipeline
├── modules/
│   ├── exploration.py             # Fase 1: Exploración de datos
│   ├── normalization.py           # Fase 2: Normalización (consolida 5 scripts)
│   ├── blocking.py                # Fase 3: Blocking (consolida 3 scripts)
│   ├── matching.py                # Fase 4: Fuzzy matching
│   ├── grouping.py                # Fase 5: Agrupación y asignación de IDs
│   ├── validation.py             # Fase 6.1: Validación automática
│   └── complete_mapping.py       # Completar mapeo final
└── archive/                       # Scripts antiguos (referencia)
    └── 01-15_*.py
```

### Ejecución Simplificada

**Ejecutar todo el pipeline:**
```bash
python scripts/pipeline.py
```

**Ejecutar una fase específica:**
```bash
python scripts/pipeline.py --phase exploration   # Solo exploración
python scripts/pipeline.py --phase normalization # Solo normalización
python scripts/pipeline.py --phase blocking      # Solo blocking
python scripts/pipeline.py --phase matching      # Solo matching
python scripts/pipeline.py --phase grouping      # Solo agrupación
python scripts/pipeline.py --phase validation    # Solo validación
python scripts/pipeline.py --phase complete      # Solo completar mapeo
```

**Ejecutar módulos individuales:**
```bash
python -m scripts.modules.exploration
python -m scripts.modules.normalization
# etc.
```

### Archivos Intermedios

Los archivos intermedios han sido reducidos. Solo se guardan los esenciales:

**Mantenidos en `results/intermediate/`:**
- `*_normalized.csv` - Versión final normalizada (esencial)
- `*_blocks.json` - Bloques optimizados (esencial para matching)
- `*_components.json` - Componentes conectados (útil para debugging)
- `*_matches.csv` - Matches encontrados (útil para análisis)

**Eliminados (procesados en memoria):**
- `*_normalized_step2_*.csv` - Archivos intermedios de normalización
- `*_with_blocking_keys.csv` - Puede regenerarse si es necesario
- `*_blocks_optimized_summary.csv` - Resúmenes redundantes

---

## Fase 1: Preparación y Exploración de Datos

**Módulo:** `scripts/modules/exploration.py` (consolida scripts 01-02)

### 1.1 Carga y Exploración Inicial
- Carga ambas bases de datos (`financial_entity_freq.csv` y `Non_financial_entity_freq.csv`)
- Calcula estadísticas descriptivas: total de registros, nombres únicos, distribución de frecuencias
- Identifica patrones comunes: roles funcionales, sufijos legales, variaciones de formato
- **Output:** `results/exploration/basic_stats.txt`

### 1.2 Análisis de Variaciones
- Identifica variaciones de nombres conocidos (referencia: Figura 10 del paper)
- Crea diccionario de patrones comunes observados
- Documenta roles funcionales encontrados ("AS COLLATERAL AGENT", "AS ADMINISTRATIVE AGENT", etc.)
- **Output:** `results/exploration/variations_analysis.txt`

---

## Fase 2: Normalización

**Módulo:** `scripts/modules/normalization.py` (consolida scripts 03-07)

**Objetivo:** Limpiar y estandarizar nombres para facilitar comparación.

Todos los pasos de normalización se ejecutan en secuencia en memoria, guardando solo el resultado final:

### 2.1 Limpieza Básica
- Convierte todo a mayúsculas
- Normaliza puntuación: `N.A.` → `NA`, `U.S.` → `US`
- Elimina espacios múltiples y trimea espacios

### 2.2 Eliminación de Roles Funcionales
- Elimina roles funcionales usando expresiones regulares:
  - "AS COLLATERAL AGENT", "AS ADMINISTRATIVE AGENT", "AS TRUSTEE", etc.
  - Variaciones con/sin comas y diferentes formatos

### 2.3 Normalización de Sufijos Legales
- Estandariza sufijos legales:
  - `NATIONAL ASSOCIATION` → `NA`
  - `CORPORATION` → `CORP`
  - `INCORPORATED` → `INC`
  - `COMPANY` → `CO`
  - `LIMITED` → `LTD`
- Maneja variaciones con/sin puntos y espacios

### 2.4 Limpieza de Elementos Comunes
- Elimina "THE" al inicio/final
- Normaliza "AND" → "&"
- Normaliza abreviaciones comunes

### 2.5 Normalización Final
- Elimina espacios múltiples finales
- Trim de espacios al inicio/final
- Crea versión normalizada final para matching

**Output:** `results/intermediate/*_normalized.csv` (solo resultado final)

---

## Fase 3: Blocking

**Módulo:** `scripts/modules/blocking.py` (consolida scripts 08-10)

**Objetivo:** Agrupar nombres por primera palabra para reducir comparaciones.

Todos los pasos de blocking se ejecutan en secuencia, guardando solo los bloques optimizados finales:

### 3.1 Extracción de Primera Palabra
- Extrae la primera palabra significativa del nombre normalizado
- Maneja casos especiales: si empieza con "THE", toma la segunda palabra
- Si es genérica ("BANK", "COMPANY"), considera segunda o tercera palabra

### 3.2 Creación de Bloques
- Agrupa todos los nombres por su `blocking_key`
- Crea estructura de bloques para búsquedas eficientes

### 3.3 Optimización de Bloques
- Identifica bloques grandes (>100 elementos)
- Aplica sub-bloqueo por segunda palabra o longitud del nombre
- Mantiene bloques pequeños para eficiencia computacional

**Output:** `results/intermediate/*_blocks.json` (solo bloques optimizados finales)

---

## Fase 4: Fuzzy Matching

**Módulo:** `scripts/modules/matching.py`

**Objetivo:** Encontrar nombres similares dentro de cada bloque.

### 4. Fuzzy Matching
- **Método:** WRatio de `rapidfuzz` (combinación inteligente de métodos character-based y token-based)
- **Threshold:** 88% (ajustado desde 85% para reducir falsos positivos)
- **Proceso:**
  - Para cada bloque, compara todos los pares de nombres usando `itertools.combinations`
  - Calcula WRatio para cada par
  - Si WRatio >= 88%, considera como match
- **Construcción de grafos:** Nombres que matchean están conectados
- **Componentes conectados:** Encuentra grupos de nombres relacionados usando DFS
- **Output:**
  - `results/intermediate/*_matches.csv` - Todos los matches encontrados
  - `results/intermediate/*_components.json` - Grupos/clusters de nombres relacionados

**Resultados:**
- Financial: 17,836 matches, 897 componentes
- Non-financial: 6,431 matches, 2,382 componentes

---

## Fase 5: Agrupación y Asignación de IDs

**Módulo:** `scripts/modules/grouping.py`

**Objetivo:** Asignar IDs únicos y seleccionar nombres estándar para cada grupo.

### 5. Agrupación y Asignación de IDs
- **Asignación de IDs:** Cada componente recibe un `entity_id` único (`financial_0`, `financial_1`, etc.)
- **Selección de nombre estándar:**
  - Ordena nombres por frecuencia (descendente)
  - Si hay empate, prefiere el más corto
  - El nombre seleccionado se convierte en `standard_name` para todo el componente
- **Identificación de casos problemáticos:**
  - Componentes con similitud promedio < 90%
  - Componentes con similitud mínima < 87%
  - Componentes grandes (>20 nombres)
- **Output:**
  - `results/final/*_entity_mapping.csv` - Mapeo completo (solo nombres con matches)
  - `results/final/*_review_cases.csv` - Casos para revisión manual

**Resultados:**
- Financial: 897 entidades únicas, 40 componentes para revisión
- Non-financial: 2,382 entidades únicas, 120 componentes para revisión

---

## Fase 6: Validación y Refinamiento

**Módulo:** `scripts/modules/validation.py`

### 6.1 Validación Automática
- Revisa grupos con baja similitud promedio (< 90%)
- Identifica posibles falsos positivos
- Verifica que nombres conocidos (Figura 10) estén correctamente agrupados
- Encuentra nombres de alta frecuencia sin matches
- **Output:**
  - `results/validation/*_validation_report.csv` - Reporte completo
  - `results/validation/*_problematic_components.csv` - Solo problemáticos
  - `results/validation/*_high_frequency_names.csv` - Nombres de alta frecuencia

**Resultados:**
- Financial: 95.5% componentes válidos (857/897)
- Non-financial: 95.0% componentes válidos (2,262/2,382)

### 6.2 Herramientas para Revisión Manual (`14_manual_review_step6_2.py`)
- **HTML interactivo:** `results/manual_review/*_review.html`
  - Visualización de componentes problemáticos
  - Botones para marcar decisiones (Válido/Dividir/Inválido)
- **CSV para Excel:** `results/manual_review/*_review_excel.csv`
  - Formato estructurado para revisión detallada
  - Columnas: `decision`, `notes` para documentar decisiones
- **Muestra aleatoria:** `results/manual_review/*_sample_review.csv`
  - 50 registros aleatorios para validación general de calidad

**Guía:** Ver `GUIA_REVISION_MANUAL.md` para instrucciones detalladas.

---

## Script Adicional: Completar Mapeo

**Módulo:** `scripts/modules/complete_mapping.py`

### Completar Mapeo Final
- **Problema identificado:** El mapeo inicial solo incluía nombres con matches (4,125 de 8,459)
- **Solución:** Agrega nombres sin matches (singletons) como entidades únicas
- **Output:** `results/final/*_entity_mapping_complete.csv`
  - **Incluye TODOS los nombres** del archivo original
  - Singletons reciben `entity_id` único y `standard_name = normalized_name`

**Resultados finales:**
- Financial: 8,459 nombres → 5,231 entidades únicas (897 agrupadas + 4,334 singletons)
- Non-financial: 21,453 nombres → 17,726 entidades únicas (2,382 agrupadas + 15,344 singletons)

---

## Estructura de Archivos Finales

### Archivo de Mapeo Completo (`*_entity_mapping_complete.csv`)

**Columnas:**
- `entity_id`: ID único de la entidad (ej: `financial_0`)
- `original_name`: Nombre original del CSV
- `normalized_name`: Versión normalizada individual
- `standard_name`: **Nombre definitivo** (igual para todo el componente)
- `frequency`: Frecuencia original
- `component_size`: Tamaño del grupo (1 si es singleton)
- `avg_similarity`: Similitud promedio en el grupo (si aplica)
- `min_similarity`: Similitud mínima en el grupo (si aplica)
- `needs_review`: Si necesita revisión manual

**Uso del archivo final:**
- **`standard_name`** es el nombre definitivo que debes usar
- Todos los nombres del mismo `entity_id` comparten el mismo `standard_name`
- Los singletons (component_size = 1) tienen `standard_name = normalized_name`

---

## Orden de Ejecución

### Método Recomendado (Nuevo)

```bash
# Ejecutar todo el pipeline de una vez
python scripts/pipeline.py

# O ejecutar fases individuales
python scripts/pipeline.py --phase exploration
python scripts/pipeline.py --phase normalization
python scripts/pipeline.py --phase blocking
python scripts/pipeline.py --phase matching
python scripts/pipeline.py --phase grouping
python scripts/pipeline.py --phase validation
python scripts/pipeline.py --phase complete
```

### Método Antiguo (Scripts Individuales)

Los scripts antiguos están archivados en `scripts/archive/` para referencia. Si necesitas ejecutarlos individualmente:

```bash
# Fase 1: Exploración
python scripts/archive/01_exploration.py
python scripts/archive/02_variation_analysis.py

# Fase 2: Normalización (ahora consolidado en modules/normalization.py)
python scripts/archive/03_normalization_step2_1.py
# ... etc
```

---

## Parámetros Clave

- **Threshold de similitud:** 88% (WRatio)
- **Tamaño mínimo de bloque para matching:** 2 nombres
- **Umbrales de validación:**
  - Similitud promedio baja: < 90%
  - Similitud mínima sospechosa: < 87%
  - Grupo grande: > 20 nombres
  - Alta frecuencia: >= 1,000

---

## Archivos de Salida Principales

### Resultados Finales
- `results/final/*_entity_mapping_complete.csv` - **Mapeo completo con todos los nombres**
- `results/final/*_review_cases.csv` - Casos para revisión manual

### Validación
- `results/validation/*_validation_report.csv` - Reporte de validación
- `results/validation/*_problematic_components.csv` - Componentes problemáticos

### Revisión Manual
- `results/manual_review/*_review.html` - HTML interactivo
- `results/manual_review/*_review_excel.csv` - CSV para Excel
- `results/manual_review/*_sample_review.csv` - Muestra aleatoria

---

## Notas Importantes

1. **`standard_name` es el nombre definitivo** que debes usar en análisis posteriores
2. **El archivo `_complete.csv` incluye TODOS los nombres**, no solo los que tienen matches
3. **Los singletons** (nombres sin matches) tienen `component_size = 1` y `standard_name = normalized_name`
4. **La revisión manual** es importante para identificar y corregir falsos positivos
5. **El threshold de 88%** fue ajustado desde 85% para reducir falsos positivos (reducción del 93% en casos problemáticos)

