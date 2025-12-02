# Resumen del Pipeline de Estandarización de Nombres

## Resumen Ejecutivo

Este pipeline estandariza nombres de entidades financieras y no financieras, agrupando variaciones del mismo nombre bajo un identificador único y un nombre estándar.

**Resultado final:**
- **Financial entities:** 8,459 nombres → 5,231 entidades únicas
- **Non-financial entities:** 21,453 nombres → 17,726 entidades únicas
- **Threshold de similitud:** 88% (ajustado desde 85% para reducir falsos positivos)

---

## Fase 1: Preparación y Exploración de Datos

### 1.1 Carga y Exploración Inicial (`01_exploration.py`)
- Carga ambas bases de datos (`financial_entity_freq.csv` y `Non_financial_entity_freq.csv`)
- Calcula estadísticas descriptivas: total de registros, nombres únicos, distribución de frecuencias
- Identifica patrones comunes: roles funcionales, sufijos legales, variaciones de formato
- **Output:** `results/exploration/basic_stats.txt`

### 1.2 Análisis de Variaciones (`02_variation_analysis.py`)
- Identifica variaciones de nombres conocidos (referencia: Figura 10 del paper)
- Crea diccionario de patrones comunes observados
- Documenta roles funcionales encontrados ("AS COLLATERAL AGENT", "AS ADMINISTRATIVE AGENT", etc.)
- **Output:** `results/exploration/variations_analysis.txt`

---

## Fase 2: Normalización

**Objetivo:** Limpiar y estandarizar nombres para facilitar comparación.

### 2.1 Limpieza Básica (`03_normalization_step2_1.py`)
- Convierte todo a mayúsculas
- Normaliza puntuación: `N.A.` → `NA`, `U.S.` → `US`
- Elimina espacios múltiples y trimea espacios
- **Output:** `results/intermediate/*_normalized_step2_1.csv`

### 2.2 Eliminación de Roles Funcionales (`04_normalization_step2_2.py`)
- Elimina roles funcionales usando expresiones regulares:
  - "AS COLLATERAL AGENT", "AS ADMINISTRATIVE AGENT", "AS TRUSTEE", etc.
  - Variaciones con/sin comas y diferentes formatos
- **Output:** `results/intermediate/*_normalized_step2_2.csv`

### 2.3 Normalización de Sufijos Legales (`05_normalization_step2_3.py`)
- Estandariza sufijos legales:
  - `NATIONAL ASSOCIATION` → `NA`
  - `CORPORATION` → `CORP`
  - `INCORPORATED` → `INC`
  - `COMPANY` → `CO`
  - `LIMITED` → `LTD`
- Maneja variaciones con/sin puntos y espacios
- **Output:** `results/intermediate/*_normalized_step2_3.csv`

### 2.4 Limpieza de Elementos Comunes (`06_normalization_step2_4.py`)
- Elimina "THE" al inicio/final
- Normaliza "AND" → "&"
- Normaliza abreviaciones comunes
- **Output:** `results/intermediate/*_normalized_step2_4.csv`

### 2.5 Normalización Final (`07_normalization_step2_5.py`)
- Elimina espacios múltiples finales
- Trim de espacios al inicio/final
- Crea versión normalizada final para matching
- **Output:** `results/intermediate/*_normalized_final.csv` → `*_normalized.csv`

---

## Fase 3: Blocking

**Objetivo:** Agrupar nombres por primera palabra para reducir comparaciones.

### 3.1 Extracción de Primera Palabra (`08_blocking_step3_1.py`)
- Extrae la primera palabra significativa del nombre normalizado
- Maneja casos especiales: si empieza con "THE", toma la segunda palabra
- Si es genérica ("BANK", "COMPANY"), considera segunda o tercera palabra
- **Output:** Agrega columna `blocking_key` a los datos normalizados

### 3.2 Creación de Bloques (`09_blocking_step3_2.py`)
- Agrupa todos los nombres por su `blocking_key`
- Crea índice de bloques para búsquedas eficientes
- Documenta tamaño de cada bloque
- **Output:** `results/intermediate/*_blocks.json`, `*_block_index.json`

### 3.3 Optimización de Bloques (`10_blocking_step3_3.py`)
- Identifica bloques grandes (>1000 elementos)
- Aplica sub-bloqueo por segunda palabra o longitud del nombre
- Mantiene bloques pequeños para eficiencia computacional
- **Output:** `results/intermediate/*_blocks_optimized.json` → `*_blocks.json`

---

## Fase 4: Fuzzy Matching

**Objetivo:** Encontrar nombres similares dentro de cada bloque.

### 4. Fuzzy Matching (`11_fuzzy_matching_step4.py`)
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

**Objetivo:** Asignar IDs únicos y seleccionar nombres estándar para cada grupo.

### 5. Agrupación y Asignación de IDs (`12_grouping_and_ids_step5.py`)
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

### 6.1 Validación Automática (`13_validation_step6_1.py`)
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

### Completar Mapeo Final (`15_complete_mapping.py`)
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

```bash
# Fase 1: Exploración
python scripts/01_exploration.py
python scripts/02_variation_analysis.py

# Fase 2: Normalización
python scripts/03_normalization_step2_1.py
python scripts/04_normalization_step2_2.py
python scripts/05_normalization_step2_3.py
python scripts/06_normalization_step2_4.py
python scripts/07_normalization_step2_5.py

# Fase 3: Blocking
python scripts/08_blocking_step3_1.py
python scripts/09_blocking_step3_2.py
python scripts/10_blocking_step3_3.py

# Fase 4: Matching
python scripts/11_fuzzy_matching_step4.py

# Fase 5: Agrupación
python scripts/12_grouping_and_ids_step5.py

# Fase 6: Validación
python scripts/13_validation_step6_1.py
python scripts/14_manual_review_step6_2.py

# Completar mapeo (incluir singletons)
python scripts/15_complete_mapping.py
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

