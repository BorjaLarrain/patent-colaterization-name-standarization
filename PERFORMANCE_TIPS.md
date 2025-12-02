# Consejos de Rendimiento - Aplicación Web

## Para Datasets Grandes (>5,000 entidades)

La aplicación está optimizada para manejar grandes volúmenes de datos. Aquí tienes consejos para maximizar el rendimiento:

### ✅ Optimizaciones Automáticas

1. **Paginación Automática**
   - Los grupos se muestran por páginas (configurable en sidebar)
   - Por defecto: 25 grupos por página
   - Puedes cambiar a 10, 50, 100 o 250 según tu necesidad

2. **Filtro por Defecto: Sin Singletons**
   - Los grupos con solo 1 nombre están ocultos por defecto
   - Esto reduce significativamente la carga inicial
   - Puedes activarlos seleccionando "Todos" en el filtro de tamaño

3. **Cache Inteligente**
   - Los datos agrupados se cachean automáticamente
   - Las búsquedas son optimizadas
   - El cache se limpia solo cuando guardas cambios

### 🎯 Mejores Prácticas

#### Para Ver Grupos:

1. **Usa Filtros Específicos**
   - Empieza con "Sin singletons" (por defecto)
   - Si buscas algo específico, usa "Búsqueda rápida"
   - Filtra por tamaño según lo que necesites

2. **Ajusta Grupos por Página**
   - Para navegación rápida: 10-25 grupos
   - Para revisar muchos: 50-100 grupos
   - Para exportar/ver todo: 100-250 grupos

3. **Usa la Búsqueda en lugar de Scroll**
   - La búsqueda es más rápida que cargar todos los grupos
   - Busca por ID de entidad o nombre estándar

#### Para Buscar:

1. **Límite de Resultados**
   - Empieza con 50-100 resultados
   - Aumenta solo si necesitas ver más

2. **Búsqueda Específica**
   - Usa palabras clave específicas
   - Busca por ID de entidad si lo conoces
   - Al menos 3 caracteres para optimizar

#### Para Editar:

1. **Trabaja con un Grupo a la Vez**
   - No abras múltiples grupos simultáneamente
   - Cierra los expanders cuando termines

2. **Guarda Frecuentemente**
   - Guarda después de grupos de cambios
   - Esto limpia el cache y mejora rendimiento

### ⚡ Optimizaciones Adicionales

Si la aplicación sigue siendo lenta:

1. **Reduce el Tamaño del Dataset**
   ```python
   # Crea un CSV con una muestra para pruebas
   import pandas as pd
   df = pd.read_csv('results/final/financial_entity_mapping_complete.csv')
   # Toma solo grupos con múltiples nombres
   sample = df[df['component_size'] > 1]
   sample.to_csv('results/manual_review/financial_sample_for_review.csv', index=False)
   ```

2. **Cierra Otras Pestañas del Navegador**
   - Reduce el uso de memoria del navegador
   - Mejora la velocidad general

3. **Usa un Navegador Más Eficiente**
   - Chrome o Edge suelen ser más rápidos
   - Cierra extensiones innecesarias

4. **Aumenta la Memoria de Python**
   ```bash
   # Si usas muchas entidades, considera aumentar memoria
   export STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
   streamlit run web_app.py
   ```

### 📊 Rendimiento Esperado

| Entidades | Tiempo de Carga | Grupos por Página Recomendado |
|-----------|----------------|-------------------------------|
| < 1,000   | < 2 segundos   | 50-100                        |
| 1,000-5,000 | 2-5 segundos | 25-50                         |
| 5,000-10,000 | 5-10 segundos | 10-25                         |
| > 10,000  | 10-20 segundos | 10 (usa filtros agresivos)    |

### 🔍 Uso de Filtros para Rendimiento

**Escenario 1: Revisar solo grupos problemáticos**
- Filtro: "Solo grupos marcados para revisión" ✓
- Filtro de tamaño: "Grandes (>20)" o "Medianos (5-20)"
- Resultado: Solo cargas grupos que necesitas revisar

**Escenario 2: Encontrar un grupo específico**
- Usa "Búsqueda rápida" en lugar de ver todos
- Escribe parte del nombre o ID
- Resultado: Encuentras rápido sin cargar todo

**Escenario 3: Revisar singletons**
- Cambia filtro a "Todos" o "Pequeños (<5)"
- Ajusta grupos por página a 100-250
- Resultado: Puedes revisar muchos singletons a la vez

### 💡 Tips Adicionales

1. **Primera Carga puede ser más lenta**
   - La primera vez que cargas datos, Streamlit los procesa
   - Cargas subsecuentes son más rápidas (cache)

2. **Si se congela**
   - Espera unos segundos (puede estar procesando)
   - Si no responde, recarga la página
   - Si persiste, reinicia Streamlit

3. **Monitorea el Rendimiento**
   - Revisa la terminal para ver tiempos de procesamiento
   - Si algo tarda mucho, usa filtros más agresivos

