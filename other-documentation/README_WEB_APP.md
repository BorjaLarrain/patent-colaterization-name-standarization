# Aplicación Web Interactiva para Revisión de Entidades

## 📋 Descripción

Esta es una aplicación web interactiva desarrollada con Streamlit que te permite revisar, editar y corregir las agrupaciones de entidades generadas por el pipeline de estandarización de nombres.

## 🚀 Funcionalidades

- **Ver todas las entidades agrupadas**: Explora todos los grupos con sus nombres y estadísticas
  - Paginación para grandes volúmenes de datos
  - Filtros avanzados (tamaño, revisión, búsqueda rápida)
  - Ocultación de singletons por defecto para mejor rendimiento
- **Búsqueda avanzada**: Busca nombres por cualquier parte del texto (original, normalizado, o ID de entidad)
  - Resultados limitados y paginados
  - Búsqueda optimizada para grandes datasets
- **Editar grupos**:
  - ✏️ Mover nombres entre grupos
  - ✂️ Dividir grupos (crear nuevos grupos)
  - 🔗 Unir grupos
  - 📝 Cambiar nombres estándar
- **Estadísticas**: Visualiza estadísticas generales y distribución de grupos
- **Guardar cambios**: Guarda tus ediciones con respaldo automático
- **Optimizado para grandes volúmenes**: Maneja eficientemente miles de entidades

## 📦 Instalación

1. **Instala las dependencias**:
```bash
pip install -r requirements_web.txt
```

2. **Asegúrate de que los archivos de mapeo existen**:
   - `results/final/financial_entity_mapping_complete.csv`
   - `results/final/non_financial_entity_mapping_complete.csv`

3. **Migra a base de datos SQLite (recomendado)**:
```bash
python migrate_to_database.py
```

Esto creará una base de datos SQLite que evitará crear múltiples archivos CSV cada vez que guardes cambios. Ver `README_DATABASE.md` para más detalles.

## 🎯 Uso

### Iniciar la aplicación

```bash
streamlit run web_app.py
```

La aplicación se abrirá automáticamente en tu navegador en `http://localhost:8501`

### Flujo de trabajo

1. **Cargar datos**: 
   - Selecciona el tipo de entidad (financial o non_financial) en el panel lateral
   - Haz clic en "🔄 Cargar Datos"

2. **Explorar grupos**:
   - Ve a la pestaña "📋 Vista de Grupos"
   - Usa los filtros para encontrar grupos específicos
   - Haz clic en "✏️ Editar" en cualquier grupo para editarlo

3. **Buscar nombres**:
   - Ve a la pestaña "🔍 Búsqueda"
   - Escribe cualquier parte del nombre que busques
   - La búsqueda busca en nombres originales, normalizados, IDs y nombres estándar

4. **Editar grupos**:
   - Ve a la pestaña "✏️ Editar Grupo" (o haz clic en "Editar" desde cualquier vista)
   - Selecciona la acción que quieres realizar:
     - **Mover nombres**: Mueve nombres de un grupo a otro
     - **Dividir grupo**: Crea un nuevo grupo con nombres seleccionados
     - **Unir grupos**: Fusiona dos grupos en uno
     - **Cambiar nombre estándar**: Cambia el nombre estándar de un grupo

5. **Guardar cambios**:
   - Ve al panel lateral
   - Haz clic en "💾 Guardar Cambios"
   - Los cambios se guardan con timestamp y también como archivo "latest"
   - Se crea automáticamente un backup del archivo original

## 📁 Archivos Generados

### Modo Base de Datos SQLite (Recomendado)

Si usas la base de datos SQLite:
- **Base de datos**: `database/entities.db` - Todo se guarda aquí, sin crear múltiples archivos
- **Backups**: `database/entities_backup_*.db` - Backups de la base de datos
- **Exportaciones**: `results/manual_review/{entity_type}_exported_*.csv` - Cuando exportes a CSV

### Modo CSV (Legacy)

Si no usas la base de datos:
- `{entity_type}_entity_mapping_edited_{timestamp}.csv`: Archivo con timestamp
- `{entity_type}_entity_mapping_edited_latest.csv`: Archivo más reciente
- `{entity_type}_backup_{timestamp}.csv`: Backup del archivo original

**💡 Recomendación**: Usa la base de datos SQLite para evitar múltiples archivos. Actívala en el sidebar de la aplicación.

## 🔍 Ejemplos de Uso

### Ejemplo 1: Corregir un falso positivo

**Problema**: Dos entidades diferentes están agrupadas incorrectamente.

**Solución**:
1. Busca una de las entidades en la pestaña "🔍 Búsqueda"
2. Haz clic en "✏️ Editar" en el grupo
3. Selecciona "Dividir grupo (crear nuevo grupo)"
4. Selecciona los nombres que pertenecen a una entidad diferente
5. Haz clic en "✅ Crear nuevo grupo"
6. Guarda los cambios

### Ejemplo 2: Unir grupos que deberían estar juntos

**Problema**: Variaciones de la misma entidad están en grupos separados.

**Solución**:
1. Busca uno de los grupos
2. Haz clic en "✏️ Editar"
3. Selecciona "Unir con otro grupo"
4. Selecciona el grupo con el que quieres unir
5. Haz clic en "✅ Unir grupos"
6. Guarda los cambios

### Ejemplo 3: Mover un nombre mal clasificado

**Problema**: Un nombre está en el grupo incorrecto.

**Solución**:
1. Busca el nombre en "🔍 Búsqueda"
2. Haz clic en "✏️ Editar" en el grupo donde está
3. Selecciona "Mover nombres a otro grupo"
4. Selecciona el nombre y el grupo destino
5. Haz clic en "✅ Mover nombres"
6. Guarda los cambios

## 💡 Consejos

- **Usa los filtros**: Los filtros en "Vista de Grupos" te ayudan a encontrar rápidamente grupos problemáticos
- **Revisa grupos grandes primero**: Los grupos con muchos nombres (>20) son más propensos a tener errores
- **Guarda frecuentemente**: Aunque la aplicación mantiene un historial, es buena práctica guardar periódicamente
- **Revisa el historial**: El panel lateral muestra el historial de cambios recientes

## 🐛 Solución de Problemas

### ⚠️ Errores de WebSocket en la terminal

Si ves errores como `WebSocketClosedError` o `StreamClosedError` en la terminal, **no te preocupes**. Estos son errores comunes e inofensivos de Streamlit que no afectan la funcionalidad de la aplicación. Ocurren cuando el navegador cierra la conexión inesperadamente (por ejemplo, al refrescar rápidamente).

**Puedes ignorarlos completamente.** La aplicación seguirá funcionando normalmente.

Si quieres reducir estos errores:
- Evita refrescar la página muy rápido
- Cierra pestañas innecesarias del navegador
- Reinicia Streamlit si los errores son muy frecuentes

**Ver más detalles en:** `TROUBLESHOOTING.md`

### La aplicación no carga los datos

- Verifica que los archivos CSV existen en `results/final/`
- Verifica que los archivos tienen el formato correcto (columnas: entity_id, original_name, normalized_name, standard_name, frequency, etc.)

### Los cambios no se guardan

- Verifica que tienes permisos de escritura en `results/manual_review/`
- Revisa que el botón "💾 Guardar Cambios" esté habilitado (solo se habilita cuando hay cambios)

### La aplicación es lenta

**Optimizaciones automáticas incluidas:**
- ✅ **Paginación**: Los grupos se muestran por páginas (configurable en sidebar)
- ✅ **Filtro por defecto**: Los singletons (<2 nombres) están ocultos por defecto
- ✅ **Búsqueda optimizada**: Búsqueda rápida con límites de resultados
- ✅ **Cache inteligente**: Los datos se cachean para mejorar rendimiento

**Consejos para mejorar rendimiento:**
- Usa el filtro "Sin singletons" por defecto (ya está activado)
- Limita los resultados de búsqueda a 50-100
- Reduce los grupos por página si es muy lento (10-25)
- Usa la búsqueda rápida en lugar de ver todos los grupos
- Para datasets muy grandes (>10,000 entidades), considera trabajar con muestras

## 📝 Notas

- Los cambios **NO** modifican los archivos originales automáticamente
- Siempre se crea un backup antes de guardar
- Los archivos editados tienen un sufijo con timestamp para mantener un historial
- El archivo `*_latest.csv` siempre contiene la versión más reciente

## 🔄 Integración con el Pipeline

Después de revisar y editar los grupos:

1. Revisa los archivos editados en `results/manual_review/`
2. Si estás satisfecho, puedes copiar el archivo `*_latest.csv` sobre el original si lo deseas
3. O usa los archivos editados directamente en tu análisis posterior

## 📞 Soporte

Si encuentras problemas o tienes sugerencias, revisa:
- Los logs de Streamlit en la terminal
- Los archivos generados en `results/manual_review/`

