# Changelog: Sistema de Base de Datos SQLite

## ✨ Nueva Funcionalidad: Base de Datos SQLite

### Problema Resuelto

Antes, cada vez que guardabas cambios en la aplicación web, se creaban múltiples archivos CSV:
- `financial_entity_mapping_edited_20241201_143022.csv`
- `financial_entity_mapping_edited_20241201_143545.csv`
- `financial_entity_mapping_edited_latest.csv`
- `financial_backup_20241201_143022.csv`
- etc.

Esto era poco práctico cuando hacías múltiples cambios.

### Solución Implementada

Ahora puedes usar una base de datos SQLite que:
- ✅ Guarda todos los cambios en un solo archivo: `database/entities.db`
- ✅ No crea múltiples archivos cada vez que guardas
- ✅ Es más rápida y eficiente
- ✅ Permite exportar a CSV cuando lo necesites

## 📦 Archivos Nuevos

1. **`database_manager.py`**: Gestor de base de datos SQLite
   - Clase `EntityDatabase` para manejar todas las operaciones
   - Importación desde CSV
   - Exportación a CSV
   - Sistema de backups
   - Historial de cambios

2. **`migrate_to_database.py`**: Script de migración
   - Migra automáticamente CSVs existentes a la base de datos
   - Verifica datos existentes
   - Opción de sobrescribir

3. **`README_DATABASE.md`**: Documentación completa del sistema

4. **`database/entities.db`**: Base de datos SQLite (se crea automáticamente)

## 🔄 Cambios en Archivos Existentes

### `web_app.py`

- ✅ Función `load_mapping_data()` actualizada para usar base de datos
- ✅ Función `save_changes()` actualizada para guardar en base de datos
- ✅ Toggle en sidebar para elegir entre base de datos o CSV
- ✅ Sección de gestión de base de datos en sidebar
- ✅ Exportación a CSV desde la interfaz
- ✅ Creación de backups desde la interfaz
- ✅ Estadísticas de la base de datos

### `README_WEB_APP.md`

- ✅ Documentación sobre uso de base de datos
- ✅ Instrucciones de migración

## 🚀 Cómo Usar

### Migración Inicial (Una vez)

```bash
python migrate_to_database.py
```

Esto migra todos tus CSV existentes a la base de datos.

### Uso Diario

1. Abre la aplicación web
2. Activa "Usar base de datos SQLite" en el sidebar
3. Carga datos
4. Haz cambios
5. Guarda - ahora guarda en la base de datos, no crea múltiples CSV

### Exportar a CSV (Cuando lo necesites)

Desde la aplicación web:
- Sidebar → "📥 Exportar a CSV"

O desde Python:
```python
from database_manager import EntityDatabase
from pathlib import Path

db = EntityDatabase(Path("database/entities.db"))
db.export_to_csv("financial", Path("export.csv"))
```

## 💡 Beneficios

1. **Menos archivos**: Un solo archivo `.db` vs múltiples CSV
2. **Más rápido**: SQLite es más eficiente que múltiples CSV
3. **Cambios directos**: Los cambios se guardan inmediatamente
4. **Historial**: Sistema de historial integrado
5. **Backups simples**: Un solo archivo para respaldar

## 🔄 Compatibilidad

- ✅ Compatible con el sistema anterior (modo CSV todavía funciona)
- ✅ Puedes alternar entre base de datos y CSV
- ✅ Los CSV originales no se modifican
- ✅ Puedes exportar desde base de datos a CSV cuando quieras

## 📊 Estructura de la Base de Datos

### Tabla: `entities`

- `id`: ID único
- `entity_id`: ID de la entidad (ej: financial_0)
- `original_name`: Nombre original
- `normalized_name`: Nombre normalizado
- `standard_name`: Nombre estándar
- `frequency`: Frecuencia
- `component_size`: Tamaño del componente
- `avg_similarity`: Similitud promedio
- `min_similarity`: Similitud mínima
- `needs_review`: Si necesita revisión
- `entity_type`: Tipo (financial/non_financial)
- `created_at`: Fecha de creación
- `updated_at`: Fecha de actualización

### Tabla: `change_history`

- `id`: ID único
- `change_type`: Tipo de cambio (move, split, merge, rename)
- `entity_id`: ID de entidad afectada
- `details`: Detalles del cambio (JSON)
- `created_at`: Timestamp

## 🛠️ Mantenimiento

### Ver estadísticas

En la aplicación web → Sidebar → "📊 Estadísticas de Base de Datos"

### Crear backup

En la aplicación web → Sidebar → "💾 Crear Backup"

O desde Python:
```python
db = EntityDatabase(Path("database/entities.db"))
backup_path = db.backup_database()
```

### Limpiar datos

```python
db = EntityDatabase(Path("database/entities.db"))
db.clear_all("financial")  # Solo financial
# o
db.clear_all()  # Todo
```

## 📝 Notas

- La base de datos SQLite es local, no requiere servidor
- El archivo `entities.db` se puede mover, copiar y respaldar fácilmente
- Si borras la base de datos, puedes re-migrar desde CSV
- Los CSV originales en `results/final/` nunca se modifican

## 🔮 Futuras Mejoras

- [ ] Interfaz para ver historial de cambios
- [ ] Revertir cambios desde historial
- [ ] Soporte para PostgreSQL (opcional)
- [ ] Sincronización entre múltiples usuarios

