# Sistema de Base de Datos SQLite

## 📋 Descripción

El sistema ahora usa una base de datos SQLite para manejar los cambios de manera más eficiente, evitando crear múltiples archivos CSV cada vez que guardas cambios.

## ✨ Ventajas

- ✅ **Un solo archivo**: Todo se guarda en `database/entities.db`
- ✅ **No más múltiples CSV**: Ya no se generan archivos con timestamps
- ✅ **Cambios instantáneos**: Los cambios se guardan directamente en la base de datos
- ✅ **Más rápido**: SQLite es más eficiente que múltiples archivos CSV
- ✅ **Exportación fácil**: Puedes exportar a CSV cuando quieras

## 🚀 Uso Rápido

### 1. Migrar datos existentes a la base de datos

Si ya tienes archivos CSV y quieres usar la base de datos:

```bash
python migrate_to_database.py
```

Esto importará automáticamente todos los CSVs encontrados a la base de datos.

### 2. Usar en la aplicación web

1. Ejecuta la aplicación:
```bash
streamlit run web_app.py
```

2. En el sidebar, **activa** "Usar base de datos SQLite"

3. Haz clic en "🔄 Cargar Datos"

4. La aplicación cargará automáticamente desde la base de datos (o migrará desde CSV si es la primera vez)

### 3. Trabajar con los datos

- Haz tus cambios normalmente
- Guarda con "💾 Guardar Cambios" - ahora guarda en la base de datos, no crea múltiples CSVs
- Exporta a CSV cuando quieras desde el sidebar

## 📁 Estructura de Archivos

```
project/
├── database/
│   └── entities.db          # Base de datos SQLite (todo en un archivo)
├── results/
│   └── final/
│       ├── financial_entity_mapping_complete.csv     # Archivo original (no se modifica)
│       └── non_financial_entity_mapping_complete.csv # Archivo original (no se modifica)
└── ...
```

## 🔧 Comandos Útiles

### Migrar datos

```bash
# Migrar todos los CSV encontrados
python migrate_to_database.py

# Sobrescribir datos existentes en la base de datos
python migrate_to_database.py --overwrite
```

### Exportar desde base de datos a CSV

En la aplicación web:
1. Ve al sidebar
2. En "🗄️ Gestión de Base de Datos"
3. Haz clic en "📥 Exportar a CSV"

O desde Python:
```python
from database_manager import EntityDatabase
from pathlib import Path

db = EntityDatabase(Path("database/entities.db"))
db.export_to_csv("financial", Path("results/manual_review/financial_exported.csv"))
```

## 🔄 Flujo de Trabajo Recomendado

### Primera vez:

1. **Migrar datos**:
   ```bash
   python migrate_to_database.py
   ```

2. **Usar la aplicación web**:
   - Activa "Usar base de datos SQLite"
   - Carga datos desde la base de datos
   - Haz tus cambios
   - Guarda (se guarda en la base de datos)

### Trabajo diario:

1. Abre la aplicación web
2. Carga datos (automático desde base de datos)
3. Haz cambios
4. Guarda cuando quieras (no se crean múltiples archivos)

### Exportar cuando necesites:

1. En el sidebar → "📥 Exportar a CSV"
2. O usa el script de migración para exportar

## 📊 Ventajas vs CSV

| Aspecto | CSV Múltiples | Base de Datos SQLite |
|---------|--------------|---------------------|
| Archivos creados | Múltiples con timestamps | Un solo archivo `.db` |
| Velocidad | Lenta con muchos archivos | Rápida |
| Cambios | Crea nuevo archivo cada vez | Actualiza en el mismo archivo |
| Historial | Múltiples archivos | Tabla de historial integrada |
| Búsqueda | Lenta | Rápida (índices) |
| Backup | Múltiples archivos | Un solo archivo |

## 🔐 Backups

La base de datos permite crear backups fácilmente:

### Desde la aplicación web:
- Sidebar → "💾 Crear Backup"
- Se crea un archivo `entities_backup_TIMESTAMP.db`

### Desde Python:
```python
from database_manager import EntityDatabase
from pathlib import Path

db = EntityDatabase(Path("database/entities.db"))
backup_path = db.backup_database()
print(f"Backup creado en: {backup_path}")
```

## 🛠️ Mantenimiento

### Ver estadísticas

En la aplicación web, en el sidebar hay una sección "📊 Estadísticas de Base de Datos" que muestra:
- Total de nombres
- Entidades únicas
- Tamaño promedio de grupos
- Tamaño del archivo de base de datos

### Limpiar datos

Si necesitas borrar datos de la base de datos:

```python
from database_manager import EntityDatabase
from pathlib import Path

db = EntityDatabase(Path("database/entities.db"))

# Borrar solo un tipo
db.clear_all("financial")

# Borrar todo
db.clear_all()
```

## ⚙️ Configuración

La base de datos se crea automáticamente en:
- **Ubicación**: `database/entities.db`
- **Formato**: SQLite 3
- **Índices**: Automáticos para mejor rendimiento

## 🔍 Historial de Cambios

La base de datos incluye una tabla de historial que registra:
- Tipo de cambio (move, split, merge, rename)
- Entity ID afectado
- Detalles del cambio
- Timestamp

Puedes ver el historial en la aplicación web (próximamente) o directamente:

```python
db = EntityDatabase(Path("database/entities.db"))
history = db.get_change_history(limit=50)
print(history)
```

## ❓ FAQ

### ¿Puedo seguir usando CSV?

Sí, puedes desactivar "Usar base de datos SQLite" en el sidebar y volver al modo CSV.

### ¿Los archivos CSV originales se modifican?

No, los archivos CSV en `results/final/` no se modifican. Solo se leen para importar a la base de datos.

### ¿Qué pasa si borro la base de datos?

Puedes volver a migrar desde los CSV originales ejecutando `migrate_to_database.py`.

### ¿Puedo usar PostgreSQL en lugar de SQLite?

El código está diseñado para SQLite por simplicidad. Si necesitas PostgreSQL, puedes modificar `database_manager.py` para usar `psycopg2` en lugar de `sqlite3`.

## 📝 Notas

- La base de datos SQLite es un archivo local, no requiere servidor
- Es compatible con todos los sistemas operativos
- El archivo `.db` se puede mover, copiar y respaldar fácilmente
- Si el archivo se corrompe, puedes restaurar desde backup o re-migrar desde CSV

