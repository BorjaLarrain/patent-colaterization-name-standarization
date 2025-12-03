"""
Script de Migración: CSV a Base de Datos SQLite
===============================================
Migra los datos desde archivos CSV a la base de datos SQLite.
Útil para inicializar la base de datos o actualizar datos existentes.
"""

from pathlib import Path
from database_manager import EntityDatabase
import pandas as pd

BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR / "results" / "final"
DATABASE_DIR = BASE_DIR / "database"
DATABASE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATABASE_DIR / "entities.db"

def migrate_csv_to_database(entity_type='financial', overwrite=False):
    """
    Migra datos desde CSV a la base de datos
    
    Args:
        entity_type: 'financial' o 'non_financial'
        overwrite: Si True, sobrescribe datos existentes. Si False, solo agrega si no existen.
    """
    csv_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
    
    if not csv_path.exists():
        print(f"❌ No se encontró el archivo: {csv_path}")
        return False
    
    print(f"\n{'='*60}")
    print(f"Migrando {entity_type} entities a base de datos")
    print(f"{'='*60}\n")
    
    print(f"📁 Archivo CSV: {csv_path}")
    print(f"💾 Base de datos: {DB_PATH}\n")
    
    # Verificar tamaño del CSV
    df = pd.read_csv(csv_path)
    print(f"📊 Total de registros en CSV: {len(df):,}")
    print(f"📊 Entidades únicas: {df['entity_id'].nunique():,}\n")
    
    # Crear/abrir base de datos
    db = EntityDatabase(DB_PATH)
    
    # Usar entity_type directamente (sin transaction_type)
    
    # Verificar datos existentes
    stats_before = db.get_statistics(entity_type)
    if stats_before['total_names'] > 0:
        if overwrite:
            print(f"⚠️  Advertencia: Ya existen {stats_before['total_names']:,} registros de {entity_type}")
            print(f"   Se sobrescribirán con los datos del CSV.\n")
            clear_existing = True
        else:
            print(f"ℹ️  Ya existen {stats_before['total_names']:,} registros de {entity_type} en la base de datos")
            print(f"   Se mantendrán los datos existentes. Use overwrite=True para reemplazarlos.\n")
            return False
    else:
        clear_existing = False
    
    # Importar datos
    print("🔄 Importando datos a la base de datos...")
    try:
        db.import_from_csv(csv_path, entity_type, clear_existing=clear_existing)
        
        # Verificar datos importados
        stats_after = db.get_statistics(entity_type)
        
        print(f"\n✓ Migración completada exitosamente!")
        print(f"  - Registros en base de datos: {stats_after['total_names']:,}")
        print(f"  - Entidades únicas: {stats_after['unique_entities']:,}")
        print(f"  - Tamaño promedio de grupos: {stats_after['avg_group_size']:.2f}\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Función principal"""
    import sys
    
    print("="*60)
    print("MIGRACIÓN: CSV → Base de Datos SQLite")
    print("="*60)
    
    # Verificar archivos CSV para ambos tipos de entidad
    csvs_found = []
    for entity_type in ['financial', 'non_financial']:
        csv_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
        if csv_path.exists():
            csvs_found.append(entity_type)
    
    if not csvs_found:
        print("\n❌ No se encontraron archivos CSV para migrar.")
        print(f"   Buscando en: {RESULTS_DIR}")
        print("\n   Asegúrate de que existen los archivos:")
        print("   - financial_entity_mapping_complete.csv")
        print("   - non_financial_entity_mapping_complete.csv\n")
        return
    
    print(f"\n✓ Archivos CSV encontrados: {len(csvs_found)} archivos")
    for entity_type in csvs_found:
        print(f"   - {entity_type}")
    print()
    
    # Migrar cada tipo
    overwrite = '--overwrite' in sys.argv or '-o' in sys.argv
    
    if overwrite:
        print("⚠️  Modo sobrescribir activado (--overwrite)\n")
    
    success_count = 0
    
    for entity_type in csvs_found:
        if migrate_csv_to_database(entity_type, overwrite=overwrite):
            success_count += 1
    
    print("="*60)
    if success_count == len(csvs_found):
        print("✓ Migración completada para todos los tipos de entidad")
    else:
        print(f"⚠️  Migración completada para {success_count}/{len(csvs_found)} tipos de entidad")
    print("="*60)
    
    print(f"\n💾 Base de datos creada en: {DB_PATH}")
    print("\n📝 Próximos pasos:")
    print("   1. Ejecuta la aplicación web: streamlit run web_app.py")
    print("   2. Activa 'Usar base de datos SQLite' en el sidebar")
    print("   3. Carga los datos desde la base de datos\n")

if __name__ == "__main__":
    main()

