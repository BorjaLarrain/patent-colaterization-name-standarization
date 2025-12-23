"""
Gestor de Base de Datos para Revisión de Entidades por Transacción
===================================================================
Sistema de base de datos SQLite para manejar cambios de manera eficiente
para entidades separadas por tipo de transacción (Security/Release).
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import re
from typing import Optional, List, Tuple

# Entity types supported
ENTITY_TYPES = ['financial_security', 'financial_release', 'non_financial_security', 'non_financial_release']


class EntityDatabaseTransaction:
    """Gestor de base de datos para entidades por transacción"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Inicializa la base de datos y crea tablas si no existen"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabla principal de entidades
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                original_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                standard_name TEXT NOT NULL,
                frequency INTEGER NOT NULL,
                component_size INTEGER NOT NULL,
                avg_similarity REAL,
                min_similarity REAL,
                needs_review BOOLEAN DEFAULT 0,
                entity_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(entity_id, original_name, entity_type)
            )
        """)
        
        # Índices para mejor rendimiento
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_id ON entities(entity_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_standard_name ON entities(standard_name)
        """)
        
        # Tabla de historial de cambios (opcional, para auditoría)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS change_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                change_type TEXT NOT NULL,
                entity_id TEXT,
                details TEXT,
                changed_by TEXT DEFAULT 'user',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def import_from_csv(self, csv_path: Path, entity_type: str, clear_existing: bool = False):
        """
        Importa datos desde un archivo CSV
        
        Args:
            csv_path: Ruta al archivo CSV
            entity_type: Tipo de entidad ('financial_security', 'financial_release', etc.)
            clear_existing: Si True, borra datos existentes del mismo tipo antes de importar
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be one of {ENTITY_TYPES}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {csv_path}")
        
        df = pd.read_csv(csv_path)
        
        # Verificar columnas necesarias
        required_cols = ['entity_id', 'original_name', 'normalized_name', 'standard_name', 'frequency']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Faltan columnas requeridas: {missing_cols}")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            if clear_existing:
                # Borrar datos existentes del mismo tipo
                cursor = conn.cursor()
                cursor.execute("DELETE FROM entities WHERE entity_type = ?", (entity_type,))
                conn.commit()
            
            # Limpiar valores nulos en columnas requeridas
            df['original_name'] = df['original_name'].fillna('').astype(str)
            df['normalized_name'] = df['normalized_name'].fillna('').astype(str)
            df['standard_name'] = df['standard_name'].fillna('').astype(str)
            
            # Si normalized_name está vacío, usar original_name en mayúsculas
            mask_empty_normalized = (df['normalized_name'].str.strip() == '')
            df.loc[mask_empty_normalized, 'normalized_name'] = df.loc[mask_empty_normalized, 'original_name'].str.upper().str.strip()
            
            # Si standard_name está vacío, usar normalized_name
            mask_empty_standard = (df['standard_name'].str.strip() == '')
            df.loc[mask_empty_standard, 'standard_name'] = df.loc[mask_empty_standard, 'normalized_name']
            
            # Eliminar filas con valores vacíos en columnas críticas
            df = df[df['original_name'].str.strip() != '']
            df = df[df['normalized_name'].str.strip() != '']
            df = df[df['standard_name'].str.strip() != '']
            
            # Preparar datos para inserción
            df['entity_type'] = entity_type
            if 'needs_review' in df.columns:
                df['needs_review'] = df['needs_review'].astype(int)
            else:
                df['needs_review'] = 0
            df['avg_similarity'] = df.get('avg_similarity', None)
            df['min_similarity'] = df.get('min_similarity', None)
            df['component_size'] = df.get('component_size', 1)
            
            # Insertar datos
            df.to_sql('entities', conn, if_exists='append', index=False)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def load_entities(self, entity_type: str) -> pd.DataFrame:
        """
        Carga todas las entidades de un tipo específico
        
        Args:
            entity_type: Tipo de entidad
            
        Returns:
            DataFrame con todas las entidades
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be one of {ENTITY_TYPES}")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                entity_id,
                original_name,
                normalized_name,
                standard_name,
                frequency,
                component_size,
                avg_similarity,
                min_similarity,
                CASE WHEN needs_review = 1 THEN 1 ELSE 0 END as needs_review
            FROM entities
            WHERE entity_type = ?
        """
        
        df = pd.read_sql_query(query, conn, params=(entity_type,))
        conn.close()
        
        # Sort entity_id numerically instead of lexicographically
        def extract_numeric_key(entity_id):
            """Extract numeric part for sorting"""
            match = re.search(r'_(\d+)$', str(entity_id))
            if match:
                return int(match.group(1))
            return 0
        
        # Sort by entity_id numerically, then by frequency descending
        df['_sort_key'] = df['entity_id'].apply(extract_numeric_key)
        df = df.sort_values(by=['_sort_key', 'frequency'], ascending=[True, False])
        df = df.drop(columns=['_sort_key'])
        
        return df
    
    def update_entities(self, df: pd.DataFrame, entity_type: str):
        """
        Actualiza las entidades en la base de datos
        
        Args:
            df: DataFrame con las entidades actualizadas
            entity_type: Tipo de entidad
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be one of {ENTITY_TYPES}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Borrar entidades existentes del tipo
            cursor.execute("DELETE FROM entities WHERE entity_type = ?", (entity_type,))
            
            # Preparar datos
            df['entity_type'] = entity_type
            if 'needs_review' in df.columns:
                df['needs_review'] = df['needs_review'].astype(int)
            else:
                df['needs_review'] = 0
            df['updated_at'] = datetime.now()
            
            # Insertar datos actualizados
            df.to_sql('entities', conn, if_exists='append', index=False)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def add_change_history(self, change_type: str, entity_id: Optional[str] = None, 
                          details: Optional[dict] = None):
        """
        Registra un cambio en el historial
        
        Args:
            change_type: Tipo de cambio ('move', 'split', 'merge', 'rename')
            entity_id: ID de entidad afectada
            details: Detalles del cambio como diccionario
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        details_json = json.dumps(details) if details else None
        
        cursor.execute("""
            INSERT INTO change_history (change_type, entity_id, details)
            VALUES (?, ?, ?)
        """, (change_type, entity_id, details_json))
        
        conn.commit()
        conn.close()
    
    def get_change_history(self, limit: int = 50) -> pd.DataFrame:
        """
        Obtiene el historial de cambios
        
        Args:
            limit: Número máximo de cambios a retornar
            
        Returns:
            DataFrame con el historial
        """
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                change_type,
                entity_id,
                details,
                changed_by,
                created_at
            FROM change_history
            ORDER BY created_at DESC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(limit,))
        conn.close()
        
        return df
    
    def export_to_csv(self, entity_type: str, output_path: Path):
        """
        Exporta entidades a un archivo CSV
        
        Args:
            entity_type: Tipo de entidad
            output_path: Ruta donde guardar el CSV
        """
        df = self.load_entities(entity_type)
        # Filter to only include required columns
        df = df[['entity_id', 'original_name', 'standard_name', 'frequency', 'component_size']]
        df.to_csv(output_path, index=False)
    
    def get_statistics(self, entity_type: str) -> dict:
        """
        Obtiene estadísticas de las entidades
        
        Args:
            entity_type: Tipo de entidad
            
        Returns:
            Diccionario con estadísticas
        """
        if entity_type not in ENTITY_TYPES:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be one of {ENTITY_TYPES}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # Total de nombres
        cursor.execute("""
            SELECT COUNT(*) FROM entities WHERE entity_type = ?
        """, (entity_type,))
        stats['total_names'] = cursor.fetchone()[0]
        
        # Total de entidades únicas
        cursor.execute("""
            SELECT COUNT(DISTINCT entity_id) FROM entities WHERE entity_type = ?
        """, (entity_type,))
        stats['unique_entities'] = cursor.fetchone()[0]
        
        # Tamaño promedio de grupos
        cursor.execute("""
            SELECT AVG(component_size) FROM entities WHERE entity_type = ?
        """, (entity_type,))
        stats['avg_group_size'] = cursor.fetchone()[0] or 0
        
        # Grupos grandes
        cursor.execute("""
            SELECT COUNT(DISTINCT entity_id) FROM entities 
            WHERE entity_type = ? AND component_size > 20
        """, (entity_type,))
        stats['large_groups'] = cursor.fetchone()[0]
        
        conn.close()
        
        return stats
    
    def backup_database(self, backup_path: Optional[Path] = None) -> Path:
        """
        Crea un backup de la base de datos
        
        Args:
            backup_path: Ruta opcional para el backup. Si no se especifica, 
                        se genera automáticamente con timestamp.
        
        Returns:
            Ruta del archivo de backup creado
        """
        import shutil
        
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.db_path.parent / f"entities_by_transaction_backup_{timestamp}.db"
        
        shutil.copy2(self.db_path, backup_path)
        return backup_path
    
    def clear_all(self, entity_type: Optional[str] = None):
        """
        Borra todos los datos (o de un tipo específico)
        
        Args:
            entity_type: Si se especifica, solo borra ese tipo. Si es None, borra todo.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if entity_type:
            if entity_type not in ENTITY_TYPES:
                raise ValueError(f"Invalid entity_type: {entity_type}. Must be one of {ENTITY_TYPES}")
            cursor.execute("DELETE FROM entities WHERE entity_type = ?", (entity_type,))
        else:
            cursor.execute("DELETE FROM entities")
        
        conn.commit()
        conn.close()

