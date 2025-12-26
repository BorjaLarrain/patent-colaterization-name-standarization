"""
Gestor de Base de Datos para Pares de Transacciones de Patentes
================================================================
Sistema de base de datos SQLite para manejar pares (firm-bank) de transacciones de patentes.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import shutil
from typing import Optional, Dict

# Transaction types supported
TRANSACTION_TYPES = ['security', 'release']


class PatentTransactionDatabase:
    """Gestor de base de datos para pares de transacciones de patentes"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Inicializa la base de datos y crea tablas si no existen"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabla principal de pares de transacciones
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS patent_pairs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_type TEXT NOT NULL,
                firm_name TEXT NOT NULL,
                bank_name TEXT NOT NULL,
                pair_name TEXT NOT NULL,
                frequency INTEGER NOT NULL,
                patent_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(transaction_type, firm_name, bank_name)
            )
        """)
        
        # Índices para mejor rendimiento
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transaction_type ON patent_pairs(transaction_type)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_pair_name ON patent_pairs(pair_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_frequency ON patent_pairs(frequency DESC)
        """)
        
        conn.commit()
        conn.close()
    
    def import_from_csv(self, csv_path: Path, transaction_type: str, clear_existing: bool = False):
        """
        Importa datos desde un archivo CSV y cuenta pares (firm-bank)
        
        Args:
            csv_path: Ruta al archivo CSV (security_patent.csv o release_patent.csv)
            transaction_type: Tipo de transacción ('security' o 'release')
            clear_existing: Si True, borra datos existentes del mismo tipo antes de importar
        """
        if transaction_type not in TRANSACTION_TYPES:
            raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {csv_path}")
        
        # Verificar columnas necesarias
        required_cols = ['or_name', 'ee_name']
        
        # Leer CSV en chunks para manejar archivos grandes
        chunk_size = 100000
        pairs_dict = {}
        total_rows = 0
        
        print(f"Processing {csv_path.name}...")
        
        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False):
            # Verificar columnas
            missing_cols = [col for col in required_cols if col not in chunk.columns]
            if missing_cols:
                raise ValueError(f"Faltan columnas requeridas: {missing_cols}")
            
            # Filtrar filas con valores nulos o vacíos
            chunk = chunk.dropna(subset=['or_name', 'ee_name'])
            chunk = chunk[chunk['or_name'].str.strip() != '']
            chunk = chunk[chunk['ee_name'].str.strip() != '']
            
            # Agrupar por (or_name, ee_name) y contar ocurrencias
            grouped = chunk.groupby(['or_name', 'ee_name']).size().reset_index(name='count')
            
            # Contar patentes únicos por par (opcional, para referencia)
            patent_counts = chunk.groupby(['or_name', 'ee_name'])['patent'].nunique().reset_index(name='patent_count')
            
            # Combinar conteos
            grouped = grouped.merge(patent_counts, on=['or_name', 'ee_name'], how='left')
            
            # Acumular en diccionario
            for _, row in grouped.iterrows():
                firm = str(row['or_name']).strip()
                bank = str(row['ee_name']).strip()
                pair_name = f"{firm} - {bank}"
                count = int(row['count'])
                patent_count = int(row['patent_count']) if pd.notna(row['patent_count']) else None
                
                if pair_name in pairs_dict:
                    pairs_dict[pair_name]['frequency'] += count
                    if patent_count is not None:
                        if pairs_dict[pair_name]['patent_count'] is None:
                            pairs_dict[pair_name]['patent_count'] = patent_count
                        else:
                            # Sumar patentes únicos (aproximación)
                            pairs_dict[pair_name]['patent_count'] += patent_count
                else:
                    pairs_dict[pair_name] = {
                        'firm_name': firm,
                        'bank_name': bank,
                        'pair_name': pair_name,
                        'frequency': count,
                        'patent_count': patent_count
                    }
            
            total_rows += len(chunk)
            if total_rows % 500000 == 0:
                print(f"  Processed {total_rows:,} rows...")
        
        print(f"  Total rows processed: {total_rows:,}")
        print(f"  Unique pairs found: {len(pairs_dict):,}")
        
        # Convertir a DataFrame
        pairs_list = list(pairs_dict.values())
        df = pd.DataFrame(pairs_list)
        df['transaction_type'] = transaction_type
        
        # Conectar a base de datos
        conn = sqlite3.connect(self.db_path)
        
        try:
            if clear_existing:
                # Borrar datos existentes del mismo tipo
                cursor = conn.cursor()
                cursor.execute("DELETE FROM patent_pairs WHERE transaction_type = ?", (transaction_type,))
                conn.commit()
            
            # Insertar datos
            df.to_sql('patent_pairs', conn, if_exists='append', index=False)
            
            conn.commit()
            print(f"  Successfully imported {len(df):,} pairs to database")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_top_pairs(self, transaction_type: str, top_n: int = 20) -> pd.DataFrame:
        """
        Obtiene los top N pares por frecuencia
        
        Args:
            transaction_type: Tipo de transacción ('security' o 'release')
            top_n: Número de pares a retornar
            
        Returns:
            DataFrame con los top pares ordenados por frecuencia descendente
        """
        if transaction_type not in TRANSACTION_TYPES:
            raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                transaction_type,
                firm_name,
                bank_name,
                pair_name,
                frequency,
                patent_count
            FROM patent_pairs
            WHERE transaction_type = ?
            ORDER BY frequency DESC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(transaction_type, top_n))
        conn.close()
        
        # Calcular porcentaje del total
        if len(df) > 0:
            total_frequency = df['frequency'].sum()
            df['percentage'] = (df['frequency'] / total_frequency * 100) if total_frequency > 0 else 0
        else:
            df['percentage'] = 0
        
        return df
    
    def get_statistics(self, transaction_type: Optional[str] = None) -> Dict:
        """
        Obtiene estadísticas de los pares
        
        Args:
            transaction_type: Tipo de transacción. Si es None, retorna estadísticas de todos los tipos.
            
        Returns:
            Diccionario con estadísticas
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        if transaction_type:
            if transaction_type not in TRANSACTION_TYPES:
                raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
            
            # Total de pares únicos
            cursor.execute("""
                SELECT COUNT(*) FROM patent_pairs WHERE transaction_type = ?
            """, (transaction_type,))
            stats['total_pairs'] = cursor.fetchone()[0]
            
            # Total de frecuencia
            cursor.execute("""
                SELECT SUM(frequency) FROM patent_pairs WHERE transaction_type = ?
            """, (transaction_type,))
            stats['total_frequency'] = cursor.fetchone()[0] or 0
            
            # Firmas únicas
            cursor.execute("""
                SELECT COUNT(DISTINCT firm_name) FROM patent_pairs WHERE transaction_type = ?
            """, (transaction_type,))
            stats['unique_firms'] = cursor.fetchone()[0]
            
            # Bancos únicos
            cursor.execute("""
                SELECT COUNT(DISTINCT bank_name) FROM patent_pairs WHERE transaction_type = ?
            """, (transaction_type,))
            stats['unique_banks'] = cursor.fetchone()[0]
            
        else:
            # Estadísticas de todos los tipos
            for ttype in TRANSACTION_TYPES:
                stats[ttype] = self.get_statistics(ttype)
        
        conn.close()
        
        return stats
    
    def load_all_pairs(self, transaction_type: str) -> pd.DataFrame:
        """
        Carga todos los pares de un tipo específico
        
        Args:
            transaction_type: Tipo de transacción
            
        Returns:
            DataFrame con todos los pares
        """
        if transaction_type not in TRANSACTION_TYPES:
            raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
        
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT 
                transaction_type,
                firm_name,
                bank_name,
                pair_name,
                frequency,
                patent_count
            FROM patent_pairs
            WHERE transaction_type = ?
            ORDER BY frequency DESC
        """
        
        df = pd.read_sql_query(query, conn, params=(transaction_type,))
        conn.close()
        
        # Calcular porcentaje del total
        if len(df) > 0:
            total_frequency = df['frequency'].sum()
            df['percentage'] = (df['frequency'] / total_frequency * 100) if total_frequency > 0 else 0
        else:
            df['percentage'] = 0
        
        return df
    
    def export_to_csv(self, transaction_type: str, output_path: Path):
        """
        Exporta pares a un archivo CSV
        
        Args:
            transaction_type: Tipo de transacción
            output_path: Ruta donde guardar el CSV
        """
        df = self.load_all_pairs(transaction_type)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
    
    def backup_database(self, backup_path: Optional[Path] = None) -> Path:
        """
        Crea un backup de la base de datos
        
        Args:
            backup_path: Ruta opcional para el backup. Si no se especifica, 
                        se genera automáticamente con timestamp.
        
        Returns:
            Ruta del archivo de backup creado
        """
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.db_path.parent / f"patent_transactions_backup_{timestamp}.db"
        
        shutil.copy2(self.db_path, backup_path)
        return backup_path
    
    def clear_all(self, transaction_type: Optional[str] = None):
        """
        Borra todos los datos (o de un tipo específico)
        
        Args:
            transaction_type: Si se especifica, solo borra ese tipo. Si es None, borra todo.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if transaction_type:
            if transaction_type not in TRANSACTION_TYPES:
                raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
            cursor.execute("DELETE FROM patent_pairs WHERE transaction_type = ?", (transaction_type,))
        else:
            cursor.execute("DELETE FROM patent_pairs")
        
        conn.commit()
        conn.close()

