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
import logging

# Transaction types supported
TRANSACTION_TYPES = ['security', 'release']

# Configure logging
logger = logging.getLogger(__name__)


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
        
        # Add standardized name columns if they don't exist (migration support)
        cursor.execute("PRAGMA table_info(patent_pairs)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        if 'firm_standard_name' not in existing_columns:
            cursor.execute("ALTER TABLE patent_pairs ADD COLUMN firm_standard_name TEXT")
            logger.info("Added firm_standard_name column to patent_pairs table")
        
        if 'bank_standard_name' not in existing_columns:
            cursor.execute("ALTER TABLE patent_pairs ADD COLUMN bank_standard_name TEXT")
            logger.info("Added bank_standard_name column to patent_pairs table")
        
        if 'pair_standard_name' not in existing_columns:
            cursor.execute("ALTER TABLE patent_pairs ADD COLUMN pair_standard_name TEXT")
            logger.info("Added pair_standard_name column to patent_pairs table")
        
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
    
    def _load_standardized_mappings(self, transaction_type: str) -> Dict[str, str]:
        """
        Load standardized name mappings for a given transaction type.
        Combines financial and non-financial mappings.
        
        Args:
            transaction_type: Transaction type ('security' or 'release')
            
        Returns:
            Dictionary mapping original_name -> standard_name
        """
        mappings = {}
        base_path = Path("results/manual_review")
        
        # Files to load based on transaction type
        mapping_files = [
            base_path / f"financial_{transaction_type}_entities_standardized.csv",
            base_path / f"non_financial_{transaction_type}_entities_standardized.csv"
        ]
        
        conflicts = []
        
        for file_path in mapping_files:
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    
                    # Verify required columns exist
                    if 'original_name' not in df.columns or 'standard_name' not in df.columns:
                        logger.warning(f"Missing required columns in {file_path.name}. Skipping.")
                        continue
                    
                    # Create mapping: original_name -> standard_name
                    for _, row in df.iterrows():
                        original = str(row['original_name']).strip()
                        standard = str(row['standard_name']).strip()
                        
                        # Check for conflicts (name in both financial and non-financial)
                        if original in mappings and mappings[original] != standard:
                            conflicts.append({
                                'name': original,
                                'existing': mappings[original],
                                'new': standard,
                                'file': file_path.name
                            })
                            # Prioritize financial (first file) - don't overwrite
                            continue
                        
                        mappings[original] = standard
                    
                    logger.info(f"Loaded {len(df):,} mappings from {file_path.name}")
                    
                except Exception as e:
                    logger.warning(f"Error loading {file_path.name}: {e}. Continuing with other files.")
            else:
                logger.warning(f"Mapping file not found: {file_path}. Continuing without it.")
        
        # Log conflicts if any
        if conflicts:
            logger.warning(f"Found {len(conflicts)} name conflicts between financial and non-financial mappings. "
                          f"Using financial mapping (first file) for conflicts.")
            for conflict in conflicts[:10]:  # Log first 10 conflicts
                logger.debug(f"Conflict: '{conflict['name']}' -> existing: '{conflict['existing']}', "
                           f"new: '{conflict['new']}' from {conflict['file']}")
        
        logger.info(f"Total mappings loaded for {transaction_type}: {len(mappings):,}")
        return mappings
    
    def import_from_csv(self, csv_path: Path, transaction_type: str, clear_existing: bool = False, use_standardized_names: bool = True):
        """
        Importa datos desde un archivo CSV y cuenta pares (firm-bank)
        
        Args:
            csv_path: Ruta al archivo CSV (security_patent.csv o release_patent.csv)
            transaction_type: Tipo de transacción ('security' o 'release')
            clear_existing: Si True, borra datos existentes del mismo tipo antes de importar
            use_standardized_names: Si True, aplica mapeo de nombres estandarizados
        """
        if transaction_type not in TRANSACTION_TYPES:
            raise ValueError(f"Invalid transaction_type: {transaction_type}. Must be one of {TRANSACTION_TYPES}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {csv_path}")
        
        # Load standardized mappings if requested
        name_mappings = {}
        if use_standardized_names:
            name_mappings = self._load_standardized_mappings(transaction_type)
            print(f"  Loaded {len(name_mappings):,} standardized name mappings")
        
        # Helper function to get standardized name
        def get_standard_name(original_name: str) -> str:
            original_clean = original_name.strip()
            return name_mappings.get(original_clean, original_clean)
        
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
                # Map firm and bank based on transaction type
                # Security: firm = or_name, bank = ee_name
                # Release: firm = ee_name, bank = or_name (REVERSED)
                if transaction_type == 'security':
                    firm_raw = str(row['or_name']).strip()
                    bank_raw = str(row['ee_name']).strip()
                else:  # release
                    firm_raw = str(row['ee_name']).strip()
                    bank_raw = str(row['or_name']).strip()
                
                # Apply standardization if enabled
                firm = get_standard_name(firm_raw) if use_standardized_names else firm_raw
                bank = get_standard_name(bank_raw) if use_standardized_names else bank_raw
                
                # Create pair names (both original and standardized)
                pair_name = f"{firm_raw} - {bank_raw}"
                pair_standard_name = f"{firm} - {bank}" if use_standardized_names else pair_name
                
                count = int(row['count'])
                patent_count = int(row['patent_count']) if pd.notna(row['patent_count']) else None
                
                # Use original pair_name as key for accumulation
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
                        'firm_name': firm_raw,
                        'bank_name': bank_raw,
                        'pair_name': pair_name,
                        'frequency': count,
                        'patent_count': patent_count
                    }
                    # Add standardized names if enabled
                    if use_standardized_names:
                        pairs_dict[pair_name]['firm_standard_name'] = firm
                        pairs_dict[pair_name]['bank_standard_name'] = bank
                        pairs_dict[pair_name]['pair_standard_name'] = pair_standard_name
            
            total_rows += len(chunk)
            if total_rows % 500000 == 0:
                print(f"  Processed {total_rows:,} rows...")
        
        print(f"  Total rows processed: {total_rows:,}")
        print(f"  Unique pairs found: {len(pairs_dict):,}")
        
        # Convertir a DataFrame
        pairs_list = list(pairs_dict.values())
        df = pd.DataFrame(pairs_list)
        df['transaction_type'] = transaction_type
        
        # Ensure standardized columns exist (fill with None if not using standardization)
        if not use_standardized_names:
            df['firm_standard_name'] = None
            df['bank_standard_name'] = None
            df['pair_standard_name'] = None
        
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
    
    def migrate_existing_data(self) -> Dict:
        """
        Migrate existing database records to include standardized names.
        Updates rows where standardized names are NULL.
        
        Returns:
            Dictionary with migration statistics
        """
        print("Starting migration of existing data to standardized names...")
        
        # Load mappings for both transaction types
        security_mappings = self._load_standardized_mappings('security')
        release_mappings = self._load_standardized_mappings('release')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {
            'security_rows_migrated': 0,
            'release_rows_migrated': 0,
            'security_unmapped_firms': set(),
            'security_unmapped_banks': set(),
            'release_unmapped_firms': set(),
            'release_unmapped_banks': set()
        }
        
        try:
            # Process security transactions
            cursor.execute("""
                SELECT id, firm_name, bank_name 
                FROM patent_pairs 
                WHERE transaction_type = 'security' 
                AND (firm_standard_name IS NULL OR bank_standard_name IS NULL)
            """)
            security_rows = cursor.fetchall()
            
            print(f"Found {len(security_rows):,} security rows to migrate")
            
            batch_size = 1000
            for i in range(0, len(security_rows), batch_size):
                batch = security_rows[i:i+batch_size]
                updates = []
                
                for row_id, firm_name, bank_name in batch:
                    firm_std = security_mappings.get(firm_name.strip(), firm_name.strip())
                    bank_std = security_mappings.get(bank_name.strip(), bank_name.strip())
                    pair_std = f"{firm_std} - {bank_std}"
                    
                    updates.append((firm_std, bank_std, pair_std, row_id))
                    
                    # Track unmapped names
                    if firm_std == firm_name.strip():
                        stats['security_unmapped_firms'].add(firm_name.strip())
                    if bank_std == bank_name.strip():
                        stats['security_unmapped_banks'].add(bank_name.strip())
                
                # Batch update
                cursor.executemany("""
                    UPDATE patent_pairs 
                    SET firm_standard_name = ?, 
                        bank_standard_name = ?, 
                        pair_standard_name = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, updates)
                
                conn.commit()
                stats['security_rows_migrated'] += len(updates)
                
                if (i + batch_size) % 5000 == 0:
                    print(f"  Migrated {stats['security_rows_migrated']:,} security rows...")
            
            # Process release transactions
            cursor.execute("""
                SELECT id, firm_name, bank_name 
                FROM patent_pairs 
                WHERE transaction_type = 'release' 
                AND (firm_standard_name IS NULL OR bank_standard_name IS NULL)
            """)
            release_rows = cursor.fetchall()
            
            print(f"Found {len(release_rows):,} release rows to migrate")
            
            for i in range(0, len(release_rows), batch_size):
                batch = release_rows[i:i+batch_size]
                updates = []
                
                for row_id, firm_name, bank_name in batch:
                    firm_std = release_mappings.get(firm_name.strip(), firm_name.strip())
                    bank_std = release_mappings.get(bank_name.strip(), bank_name.strip())
                    pair_std = f"{firm_std} - {bank_std}"
                    
                    updates.append((firm_std, bank_std, pair_std, row_id))
                    
                    # Track unmapped names
                    if firm_std == firm_name.strip():
                        stats['release_unmapped_firms'].add(firm_name.strip())
                    if bank_std == bank_name.strip():
                        stats['release_unmapped_banks'].add(bank_name.strip())
                
                # Batch update
                cursor.executemany("""
                    UPDATE patent_pairs 
                    SET firm_standard_name = ?, 
                        bank_standard_name = ?, 
                        pair_standard_name = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, updates)
                
                conn.commit()
                stats['release_rows_migrated'] += len(updates)
                
                if (i + batch_size) % 5000 == 0:
                    print(f"  Migrated {stats['release_rows_migrated']:,} release rows...")
            
            print(f"Migration complete!")
            print(f"  Security: {stats['security_rows_migrated']:,} rows migrated")
            print(f"  Release: {stats['release_rows_migrated']:,} rows migrated")
            print(f"  Security unmapped firms: {len(stats['security_unmapped_firms']):,}")
            print(f"  Security unmapped banks: {len(stats['security_unmapped_banks']):,}")
            print(f"  Release unmapped firms: {len(stats['release_unmapped_firms']):,}")
            print(f"  Release unmapped banks: {len(stats['release_unmapped_banks']):,}")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
        
        # Convert sets to counts for return
        return {
            'security_rows_migrated': stats['security_rows_migrated'],
            'release_rows_migrated': stats['release_rows_migrated'],
            'security_unmapped_firms_count': len(stats['security_unmapped_firms']),
            'security_unmapped_banks_count': len(stats['security_unmapped_banks']),
            'release_unmapped_firms_count': len(stats['release_unmapped_firms']),
            'release_unmapped_banks_count': len(stats['release_unmapped_banks'])
        }
    
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
                firm_standard_name,
                bank_standard_name,
                pair_standard_name,
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
                firm_standard_name,
                bank_standard_name,
                pair_standard_name,
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

