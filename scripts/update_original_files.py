#!/usr/bin/env python3
"""
Update original data files with entity_id and standard_name from standardized files.

This script:
1. Creates backups of original files
2. Loads standardized data
3. Joins original files with standardized data
4. Adds entity_id and standard_name columns
5. Reports statistics on matches
"""

import pandas as pd
import os
import shutil
from pathlib import Path
from datetime import datetime

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
ORIGINAL_DATA_DIR = PROJECT_ROOT / "original-data"
BACKUP_DIR = ORIGINAL_DATA_DIR / "backup"
STANDARDIZED_DIR = PROJECT_ROOT / "results" / "manual_review"

# File mappings
FILE_CONFIGS = [
    {
        "original_file": "financial_entity_freq_pledge.csv",
        "standardized_file": "financial_entities_standardized.csv",
        "name_column": "ee_name",
        "entity_type": "financial"
    },
    {
        "original_file": "financial_entity_freq_release.csv",
        "standardized_file": "financial_entities_standardized.csv",
        "name_column": "or_name",
        "entity_type": "financial"
    },
    {
        "original_file": "non_financial_entity_freq_pledge.csv",
        "standardized_file": "non_financial_entities_standardized.csv",
        "name_column": "or_name",
        "entity_type": "non_financial"
    },
    {
        "original_file": "nonfinancial_entity_freq_release.csv",
        "standardized_file": "non_financial_entities_standardized.csv",
        "name_column": "ee_name",
        "entity_type": "non_financial"
    }
]


def create_backup_directory():
    """Create backup directory and copy original files."""
    print("📁 Creating backup directory...")
    BACKUP_DIR.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for config in FILE_CONFIGS:
        original_path = ORIGINAL_DATA_DIR / config["original_file"]
        if original_path.exists():
            backup_path = BACKUP_DIR / f"{config['original_file']}.backup"
            shutil.copy2(original_path, backup_path)
            print(f"   ✓ Backed up: {config['original_file']}")
        else:
            print(f"   ⚠ Warning: {config['original_file']} not found")
    
    print(f"✅ Backups created in: {BACKUP_DIR}\n")


def load_standardized_data():
    """Load standardized CSV files and create lookup dictionaries."""
    print("📊 Loading standardized data...")
    
    # Load financial entities
    financial_path = STANDARDIZED_DIR / "financial_entities_standardized.csv"
    financial_df = pd.read_csv(financial_path)
    print(f"   ✓ Loaded {len(financial_df):,} financial entity records")
    
    # Load non-financial entities
    non_financial_path = STANDARDIZED_DIR / "non_financial_entities_standardized.csv"
    non_financial_df = pd.read_csv(non_financial_path)
    print(f"   ✓ Loaded {len(non_financial_df):,} non-financial entity records")
    
    # Create lookup dictionaries: normalized_name -> (entity_id, standard_name)
    # Use first occurrence if duplicates exist (shouldn't happen, but handle it)
    financial_lookup = {}
    for _, row in financial_df.iterrows():
        normalized_name = str(row['original_name']).upper().strip()
        if normalized_name not in financial_lookup:
            financial_lookup[normalized_name] = (
                row['entity_id'],
                row['standard_name']
            )
    
    non_financial_lookup = {}
    for _, row in non_financial_df.iterrows():
        normalized_name = str(row['original_name']).upper().strip()
        if normalized_name not in non_financial_lookup:
            non_financial_lookup[normalized_name] = (
                row['entity_id'],
                row['standard_name']
            )
    
    print(f"   ✓ Created lookup with {len(financial_lookup):,} financial entries")
    print(f"   ✓ Created lookup with {len(non_financial_lookup):,} non-financial entries\n")
    
    return financial_lookup, non_financial_lookup


def process_file(config, lookup_dict):
    """Process a single original file and add entity_id and standard_name."""
    original_path = ORIGINAL_DATA_DIR / config["original_file"]
    name_column = config["name_column"]
    
    print(f"📝 Processing: {config['original_file']}")
    
    # Load original file
    df = pd.read_csv(original_path)
    original_count = len(df)
    print(f"   ✓ Loaded {original_count:,} rows")
    
    # Normalize names for matching
    df['_normalized_name'] = df[name_column].astype(str).str.upper().str.strip()
    
    # Add entity_id and standard_name columns
    df['entity_id'] = None
    df['standard_name'] = None
    
    # Match with lookup dictionary
    matched_count = 0
    for idx, row in df.iterrows():
        normalized_name = row['_normalized_name']
        if normalized_name in lookup_dict:
            entity_id, standard_name = lookup_dict[normalized_name]
            df.at[idx, 'entity_id'] = entity_id
            df.at[idx, 'standard_name'] = standard_name
            matched_count += 1
    
    # Remove temporary column
    df = df.drop(columns=['_normalized_name'])
    
    # Reorder columns: name_column, freq, entity_id, standard_name
    columns_order = [name_column, 'freq', 'entity_id', 'standard_name']
    df = df[columns_order]
    
    # Save updated file
    df.to_csv(original_path, index=False)
    
    unmatched_count = original_count - matched_count
    match_percentage = (matched_count / original_count * 100) if original_count > 0 else 0
    
    print(f"   ✓ Matched: {matched_count:,} rows ({match_percentage:.1f}%)")
    print(f"   ⚠ Unmatched: {unmatched_count:,} rows")
    print(f"   ✅ Saved updated file\n")
    
    return {
        'file': config['original_file'],
        'total_rows': original_count,
        'matched_rows': matched_count,
        'unmatched_rows': unmatched_count,
        'match_percentage': match_percentage
    }


def main():
    """Main execution function."""
    print("=" * 70)
    print("Update Original Files with Entity IDs")
    print("=" * 70)
    print()
    
    # Step 1: Create backups
    create_backup_directory()
    
    # Step 2: Load standardized data
    financial_lookup, non_financial_lookup = load_standardized_data()
    
    # Step 3: Process each file
    print("🔄 Processing original files...")
    print()
    
    results = []
    for config in FILE_CONFIGS:
        # Select appropriate lookup dictionary
        if config['entity_type'] == 'financial':
            lookup_dict = financial_lookup
        else:
            lookup_dict = non_financial_lookup
        
        # Process file
        result = process_file(config, lookup_dict)
        results.append(result)
    
    # Step 4: Print summary statistics
    print("=" * 70)
    print("Summary Statistics")
    print("=" * 70)
    print()
    
    total_rows = sum(r['total_rows'] for r in results)
    total_matched = sum(r['matched_rows'] for r in results)
    total_unmatched = sum(r['unmatched_rows'] for r in results)
    overall_match_percentage = (total_matched / total_rows * 100) if total_rows > 0 else 0
    
    print(f"Total rows processed: {total_rows:,}")
    print(f"Total matched: {total_matched:,} ({overall_match_percentage:.1f}%)")
    print(f"Total unmatched: {total_unmatched:,}")
    print()
    
    print("Per-file breakdown:")
    for result in results:
        print(f"  {result['file']}:")
        print(f"    Total: {result['total_rows']:,}")
        print(f"    Matched: {result['matched_rows']:,} ({result['match_percentage']:.1f}%)")
        print(f"    Unmatched: {result['unmatched_rows']:,}")
        print()
    
    print("=" * 70)
    print("✅ Update complete!")
    print("=" * 70)
    print(f"\nBackups saved in: {BACKUP_DIR}")
    print(f"Updated files in: {ORIGINAL_DATA_DIR}")


if __name__ == "__main__":
    main()
