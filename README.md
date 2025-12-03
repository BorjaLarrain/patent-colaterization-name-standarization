# Entity Name Standardization Pipeline

## Abstract

This pipeline standardizes financial and non-financial entity names by grouping name variations under a unique identifier and standard name. The process consists of seven main steps:

1. **Exploration**: Analyzes input data to identify patterns, functional roles, and legal suffixes
2. **Normalization**: Cleans and standardizes names (removes functional roles, normalizes legal suffixes, handles punctuation)
3. **Blocking**: Groups names by first word to reduce computational complexity
4. **Fuzzy Matching**: Uses WRatio (88% similarity threshold) to find similar names within blocks and builds connected components
5. **Grouping**: Assigns unique entity IDs and selects standard names (preferring highest frequency, then shortest)
6. **Validation**: Identifies problematic groups (low similarity, large groups) for manual review
7. **Complete Mapping**: Adds singleton names (without matches) as unique entities

**Final Results:**
- **Financial entities:** 8,459 names → 5,231 unique entities
- **Non-financial entities:** 21,453 names → 17,726 unique entities

---

## Quick Start

### Run the complete pipeline:
```bash
python scripts/pipeline.py
```

### Run a specific phase:
```bash
python scripts/pipeline.py --phase exploration
python scripts/pipeline.py --phase normalization
python scripts/pipeline.py --phase blocking
python scripts/pipeline.py --phase matching
python scripts/pipeline.py --phase grouping
python scripts/pipeline.py --phase validation
python scripts/pipeline.py --phase complete
```

---

## Project Structure

```
scripts/
├── pipeline.py                    # Main script - runs entire pipeline
├── modules/
│   ├── exploration.py             # Phase 1: Data exploration
│   ├── normalization.py           # Phase 2: Name normalization
│   ├── blocking.py                # Phase 3: Blocking by first word
│   ├── matching.py                # Phase 4: Fuzzy matching
│   ├── grouping.py                # Phase 5: Grouping and ID assignment
│   ├── validation.py              # Phase 6: Validation
│   └── complete_mapping.py        # Phase 7: Complete mapping
```

---

## Pipeline Phases

### Phase 1: Exploration
- Loads input data (`financial_entity_freq_pledge.csv`, `non_financial_entity_freq_pledge.csv`)
- Calculates descriptive statistics
- Identifies common patterns (functional roles, legal suffixes)
- **Output:** `results/exploration/basic_stats.txt`, `variations_analysis.txt`

### Phase 2: Normalization
Cleans and standardizes names:
- Converts to uppercase, normalizes punctuation
- Removes functional roles ("AS COLLATERAL AGENT", "AS TRUSTEE", etc.)
- Normalizes legal suffixes (CORPORATION → CORP, INCORPORATED → INC, etc.)
- Removes common elements ("THE", normalizes "AND" → "&")
- **Output:** `results/intermediate/*_normalized.csv`

### Phase 3: Blocking
- Extracts first significant word as blocking key
- Groups names by blocking key
- Optimizes large blocks (>200 elements) with sub-blocking
- **Output:** `results/intermediate/*_blocks.json`

### Phase 4: Fuzzy Matching
- Compares name pairs within each block using WRatio (rapidfuzz)
- **Similarity threshold:** 88%
- Builds graph of connected names
- Finds connected components using DFS
- **Output:** `results/intermediate/*_matches.csv`, `*_components.json`

### Phase 5: Grouping
- Assigns unique `entity_id` to each component
- Selects `standard_name` (highest frequency, then shortest)
- Identifies problematic cases for review
- **Output:** `results/final/*_entity_mapping.csv`, `*_review_cases.csv`

### Phase 6: Validation
- Validates groups with low average similarity (< 90%)
- Identifies potential false positives
- Finds high-frequency names without matches
- **Output:** `results/validation/*_validation_report.csv`, `*_problematic_components.csv`

### Phase 7: Complete Mapping
- Adds singleton names (without matches) as unique entities
- Ensures all original names are included in final mapping
- **Output:** `results/final/*_entity_mapping_complete.csv`

---

## Key Parameters

- **Similarity threshold:** 88% (WRatio)
- **Minimum block size for matching:** 2 names
- **Validation thresholds:**
  - Low average similarity: < 90%
  - Suspicious minimum similarity: < 87%
  - Large group: > 20 names
  - High frequency: ≥ 1,000

---

## Output Files

### Final Results
- `results/final/*_entity_mapping_complete.csv` - **Complete mapping with all names**
  - Columns: `entity_id`, `original_name`, `normalized_name`, `standard_name`, `frequency`, `component_size`, `avg_similarity`, `min_similarity`, `needs_review`
- `results/final/*_review_cases.csv` - Cases for manual review

### Intermediate Files
- `results/intermediate/*_normalized.csv` - Normalized names
- `results/intermediate/*_blocks.json` - Optimized blocks
- `results/intermediate/*_matches.csv` - All matches found
- `results/intermediate/*_components.json` - Connected components

### Validation
- `results/validation/*_validation_report.csv` - Validation report
- `results/validation/*_problematic_components.csv` - Problematic components

---

## Important Notes

1. **`standard_name` is the definitive name** to use in subsequent analyses
2. **The `_complete.csv` file includes ALL names**, not just those with matches
3. **Singletons** (names without matches) have `component_size = 1` and `standard_name = normalized_name`
4. **Manual review** is important to identify and correct false positives
5. **The 88% threshold** was adjusted from 85% to reduce false positives (93% reduction in problematic cases)
