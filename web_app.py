"""
Interactive Web Application for Entity Review
=============================================
Streamlit application to review, edit, and correct entity groupings.

Features:
- View all grouped entities
- Search by name or ID
- Move names between groups
- Split groups
- Merge groups
- Create new groups
- Save changes
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import re
import sys
import logging
from database_manager import EntityDatabase

# Configure logging to reduce noise
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Path configuration
BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR / "results" / "final"
MANUAL_REVIEW_DIR = BASE_DIR / "results" / "manual_review"
MANUAL_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_DIR = BASE_DIR / "database"
DATABASE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATABASE_DIR / "entities.db"

# Page configuration
st.set_page_config(
    page_title="Entity Review",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suppress Streamlit warnings if needed
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Dark mode CSS
DARK_MODE_CSS = """
<style>
    :root {
        --dark-bg: #0e1117;
        --dark-secondary-bg: #262730;
        --dark-text: #fafafa;
        --dark-border: #3a3a3a;
        --dark-input-bg: #1e1e1e;
    }
    
    /* Main app background */
    .stApp {
        background-color: var(--dark-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Main content area */
    .main .block-container {
        background-color: var(--dark-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: var(--dark-secondary-bg) !important;
    }
    [data-testid="stSidebar"] * {
        color: var(--dark-text) !important;
    }
    
    /* Toolbar */
    .stAppToolbar,
    [data-testid="stToolbar"] {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* All labels */
    label, .stSelectbox label, .stTextInput label, .stCheckbox label, 
    .stRadio label, .stNumberInput label, .stMultiselect label {
        color: var(--dark-text) !important;
    }
    
    /* Selectbox dropdowns and options - comprehensive targeting */
    .stSelectbox > div > div,
    .stSelectbox > div > div > div,
    .stSelectbox [class*="st-bn"],
    .stSelectbox [class*="st-cm"],
    .stSelectbox [class*="st-cw"],
    .stSelectbox [class*="st-cx"],
    .stSelectbox [class*="st-cy"],
    .stSelectbox [class*="st-cz"],
    .stSelectbox [class*="st-d0"],
    .stSelectbox [class*="st-d1"],
    .stSelectbox [class*="st-d2"],
    .stSelectbox [class*="st-d3"],
    .stSelectbox [class*="st-d4"],
    div[class*="st-bn"][class*="st-cm"],
    div[class*="st-bn"][class*="st-cw"],
    div[class*="st-bn"][class*="st-cx"],
    div[class*="st-bn"][class*="st-cy"],
    div[class*="st-bn"][class*="st-cz"],
    div[class*="st-bn"][class*="st-d0"],
    div[class*="st-bn"][class*="st-d1"],
    div[class*="st-bn"][class*="st-d2"],
    div[class*="st-bn"][class*="st-d3"],
    div[class*="st-bn"][class*="st-d4"],
    .stSelectbox select,
    .stSelectbox option {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
    }
    
    /* Selectbox focused/open state */
    .stSelectbox [class*="st-bn"][class*="st-cx"],
    .stSelectbox [class*="st-bn"][class*="st-cy"],
    .stSelectbox [class*="st-bn"][class*="st-cz"],
    .stSelectbox [class*="st-bn"][class*="st-d0"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Target all divs with Streamlit classes - very specific */
    div[class*="st-bn"],
    div[class*="st-cm"],
    div[class*="st-cw"],
    div[class*="st-cx"],
    div[class*="st-cy"],
    div[class*="st-cz"],
    div[class*="st-d0"],
    div[class*="st-d1"],
    div[class*="st-d2"],
    div[class*="st-d3"],
    div[class*="st-d4"],
    div[class*="st-b3"],
    div[class*="st-cg"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Multiple class combinations */
    div.st-bn.st-cm,
    div.st-bn.st-cw,
    div.st-bn.st-cx,
    div.st-bn.st-cy,
    div.st-bn.st-cz,
    div.st-bn.st-d0,
    div.st-bn.st-d1,
    div.st-bn.st-d2,
    div.st-bn.st-d3,
    div.st-bn.st-d4,
    div.st-bn.st-b3,
    div.st-bn.st-cm.st-cw,
    div.st-bn.st-cm.st-cx,
    div.st-bn.st-cm.st-cy,
    div.st-bn.st-cm.st-cz,
    div.st-bn.st-cm.st-d0,
    div.st-bn.st-cm.st-cw.st-cx,
    div.st-bn.st-cm.st-cw.st-cy,
    div.st-bn.st-cm.st-cw.st-cz,
    div.st-bn.st-cm.st-cw.st-d0,
    div.st-bn.st-cm.st-cw.st-cx.st-cy,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0.st-cg,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0.st-cg.st-d1,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0.st-cg.st-d1.st-d2,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0.st-cg.st-d1.st-d2.st-d3,
    div.st-bn.st-cm.st-cw.st-cx.st-cy.st-b3.st-cz.st-d0.st-cg.st-d1.st-d2.st-d3.st-d4 {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* All input elements */
    input[type="text"],
    input[type="number"],
    input[type="search"],
    select,
    textarea {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
        caret-color: var(--dark-text) !important; /* Make cursor visible */
    }
    
    /* Text input containers */
    .stTextInput > div > div > input,
    .stTextInput input {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
        caret-color: var(--dark-text) !important; /* Make cursor visible */
    }
    
    /* Number input */
    .stNumberInput > div > div > input {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
        caret-color: var(--dark-text) !important; /* Make cursor visible */
    }
    
    /* Number input step buttons (up/down arrows) */
    button[data-testid="stNumberInputStepUp"],
    button[data-testid="stNumberInputStepDown"],
    [data-testid="stNumberInputStepUp"],
    [data-testid="stNumberInputStepDown"],
    .stNumberInput button,
    .stNumberInput [class*="st-emotion-cache"] button,
    button.st-emotion-cache-hp90kq,
    button.eaba2yi2,
    [class*="st-emotion-cache-hp90kq"],
    [class*="eaba2yi2"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
    }
    
    button[data-testid="stNumberInputStepUp"]:hover:not(:disabled),
    button[data-testid="stNumberInputStepDown"]:hover:not(:disabled),
    [data-testid="stNumberInputStepUp"]:hover:not(:disabled),
    [data-testid="stNumberInputStepDown"]:hover:not(:disabled),
    .stNumberInput button:hover:not(:disabled) {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    
    button[data-testid="stNumberInputStepUp"]:disabled,
    button[data-testid="stNumberInputStepDown"]:disabled,
    [data-testid="stNumberInputStepUp"]:disabled,
    [data-testid="stNumberInputStepDown"]:disabled {
        opacity: 0.5 !important;
        cursor: not-allowed !important;
    }
    
    /* Ensure caret is visible in all text inputs */
    input,
    textarea {
        caret-color: var(--dark-text) !important;
    }
    
    /* Focus state - make caret more visible */
    input:focus,
    textarea:focus {
        caret-color: #ffffff !important; /* Bright white for visibility */
        outline-color: var(--dark-text) !important;
    }
    
    /* Multiselect */
    .stMultiSelect > div > div,
    .stMultiSelect [class*="st-bn"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
    }
    
    /* Checkbox */
    .stCheckbox label {
        color: var(--dark-text) !important;
    }
    
    /* Radio buttons */
    .stRadio label {
        color: var(--dark-text) !important;
    }
    
    /* Metrics */
    .stMetric {
        background-color: var(--dark-secondary-bg) !important;
        padding: 1rem;
        border-radius: 0.5rem;
        color: var(--dark-text) !important;
    }
    .stMetric label {
        color: var(--dark-text) !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: var(--dark-text) !important;
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: var(--dark-text) !important;
    }
    
    /* DataFrames */
    .stDataFrame {
        background-color: var(--dark-secondary-bg) !important;
    }
    .stDataFrame table {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    .stDataFrame th,
    .stDataFrame td {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
    }
    
    /* Element containers */
    .element-container {
        color: var(--dark-text) !important;
    }
    
    /* All text elements - exclude data frame editor using :not() */
    h1:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    h2:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    h3:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    h4:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    h5:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    h6:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    p:not(.dvn-scroller):not(.stDataFrameGlideDataEditor),
    span:not(.dvn-scroller):not(.stDataFrameGlideDataEditor) {
        color: var(--dark-text) !important;
    }
    
    /* General div rule - exclude data frame editor completely */
    div:not([class*="dvn-scroller"]):not([class*="stDataFrameGlideDataEditor"]):not(.dvn-scroller):not(.stDataFrameGlideDataEditor):not(.dvn-scroller *):not(.stDataFrameGlideDataEditor *) {
        color: var(--dark-text) !important;
    }
    
    /* Markdown */
    .stMarkdown {
        color: var(--dark-text) !important;
    }
    .stMarkdown p,
    .stMarkdown li,
    .stMarkdown ul,
    .stMarkdown ol {
        color: var(--dark-text) !important;
    }
    
    /* Expanders */
    .stExpander {
        background-color: var(--dark-secondary-bg) !important;
        border: 1px solid var(--dark-border) !important;
    }
    .stExpander label {
        color: var(--dark-text) !important;
    }
    .stExpander [data-testid="stExpander"] {
        background-color: var(--dark-secondary-bg) !important;
    }
    
    /* Buttons */
    .stButton > button {
        background-color: #1f77b4 !important;
        color: white !important;
        border-color: #1f77b4 !important;
    }
    .stButton > button:hover {
        background-color: #1565a0 !important;
    }
    
    /* Info/Warning/Success/Error boxes */
    .stInfo,
    .stWarning,
    .stSuccess,
    .stError {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        background-color: var(--dark-secondary-bg) !important;
    }
    .stTabs [data-baseweb="tab"] {
        color: var(--dark-text) !important;
        background-color: var(--dark-secondary-bg) !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: var(--dark-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Dropdown menus */
    [role="listbox"],
    [role="option"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* BaseWeb components (used by Streamlit for dropdowns) */
    [data-baseweb="select"],
    [data-baseweb="popover"],
    [data-baseweb="menu"],
    [data-baseweb="menu-item"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Override any white backgrounds */
    div[style*="background-color: rgb(255, 255, 255)"],
    div[style*="background-color:white"],
    div[style*="background-color:#ffffff"],
    div[style*="background-color:#fff"] {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Ensure text is visible in all Streamlit components */
    .stSelectbox,
    .stTextInput,
    .stNumberInput,
    .stMultiselect,
    .stRadio,
    .stCheckbox {
        color: var(--dark-text) !important;
    }
    
    /* Fix for any remaining white backgrounds - but exclude data frame editor */
    *:not(.dvn-scroller):not(.stDataFrameGlideDataEditor):not([class*="dvn-scroller"]):not([class*="stDataFrameGlideDataEditor"]):not(.dvn-scroller *):not(.stDataFrameGlideDataEditor *) {
        color: inherit;
    }
    
    /* Specific fix for toolbar text */
    .stAppToolbar *,
    [data-testid="stToolbar"] * {
        color: var(--dark-text) !important;
    }
    
    /* List items (like in expanders/menus) */
    li,
    li[class*="st-emotion"],
    li[class*="e8lvnlb5"],
    li[class*="e1e4lema2"],
    .st-emotion-cache-1jfa4hj,
    .e8lvnlb5 {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Secondary buttons */
    button[data-testid="stBaseButton-secondary"],
    button[data-testid="stBaseButton-secondary"] *,
    button.kind-secondary,
    button[class*="st-emotion-cache-1v8qxnj"],
    button[class*="st-emotion-cache-g4hvad"],
    .st-emotion-cache-1v8qxnj,
    .st-emotion-cache-g4hvad,
    .e1e4lema2 {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
        border-color: var(--dark-border) !important;
    }
    
    button[data-testid="stBaseButton-secondary"]:hover,
    button.kind-secondary:hover {
        background-color: var(--dark-input-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* Element toolbar containers */
    [data-testid="stElementToolbarButtonContainer"],
    div[data-testid="stElementToolbarButtonContainer"],
    .st-emotion-cache-rw8y77,
    .et0utro1 {
        background-color: var(--dark-secondary-bg) !important;
        color: var(--dark-text) !important;
    }
    
    /* All emotion cache classes - but exclude data frame editor */
    [class*="st-emotion-cache"]:not(.stDataFrameGlideDataEditor):not([class*="dvn-scroller"]),
    [class*="e1e4lema2"],
    [class*="e8lvnlb5"],
    [class*="et0utro1"] {
        color: var(--dark-text) !important;
    }
    
    /* ========================================================================
       Data Frame Editor - COMPLETELY EXCLUDED from dark mode
       No styling applied - preserves original appearance
       ======================================================================== */
    
    /* Exclude data frame editor from all dark mode rules */
    .dvn-scroller,
    .stDataFrameGlideDataEditor,
    div[class*="dvn-scroller"],
    div[class*="stDataFrameGlideDataEditor"],
    .dvn-scroller *,
    .stDataFrameGlideDataEditor *,
    div[class*="dvn-scroller"] *,
    div[class*="stDataFrameGlideDataEditor"] * {
        /* No dark mode styling - use original Streamlit defaults */
        color: initial !important;
        background-color: initial !important;
    }
</style>
"""

def apply_dark_mode():
    """Apply dark mode CSS if enabled"""
    if st.session_state.get('dark_mode', False):
        st.markdown(DARK_MODE_CSS, unsafe_allow_html=True)

def get_database():
    """Get or create the database instance"""
    if 'db' not in st.session_state:
        st.session_state.db = EntityDatabase(DB_PATH)
    return st.session_state.db

def load_mapping_data(entity_type='financial', transaction_type='pledge', use_database=True):
    """
    Load mapping data from database or CSV
    
    Args:
        entity_type: Entity type ('financial' or 'non_financial')
        transaction_type: Transaction type ('pledge' or 'release')
        use_database: If True, use database. If False, use CSV
    """
    try:
        suffix = f"_{transaction_type}" if transaction_type != 'pledge' else ""
        
        if use_database:
            db = get_database()
            
            # Try loading with transaction type suffix first
            db_entity_type = f"{entity_type}_{transaction_type}"
            try:
                stats = db.get_statistics(db_entity_type)
                if stats['total_names'] > 0:
                    return db.load_entities(db_entity_type)
            except:
                pass
            
            # Fallback to legacy entity_type without transaction suffix
            try:
                stats = db.get_statistics(entity_type)
                if stats['total_names'] > 0:
                    return db.load_entities(entity_type)
            except:
                pass
            
            # If no data in DB, try importing from CSV
            csv_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete{suffix}.csv"
            if csv_path.exists():
                # Import from CSV to database
                with st.spinner("Migrating data from CSV to database..."):
                    db.import_from_csv(csv_path, db_entity_type, clear_existing=True)
                return db.load_entities(db_entity_type)
            else:
                # Try legacy file name (without suffix)
                csv_path_legacy = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
                if csv_path_legacy.exists():
                    with st.spinner("Migrating data from CSV to database..."):
                        db.import_from_csv(csv_path_legacy, db_entity_type, clear_existing=True)
                    return db.load_entities(db_entity_type)
                return None
        else:
            # Load directly from CSV (legacy mode)
            file_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete{suffix}.csv"
            if not file_path.exists():
                # Try legacy file name
                file_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
                if not file_path.exists():
                    return None
            return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None

def initialize_session_state():
    """Initialize session state"""
    if 'df_original' not in st.session_state:
        st.session_state.df_original = None
    if 'df_edited' not in st.session_state:
        st.session_state.df_edited = None
    if 'changes_made' not in st.session_state:
        st.session_state.changes_made = False
    if 'edit_history' not in st.session_state:
        st.session_state.edit_history = []
    if 'selected_entity_id' not in st.session_state:
        st.session_state.selected_entity_id = None
    if 'use_database' not in st.session_state:
        st.session_state.use_database = True  # Use database by default
    if 'dark_mode' not in st.session_state:
        st.session_state.dark_mode = False
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = None
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1
    if 'groups_per_page' not in st.session_state:
        st.session_state.groups_per_page = 25
    if 'last_filter_state' not in st.session_state:
        st.session_state.last_filter_state = None
    if 'transaction_type' not in st.session_state:
        st.session_state.transaction_type = 'pledge'

@st.cache_data
def group_by_entity(df):
    """Group names by entity_id (with cache to improve performance)"""
    grouped = defaultdict(list)
    for _, row in df.iterrows():
        grouped[row['entity_id']].append({
            'original_name': row['original_name'],
            'normalized_name': row['normalized_name'],
            'standard_name': row['standard_name'],
            'frequency': row['frequency'],
            'component_size': row['component_size'],
            'avg_similarity': row.get('avg_similarity'),
            'min_similarity': row.get('min_similarity'),
            'needs_review': row.get('needs_review', False)
        })
    return dict(grouped)

def calculate_group_stats(group):
    """Calculate statistics for a group"""
    total_freq = sum(n['frequency'] for n in group)
    names_count = len(group)
    standard_name = group[0]['standard_name'] if group else ""
    
    # Obtener similitudes si existen
    similarities = [n['avg_similarity'] for n in group if n['avg_similarity'] is not None]
    avg_sim = sum(similarities) / len(similarities) if similarities else None
    
    return {
        'total_frequency': total_freq,
        'names_count': names_count,
        'standard_name': standard_name,
        'avg_similarity': avg_sim
    }

def apply_changes(df, changes):
    """Apply changes to dataframe"""
    df_new = df.copy()
    
    for change in changes:
        change_type = change['type']
        
        if change_type == 'move_name':
            # Move a name to another entity_id
            old_entity_id = change['old_entity_id']
            new_entity_id = change['new_entity_id']
            original_name = change['original_name']
            
            # Update entity_id
            mask = (df_new['entity_id'] == old_entity_id) & (df_new['original_name'] == original_name)
            if mask.any():
                df_new.loc[mask, 'entity_id'] = new_entity_id
                # Update standard_name of the new group
                new_group = df_new[df_new['entity_id'] == new_entity_id]
                if len(new_group) > 0:
                    new_standard = new_group.iloc[0]['standard_name']
                    df_new.loc[mask, 'standard_name'] = new_standard
                # Update component_size
                df_new.loc[mask, 'component_size'] = len(df_new[df_new['entity_id'] == new_entity_id])
        
        elif change_type == 'split_group':
            # Split a group: create new entity_id
            old_entity_id = change['old_entity_id']
            names_to_split = change['names']
            new_entity_id = change['new_entity_id']
            
            # Move names to new group
            mask = (df_new['entity_id'] == old_entity_id) & (df_new['original_name'].isin(names_to_split))
            if mask.any():
                df_new.loc[mask, 'entity_id'] = new_entity_id
                # The standard_name will be the most frequent of the new group
                new_group = df_new[df_new['entity_id'] == new_entity_id]
                if len(new_group) > 0:
                    new_standard = new_group.nlargest(1, 'frequency').iloc[0]['normalized_name']
                    df_new.loc[mask, 'standard_name'] = new_standard
                # Update component_size
                for entity_id in [old_entity_id, new_entity_id]:
                    size = len(df_new[df_new['entity_id'] == entity_id])
                    df_new.loc[df_new['entity_id'] == entity_id, 'component_size'] = size
        
        elif change_type == 'merge_groups':
            # Merge groups
            source_entity_id = change['source_entity_id']
            target_entity_id = change['target_entity_id']
            
            # Move all names to target group
            mask = df_new['entity_id'] == source_entity_id
            if mask.any():
                df_new.loc[mask, 'entity_id'] = target_entity_id
                # Update standard_name to target group's
                target_standard = df_new[df_new['entity_id'] == target_entity_id].iloc[0]['standard_name']
                df_new.loc[mask, 'standard_name'] = target_standard
                # Update component_size
                size = len(df_new[df_new['entity_id'] == target_entity_id])
                df_new.loc[df_new['entity_id'] == target_entity_id, 'component_size'] = size
        
        elif change_type == 'change_standard_name':
            # Change the standard name of a group
            entity_id = change['entity_id']
            new_standard_name = change['new_standard_name']
            
            mask = df_new['entity_id'] == entity_id
            df_new.loc[mask, 'standard_name'] = new_standard_name
    
    return df_new

def get_next_entity_id(df, prefix='financial'):
    """Get the next available ID for a new entity"""
    existing_ids = df['entity_id'].unique()
    max_num = 0
    
    for eid in existing_ids:
        match = re.match(rf'{prefix}_(\d+)', str(eid))
        if match:
            num = int(match.group(1))
            max_num = max(max_num, num)
    
    return f"{prefix}_{max_num + 1}"

def save_changes(df, entity_type='financial', transaction_type='pledge', backup=False, use_database=True):
    """
    Save changes to database or CSV
    
    Args:
        df: DataFrame with changes
        entity_type: Entity type
        transaction_type: Transaction type ('pledge' or 'release')
        backup: Whether to create backup (default False, only created when explicitly requested)
        use_database: If True, save to database. If False, save CSV
    
    Returns:
        Path of saved file or confirmation message
    """
    suffix = f"_{transaction_type}" if transaction_type != 'pledge' else ""
    
    if use_database:
        try:
            db = get_database()
            
            # Create database backup if requested
            if backup:
                backup_path = db.backup_database()
            
            # Update entities in database with transaction type
            db_entity_type = f"{entity_type}_{transaction_type}"
            db.update_entities(df, db_entity_type)
            
            return f"Changes saved to database: {DB_PATH.name}"
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            raise
    else:
        # Legacy mode: save CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create backup if requested
        if backup:
            original_file = RESULTS_DIR / f"{entity_type}_entity_mapping_complete{suffix}.csv"
            if original_file.exists():
                backup_file = MANUAL_REVIEW_DIR / f"{entity_type}_{transaction_type}_backup_{timestamp}.csv"
                df_original = pd.read_csv(original_file)
                df_original.to_csv(backup_file, index=False)
        
        # Save edited file
        edited_file = MANUAL_REVIEW_DIR / f"{entity_type}_{transaction_type}_entity_mapping_edited_{timestamp}.csv"
        df.to_csv(edited_file, index=False)
        
        # Also save as "latest" file
        latest_file = MANUAL_REVIEW_DIR / f"{entity_type}_{transaction_type}_entity_mapping_edited_latest.csv"
        df.to_csv(latest_file, index=False)
        
        return edited_file, latest_file

# ============================================================================
# MAIN INTERFACE
# ============================================================================

def main():
    initialize_session_state()
    
    # Apply dark mode if enabled
    apply_dark_mode()
    
    st.title("🔍 Entity Review and Editing")
    
    # Note about WebSocket errors (collapsible)
    with st.expander("ℹ️ Note about terminal errors"):
        st.info("""
        If you see `WebSocketClosedError` errors in the terminal, **don't worry**. 
        These are common and harmless Streamlit errors that occur when the browser 
        closes the connection unexpectedly. **They do not affect the application's functionality.**
        
        You can ignore them completely. The application will continue to work normally.
        """)
    
    st.markdown("---")
    
    # Sidebar: Entity type selection
    with st.sidebar:
        st.header("Settings")
        
        # Dark mode toggle
        dark_mode = st.checkbox("🌙 Dark Mode", value=st.session_state.dark_mode,
                                help="Toggle dark mode theme")
        if dark_mode != st.session_state.dark_mode:
            st.session_state.dark_mode = dark_mode
            st.rerun()
        
        st.markdown("---")
        
        entity_type = st.selectbox(
            "Entity Type",
            ['financial', 'non_financial'],
            index=0
        )
        
        transaction_type = st.selectbox(
            "Transaction Type",
            ['pledge', 'release'],
            index=0 if st.session_state.transaction_type == 'pledge' else 1,
            help="Select whether to view pledge or release transactions"
        )
        st.session_state.transaction_type = transaction_type
        
        # Toggle to use database or CSV
        use_db = st.checkbox("Use SQLite Database", value=st.session_state.use_database,
                            help="If enabled, uses SQLite database. Otherwise, uses CSV files.")
        st.session_state.use_database = use_db
        
        if st.button("🔄 Load Data", type="primary"):
            with st.spinner("Loading data..."):
                df = load_mapping_data(entity_type, transaction_type=transaction_type, use_database=use_db)
                if df is not None:
                    st.session_state.df_original = df.copy()
                    st.session_state.df_edited = df.copy()
                    st.session_state.changes_made = False
                    st.session_state.edit_history = []
                    if use_db:
                        db = get_database()
                        stats = db.get_statistics(entity_type)
                        st.success(f"✓ Loaded {len(df):,} names from database")
                        st.info(f"📊 {stats['unique_entities']:,} unique entities in database")
                    else:
                        st.success(f"✓ Loaded {len(df):,} names from CSV")
                else:
                    st.error(f"File not found for {entity_type}")
        
        st.markdown("---")
        
        if st.session_state.df_edited is not None:
            st.subheader("Status")
            total_entities = st.session_state.df_edited['entity_id'].nunique()
            total_names = len(st.session_state.df_edited)
            
            st.metric("Unique Entities", f"{total_entities:,}")
            st.metric("Total Names", f"{total_names:,}")
            
            if st.session_state.changes_made:
                st.warning("⚠️ There are unsaved changes")
            
            st.markdown("---")
            
            if st.button("💾 Save Changes", type="primary", disabled=not st.session_state.changes_made):
                with st.spinner("Saving changes..."):
                    try:
                        result = save_changes(
                            st.session_state.df_edited,
                            entity_type=entity_type,
                            transaction_type=st.session_state.transaction_type,
                            use_database=st.session_state.use_database
                        )
                        st.session_state.changes_made = False
                        # Clear cache after saving
                        group_by_entity.clear()
                        
                        if st.session_state.use_database:
                            st.success(f"✓ Changes saved to database")
                            st.info(f"📁 Database: `database/entities.db`")
                            st.balloons()
                        else:
                            edited_file, latest_file = result
                            st.success(f"✓ Changes saved to:\n{edited_file.name}")
                            st.info(f"Latest file: {latest_file.name}")
                            st.balloons()
                    except Exception as e:
                        st.error(f"❌ Error saving: {str(e)}")
                        logger.exception("Error saving changes")
        
        st.markdown("---")
        
        # Database management
        if st.session_state.use_database:
            st.subheader("🗄️ Database Management")
            
            col_db1, col_db2 = st.columns(2)
            
            with col_db1:
                if st.button("📥 Export to CSV", help="Export current data to CSV"):
            try:
                db = get_database()
                db_entity_type = f"{entity_type}_{st.session_state.transaction_type}"
                export_path = MANUAL_REVIEW_DIR / f"{entity_type}_{st.session_state.transaction_type}_exported_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                db.export_to_csv(db_entity_type, export_path)
                st.success(f"✓ Exported to: {export_path.name}")
            except Exception as e:
                st.error(f"Error exporting: {e}")
            
            with col_db2:
                if st.button("💾 Create Backup", help="Create a database backup"):
                    try:
                        db = get_database()
                        backup_path = db.backup_database()
                        st.success(f"✓ Backup created: {backup_path.name}")
                    except Exception as e:
                        st.error(f"Error creating backup: {e}")
            
            # Database information
            try:
                db = get_database()
                db_entity_type = f"{entity_type}_{st.session_state.transaction_type}"
                stats = db.get_statistics(db_entity_type)
                
                with st.expander("📊 Database Statistics"):
                    st.metric("Total Names", f"{stats['total_names']:,}")
                    st.metric("Unique Entities", f"{stats['unique_entities']:,}")
                    st.metric("Average Group Size", f"{stats['avg_group_size']:.1f}")
                    st.metric("Large Groups (>20)", f"{stats['large_groups']:,}")
                    
                    # Database file size
                    if DB_PATH.exists():
                        db_size = DB_PATH.stat().st_size / (1024 * 1024)  # MB
                        st.metric("Database Size", f"{db_size:.2f} MB")
            except Exception as e:
                st.warning(f"Could not load statistics: {e}")
        
        st.markdown("---")
        st.markdown("### 📝 Change History")
        if st.session_state.edit_history:
            for i, change in enumerate(st.session_state.edit_history[-5:], 1):
                st.text(f"{i}. {change}")
        else:
            st.text("No changes yet")
    
    # Main content
    if st.session_state.df_edited is None:
        st.info("👈 Please load data from the sidebar panel")
        return
    
    # Tabs for different views
    # Determine which tab to show first (if Edit button was clicked)
    tab_labels = ["📋 Group View", "🔍 Search", "✏️ Edit Group", "📊 Statistics"]
    
    # If active_tab is set and selected_entity_id exists, switch to Edit tab
    if st.session_state.active_tab == "edit" and st.session_state.selected_entity_id:
        # Use JavaScript to switch to the Edit tab (index 2)
        # This runs after Streamlit finishes rendering
        st.markdown("""
        <script>
        function switchToEditTab() {
            var tabs = document.querySelectorAll('[data-baseweb="tab"]');
            if (tabs.length > 2) {
                tabs[2].click();
                return true;
            }
            return false;
        }
        
        // Try immediately
        if (!switchToEditTab()) {
            // If tabs aren't ready, wait a bit and try again
            setTimeout(function() {
                if (!switchToEditTab()) {
                    // Last attempt after a longer delay
                    setTimeout(switchToEditTab, 500);
                }
            }, 200);
        }
        </script>
        """, unsafe_allow_html=True)
        st.session_state.active_tab = None  # Reset after switching
    
    tab1, tab2, tab3, tab4 = st.tabs(tab_labels)
    
    # TAB 1: Group View
    with tab1:
        st.header("Group View")
        
        # Performance information
        if st.session_state.df_edited is not None:
            total_entities = st.session_state.df_edited['entity_id'].nunique()
            total_names = len(st.session_state.df_edited)
            if total_entities > 1000:
                st.info(f"ℹ️ **Optimization active**: With {total_entities:,} entities, it's recommended to use filters to improve performance. Singletons are hidden by default.")
        
        # Filters
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                ['Total Frequency', 'Number of Names', 'Entity ID'],
                index=0
            )
        with col2:
            filter_size = st.selectbox(
                "Filter by Size",
                ['No Singletons (<2)', 'All', 'Large (>20)', 'Medium (5-20)', 'Small (2-5)'],
                index=0  # Hide singletons by default
            )
        with col3:
            filter_review = st.checkbox("Only groups marked for review", value=False)
        with col4:
            # Quick search by ID or standard name
            quick_search = st.text_input("🔍 Quick Search (ID or name)", "")
        
        # Check if filters changed and reset page if needed
        current_filter_state = (sort_by, filter_size, filter_review, quick_search)
        if st.session_state.last_filter_state is not None and current_filter_state != st.session_state.last_filter_state:
            st.session_state.current_page = 1
        st.session_state.last_filter_state = current_filter_state
        
        # Group data (already has cache in the function)
        grouped = group_by_entity(st.session_state.df_edited)
        
        # Filter groups
        filtered_groups = {}
        for entity_id, names in grouped.items():
            stats = calculate_group_stats(names)
            
            # Filter by size (hide singletons by default)
            if filter_size == 'No Singletons (<2)' and stats['names_count'] < 2:
                continue
            elif filter_size == 'Large (>20)' and stats['names_count'] <= 20:
                continue
            elif filter_size == 'Medium (5-20)' and not (5 <= stats['names_count'] <= 20):
                continue
            elif filter_size == 'Small (2-5)' and not (2 <= stats['names_count'] < 5):
                continue
            
            # Filter by review
            if filter_review and not any(n.get('needs_review', False) for n in names):
                continue
            
            # Quick search
            if quick_search:
                search_lower = quick_search.lower()
                if (search_lower not in entity_id.lower() and 
                    search_lower not in stats['standard_name'].lower()):
                    continue
            
            filtered_groups[entity_id] = names
        
        # Sort groups
        if sort_by == 'Total Frequency':
            sorted_groups = sorted(
                filtered_groups.items(),
                key=lambda x: calculate_group_stats(x[1])['total_frequency'],
                reverse=True
            )
        elif sort_by == 'Number of Names':
            sorted_groups = sorted(
                filtered_groups.items(),
                key=lambda x: calculate_group_stats(x[1])['names_count'],
                reverse=True
            )
        else:
            sorted_groups = sorted(filtered_groups.items())
        
        # Pagination controls - moved to main content area
        groups_per_page = st.session_state.groups_per_page
        total_pages = (len(sorted_groups) + groups_per_page - 1) // groups_per_page if len(sorted_groups) > 0 else 1
        
        # Ensure current_page is within valid range
        if st.session_state.current_page > total_pages:
            st.session_state.current_page = total_pages
        if st.session_state.current_page < 1:
            st.session_state.current_page = 1
        
        page_number = st.session_state.current_page
        
        # Pagination controls row - compact layout
        col_pag1, col_pag2, col_pag3, col_pag4 = st.columns([2, 2, 3, 3])
        
        with col_pag1:
            st.markdown("**Groups per page:**")
            new_groups_per_page = st.selectbox(
                "Groups per Page",
                [10, 25, 50, 100, 250],
                index=[10, 25, 50, 100, 250].index(groups_per_page) if groups_per_page in [10, 25, 50, 100, 250] else 1,
                label_visibility="collapsed",
                key="groups_per_page_selector"
            )
            if new_groups_per_page != groups_per_page:
                st.session_state.groups_per_page = new_groups_per_page
                # Recalculate page number to stay on same relative position
                if total_pages > 1:
                    relative_position = (page_number - 1) / total_pages
                    new_total_pages = (len(sorted_groups) + new_groups_per_page - 1) // new_groups_per_page if len(sorted_groups) > 0 else 1
                    st.session_state.current_page = max(1, min(new_total_pages, int(relative_position * new_total_pages) + 1))
                st.rerun()
        
        with col_pag2:
            if total_pages > 1:
                st.markdown("**Go to page:**")
                new_page = st.number_input(
                    "Go to page:",
                    min_value=1,
                    max_value=total_pages,
                    value=page_number,
                    step=1,
                    label_visibility="collapsed",
                    key="page_number_input_top"
                )
                if new_page != page_number:
                    st.session_state.current_page = new_page
                    st.rerun()
            else:
                st.markdown("**All groups shown**")
        
        with col_pag3:
            if total_pages > 1:
                st.markdown(f"**Page {page_number} of {total_pages}**")
                # Navigation buttons
                nav_col1, nav_col2, nav_col3, nav_col4 = st.columns(4)
                with nav_col1:
                    if st.button("⏮️ First", disabled=(page_number == 1), use_container_width=True, key="nav_first_top"):
                        st.session_state.current_page = 1
                        st.rerun()
                with nav_col2:
                    if st.button("◀️ Previous", disabled=(page_number == 1), use_container_width=True, key="nav_prev_top"):
                        st.session_state.current_page = max(1, page_number - 1)
                        st.rerun()
                with nav_col3:
                    if st.button("Next ▶️", disabled=(page_number >= total_pages), use_container_width=True, key="nav_next_top"):
                        st.session_state.current_page = min(total_pages, page_number + 1)
                        st.rerun()
                with nav_col4:
                    if st.button("Last ⏭️", disabled=(page_number >= total_pages), use_container_width=True, key="nav_last_top"):
                        st.session_state.current_page = total_pages
                        st.rerun()
        
        # Calculate paginated groups
        if total_pages > 1:
            start_idx = (page_number - 1) * groups_per_page
            end_idx = start_idx + groups_per_page
            paginated_groups = sorted_groups[start_idx:end_idx]
        else:
            paginated_groups = sorted_groups
        
        # Show information
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.metric("Total Groups Found", f"{len(sorted_groups):,}")
        with col_info2:
            st.metric("Showing Groups", f"{len(paginated_groups):,}")
        with col_info3:
            if total_pages > 1:
                st.metric("Page", f"{page_number}/{total_pages}")
            else:
                st.metric("Page", "1/1")
        
        st.markdown("---")
        
        # Show paginated groups
        if len(paginated_groups) == 0:
            st.warning("No groups found with the selected filters.")
        else:
            for entity_id, names in paginated_groups:
                stats = calculate_group_stats(names)
                
                with st.expander(
                    f"**{entity_id}** | {stats['names_count']} names | "
                    f"Frequency: {stats['total_frequency']:,} | "
                    f"Standard: {stats['standard_name'][:50]}..."
                ):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Standard Name:** {stats['standard_name']}")
                        if stats['avg_similarity']:
                            st.markdown(f"**Average Similarity:** {stats['avg_similarity']:.1f}%")
                        
                        st.markdown("**Names in this group:**")
                        names_df = pd.DataFrame(names)
                        names_df = names_df[['original_name', 'normalized_name', 'frequency']]
                        names_df = names_df.sort_values('frequency', ascending=False)
                        
                        # Limit dataframe height for large groups
                        max_rows_to_show = 20
                        if len(names_df) > max_rows_to_show:
                            st.dataframe(
                                names_df.head(max_rows_to_show),
                                use_container_width=True,
                                hide_index=True,
                                height=400
                            )
                            st.info(f"Showing first {max_rows_to_show} of {len(names_df)} names. Use the 'Edit Group' tab to see all.")
                        else:
                            st.dataframe(
                                names_df,
                                use_container_width=True,
                                hide_index=True,
                                height=min(400, len(names) * 35 + 50)
                            )
                    
                    with col2:
                        st.markdown("**Actions:**")
                        if st.button("✏️ Edit", key=f"edit_{entity_id}"):
                            st.session_state.selected_entity_id = entity_id
                            st.session_state.active_tab = "edit"
                            st.rerun()
            
            # Navigation buttons at the bottom of the page
            if total_pages > 1:
                st.markdown("---")
                st.markdown("### Navigation")
                col_bot1, col_bot2, col_bot3, col_bot4, col_bot5 = st.columns([1, 1, 1, 1, 2])
                
                with col_bot1:
                    if st.button("⏮️ First", disabled=(page_number == 1), use_container_width=True, key="nav_first_bottom"):
                        st.session_state.current_page = 1
                        st.rerun()
                with col_bot2:
                    if st.button("◀️ Previous", disabled=(page_number == 1), use_container_width=True, key="nav_prev_bottom"):
                        st.session_state.current_page = max(1, page_number - 1)
                        st.rerun()
                with col_bot3:
                    st.markdown(f"**Page {page_number} of {total_pages}**")
                with col_bot4:
                    if st.button("Next ▶️", disabled=(page_number >= total_pages), use_container_width=True, key="nav_next_bottom"):
                        st.session_state.current_page = min(total_pages, page_number + 1)
                        st.rerun()
                with col_bot5:
                    if st.button("Last ⏭️", disabled=(page_number >= total_pages), use_container_width=True, key="nav_last_bottom"):
                        st.session_state.current_page = total_pages
                        st.rerun()
    
    # TAB 2: Search
    with tab2:
        st.header("Name Search")
        st.caption("🔍 Search names by any part of the text. For best results, search by keywords.")
        
        col_search1, col_search2 = st.columns([3, 1])
        with col_search1:
            search_query = st.text_input(
                "Search by name (original or normalized), entity ID or standard name",
                placeholder="E.g.: BANK OF AMERICA or financial_0"
            )
        with col_search2:
            max_results = st.selectbox("Result Limit", [50, 100, 250, 500], index=1)
        
        if search_query:
            with st.spinner("Searching..."):
                df_search = st.session_state.df_edited.copy()
                
                # Optimize search: only search if there are at least 3 characters
                if len(search_query) < 3:
                    st.warning("⚠️ Please enter at least 3 characters to search")
                else:
                    # Search in multiple columns (more efficient)
                    search_lower = search_query.lower()
                    mask = (
                        df_search['original_name'].str.lower().str.contains(search_lower, na=False) |
                        df_search['normalized_name'].str.lower().str.contains(search_lower, na=False) |
                        df_search['entity_id'].str.lower().str.contains(search_lower, na=False) |
                        df_search['standard_name'].str.lower().str.contains(search_lower, na=False)
                    )
                    
                    results = df_search[mask]
                    
                    if len(results) > 0:
                        # Limit results
                        if len(results) > max_results:
                            st.warning(f"⚠️ Found {len(results):,} results, showing only the first {max_results}")
                            results = results.head(max_results)
                        
                        st.success(f"✓ Found {len(results):,} results")
                        
                        # Group by entity_id
                        entity_ids_found = results['entity_id'].unique()
                        st.write(f"**In {len(entity_ids_found)} different entity(ies)**")
                        
                        # Pagination for multiple results
                        if len(entity_ids_found) > 10:
                            results_per_page = st.selectbox("Entities per Page", [5, 10, 20], index=1)
                            total_entity_pages = (len(entity_ids_found) + results_per_page - 1) // results_per_page
                            entity_page = st.number_input(
                                f"Entity Page (of {total_entity_pages})",
                                min_value=1,
                                max_value=total_entity_pages,
                                value=1,
                                step=1
                            )
                            start_idx = (entity_page - 1) * results_per_page
                            end_idx = start_idx + results_per_page
                            paginated_entities = entity_ids_found[start_idx:end_idx]
                        else:
                            paginated_entities = entity_ids_found
                        
                        for entity_id in paginated_entities:
                            entity_results = results[results['entity_id'] == entity_id]
                            
                            with st.expander(f"**{entity_id}** ({len(entity_results)} names found)"):
                                display_cols = ['original_name', 'normalized_name', 'standard_name', 'frequency']
                                st.dataframe(
                                    entity_results[display_cols].sort_values('frequency', ascending=False),
                                    use_container_width=True,
                                    hide_index=True,
                                    height=min(400, len(entity_results) * 35 + 50)
                                )
                                
                                if st.button("✏️ Edit", key=f"search_edit_{entity_id}"):
                                    st.session_state.selected_entity_id = entity_id
                                    st.session_state.active_tab = "edit"
                                    st.rerun()
                    else:
                        st.warning("No results found. Try other keywords.")
    
    # TAB 3: Edit Group
    with tab3:
        st.header("Edit Group")
        
        if st.session_state.selected_entity_id is None:
            st.info("Select a group to edit from the group view or search")
        else:
            entity_id = st.session_state.selected_entity_id
            entity_data = st.session_state.df_edited[
                st.session_state.df_edited['entity_id'] == entity_id
            ]
            
            if len(entity_data) == 0:
                st.warning("The selected group no longer exists")
                st.session_state.selected_entity_id = None
            else:
                st.subheader(f"Editing: **{entity_id}**")
                
                # Group information
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Number of Names", len(entity_data))
                with col2:
                    st.metric("Total Frequency", f"{entity_data['frequency'].sum():,}")
                with col3:
                    st.metric("Standard Name", entity_data.iloc[0]['standard_name'][:30] + "...")
                with col4:
                    current_size = entity_data.iloc[0]['component_size']
                    st.metric("Current Size", current_size)
                
                st.markdown("---")
                
                # Edit options
                edit_option = st.radio(
                    "What would you like to do?",
                    [
                        "Move names to another group",
                        "Split group (create new group)",
                        "Merge with another group",
                        "Change standard name",
                        "View all names"
                    ]
                )
                
                # Show group names
                st.markdown("### Names in this group:")
                names_display = entity_data[['original_name', 'normalized_name', 'frequency']].copy()
                names_display = names_display.sort_values('frequency', ascending=False)
                st.dataframe(names_display, use_container_width=True, hide_index=True)
                
                st.markdown("---")
                
                # OPTION 1: Move names to another group
                if edit_option == "Move names to another group":
                    st.subheader("Move names to another group")
                    
                    # Select names to move
                    names_to_move = st.multiselect(
                        "Select names to move:",
                        options=entity_data['original_name'].tolist(),
                        default=[]
                    )
                    
                    if names_to_move:
                        # Find target group
                        all_entity_ids = sorted(st.session_state.df_edited['entity_id'].unique().tolist())
                        target_entity_id = st.selectbox(
                            "Select target group:",
                            options=all_entity_ids,
                            index=0 if entity_id not in all_entity_ids else all_entity_ids.index(entity_id)
                        )
                        
                        if st.button("✅ Move Names", type="primary"):
                            # Get standard_name of target group before moving
                            target_group = st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == target_entity_id
                            ]
                            target_standard = target_group.iloc[0]['standard_name'] if len(target_group) > 0 else None
                            
                            # Move all selected names
                            for name in names_to_move:
                                mask = (st.session_state.df_edited['entity_id'] == entity_id) & \
                                       (st.session_state.df_edited['original_name'] == name)
                                st.session_state.df_edited.loc[mask, 'entity_id'] = target_entity_id
                                if target_standard:
                                    st.session_state.df_edited.loc[mask, 'standard_name'] = target_standard
                                st.session_state.edit_history.append(f"Move '{name[:50]}...' from {entity_id} to {target_entity_id}")
                            
                            # Update component_size for both groups
                            for eid in [entity_id, target_entity_id]:
                                size = len(st.session_state.df_edited[st.session_state.df_edited['entity_id'] == eid])
                                if size > 0:  # Only update if group still exists
                                    st.session_state.df_edited.loc[
                                        st.session_state.df_edited['entity_id'] == eid,
                                        'component_size'
                                    ] = size
                            
                            st.session_state.changes_made = True
                            st.success(f"✓ {len(names_to_move)} name(s) moved")
                            
                            # If original group is empty, clear selection
                            remaining_in_group = len(st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == entity_id
                            ])
                            if remaining_in_group == 0:
                                st.session_state.selected_entity_id = None
                                st.info("The original group is now empty and was removed")
                            
                            st.rerun()
                
                # OPTION 2: Split group
                elif edit_option == "Split group (create new group)":
                    st.subheader("Split Group")
                    
                    names_to_split = st.multiselect(
                        "Select names to create a new group:",
                        options=entity_data['original_name'].tolist(),
                        default=[]
                    )
                    
                    if names_to_split:
                        if st.button("✅ Create New Group", type="primary"):
                            # Create new entity_id
                            prefix = entity_id.split('_')[0]
                            new_entity_id = get_next_entity_id(st.session_state.df_edited, prefix)
                            
                            # Move names to new group
                            mask = (st.session_state.df_edited['entity_id'] == entity_id) & \
                                   (st.session_state.df_edited['original_name'].isin(names_to_split))
                            
                            st.session_state.df_edited.loc[mask, 'entity_id'] = new_entity_id
                            
                            # Set new standard_name (most frequent)
                            new_group = st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == new_entity_id
                            ]
                            new_standard = new_group.nlargest(1, 'frequency').iloc[0]['normalized_name']
                            st.session_state.df_edited.loc[mask, 'standard_name'] = new_standard
                            
                            # Update component_size
                            for eid in [entity_id, new_entity_id]:
                                size = len(st.session_state.df_edited[st.session_state.df_edited['entity_id'] == eid])
                                st.session_state.df_edited.loc[
                                    st.session_state.df_edited['entity_id'] == eid,
                                    'component_size'
                                ] = size
                            
                            st.session_state.edit_history.append(f"Split {entity_id}: {len(names_to_split)} names → {new_entity_id}")
                            st.session_state.changes_made = True
                            st.success(f"✓ Group split. New group: {new_entity_id}")
                            st.rerun()
                
                # OPTION 3: Merge with another group
                elif edit_option == "Merge with another group":
                    st.subheader("Merge with Another Group")
                    
                    all_entity_ids = sorted(st.session_state.df_edited['entity_id'].unique().tolist())
                    target_entity_id = st.selectbox(
                        "Select group to merge with:",
                        options=[eid for eid in all_entity_ids if eid != entity_id]
                    )
                    
                    if target_entity_id:
                        target_info = st.session_state.df_edited[
                            st.session_state.df_edited['entity_id'] == target_entity_id
                        ]
                        st.info(f"The target group has {len(target_info)} names")
                        
                        if st.button("✅ Merge Groups", type="primary"):
                            # Move all names to target group
                            mask = st.session_state.df_edited['entity_id'] == entity_id
                            st.session_state.df_edited.loc[mask, 'entity_id'] = target_entity_id
                            
                            # Update standard_name to target group's
                            target_standard = target_info.iloc[0]['standard_name']
                            st.session_state.df_edited.loc[mask, 'standard_name'] = target_standard
                            
                            # Update component_size
                            size = len(st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == target_entity_id
                            ])
                            st.session_state.df_edited.loc[
                                st.session_state.df_edited['entity_id'] == target_entity_id,
                                'component_size'
                            ] = size
                            
                            st.session_state.edit_history.append(f"Merge {entity_id} with {target_entity_id}")
                            st.session_state.changes_made = True
                            st.success(f"✓ Groups merged into {target_entity_id}")
                            st.session_state.selected_entity_id = None
                            st.rerun()
                
                # OPTION 4: Change standard name
                elif edit_option == "Change standard name":
                    st.subheader("Change Standard Name")
                    
                    current_standard = entity_data.iloc[0]['standard_name']
                    st.info(f"**Current standard name:** {current_standard}")
                    
                    # Options: select from existing names or write a new one
                    option = st.radio(
                        "Select from existing names or write new:",
                        ["Select Existing", "Write New"]
                    )
                    
                    if option == "Select Existing":
                        new_standard = st.selectbox(
                            "Select new standard name:",
                            options=sorted(entity_data['normalized_name'].unique())
                        )
                    else:
                        new_standard = st.text_input("New standard name:", value=current_standard)
                    
                    if st.button("✅ Change Standard Name", type="primary"):
                        mask = st.session_state.df_edited['entity_id'] == entity_id
                        st.session_state.df_edited.loc[mask, 'standard_name'] = new_standard
                        
                        st.session_state.edit_history.append(f"Change standard_name of {entity_id} to '{new_standard}'")
                        st.session_state.changes_made = True
                        st.success(f"✓ Standard name updated to: {new_standard}")
                        st.rerun()
    
    # TAB 4: Statistics
    with tab4:
        st.header("General Statistics")
        
        df = st.session_state.df_edited
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Names", f"{len(df):,}")
        with col2:
            st.metric("Unique Entities", f"{df['entity_id'].nunique():,}")
        with col3:
            grouped_sizes = df.groupby('entity_id').size()
            st.metric("Average Size", f"{grouped_sizes.mean():.1f}")
        with col4:
            st.metric("Maximum Size", f"{grouped_sizes.max():,}")
        
        st.markdown("---")
        
        # Size distribution
        st.subheader("Group Size Distribution")
        size_dist = df.groupby('entity_id').size().value_counts().sort_index()
        st.bar_chart(size_dist)
        
        # Top groups by frequency
        st.subheader("Top 10 Groups by Total Frequency")
        top_groups = df.groupby('entity_id').agg({
            'frequency': 'sum',
            'original_name': 'count'
        }).rename(columns={'original_name': 'count'}).sort_values('frequency', ascending=False).head(10)
        st.dataframe(top_groups, use_container_width=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Critical error in application: {str(e)}")
        st.info("Please reload the page or restart the application.")
        logger.exception("Critical error in application")

