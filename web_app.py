"""
Aplicación Web Interactiva para Revisión de Entidades
=====================================================
Aplicación Streamlit para revisar, editar y corregir agrupaciones de entidades.

Funcionalidades:
- Ver todas las entidades agrupadas
- Buscar por nombre o ID
- Mover nombres entre grupos
- Dividir grupos
- Unir grupos
- Crear nuevos grupos
- Guardar cambios
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

# Configurar logging para reducir ruido
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Configuración de paths
BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR / "results" / "final"
MANUAL_REVIEW_DIR = BASE_DIR / "results" / "manual_review"
MANUAL_REVIEW_DIR.mkdir(parents=True, exist_ok=True)

# Configuración de página
st.set_page_config(
    page_title="Revisión de Entidades",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings de Streamlit si es necesario
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# Cache para cargar datos
@st.cache_data(show_spinner=True)
def load_mapping_data(entity_type='financial'):
    """Carga los datos de mapeo"""
    try:
        file_path = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
        
        if not file_path.exists():
            return None
        
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logger.error(f"Error cargando datos: {e}")
        return None

def initialize_session_state():
    """Inicializa el estado de la sesión"""
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

@st.cache_data
def group_by_entity(df):
    """Agrupa nombres por entity_id (con cache para mejorar rendimiento)"""
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
    """Calcula estadísticas de un grupo"""
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
    """Aplica cambios al dataframe"""
    df_new = df.copy()
    
    for change in changes:
        change_type = change['type']
        
        if change_type == 'move_name':
            # Mover un nombre a otro entity_id
            old_entity_id = change['old_entity_id']
            new_entity_id = change['new_entity_id']
            original_name = change['original_name']
            
            # Actualizar entity_id
            mask = (df_new['entity_id'] == old_entity_id) & (df_new['original_name'] == original_name)
            if mask.any():
                df_new.loc[mask, 'entity_id'] = new_entity_id
                # Actualizar standard_name del nuevo grupo
                new_group = df_new[df_new['entity_id'] == new_entity_id]
                if len(new_group) > 0:
                    new_standard = new_group.iloc[0]['standard_name']
                    df_new.loc[mask, 'standard_name'] = new_standard
                # Actualizar component_size
                df_new.loc[mask, 'component_size'] = len(df_new[df_new['entity_id'] == new_entity_id])
        
        elif change_type == 'split_group':
            # Dividir un grupo: crear nuevo entity_id
            old_entity_id = change['old_entity_id']
            names_to_split = change['names']
            new_entity_id = change['new_entity_id']
            
            # Mover nombres al nuevo grupo
            mask = (df_new['entity_id'] == old_entity_id) & (df_new['original_name'].isin(names_to_split))
            if mask.any():
                df_new.loc[mask, 'entity_id'] = new_entity_id
                # El standard_name será el más frecuente del nuevo grupo
                new_group = df_new[df_new['entity_id'] == new_entity_id]
                if len(new_group) > 0:
                    new_standard = new_group.nlargest(1, 'frequency').iloc[0]['normalized_name']
                    df_new.loc[mask, 'standard_name'] = new_standard
                # Actualizar component_size
                for entity_id in [old_entity_id, new_entity_id]:
                    size = len(df_new[df_new['entity_id'] == entity_id])
                    df_new.loc[df_new['entity_id'] == entity_id, 'component_size'] = size
        
        elif change_type == 'merge_groups':
            # Unir grupos
            source_entity_id = change['source_entity_id']
            target_entity_id = change['target_entity_id']
            
            # Mover todos los nombres al grupo destino
            mask = df_new['entity_id'] == source_entity_id
            if mask.any():
                df_new.loc[mask, 'entity_id'] = target_entity_id
                # Actualizar standard_name al del grupo destino
                target_standard = df_new[df_new['entity_id'] == target_entity_id].iloc[0]['standard_name']
                df_new.loc[mask, 'standard_name'] = target_standard
                # Actualizar component_size
                size = len(df_new[df_new['entity_id'] == target_entity_id])
                df_new.loc[df_new['entity_id'] == target_entity_id, 'component_size'] = size
        
        elif change_type == 'change_standard_name':
            # Cambiar el nombre estándar de un grupo
            entity_id = change['entity_id']
            new_standard_name = change['new_standard_name']
            
            mask = df_new['entity_id'] == entity_id
            df_new.loc[mask, 'standard_name'] = new_standard_name
    
    return df_new

def get_next_entity_id(df, prefix='financial'):
    """Obtiene el siguiente ID disponible para una nueva entidad"""
    existing_ids = df['entity_id'].unique()
    max_num = 0
    
    for eid in existing_ids:
        match = re.match(rf'{prefix}_(\d+)', str(eid))
        if match:
            num = int(match.group(1))
            max_num = max(max_num, num)
    
    return f"{prefix}_{max_num + 1}"

def save_changes(df, entity_type='financial', backup=True):
    """Guarda los cambios en un nuevo archivo"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Crear backup si se solicita
    if backup:
        original_file = RESULTS_DIR / f"{entity_type}_entity_mapping_complete.csv"
        if original_file.exists():
            backup_file = MANUAL_REVIEW_DIR / f"{entity_type}_backup_{timestamp}.csv"
            df_original = pd.read_csv(original_file)
            df_original.to_csv(backup_file, index=False)
    
    # Guardar archivo editado
    edited_file = MANUAL_REVIEW_DIR / f"{entity_type}_entity_mapping_edited_{timestamp}.csv"
    df.to_csv(edited_file, index=False)
    
    # También guardar como el archivo "latest"
    latest_file = MANUAL_REVIEW_DIR / f"{entity_type}_entity_mapping_edited_latest.csv"
    df.to_csv(latest_file, index=False)
    
    return edited_file, latest_file

# ============================================================================
# INTERFAZ PRINCIPAL
# ============================================================================

def main():
    initialize_session_state()
    
    st.title("🔍 Revisión y Edición de Entidades Agrupadas")
    
    # Nota sobre errores de WebSocket (colapsable)
    with st.expander("ℹ️ Nota sobre errores en la terminal"):
        st.info("""
        Si ves errores de `WebSocketClosedError` en la terminal, **no te preocupes**. 
        Estos son errores comunes e inofensivos de Streamlit que ocurren cuando el navegador 
        cierra la conexión inesperadamente. **No afectan la funcionalidad de la aplicación.**
        
        Puedes ignorarlos completamente. La aplicación seguirá funcionando normalmente.
        """)
    
    st.markdown("---")
    
    # Sidebar: Selección de tipo de entidad
    with st.sidebar:
        st.header("Configuración")
        entity_type = st.selectbox(
            "Tipo de entidad",
            ['financial', 'non_financial'],
            index=0
        )
        
        if st.button("🔄 Cargar Datos", type="primary"):
            with st.spinner("Cargando datos..."):
                df = load_mapping_data(entity_type)
                if df is not None:
                    st.session_state.df_original = df.copy()
                    st.session_state.df_edited = df.copy()
                    st.session_state.changes_made = False
                    st.session_state.edit_history = []
                    st.success(f"✓ Cargados {len(df):,} nombres")
                else:
                    st.error(f"No se encontró el archivo para {entity_type}")
        
        st.markdown("---")
        
        if st.session_state.df_edited is not None:
            st.subheader("Estado")
            total_entities = st.session_state.df_edited['entity_id'].nunique()
            total_names = len(st.session_state.df_edited)
            
            st.metric("Entidades únicas", f"{total_entities:,}")
            st.metric("Total nombres", f"{total_names:,}")
            
            if st.session_state.changes_made:
                st.warning("⚠️ Hay cambios sin guardar")
            
            st.markdown("---")
            
            if st.button("💾 Guardar Cambios", type="primary", disabled=not st.session_state.changes_made):
                with st.spinner("Guardando cambios..."):
                    edited_file, latest_file = save_changes(
                        st.session_state.df_edited,
                        entity_type=entity_type
                    )
                    st.session_state.changes_made = False
                    # Limpiar cache después de guardar
                    group_by_entity.clear()
                    st.success(f"✓ Cambios guardados en:\n{edited_file.name}")
                    st.info(f"Archivo más reciente: {latest_file.name}")
                    st.balloons()  # Celebración visual
        
        st.markdown("---")
        st.markdown("### 📝 Historial de Cambios")
        if st.session_state.edit_history:
            for i, change in enumerate(st.session_state.edit_history[-5:], 1):
                st.text(f"{i}. {change}")
        else:
            st.text("No hay cambios aún")
    
    # Contenido principal
    if st.session_state.df_edited is None:
        st.info("👈 Por favor, carga los datos desde el panel lateral")
        return
    
    # Tabs para diferentes vistas
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Vista de Grupos",
        "🔍 Búsqueda",
        "✏️ Editar Grupo",
        "📊 Estadísticas"
    ])
    
    # TAB 1: Vista de Grupos
    with tab1:
        st.header("Vista de Grupos")
        
        # Información de rendimiento
        if st.session_state.df_edited is not None:
            total_entities = st.session_state.df_edited['entity_id'].nunique()
            total_names = len(st.session_state.df_edited)
            if total_entities > 1000:
                st.info(f"ℹ️ **Optimización activa**: Con {total_entities:,} entidades, se recomienda usar filtros para mejorar el rendimiento. Los singletons están ocultos por defecto.")
        
        # Filtros
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            sort_by = st.selectbox(
                "Ordenar por",
                ['Frecuencia total', 'Número de nombres', 'ID de entidad'],
                index=0
            )
        with col2:
            filter_size = st.selectbox(
                "Filtrar por tamaño",
                ['Sin singletons (<2)', 'Todos', 'Grandes (>20)', 'Medianos (5-20)', 'Pequeños (2-5)'],
                index=0  # Por defecto ocultar singletons
            )
        with col3:
            filter_review = st.checkbox("Solo grupos marcados para revisión", value=False)
        with col4:
            # Búsqueda rápida por ID o nombre estándar
            quick_search = st.text_input("🔍 Búsqueda rápida (ID o nombre)", "")
        
        # Agrupar datos (ya tiene cache en la función)
        grouped = group_by_entity(st.session_state.df_edited)
        
        # Filtrar grupos
        filtered_groups = {}
        for entity_id, names in grouped.items():
            stats = calculate_group_stats(names)
            
            # Filtro por tamaño (por defecto ocultar singletons)
            if filter_size == 'Sin singletons (<2)' and stats['names_count'] < 2:
                continue
            elif filter_size == 'Grandes (>20)' and stats['names_count'] <= 20:
                continue
            elif filter_size == 'Medianos (5-20)' and not (5 <= stats['names_count'] <= 20):
                continue
            elif filter_size == 'Pequeños (2-5)' and not (2 <= stats['names_count'] < 5):
                continue
            
            # Filtro por revisión
            if filter_review and not any(n.get('needs_review', False) for n in names):
                continue
            
            # Búsqueda rápida
            if quick_search:
                search_lower = quick_search.lower()
                if (search_lower not in entity_id.lower() and 
                    search_lower not in stats['standard_name'].lower()):
                    continue
            
            filtered_groups[entity_id] = names
        
        # Ordenar grupos
        if sort_by == 'Frecuencia total':
            sorted_groups = sorted(
                filtered_groups.items(),
                key=lambda x: calculate_group_stats(x[1])['total_frequency'],
                reverse=True
            )
        elif sort_by == 'Número de nombres':
            sorted_groups = sorted(
                filtered_groups.items(),
                key=lambda x: calculate_group_stats(x[1])['names_count'],
                reverse=True
            )
        else:
            sorted_groups = sorted(filtered_groups.items())
        
        # Paginación
        groups_per_page = st.sidebar.selectbox(
            "Grupos por página",
            [10, 25, 50, 100, 250],
            index=1 if len(sorted_groups) > 100 else 2
        )
        
        total_pages = (len(sorted_groups) + groups_per_page - 1) // groups_per_page
        
        if total_pages > 1:
            page_number = st.sidebar.number_input(
                f"Página (de {total_pages})",
                min_value=1,
                max_value=total_pages,
                value=1,
                step=1
            )
            start_idx = (page_number - 1) * groups_per_page
            end_idx = start_idx + groups_per_page
            paginated_groups = sorted_groups[start_idx:end_idx]
        else:
            paginated_groups = sorted_groups
            page_number = 1
        
        # Mostrar información
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.metric("Total grupos encontrados", f"{len(sorted_groups):,}")
        with col_info2:
            st.metric("Mostrando grupos", f"{len(paginated_groups):,}")
        with col_info3:
            if total_pages > 1:
                st.metric("Página", f"{page_number}/{total_pages}")
        
        st.markdown("---")
        
        # Mostrar grupos paginados
        if len(paginated_groups) == 0:
            st.warning("No se encontraron grupos con los filtros seleccionados.")
        else:
            for entity_id, names in paginated_groups:
                stats = calculate_group_stats(names)
                
                with st.expander(
                    f"**{entity_id}** | {stats['names_count']} nombres | "
                    f"Frecuencia: {stats['total_frequency']:,} | "
                    f"Estándar: {stats['standard_name'][:50]}..."
                ):
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown(f"**Nombre estándar:** {stats['standard_name']}")
                        if stats['avg_similarity']:
                            st.markdown(f"**Similitud promedio:** {stats['avg_similarity']:.1f}%")
                        
                        st.markdown("**Nombres en este grupo:**")
                        names_df = pd.DataFrame(names)
                        names_df = names_df[['original_name', 'normalized_name', 'frequency']]
                        names_df = names_df.sort_values('frequency', ascending=False)
                        
                        # Limitar altura de dataframe para grupos grandes
                        max_rows_to_show = 20
                        if len(names_df) > max_rows_to_show:
                            st.dataframe(
                                names_df.head(max_rows_to_show),
                                use_container_width=True,
                                hide_index=True,
                                height=400
                            )
                            st.info(f"Mostrando primeros {max_rows_to_show} de {len(names_df)} nombres. Usa la pestaña 'Editar Grupo' para ver todos.")
                        else:
                            st.dataframe(
                                names_df,
                                use_container_width=True,
                                hide_index=True,
                                height=min(400, len(names) * 35 + 50)
                            )
                    
                    with col2:
                        st.markdown("**Acciones:**")
                        if st.button("✏️ Editar", key=f"edit_{entity_id}"):
                            st.session_state.selected_entity_id = entity_id
                            st.rerun()
    
    # TAB 2: Búsqueda
    with tab2:
        st.header("Búsqueda de Nombres")
        st.caption("🔍 Busca nombres por cualquier parte del texto. Para mejores resultados, busca por palabras clave.")
        
        col_search1, col_search2 = st.columns([3, 1])
        with col_search1:
            search_query = st.text_input(
                "Buscar por nombre (original o normalizado), ID de entidad o nombre estándar",
                placeholder="Ej: BANK OF AMERICA o financial_0"
            )
        with col_search2:
            max_results = st.selectbox("Límite de resultados", [50, 100, 250, 500], index=1)
        
        if search_query:
            with st.spinner("Buscando..."):
                df_search = st.session_state.df_edited.copy()
                
                # Optimizar búsqueda: solo buscar si hay al menos 3 caracteres
                if len(search_query) < 3:
                    st.warning("⚠️ Por favor ingresa al menos 3 caracteres para buscar")
                else:
                    # Buscar en múltiples columnas (más eficiente)
                    search_lower = search_query.lower()
                    mask = (
                        df_search['original_name'].str.lower().str.contains(search_lower, na=False) |
                        df_search['normalized_name'].str.lower().str.contains(search_lower, na=False) |
                        df_search['entity_id'].str.lower().str.contains(search_lower, na=False) |
                        df_search['standard_name'].str.lower().str.contains(search_lower, na=False)
                    )
                    
                    results = df_search[mask]
                    
                    if len(results) > 0:
                        # Limitar resultados
                        if len(results) > max_results:
                            st.warning(f"⚠️ Se encontraron {len(results):,} resultados, mostrando solo los primeros {max_results}")
                            results = results.head(max_results)
                        
                        st.success(f"✓ Encontrados {len(results):,} resultados")
                        
                        # Agrupar por entity_id
                        entity_ids_found = results['entity_id'].unique()
                        st.write(f"**En {len(entity_ids_found)} entidad(es) diferente(s)**")
                        
                        # Paginación para resultados múltiples
                        if len(entity_ids_found) > 10:
                            results_per_page = st.selectbox("Entidades por página", [5, 10, 20], index=1)
                            total_entity_pages = (len(entity_ids_found) + results_per_page - 1) // results_per_page
                            entity_page = st.number_input(
                                f"Página de entidades (de {total_entity_pages})",
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
                            
                            with st.expander(f"**{entity_id}** ({len(entity_results)} nombres encontrados)"):
                                display_cols = ['original_name', 'normalized_name', 'standard_name', 'frequency']
                                st.dataframe(
                                    entity_results[display_cols].sort_values('frequency', ascending=False),
                                    use_container_width=True,
                                    hide_index=True,
                                    height=min(400, len(entity_results) * 35 + 50)
                                )
                                
                                if st.button("✏️ Editar", key=f"search_edit_{entity_id}"):
                                    st.session_state.selected_entity_id = entity_id
                                    st.rerun()
                    else:
                        st.warning("No se encontraron resultados. Intenta con otras palabras clave.")
    
    # TAB 3: Editar Grupo
    with tab3:
        st.header("Editar Grupo")
        
        if st.session_state.selected_entity_id is None:
            st.info("Selecciona un grupo para editar desde la vista de grupos o búsqueda")
        else:
            entity_id = st.session_state.selected_entity_id
            entity_data = st.session_state.df_edited[
                st.session_state.df_edited['entity_id'] == entity_id
            ]
            
            if len(entity_data) == 0:
                st.warning("El grupo seleccionado ya no existe")
                st.session_state.selected_entity_id = None
            else:
                st.subheader(f"Editando: **{entity_id}**")
                
                # Información del grupo
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Número de nombres", len(entity_data))
                with col2:
                    st.metric("Frecuencia total", f"{entity_data['frequency'].sum():,}")
                with col3:
                    st.metric("Nombre estándar", entity_data.iloc[0]['standard_name'][:30] + "...")
                with col4:
                    current_size = entity_data.iloc[0]['component_size']
                    st.metric("Tamaño actual", current_size)
                
                st.markdown("---")
                
                # Opciones de edición
                edit_option = st.radio(
                    "¿Qué quieres hacer?",
                    [
                        "Mover nombres a otro grupo",
                        "Dividir grupo (crear nuevo grupo)",
                        "Unir con otro grupo",
                        "Cambiar nombre estándar",
                        "Ver todos los nombres"
                    ]
                )
                
                # Mostrar nombres del grupo
                st.markdown("### Nombres en este grupo:")
                names_display = entity_data[['original_name', 'normalized_name', 'frequency']].copy()
                names_display = names_display.sort_values('frequency', ascending=False)
                st.dataframe(names_display, use_container_width=True, hide_index=True)
                
                st.markdown("---")
                
                # OPCION 1: Mover nombres a otro grupo
                if edit_option == "Mover nombres a otro grupo":
                    st.subheader("Mover nombres a otro grupo")
                    
                    # Seleccionar nombres a mover
                    names_to_move = st.multiselect(
                        "Selecciona nombres a mover:",
                        options=entity_data['original_name'].tolist(),
                        default=[]
                    )
                    
                    if names_to_move:
                        # Buscar grupo destino
                        all_entity_ids = sorted(st.session_state.df_edited['entity_id'].unique().tolist())
                        target_entity_id = st.selectbox(
                            "Selecciona el grupo destino:",
                            options=all_entity_ids,
                            index=0 if entity_id not in all_entity_ids else all_entity_ids.index(entity_id)
                        )
                        
                        if st.button("✅ Mover nombres", type="primary"):
                            # Obtener standard_name del grupo destino antes de mover
                            target_group = st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == target_entity_id
                            ]
                            target_standard = target_group.iloc[0]['standard_name'] if len(target_group) > 0 else None
                            
                            # Mover todos los nombres seleccionados
                            for name in names_to_move:
                                mask = (st.session_state.df_edited['entity_id'] == entity_id) & \
                                       (st.session_state.df_edited['original_name'] == name)
                                st.session_state.df_edited.loc[mask, 'entity_id'] = target_entity_id
                                if target_standard:
                                    st.session_state.df_edited.loc[mask, 'standard_name'] = target_standard
                                st.session_state.edit_history.append(f"Mover '{name[:50]}...' de {entity_id} a {target_entity_id}")
                            
                            # Actualizar component_size para ambos grupos
                            for eid in [entity_id, target_entity_id]:
                                size = len(st.session_state.df_edited[st.session_state.df_edited['entity_id'] == eid])
                                if size > 0:  # Solo actualizar si el grupo aún existe
                                    st.session_state.df_edited.loc[
                                        st.session_state.df_edited['entity_id'] == eid,
                                        'component_size'
                                    ] = size
                            
                            st.session_state.changes_made = True
                            st.success(f"✓ {len(names_to_move)} nombre(s) movido(s)")
                            
                            # Si el grupo original quedó vacío, limpiar selección
                            remaining_in_group = len(st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == entity_id
                            ])
                            if remaining_in_group == 0:
                                st.session_state.selected_entity_id = None
                                st.info("El grupo original quedó vacío y fue eliminado")
                            
                            st.rerun()
                
                # OPCION 2: Dividir grupo
                elif edit_option == "Dividir grupo (crear nuevo grupo)":
                    st.subheader("Dividir grupo")
                    
                    names_to_split = st.multiselect(
                        "Selecciona nombres para crear un nuevo grupo:",
                        options=entity_data['original_name'].tolist(),
                        default=[]
                    )
                    
                    if names_to_split:
                        if st.button("✅ Crear nuevo grupo", type="primary"):
                            # Crear nuevo entity_id
                            prefix = entity_id.split('_')[0]
                            new_entity_id = get_next_entity_id(st.session_state.df_edited, prefix)
                            
                            # Mover nombres al nuevo grupo
                            mask = (st.session_state.df_edited['entity_id'] == entity_id) & \
                                   (st.session_state.df_edited['original_name'].isin(names_to_split))
                            
                            st.session_state.df_edited.loc[mask, 'entity_id'] = new_entity_id
                            
                            # Establecer nuevo standard_name (el más frecuente)
                            new_group = st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == new_entity_id
                            ]
                            new_standard = new_group.nlargest(1, 'frequency').iloc[0]['normalized_name']
                            st.session_state.df_edited.loc[mask, 'standard_name'] = new_standard
                            
                            # Actualizar component_size
                            for eid in [entity_id, new_entity_id]:
                                size = len(st.session_state.df_edited[st.session_state.df_edited['entity_id'] == eid])
                                st.session_state.df_edited.loc[
                                    st.session_state.df_edited['entity_id'] == eid,
                                    'component_size'
                                ] = size
                            
                            st.session_state.edit_history.append(f"Dividir {entity_id}: {len(names_to_split)} nombres → {new_entity_id}")
                            st.session_state.changes_made = True
                            st.success(f"✓ Grupo dividido. Nuevo grupo: {new_entity_id}")
                            st.rerun()
                
                # OPCION 3: Unir con otro grupo
                elif edit_option == "Unir con otro grupo":
                    st.subheader("Unir con otro grupo")
                    
                    all_entity_ids = sorted(st.session_state.df_edited['entity_id'].unique().tolist())
                    target_entity_id = st.selectbox(
                        "Selecciona el grupo con el que unir:",
                        options=[eid for eid in all_entity_ids if eid != entity_id]
                    )
                    
                    if target_entity_id:
                        target_info = st.session_state.df_edited[
                            st.session_state.df_edited['entity_id'] == target_entity_id
                        ]
                        st.info(f"El grupo destino tiene {len(target_info)} nombres")
                        
                        if st.button("✅ Unir grupos", type="primary"):
                            # Mover todos los nombres al grupo destino
                            mask = st.session_state.df_edited['entity_id'] == entity_id
                            st.session_state.df_edited.loc[mask, 'entity_id'] = target_entity_id
                            
                            # Actualizar standard_name al del grupo destino
                            target_standard = target_info.iloc[0]['standard_name']
                            st.session_state.df_edited.loc[mask, 'standard_name'] = target_standard
                            
                            # Actualizar component_size
                            size = len(st.session_state.df_edited[
                                st.session_state.df_edited['entity_id'] == target_entity_id
                            ])
                            st.session_state.df_edited.loc[
                                st.session_state.df_edited['entity_id'] == target_entity_id,
                                'component_size'
                            ] = size
                            
                            st.session_state.edit_history.append(f"Unir {entity_id} con {target_entity_id}")
                            st.session_state.changes_made = True
                            st.success(f"✓ Grupos unidos en {target_entity_id}")
                            st.session_state.selected_entity_id = None
                            st.rerun()
                
                # OPCION 4: Cambiar nombre estándar
                elif edit_option == "Cambiar nombre estándar":
                    st.subheader("Cambiar nombre estándar")
                    
                    current_standard = entity_data.iloc[0]['standard_name']
                    st.info(f"**Nombre estándar actual:** {current_standard}")
                    
                    # Opciones: seleccionar de los nombres existentes o escribir uno nuevo
                    option = st.radio(
                        "Seleccionar de nombres existentes o escribir nuevo:",
                        ["Seleccionar existente", "Escribir nuevo"]
                    )
                    
                    if option == "Seleccionar existente":
                        new_standard = st.selectbox(
                            "Selecciona nuevo nombre estándar:",
                            options=sorted(entity_data['normalized_name'].unique())
                        )
                    else:
                        new_standard = st.text_input("Nuevo nombre estándar:", value=current_standard)
                    
                    if st.button("✅ Cambiar nombre estándar", type="primary"):
                        mask = st.session_state.df_edited['entity_id'] == entity_id
                        st.session_state.df_edited.loc[mask, 'standard_name'] = new_standard
                        
                        st.session_state.edit_history.append(f"Cambiar standard_name de {entity_id} a '{new_standard}'")
                        st.session_state.changes_made = True
                        st.success(f"✓ Nombre estándar actualizado a: {new_standard}")
                        st.rerun()
    
    # TAB 4: Estadísticas
    with tab4:
        st.header("Estadísticas Generales")
        
        df = st.session_state.df_edited
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total nombres", f"{len(df):,}")
        with col2:
            st.metric("Entidades únicas", f"{df['entity_id'].nunique():,}")
        with col3:
            grouped_sizes = df.groupby('entity_id').size()
            st.metric("Tamaño promedio", f"{grouped_sizes.mean():.1f}")
        with col4:
            st.metric("Tamaño máximo", f"{grouped_sizes.max():,}")
        
        st.markdown("---")
        
        # Distribución de tamaños
        st.subheader("Distribución de tamaños de grupos")
        size_dist = df.groupby('entity_id').size().value_counts().sort_index()
        st.bar_chart(size_dist)
        
        # Top grupos por frecuencia
        st.subheader("Top 10 grupos por frecuencia total")
        top_groups = df.groupby('entity_id').agg({
            'frequency': 'sum',
            'original_name': 'count'
        }).rename(columns={'original_name': 'count'}).sort_values('frequency', ascending=False).head(10)
        st.dataframe(top_groups, use_container_width=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Error crítico en la aplicación: {str(e)}")
        st.info("Por favor, recarga la página o reinicia la aplicación.")
        logger.exception("Error crítico en la aplicación")

