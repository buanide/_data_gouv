import streamlit as st
import pandas as pd
from collections import defaultdict
import re
import os

# Configuration de la page
st.set_page_config(
    page_title="Orange Cameroun - Système d'Analyse de Dépendances",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Couleurs Orange Cameroun
PRIMARY_COLOR = "#FF7900"  # Orange principal
SECONDARY_COLOR = "#000000"  # Noir
BG_COLOR = "#FFFFFF"  # Blanc

# Appliquer CSS personnalisé pour les couleurs Orange
st.markdown(f"""
<style>
    .stApp {{
        background-color: {BG_COLOR};
    }}
    .main .block-container {{
        padding-top: 2rem;
    }}
    h1, h2, h3 {{
        color: {PRIMARY_COLOR};
    }}
    .stButton button {{
        background-color: {PRIMARY_COLOR};
        color: white;
    }}
    .stButton button:hover {{
        background-color: {PRIMARY_COLOR};
        color: white;
        opacity: 0.8;
    }}
    .st-bo {{
        border-color: {PRIMARY_COLOR};
    }}
    .st-cc {{
        color: {PRIMARY_COLOR};
    }}
    .stProgress .st-ev {{
        background-color: {PRIMARY_COLOR};
    }}
    .stSidebar {{
        background-color: #F5F5F5;
    }}
    .small-font {{
        font-size: 0.8rem;
    }}
    .result-box {{
        border-left: 3px solid {PRIMARY_COLOR};
        padding-left: 10px;
        margin: 10px 0;
        background-color: #F9F9F9;
    }}
</style>
""", unsafe_allow_html=True)

# Logo Orange 
st.sidebar.markdown(f"""
<div style="text-align: center; padding: 1rem 0;">
    <h2 style="color: {PRIMARY_COLOR};">Orange Cameroun</h2>
    <h3>Système d'Analyse de Dépendances</h3>
</div>
""", unsafe_allow_html=True)

# Fonction pour charger les données
@st.cache_data
def load_data(uploaded_file):
    if uploaded_file is not None:
        try:
            data = pd.read_csv(uploaded_file, encoding='ISO-8859-1', sep=';', dtype=str)
            data = data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            return data, True
        except Exception as e:
            st.error(f"Erreur lors du chargement du fichier: {e}")
            return None, False
    return None, False

# Sélection et chargement du fichier
st.sidebar.header("Configuration")
uploaded_file = st.sidebar.file_uploader("Charger un fichier de dépendances CSV", type=['csv'])

data = None
data_loaded = False

if uploaded_file:
    data, data_loaded = load_data(uploaded_file)

# Si aucun fichier n'est chargé, proposer d'utiliser les données de démonstration
if not data_loaded:
    if st.sidebar.button("Utiliser données de démo"):
        # Créer un petit jeu de données de démonstration
        demo_data = {
            'Table_RDMS': ['system.users', 'system.roles', ''],
            'Table_Hive': ['', '', 'datamart.customers'],
            'Dep_datalake_1': ['datamart.sales', 'datamart.customers', ''],
            'Server': ['srv-prod-01', 'srv-prod-01', 'srv-hive-02'],
            'Raw_Path': ['/data/users', '/data/roles', '/warehouse/customers'],
            'Flux_Name': ['USER_IMPORT', 'ROLE_SYNC', 'CUSTOMER_ETL'],
            'Nb_processor': ['2', '1', '4'],
            'Nb_disabled_processors': ['0', '0', '1'],
            'Hostname': ['192.168.1.10', '192.168.1.10', '192.168.1.20'],
            'Port': ['1521', '1521', '10000'],
            'username': ['svc_etl', 'svc_etl', 'hive_user']
        }
        data = pd.DataFrame(demo_data)
        data_loaded = True

# Interface principale
st.title("Système d'Analyse de Dépendances")

if not data_loaded:
    st.info("Veuillez charger un fichier CSV ou utiliser les données de démonstration pour commencer.")
    st.markdown("""
    ### Structure attendue du fichier
    Le fichier CSV doit contenir les colonnes suivantes:
    - Table_RDMS: Tables de base de données relationnelles
    - Table_Hive: Tables Hive
    - Dep_datalake_X: Colonnes de dépendances
    - Server, Raw_Path, Flux_Name, etc.: Métadonnées de serveur
    """)
else:
    # Définition des colonnes de métadonnées serveur
    server_metadata_columns = ['Server', 'Raw_Path', 'Flux_Name', 'Nb_processor', 
                           'Nb_disabled_processors', 'Hostname', 'Port', 'username']
    
    # Classe sérialisable pour remplacer le defaultdict(lambda: defaultdict(set))
    class NestedDefaultDict:
        def __init__(self):
            self.data = {}
        
        def __getitem__(self, key):
            if key not in self.data:
                self.data[key] = {}
            return self.data[key]
        
        def get(self, key, default=None):
            return self.data.get(key, default)
        
        def items(self):
            return self.data.items()
    
    # Construction du graphe
    @st.cache_data
    def build_graph(data):
        graph = defaultdict(set)
        reverse_graph = defaultdict(set)
        table_mapping = {}
        all_tables = set()
        table_metadata = NestedDefaultDict()
        table_origin = defaultdict(set)
        
        for _, row in data.iterrows():
            # Collecter les tables
            tables_by_column = {}
            
            # Ajouter Table_RDMS et Table_Hive
            if pd.notna(row.get('Table_RDMS', None)) and str(row['Table_RDMS']).strip():
                tables_by_column['Table_RDMS'] = row['Table_RDMS']
            if pd.notna(row.get('Table_Hive', None)) and str(row['Table_Hive']).strip():
                tables_by_column['Table_Hive'] = row['Table_Hive']
            
            # Ajouter les colonnes Dep_datalake
            dep_columns = [col for col in data.columns if col.startswith('Dep_datalake')]
            for col in dep_columns:
                if col in row and pd.notna(row.get(col, None)) and str(row[col]).strip():
                    tables_by_column[col] = row[col]
            
            table_values = list(tables_by_column.values())
            
            # Enregistrer les tables avec leur origine
            for col_name, value in tables_by_column.items():
                value_lower = value.lower()
                table_mapping[value_lower] = value
                all_tables.add(value_lower)
                
                # Enregistrer la colonne d'origine
                table_origin[value_lower].add(col_name)
                
                # Stocker les métadonnées
                for col in server_metadata_columns:
                    if col in row and pd.notna(row[col]) and str(row[col]).strip():
                        if col not in table_metadata[value_lower]:
                            table_metadata[value_lower][col] = set()
                        table_metadata[value_lower][col].add(row[col])
            
            # Créer les relations entre tables
            for i in range(len(table_values)):
                for j in range(i+1, len(table_values)):
                    src, dep = table_values[i], table_values[j]
                    src_lower, dep_lower = src.lower(), dep.lower()
                    graph[src_lower].add(dep_lower)
                    reverse_graph[dep_lower].add(src_lower)
        
        return graph, reverse_graph, table_mapping, all_tables, table_metadata, table_origin
    
    # Construire le graphe
    graph, reverse_graph, table_mapping, all_tables, table_metadata, table_origin = build_graph(data)
    
    def get_original_case(table_lower):
        """Retourne le nom original de la table"""
        return table_mapping.get(table_lower, table_lower.upper())
    
    # Interface utilisateur pour l'analyse
    st.markdown("### Recherche de table")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # Entrée pour le nom de la table
        table_input = st.text_input("Nom de la table (ex: schema.table)")
    
    with col2:
        # Mode d'analyse
        analysis_mode = st.radio(
            "Mode d'analyse",
            ["Dépendances directes", "Utilisateurs", "Métadonnées serveur", "Diagnostic complet"],
            horizontal=True
        )
    
    if table_input:
        table_match = re.search(r'([a-z0-9_]+\.[a-z0-9_]+)', table_input, re.I)
        
        if not table_match:
            st.warning("Format de table invalide. Utilisez le format schema.table (ex: datamart.clients)")
        else:
            table = table_match.group(1)
            table_lower = table.lower()
            table_original = get_original_case(table_lower)
            
            # Vérifier si la table existe
            if table_lower not in all_tables:
                st.error(f"La table '{table_original}' n'existe pas dans le système.")
                
                # Suggestions
                similar = [t for t in table_mapping.values() 
                          if table_lower.split('.')[0] in t.lower()]
                if similar:
                    st.markdown("#### Tables similaires :")
                    for s in similar[:5]:
                        st.markdown(f"- {s}")
            else:
                st.markdown(f"### Résultats pour '{table_original}'")
                
                # Afficher les résultats selon le mode d'analyse
                if analysis_mode == "Dépendances directes":
                    deps = sorted([get_original_case(d) for d in graph.get(table_lower, set())])
                    st.markdown(f"#### Dépendances directes ({len(deps)})")
                    if deps:
                        with st.container():
                            st.markdown('<div class="result-box">', unsafe_allow_html=True)
                            for dep in deps:
                                st.markdown(f"- {dep}")
                            st.markdown('</div>', unsafe_allow_html=True)
                    else:
                        st.info("Aucune dépendance directe trouvée.")
                

                elif analysis_mode == "Utilisateurs":
                    users = sorted([get_original_case(u) for u in reverse_graph.get(table_lower, set())])
                    st.markdown(f"#### Utilisateurs ({len(users)})")
                    if users:
                        with st.container():
                            st.markdown('<div class="result-box">', unsafe_allow_html=True)
                            for user in users:
                                st.markdown(f"- {user}")
                            st.markdown('</div>', unsafe_allow_html=True)
                    else:
                        st.info("Aucun utilisateur trouvé.")
                
                elif analysis_mode == "Métadonnées serveur":
                    st.markdown("#### Métadonnées serveur")
                    
                    # Afficher les métadonnées disponibles
                    has_metadata = False
                    metadata = table_metadata.get(table_lower, {})
                    if metadata:
                        for key in server_metadata_columns:
                            if key in metadata and metadata[key]:
                                has_metadata = True
                                values = sorted(metadata[key])
                                with st.expander(f"{key} ({len(values)})"):
                                    for value in values:
                                        st.markdown(f"- {value}")
                    
                    if not has_metadata:
                        st.info("Aucune métadonnée serveur disponible pour cette table.")
                
                elif analysis_mode == "Diagnostic complet":
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("#### Origine")
                        if table_lower in table_origin:
                            origins = ", ".join(sorted(table_origin[table_lower]))
                            st.markdown(f"Table trouvée dans les colonnes: **{origins}**")
                        else:
                            st.info("Origine inconnue")
                        
                        st.markdown("#### Dépendances")
                        deps = graph.get(table_lower, set())
                        if deps:
                            st.markdown(f"**{len(deps)}** tables dépendantes")
                            with st.expander("Voir les dépendances"):
                                for dep in sorted([get_original_case(d) for d in deps]):
                                    st.markdown(f"- {dep}")
                        else:
                            st.info("Aucune dépendance")
                    
                    with col2:
                        st.markdown("#### Métadonnées disponibles")
                        metadata_keys = []
                        metadata = table_metadata.get(table_lower, {})
                        if metadata:
                            metadata_keys = [k for k, v in metadata.items() if v]
                        
                        if metadata_keys:
                            st.markdown(f"{', '.join(metadata_keys)}")
                            with st.expander("Voir les métadonnées"):
                                for key in sorted(metadata_keys):
                                    st.markdown(f"**{key}**:")
                                    for value in sorted(metadata[key]):
                                        st.markdown(f"- {value}")
                        else:
                            st.info("Aucune métadonnée disponible")
                        
                        st.markdown("#### Utilisateurs")
                        users = reverse_graph.get(table_lower, set())
                        if users:
                            st.markdown(f"**{len(users)}** tables utilisatrices")
                            with st.expander("Voir les utilisateurs"):
                                for user in sorted([get_original_case(u) for u in users]):
                                    st.markdown(f"- {user}")
                        else:
                            st.info("Aucun utilisateur")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center" class="small-font">
        <p>© Orange Cameroun - Système d'Analyse de Dépendances</p>
    </div>
    """, unsafe_allow_html=True)