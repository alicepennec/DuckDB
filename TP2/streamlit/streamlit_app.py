import pandas as pd
import streamlit as st
import duckdb 

# Bases de données disponibles
db_files = {
    "Covid": "/data/my_database.duckdb",
    "Products": "/data/database_API.duckdb"
}

st.set_page_config(layout="wide")
st.title("🗂️ Explorateur de bases DuckDB")

# --- Choix de la base ---
st.sidebar.header("Paramètres")
db_choice = st.sidebar.selectbox("Choisir une base :", list(db_files.keys()))

db_path = db_files[db_choice]

# Connexion à la base choisie
con = duckdb.connect(db_path, read_only=True)

# --- Récupération des tables ---
tables = con.execute("SHOW TABLES").fetchdf()

if tables.empty:
    st.warning(f"Aucune table trouvée dans `{db_choice}`.")
else:
    # --- Choix de la table ---
    table_choice = st.sidebar.selectbox("Choisir une table :", tables["name"])

    # --- Aperçu des données ---
    df = con.execute(f"SELECT * FROM {table_choice} LIMIT 100").fetchdf()

    st.subheader(f"Aperçu de la table `{table_choice}`")
    st.dataframe(df, use_container_width=True)

    # --- Téléchargement CSV ---
    st.download_button(
        label="📥 Télécharger en CSV",
        data=df.to_csv(index=False),
        file_name=f"{table_choice}.csv",
        mime="text/csv"
    )

# Fermeture connexion
con.close()