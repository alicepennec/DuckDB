import pandas as pd
import streamlit as st
import duckdb 

# Bases de données disponibles
db_files = {
    "Covid": "/data/my_database.duckdb",
    "Weather": "/data/database_api.duckdb",
    "Jeux": "/data/jo_database.duckdb"
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
    for table_name in tables["name"]:
        st.markdown(f"### 📊 Table `{table_name}`")

        # Aperçu limité à 100 lignes
        df = con.execute(f"SELECT * FROM {table_name} LIMIT 100").fetchdf()
        st.dataframe(df, use_container_width=True)
        
        # Requêtes sur les bases
        query = st.text_area("Entrez votre requête SQL :", "SELECT * from {table_name}")
        if st.button("Exécuter la requête"):
            try:
                df = con.execute(query).fetchdf()
                st.success(f"Requête exécutée avec succès")
                st.dataframe(df)
            except Exception as e:
                st.error(f"Erreur dans la requête : {e}")

        # Bouton de téléchargement
        st.download_button(
            label=f"📥 Télécharger `{table_name}` en CSV",
            data=df.to_csv(index=False),
            file_name=f"{table_name}.csv",
            mime="text/csv"
        )
        
        # Suppression de table
        if st.button("❌ Supprimer la table"):
            df = con.execute(f"DROP table {table_name}").fetchdf()
        
        
        st.markdown("---")

# Fermeture connexion
con.close()
