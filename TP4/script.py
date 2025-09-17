import duckdb
import os
import pandas as pd
from pathlib import Path

csv_path = '/data/fact_resultats_epreuves.csv'
db_path = '/data/jo_database.duckdb'


# Récupération des données CSV
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"❌ Le fichier {csv_path} est introuvable.")

data = pd.read_csv(csv_path)
data.info()

df = data.drop(columns=['id_resultat_source', 'id_athlete_base_resultats', 'id_personne', 'id_equipe', 'id_pays', 'pays_en_base_resultats', 'performance_finale_texte', 'performance_finale', 'id_evenement', 'evenement_en', 'id_edition', 'id_competition_sport', 'id_type_competition', 'id_ville_edition', 'id_nation_edition_base_resultats', 'id_sport', 'sport', 'id_discipline_administrative', 'id_specialite', 'id_epreuve', 'id_federation'])

# Partitionner les données en fonction des années et les exporter sous format Parquet
Path("../parquet_data").mkdir(exist_ok=True)
for annee, subset in df.groupby('edition_saison'):
    subset.to_parquet(f"../parquet_data/jo_{annee}.parquet", index=False)

# Connexion à la base DuckDB
con = duckdb.connect(database=db_path)

# Création de la table JO et ingestion des données dans la base
try:
    con.execute("CREATE TABLE IF NOT EXISTS jeux AS SELECT * FROM read_parquet('../parquet_data/*.parquet')")
    con.execute("INSERT INTO jeux SELECT * FROM df")
except Exception as e:
    print(f"Failed to create table : {e}")