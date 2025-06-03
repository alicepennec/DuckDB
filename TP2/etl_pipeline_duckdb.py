import duckdb 
import requests
import pandas as pd
import logging
import os

# Initialisation du logger
logging.basicConfig(
    filename="/data/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Récupération automatique des données issues d'une API
url = 'https://world.openfoodfacts.net/api/v2/search'
params= {
    'fields': 'product_name, nutriscore_score,nutriscore_grade,coutries,brands',
    'page_size': 100,
    'json': True
}

try:
    response = requests.get(url, params=params)
    logging.info("Request to {url} successful.")
except requests.exceptions.RequestException as e:
    logging.error("Request to {url} failed: {e}")
    raise

try:
    data= response.json().get('products', [])
    logging.info("JSON data retrieved successfully.")
except requests.exceptions.RequestException as e:
    logging.error("Failed to parse JSON data: {e}")
    raise

try:
    df = pd.DataFrame(data)
    logging.info("JSON data converted to DataFrame successfully")
except requests.exceptions.RequestException as e:
    logging.error("Failed to convert JSON data to DataFrame: {e}")
    raise

# Nettoyage des données
try:
    df = df.dropna(subset=['product_name'])
    df = df.drop_duplicates()
    df = df.fillna('')
    logging.info("DataFrame cleaned successfully")
except requests.exceptions.RequestException as e:
    logging.error("Failed to clean DataFrame: {e}")
    raise

# Création d'une base de données persistantes : 
try:
    con = duckdb.connect(database='/data/database_API.duckdb', read_only=False)
    con.execute("""CREATE TABLE IF NOT EXISTS products AS SELECT * FROM df""")
    logging.info("DuckDB connection established and table created successfully.")
except requests.exceptions.RequestException as e:
    logging.error("Failed to connect to DuckDB or create table: {e}")
    raise

# Ingestion des données dans la base DuckDB
try:
    con.execute("""INSERT INTO products SELECT * FROM df""")
    logging.info("DataFrame inserted into DuckDB table.")
except requests.exceptions.RequestException as e:
    logging.error("Failed to insert DataFrame to DuckDb table: {e}")
    raise

# Sauvegarder les données propres en Parquet
try:
    df.to_parquet('/data/products.parquet')
    logging.info("DataFrame saved to Parquet file successfully.")
except requests.exceptions.RequestException as e:
    logging.error("Failed to save DataFrame to Parquet file: {e}")
    raise


# Mise en place d'un cron dans le container (ou via docker-compose)
