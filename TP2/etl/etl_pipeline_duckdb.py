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

db_path="/data/database_API.duckdb"

api_key = "300e45999bbcb67591f843c8f5f1917d"
city = "Paris"

# Récupération API
url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric'.format(city, api_key)

try:
    data = requests.get(url).json()
    logging.info(f"Request to {url} successful")
except  requests.exceptions.RequestException as e:
    logging.error(f"Request to {url} failed: {e}")
    raise    

try:
    df= pd.json_normalize(data)
    logging.info("JSON data converted to a DataFrame successfully")
except Exception as e: 
    logging.error(f"Failed to convert JSON data to Dataframe: {e}")
    raise

try:
    df = df.dropna()
    logging.info("Data treatment successful")
except Exception as e:
    logging.error(f"Failed to treat data: {e}")
    raise

try:
    con = duckdb.connect(database=db_path)
    logging.info("Connection to DuckDB successful")
except Exception as e:
    logging.error(f"Failed to connect to DuckDB")
    raise

# Créer la table products
try:
    con.execute("CREATE TABLE IF NOT EXISTS weather AS SELECT * FROM df")
    con.execute("INSERT INTO weather SELECT * FROM df")
    logging.info("DuckDB table created and Dataframe savec successfully")
except Exception as e:
    logging.error(f"Failed to create table and to save Dataframe: {e}")
    raise

try:
    df.to_parquet("/data/weather_data.parquet")
    logging.info("Dataframe saved to parquet file successfully")
except Exception as e:
    logging.error(f"Failed to saved Dataframe to Parquet file: {e}")
    raise

con.close()
logging.info("Weather table created/updated")
