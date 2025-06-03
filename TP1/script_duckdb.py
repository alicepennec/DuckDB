import duckdb
import time
import os

csv_path = '/data/covid.csv'
parquet_path = '/data/covid.parquet'

if not os.path.exists(csv_path):
    raise FileNotFoundError(f"❌ Le fichier {csv_path} est introuvable.")

# Connexion à la base DuckDB
con = duckdb.connect(database='/data/my_database.duckdb', read_only=False)

# Charger le CSV et renommer les colonnes
print("Loading CSV...")
try:
    con.execute(f"""
        CREATE OR REPLACE TABLE covid AS
        SELECT 
            "Province/State" AS province_state,
            "Country/Region" AS country_region,
            "Lat"::DOUBLE  AS lat,
            "Long"::DOUBLE AS lon,
            "Date"::DATE AS date,
            CAST("Confirmed" AS INTEGER) AS confirmed,
            CAST("Deaths" AS INTEGER) AS deaths,
            CAST("Recovered" AS INTEGER) AS recovered,
            CAST("Active" AS INTEGER) AS active,
            "WHO Region" AS who_region 
        FROM read_csv_auto('{csv_path}', HEADER=TRUE);
    """)
    print("CSV loaded into table")
except Exception as e:
    print(f"Erreur lors du chargement CSV : {e}")

# Exporter au format Parquet
print("Exporting to Parquet...")
con.execute(f"COPY covid TO '{parquet_path}' (FORMAT 'parquet');")

# Requêtes exploratoires
print("\nTop 10 villes avec le plus de cas :")
print(con.execute("""
    SELECT country_region, SUM(confirmed) AS total_confirmed
    FROM covid
    GROUP BY country_region
    ORDER BY total_confirmed DESC
    LIMIT 10;
""").fetchdf())

print("\nÉvolution temporelle moyenne des cas :")
print(con.execute("""
    SELECT date, AVG(confirmed) AS avg_confirmed
    FROM covid
    GROUP BY date
    ORDER BY date;
""").fetchdf())

print("\nCas confirmés par région OMS :")
print(con.execute("""
    SELECT who_region, SUM(confirmed) AS total_confirmed
    FROM covid
    GROUP BY who_region
    ORDER BY total_confirmed DESC;
""").fetchdf())

print("\nBenchmark lecture CSV :")
start_csv = time.time()
con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}');").fetchone()
end_csv = time.time()
print(f"Durée lecture CSV : {end_csv - start_csv:.4f} sec")

print("Benchmark lecture Parquet :")
start_parquet = time.time()
con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}');").fetchone()
end_parquet = time.time()
print(f"Durée lecture Parquet : {end_parquet - start_parquet:.4f} sec")

con.close()
