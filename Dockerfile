FROM python:3.11-slim

# Créer le dossier de travail
WORKDIR /app

# Installer DuckDB et pandas
RUN pip install --no-cache-dir duckdb pandas

# Copier le script dans le conteneur
COPY script_duckdb.py .

# Point d'entrée
CMD ["python", "script_duckdb.py"]