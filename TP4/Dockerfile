FROM python:3.11-slim

# Créer le dossier de travail
WORKDIR /app

# Installer pandas duckdb
RUN pip install --no-cache-dir pandas duckdb

# Copier le script dans le conteneur
COPY script.py .

# Point d'entrée
CMD ["python", "script.py"]