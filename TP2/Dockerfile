FROM python:3.11-slim

# Installer cron
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*

# Créer le dossier de travail
WORKDIR /app

COPY requirements.txt .

# Installer requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script dans le conteneur
COPY etl_pipeline_duckdb.py .
COPY crontab.txt /etc/cron.d/etl-cron

# Donner les droits à la crontab
RUN chmod 0644 /etc/cron.d/etl-cron 

# Monter volume de logs et base persistante
VOLUME [ "/data" ]

# Point d'entrée
CMD ["cron", "-f"]