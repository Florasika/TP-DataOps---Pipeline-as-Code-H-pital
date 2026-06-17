# Démo Pipeline as Code - Hôpital V2.1 corrigée

Cette version corrige le démarrage Airflow :
- `airflow-init` initialise la base metadata Airflow
- `airflow-init` crée ou reset le user `admin/admin`
- `airflow-webserver` expose l'UI
- `airflow-scheduler` tourne dans un container séparé
- Airflow utilise une vraie base PostgreSQL metadata `airflow_db`, plus propre que SQLite

## Lancer proprement

Depuis WSL2, dans le dossier du projet :

```bash
docker compose down -v --remove-orphans
docker compose up -d
```

Le premier démarrage peut prendre un peu de temps car Airflow installe :
- pandas
- boto3
- psycopg2-binary
- unidecode
- great_expectations

## Vérifier les containers

```bash
docker compose ps
```

Tu dois voir notamment :
- `demo_postgres_hospital`
- `demo_minio`
- `demo_airflow_init` en état exited 0
- `demo_airflow_webserver`
- `demo_airflow_scheduler`
- `demo_grafana`

## Si Airflow ne répond pas encore

Regarde les logs :

```bash
docker logs demo_airflow_init --tail=100
docker logs demo_airflow_webserver --tail=100
docker logs demo_airflow_scheduler --tail=100
```

## Interfaces

### Airflow

http://localhost:8080

```text
admin / admin
```

### MinIO

http://localhost:9001

```text
minioadmin / minioadmin
```

### Grafana

http://localhost:3000

```text
admin / admin
```

## Lancer la démo

1. Va dans Airflow.
2. Lance le DAG `ingest_patients_from_minio`.
3. Va dans Grafana.
4. Ouvre le dashboard `Pipeline as Code / Hôpital - Pipeline Data Quality`.

## Reset complet

```bash
docker compose down -v --remove-orphans
docker compose up -d
```
