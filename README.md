# 🏥 TP DataOps – Pipeline Hospitalier
<img width="1914" height="879" alt="Capture d&#39;écran 2026-06-17 111632" src="https://github.com/user-attachments/assets/a82a23e4-c443-40a5-9ec8-57d35db5aef3" />


## Présentation

Ce projet met en œuvre un pipeline DataOps complet simulant le traitement de données patients dans un environnement hospitalier.

L'objectif est de démontrer comment des données brutes provenant de fichiers CSV peuvent être :

* collectées depuis un stockage objet ;
* nettoyées et standardisées ;
* contrôlées grâce à des règles de qualité ;
* chargées dans une base de données ;
* visualisées via des tableaux de bord métier.

Le projet repose sur une architecture moderne basée sur Docker et plusieurs outils largement utilisés dans les environnements Data Engineering et DataOps.

---

# 🎯 Objectifs pédagogiques

Ce TP permet de comprendre :

* l'orchestration de pipelines avec Airflow ;
* le stockage de fichiers avec MinIO ;
* le nettoyage de données avec Pandas ;
* la validation de qualité avec Great Expectations ;
* le stockage analytique dans PostgreSQL ;
* la création d'indicateurs et dashboards dans Grafana.

L'étudiant apprend à observer un pipeline existant, analyser son fonctionnement, identifier des problèmes de qualité de données et mettre en place des contrôles adaptés.

---

# 🏗 Architecture

```text
CSV Patients
      │
      ▼
 MinIO (Stockage)
      │
      ▼
 Airflow (Orchestration)
      │
      ▼
 Pandas (Nettoyage)
      │
      ▼
 Great Expectations
 (Contrôle Qualité)
      │
      ▼
 PostgreSQL
      │
      ▼
 Grafana
```

---

# ⚙ Technologies utilisées

| Outil              | Rôle                                    |
| ------------------ | --------------------------------------- |
| Docker             | Conteneurisation de l'environnement     |
| Apache Airflow     | Orchestration du pipeline               |
| MinIO              | Stockage objet compatible S3            |
| Pandas             | Transformation et nettoyage des données |
| Great Expectations | Validation de la qualité des données    |
| PostgreSQL         | Stockage des données et métriques       |
| Grafana            | Visualisation et monitoring             |

---

# 📂 Structure du projet

```text
project/
│
├── dags/
│   └── ingest_patients_from_minio.py
│
├── data/
│   └── csv/
│
├── sql/
│
├── grafana/
│
├── docker-compose.yml
│
└── README.md
```

---

# 🚀 Démarrage rapide

## Arrêt de l'environnement

```bash
docker compose down -v --remove-orphans
```

## Démarrage des services

```bash
docker compose up -d
```

## Vérification des conteneurs

```bash
docker compose ps
```

---

# 🌐 Interfaces

## Airflow

URL :

```text
http://localhost:8080
```

Identifiants :

```text
admin
admin
```

Fonction :

* orchestration du pipeline ;
* exécution des DAGs ;
* consultation des logs.

---

## MinIO

URL :

```text
http://localhost:9001
```

Identifiants :

```text
minioadmin
minioadmin
```

Fonction :

* stockage des fichiers CSV sources ;
* simulation d'un stockage S3.

---

## Grafana

URL :

```text
http://localhost:3000
```

Identifiants :

```text
admin
admin
```

Fonction :

* visualisation des indicateurs ;
* suivi de la qualité des données ;
* monitoring des exécutions du pipeline.

---

# 🔍 Contrôle qualité des données

Les données sont validées grâce à Great Expectations.

Exemples de contrôles :

* présence des colonnes obligatoires ;
* contrôle des valeurs nulles ;
* vérification de la plage d'âge ;
* validation des services hospitaliers ;
* cohérence statistique des données.

Les résultats des validations sont stockés dans PostgreSQL et affichés dans Grafana.

---

# 🗄 Base de données

Principales tables :

| Table                | Description                  |
| -------------------- | ---------------------------- |
| patient              | Patients validés             |
| patient_reject       | Lignes rejetées              |
| ingestion_run        | Historique des exécutions    |
| ge_validation_result | Résultats Great Expectations |

Principales vues :

| Vue                   | Description                |
| --------------------- | -------------------------- |
| v_last_ingestion      | Dernière exécution         |
| v_patients_by_service | Répartition des patients   |
| v_rejects_by_reason   | Analyse des rejets         |
| v_ge_last_results     | Derniers contrôles qualité |

---

# 📊 Dashboard Grafana

Le tableau de bord permet notamment de suivre :

* le nombre de lignes brutes ;
* le nombre de patients chargés ;
* le nombre de rejets ;
* les doublons détectés ;
* la répartition par service ;
* les résultats Great Expectations ;
* l'historique des exécutions.

---

# 🎓 Compétences développées

* DataOps
* Data Quality
* Data Engineering
* SQL
* Docker
* Monitoring de pipelines
* Gouvernance de la donnée
* Observabilité des traitements

---

# ✅ Résultat attendu

À la fin du TP, le pipeline doit :

1. Charger les fichiers CSV depuis MinIO.
2. Nettoyer et standardiser les données.
3. Vérifier leur qualité avec Great Expectations.
4. Charger les données valides dans PostgreSQL.
5. Stocker les lignes rejetées avec leur motif.
6. Exposer des indicateurs exploitables dans Grafana.

Le projet illustre ainsi les bonnes pratiques de mise en qualité et de supervision des données dans un environnement DataOps moderne.
