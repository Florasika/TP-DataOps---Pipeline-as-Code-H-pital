CREATE DATABASE airflow_db;

CREATE TABLE IF NOT EXISTS service (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(120) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS patient (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(120) NOT NULL,
    prenom VARCHAR(120) NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 130),
    pathologie TEXT NOT NULL,
    service_id INTEGER NOT NULL REFERENCES service(id),
    source_file VARCHAR(255),
    ingestion_run_id INTEGER,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nom, prenom, age, pathologie, service_id)
);

CREATE TABLE IF NOT EXISTS ingestion_run (
    id SERIAL PRIMARY KEY,
    dag_run_id TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    source_files_count INTEGER DEFAULT 0,
    raw_rows INTEGER DEFAULT 0,
    cleaned_rows INTEGER DEFAULT 0,
    inserted_rows INTEGER DEFAULT 0,
    rejected_rows INTEGER DEFAULT 0,
    duplicate_rows INTEGER DEFAULT 0,
    ge_success BOOLEAN,
    age_min NUMERIC,
    age_avg NUMERIC,
    age_max NUMERIC
);

CREATE TABLE IF NOT EXISTS patient_reject (
    id SERIAL PRIMARY KEY,
    ingestion_run_id INTEGER REFERENCES ingestion_run(id),
    source_file VARCHAR(255),
    raw_payload JSONB,
    reject_reason TEXT NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ge_validation_result (
    id SERIAL PRIMARY KEY,
    ingestion_run_id INTEGER REFERENCES ingestion_run(id),
    expectation_name TEXT NOT NULL,
    column_name TEXT,
    success BOOLEAN NOT NULL,
    observed_value TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO service (nom) VALUES
    ('Chirurgie cardiovasculaire'),
    ('Orthopédie'),
    ('Pédiatrie'),
    ('Gynécologie'),
    ('Neurologie'),
    ('Endocrinologie'),
    ('Cardiologie'),
    ('Urgences')
ON CONFLICT (nom) DO NOTHING;

CREATE OR REPLACE VIEW v_patients_by_service AS
SELECT s.nom AS service, COUNT(*) AS nb_patients
FROM patient p JOIN service s ON s.id = p.service_id
GROUP BY s.nom;

CREATE OR REPLACE VIEW v_last_ingestion AS
SELECT * FROM ingestion_run ORDER BY id DESC LIMIT 1;

CREATE OR REPLACE VIEW v_rejects_by_reason AS
SELECT reject_reason, COUNT(*) AS nb_rejets
FROM patient_reject GROUP BY reject_reason;

CREATE OR REPLACE VIEW v_ge_last_results AS
SELECT g.expectation_name, COALESCE(g.column_name, '-') AS column_name, g.success, g.observed_value, g.created_at
FROM ge_validation_result g
WHERE g.ingestion_run_id = (SELECT MAX(id) FROM ingestion_run)
ORDER BY g.success ASC, g.expectation_name;
