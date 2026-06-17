ffrom __future__ import annotations

import io
import os
import re
from datetime import datetime

import boto3
import great_expectations as ge
import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from psycopg2.extras import Json
from unidecode import unidecode


BUCKET = os.environ["MINIO_BUCKET"]

VALID_SERVICES = [
    "Cardiologie",
    "Chirurgie cardiovasculaire",
    "Orthopédie",
    "Pédiatrie",
    "Gynécologie",
    "Neurologie",
    "Endocrinologie",
    "Urgences",
]

COLUMN_MAPPING = {
    "nom": "nom",
    "nom_patient": "nom",
    "last_name": "nom",
    "prenom": "prenom",
    "prenom_patient": "prenom",
    "first_name": "prenom",
    "age": "age",
    "age_patient": "age",
    "years_old": "age",
    "pathologie": "pathologie",
    "diagnostic_pathologie": "pathologie",
    "disease": "pathologie",
    "service": "service",
    "service_destination": "service",
    "service_demande": "service",
    "department": "service",
}

SERVICE_MAPPING = {
    "cardio": "Cardiologie",
    "cardiologie": "Cardiologie",
    "chir_cardio": "Chirurgie cardiovasculaire",
    "chirurgie_cardiovasculaire": "Chirurgie cardiovasculaire",
    "chirurgie_cardio": "Chirurgie cardiovasculaire",
    "ortho": "Orthopédie",
    "orthopedie": "Orthopédie",
    "pediatrie": "Pédiatrie",
    "gynecologie": "Gynécologie",
    "neuro": "Neurologie",
    "neurologie": "Neurologie",
    "endocrinologie": "Endocrinologie",
    "urgences": "Urgences",
}


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def normalize_key(value: object) -> str:
    text = normalize_text(value).lower()
    text = unidecode(text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def make_json_safe(value):
    """
    Convertit récursivement les objets pandas/numpy/GE en JSON valide.
    Objectif : éviter NaN, ndarray, Series, Timestamp, etc. dans PostgreSQL JSONB.
    """

    # Dict
    if isinstance(value, dict):
        return {str(k): make_json_safe(v) for k, v in value.items()}

    # List / tuple / set
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(v) for v in value]

    # Objets numpy / pandas qui ressemblent à des tableaux
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        return make_json_safe(value.tolist())

    # Timestamp pandas
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        try:
            return value.isoformat()
        except Exception:
            pass

    # NaN / NaT / None
    try:
        is_null = pd.isna(value)

        # pd.isna(array) retourne un tableau, pas un booléen
        if isinstance(is_null, (bool, type(None))):
            if is_null:
                return None
    except Exception:
        pass

    # Types simples JSON
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    # Fallback
    return str(value)


def parse_age(value: object) -> int | None:
    text = normalize_text(value).lower()

    if not text:
        return None

    if "mois" in text:
        return 0

    match = re.search(r"-?\d+", text)
    if not match:
        return None

    age = int(match.group())

    if age < 0 or age > 130:
        return None

    return age


def read_csv_smart(raw: bytes) -> pd.DataFrame:
    text = raw.decode("utf-8-sig")
    first_line = text.splitlines()[0]
    sep = ";" if first_line.count(";") > first_line.count(",") else ","
    return pd.read_csv(io.StringIO(text), sep=sep)


def pg_conn():
    return psycopg2.connect(
        host=os.environ["HOSPITAL_PG_HOST"],
        port=os.environ["HOSPITAL_PG_PORT"],
        dbname=os.environ["HOSPITAL_PG_DB"],
        user=os.environ["HOSPITAL_PG_USER"],
        password=os.environ["HOSPITAL_PG_PASSWORD"],
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


@dag(
    dag_id="ingest_patients_from_minio",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pipeline-as-code", "hopital", "great-expectations"],
)
def ingest_patients_from_minio():

    @task
    def start_ingestion() -> int:
        context = get_current_context()
        airflow_dag_run_id = context["dag_run"].run_id

        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_run (dag_run_id)
                    VALUES (%s)
                    RETURNING id
                    """,
                    (airflow_dag_run_id,),
                )
                return cur.fetchone()[0]

    @task
    def list_csv_files() -> list[str]:
        s3 = s3_client()
        response = s3.list_objects_v2(Bucket=BUCKET)

        return [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".csv")
        ]

    @task
    def build_file_work_items(ingestion_id: int, file_keys: list[str]) -> list[dict]:
        return [
            {
                "ingestion_id": ingestion_id,
                "file_key": file_key,
            }
            for file_key in file_keys
        ]

    @task
    def extract_and_clean_file(ingestion_id: int, file_key: str) -> dict:
        s3 = s3_client()
        obj = s3.get_object(Bucket=BUCKET, Key=file_key)
        raw = obj["Body"].read()

        df = read_csv_smart(raw)
        raw_rows = len(df)

        df.columns = [
            COLUMN_MAPPING.get(normalize_key(col), normalize_key(col))
            for col in df.columns
        ]

        required_cols = {"nom", "prenom", "age", "pathologie", "service"}
        missing = required_cols - set(df.columns)

        clean_rows = []
        rejected_rows = 0

        with pg_conn() as conn:
            with conn.cursor() as cur:

                if missing:
                    for _, row in df.iterrows():
                        cur.execute(
                            """
                            INSERT INTO patient_reject
                                (ingestion_run_id, source_file, raw_payload, reject_reason)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                ingestion_id,
                                file_key,
                                Json(make_json_safe(row.to_dict())),
                                f"colonnes manquantes: {sorted(missing)}",
                            ),
                        )
                        rejected_rows += 1

                    return {
                        "source_file": file_key,
                        "raw_rows": raw_rows,
                        "clean_rows": [],
                        "cleaned_rows": 0,
                        "rejected_rows": rejected_rows,
                    }

                for _, row in df.iterrows():
                    raw_payload = row.to_dict()

                    nom = normalize_text(row.get("nom")).title()
                    prenom = normalize_text(row.get("prenom")).title()
                    age = parse_age(row.get("age"))
                    pathologie = normalize_text(row.get("pathologie"))
                    service_raw_value = row.get("service")
                    service_raw = normalize_key(service_raw_value)
                    service = SERVICE_MAPPING.get(service_raw)

                    reject_reason = None

                    if nom.lower() == "test" or prenom.lower() == "patient":
                        reject_reason = "ligne de test"
                    elif not nom:
                        reject_reason = "nom manquant"
                    elif not prenom:
                        reject_reason = "prenom manquant"
                    elif age is None:
                        reject_reason = "age invalide"
                    elif not pathologie:
                        reject_reason = "pathologie manquante"
                    elif not service:
                        reject_reason = f"service inconnu: {service_raw_value}"

                    if reject_reason:
                        cur.execute(
                            """
                            INSERT INTO patient_reject
                                (ingestion_run_id, source_file, raw_payload, reject_reason)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                ingestion_id,
                                file_key,
                                Json(make_json_safe(raw_payload)),
                                reject_reason,
                            ),
                        )
                        rejected_rows += 1
                        continue

                    clean_rows.append(
                        {
                            "nom": nom,
                            "prenom": prenom,
                            "age": age,
                            "pathologie": pathologie,
                            "service": service,
                            "source_file": file_key,
                        }
                    )

        return {
            "source_file": file_key,
            "raw_rows": raw_rows,
            "clean_rows": clean_rows,
            "cleaned_rows": len(clean_rows),
            "rejected_rows": rejected_rows,
        }

    @task
    def consolidate_clean_rows(file_results: list[dict]) -> dict:
        all_rows = []
        raw_rows = 0
        rejected_rows = 0

        for result in file_results:
            raw_rows += result["raw_rows"]
            rejected_rows += result["rejected_rows"]
            all_rows.extend(result["clean_rows"])

        if not all_rows:
            return {
                "raw_rows": raw_rows,
                "rejected_rows": rejected_rows,
                "clean_rows": [],
                "cleaned_rows": 0,
                "duplicate_rows": 0,
            }

        df = pd.DataFrame(all_rows)

        before_dedup = len(df)
        df = df.drop_duplicates(
            subset=["nom", "prenom", "age", "pathologie", "service"]
        )
        duplicate_rows = before_dedup - len(df)

        return {
            "raw_rows": raw_rows,
            "rejected_rows": rejected_rows,
            "clean_rows": df.to_dict(orient="records"),
            "cleaned_rows": len(df),
            "duplicate_rows": duplicate_rows,
        }

    @task
    def validate_data_quality(ingestion_id: int, consolidated: dict) -> dict:
        clean_rows = consolidated["clean_rows"]

        if not clean_rows:
            with pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ge_validation_result
                            (ingestion_run_id, expectation_name, column_name, success, observed_value, details)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            ingestion_id,
                            "expect_table_row_count_to_be_between",
                            None,
                            False,
                            "0",
                            Json({"message": "aucune ligne propre à valider"}),
                        ),
                    )

            return {"ge_success": False}

        df = pd.DataFrame(clean_rows)
        gx_df = ge.from_pandas(df)

        gx_df.expect_table_columns_to_match_set(
            ["nom", "prenom", "age", "pathologie", "service", "source_file"],
            exact_match=False,
        )

        for col in ["nom", "prenom", "age", "pathologie", "service"]:
            gx_df.expect_column_values_to_not_be_null(col)

        gx_df.expect_column_values_to_be_between("age", min_value=0, max_value=130)
        gx_df.expect_column_values_to_be_in_set("service", VALID_SERVICES)

        gx_df.expect_column_mean_to_be_between("age", min_value=5, max_value=90)
        gx_df.expect_column_median_to_be_between("age", min_value=5, max_value=90)

        gx_df.expect_column_value_lengths_to_be_between(
            "nom", min_value=2, max_value=120
        )
        gx_df.expect_column_value_lengths_to_be_between(
            "prenom", min_value=2, max_value=120
        )

        gx_df.expect_table_row_count_to_be_between(min_value=1, max_value=1_000_000)

        validation_result = gx_df.validate()
        ge_success = bool(validation_result.success)

        with pg_conn() as conn:
            with conn.cursor() as cur:
                for result in validation_result.results:
                    expectation_config = result.expectation_config
                    kwargs = expectation_config.kwargs

                    expectation_name = expectation_config.expectation_type
                    column_name = kwargs.get("column")
                    observed_value = result.result.get("observed_value")

                    cur.execute(
                        """
                        INSERT INTO ge_validation_result
                            (ingestion_run_id, expectation_name, column_name, success, observed_value, details)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            ingestion_id,
                            expectation_name,
                            column_name,
                            bool(result.success),
                            None if observed_value is None else str(observed_value),
                            Json(make_json_safe(result.result)),
                        ),
                    )

        return {"ge_success": ge_success}

    @task
    def load_patients_to_postgres(ingestion_id: int, consolidated: dict) -> dict:
        clean_rows = consolidated["clean_rows"]

        if not clean_rows:
            return {"inserted_rows": 0}

        inserted_rows = 0

        with pg_conn() as conn:
            with conn.cursor() as cur:
                for row in clean_rows:
                    cur.execute(
                        "SELECT id FROM service WHERE nom = %s",
                        (row["service"],),
                    )
                    result = cur.fetchone()

                    if result is None:
                        cur.execute(
                            """
                            INSERT INTO service (nom)
                            VALUES (%s)
                            ON CONFLICT (nom) DO NOTHING
                            RETURNING id
                            """,
                            (row["service"],),
                        )
                        result = cur.fetchone()

                        if result is None:
                            cur.execute(
                                "SELECT id FROM service WHERE nom = %s",
                                (row["service"],),
                            )
                            result = cur.fetchone()

                    service_id = result[0]

                    cur.execute(
                        """
                        INSERT INTO patient
                            (nom, prenom, age, pathologie, service_id, source_file, ingestion_run_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (nom, prenom, age, pathologie, service_id)
                        DO NOTHING
                        """,
                        (
                            row["nom"],
                            row["prenom"],
                            int(row["age"]),
                            row["pathologie"],
                            service_id,
                            row["source_file"],
                            ingestion_id,
                        ),
                    )

                    inserted_rows += cur.rowcount

        return {"inserted_rows": inserted_rows}

    @task
    def finalize_ingestion(
        ingestion_id: int,
        file_keys: list[str],
        consolidated: dict,
        ge_result: dict,
        load_result: dict,
    ) -> dict:
        clean_rows = consolidated["clean_rows"]

        if clean_rows:
            df = pd.DataFrame(clean_rows)
            age_min = float(df["age"].min())
            age_avg = float(df["age"].mean())
            age_max = float(df["age"].max())
        else:
            age_min = None
            age_avg = None
            age_max = None

        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_run
                    SET finished_at = CURRENT_TIMESTAMP,
                        source_files_count = %s,
                        raw_rows = %s,
                        cleaned_rows = %s,
                        inserted_rows = %s,
                        rejected_rows = %s,
                        duplicate_rows = %s,
                        ge_success = %s,
                        age_min = %s,
                        age_avg = %s,
                        age_max = %s
                    WHERE id = %s
                    """,
                    (
                        len(file_keys),
                        consolidated["raw_rows"],
                        consolidated["cleaned_rows"],
                        load_result["inserted_rows"],
                        consolidated["rejected_rows"],
                        consolidated["duplicate_rows"],
                        ge_result["ge_success"],
                        age_min,
                        age_avg,
                        age_max,
                        ingestion_id,
                    ),
                )

        return {
            "ingestion_id": ingestion_id,
            "source_files_count": len(file_keys),
            "raw_rows": consolidated["raw_rows"],
            "cleaned_rows": consolidated["cleaned_rows"],
            "inserted_rows": load_result["inserted_rows"],
            "rejected_rows": consolidated["rejected_rows"],
            "duplicate_rows": consolidated["duplicate_rows"],
            "ge_success": ge_result["ge_success"],
            "age_min": age_min,
            "age_avg": age_avg,
            "age_max": age_max,
        }

    ingestion_id = start_ingestion()
    file_keys = list_csv_files()
    work_items = build_file_work_items(ingestion_id, file_keys)

    file_results = extract_and_clean_file.expand_kwargs(work_items)

    consolidated = consolidate_clean_rows(file_results)
    ge_result = validate_data_quality(ingestion_id, consolidated)
    load_result = load_patients_to_postgres(ingestion_id, consolidated)

    finalize_ingestion(
        ingestion_id=ingestion_id,
        file_keys=file_keys,
        consolidated=consolidated,
        ge_result=ge_result,
        load_result=load_result,
    )


ingest_patients_from_minio()