from datetime import datetime, timezone
from pathlib import Path
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from shared.db import connect

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"

def task_validate_raw(**context) -> str:
    """1. Validasi keberadaan folder data"""
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    print(f"Validating raw data for run: {run_id}")

    required_folders = ["operational", "events", "inventory", "reference"]
    missing = [folder for folder in required_folders if not (DATA_ROOT / folder).exists()]
    if missing:
        raise AirflowException(f"Folder raw data tidak lengkap: {missing}")

    # Catat awal run ke ops.pipeline_runs
    with connect("local") as conn:
        conn.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at_utc, status, current_stage)
            VALUES (%s, %s, 'RUNNING', 'validate_raw')
            ON CONFLICT (run_id) DO UPDATE SET status = 'RUNNING'
            """,
            (run_id, datetime.now(timezone.utc))
        )
        conn.commit()

    context["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    return run_id

def task_initialize_schema(**context) -> None:
    """2. Inisialisasi schema ops, bronze, silver, dan gold"""
    schema_files = [
        PROJECT_ROOT / "database" / "schemas" / "ops.sql",
        PROJECT_ROOT / "database" / "schemas" / "bronze.sql",
        PROJECT_ROOT / "database" / "schemas" / "silver.sql",
        PROJECT_ROOT / "database" / "schemas" / "gold.sql",
    ]
    with connect("local") as conn:
        for sql_file in schema_files:
            if sql_file.exists():
                print(f"Applying {sql_file.name}...")
                conn.execute(sql_file.read_text(encoding="utf-8"))
        conn.commit()

def task_load_bronze(**context) -> None:
    """3. Muat seluruh raw data ke Bronze."""
    from pipelines.bronze.build_bronze import load_bronze
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    load_bronze(run_id, target="local")

def task_build_silver(**context) -> None:
    """4. Transformasi data ke Silver (Clean vs Reject)"""
    from pipelines.silver.build_silver import build_silver
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    build_silver(run_id, target="local")

def task_quality_gate(**context) -> None:
    """5. Quality Gate: Cek kelayakan data sebelum lanjut ke Gold."""
    from pipelines.quality.checks import run_silver_quality_gate
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    try:
        run_silver_quality_gate(run_id, target="local")
    except Exception as e:
        with connect("local") as conn:
            conn.execute(
                "UPDATE ops.pipeline_runs SET status = 'FAILED', error_message = %s, current_stage = 'quality_gate' WHERE run_id = %s",
                (str(e), run_id)
            )
            conn.commit()
        raise AirflowException(f"Quality gate gagal: {e}")

def task_build_gold(**context) -> None:
    """6. Transformasi data ke Gold layer."""
    from pipelines.gold.build_gold import build_gold
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    build_gold(run_id, target="local")

def task_validate_gold_and_record_metadata(**context) -> None:
    """7. Validasi final Gold layer dan catat status SUCCESS di ops.pipeline_runs."""
    run_id = context["ti"].xcom_pull(task_ids="validate_raw", key="pipeline_run_id")
    with connect("local") as conn:
        gold_order_count = conn.execute(
            "SELECT COUNT(*) FROM gold.order_360 WHERE pipeline_run_id = %s", (run_id,)
        ).fetchone()[0]

        if gold_order_count == 0:
            raise AirflowException("Gold validation failed: gold.order_360 tidak memiliki record!")

        conn.execute(
            """
            UPDATE ops.pipeline_runs
            SET status = 'SUCCESS',
                current_stage = 'completed',
                completed_at_utc = %s
            WHERE run_id = %s
            """,
            (datetime.now(timezone.utc), run_id)
        )
        conn.commit()
    print(f"Pipeline {run_id} berhasil diselesaikan dengan sukses!")

# Definisi DAG
with DAG(
    dag_id="omnichannel_retail_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"owner": "student", "retries": 0},
    tags=["retail", "medallion", "bronze", "silver", "gold"],
) as dag:


    t1 = PythonOperator(task_id="initialize_schema", python_callable=task_initialize_schema)
    t2 = PythonOperator(task_id="validate_raw", python_callable=task_validate_raw)
    t3 = PythonOperator(task_id="load_bronze", python_callable=task_load_bronze)
    t4 = PythonOperator(task_id="build_silver", python_callable=task_build_silver)
    t5 = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)
    t6 = PythonOperator(task_id="build_gold", python_callable=task_build_gold)
    t7 = PythonOperator(task_id="validate_gold_and_record_metadata", python_callable=task_validate_gold_and_record_metadata)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7