"""Optional Airflow DAG — subprocess to CLI (no duplicate ETL logic)."""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# nyc-taxi-etl project root (parent of dags/)
ROOT = Path(__file__).resolve().parents[1]
ETL_PYTHON = ROOT / ".venv" / "bin" / "python"

with DAG(
    dag_id="taxi_monthly_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "etl"],
) as dag:
    run_etl = BashOperator(
        task_id="run_monthly_etl",
        bash_command=(
            f"cd {ROOT} && {ETL_PYTHON} scripts/run_pipeline.py "
            "--year {{ dag_run.conf.get('year', 2024) }} "
            "--month {{ dag_run.conf.get('month', 5) }}"
        ),
    )
