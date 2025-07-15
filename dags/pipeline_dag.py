from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from pipeline.airflow_ingestion import airflow_run_ingestion

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="pipeline_nfe",
    default_args=default_args,
    catchup=False,
    tags=["raw", "ingestao", "nfe"],
    description="Pipeline de ingestão de NF-e do Portal da Transparência.",
):  # type: ignore

    ingest_task = PythonOperator(
        task_id="executar_pipeline_nfe",
        python_callable=airflow_run_ingestion,
        params={
            "organ_code": "36000",
            "year_emission": 2024,
            "page_number": 1700,
            "max_pages": 3000,
        },
    )
