from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import today
import subprocess
import pendulum

# =========================
# Default arguments
# =========================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# =========================
# DAG definition
# =========================
with DAG(
    dag_id="customer_churn_pipeline",
    default_args=default_args,
    description="Customer Churn ETL Pipeline: Clean & Load",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule='@daily', # Run daily at 6 AM
    catchup=False,
) as dag:

    # -------------------------
    # Task 1: Clean data
    # -------------------------
    def run_clean_script():
        """
        Run clean_customer_churn.py script
        """
        subprocess.run(
            ["python", "/opt/airflow/scripts/clean_customer_churn.py"],
            check=True
        )

    clean_data_task = PythonOperator(
        task_id="clean_customer_churn_data",
        python_callable=run_clean_script,
    )

    # -------------------------
    # Task 2: Load data
    # -------------------------
    def run_load_script():
        """
        Run load_customer_churn.py script
        """
        subprocess.run(
            ["python", "/opt/airflow/scripts/load_customer_churn.py"],
            check=True
        )

    load_data_task = PythonOperator(
        task_id="load_customer_churn_to_postgres",
        python_callable=run_load_script,
    )

    # -------------------------
    # Pipeline order
    # -------------------------
    clean_data_task >> load_data_task
