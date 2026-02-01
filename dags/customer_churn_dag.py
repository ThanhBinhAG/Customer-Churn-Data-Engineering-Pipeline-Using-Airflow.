from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import sys
import pendulum

sys.path.insert(0, "/opt/airflow/scripts")

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
    description="Customer Churn ETL Pipeline: Clean (in memory) & Load to Postgres",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:

    # -------------------------
    # Task 1: Clean data (no file output, return DataFrame for XCom)
    # -------------------------
    def run_clean():
        import clean_customer_churn as clean_module
        return clean_module.get_cleaned_data()

    clean_data_task = PythonOperator(
        task_id="clean_customer_churn_data",
        python_callable=run_clean,
    )

    # -------------------------
    # Task 2: Load data (receive DataFrame from XCom, push to Postgres)
    # -------------------------
    def run_load(ti):
        import load_customer_churn as load_module
        df = ti.xcom_pull(task_ids="clean_customer_churn_data")
        if df is None:
            raise ValueError("No data from clean task.")
        load_module.load_to_postgres_from_dataframe(df)

    load_data_task = PythonOperator(
        task_id="load_customer_churn_to_postgres",
        python_callable=run_load,
    )

    # -------------------------
    # Pipeline order
    # -------------------------
    clean_data_task >> load_data_task
