from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

DAG_ID = "postgres_operator_dag_read"

def fetch_pet_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records("SELECT * FROM pet;")
    for record in records:
        print(record)
    return records  # Optional: store in XCom

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 19),
    schedule=None,
    catchup=False,
) as dag:
    read_pet_table_hook = PythonOperator(
        task_id="read_pet_table_hook",
        python_callable=fetch_pet_table,
    )
    read_pet_table_xcom = SQLExecuteQueryOperator(
        task_id="read_pet_table_xcom",
        conn_id="postgres_default",
        sql="""
            SELECT * FROM pet;
            """,
        return_last=True,
    )

read_pet_table_hook >> read_pet_table_xcom