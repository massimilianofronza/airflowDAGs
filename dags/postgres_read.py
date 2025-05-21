from airflow.models.dag import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "postgres_operator_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 19),
    schedule=None,
    catchup=False,
) as dag:
    read_pet_table = SQLExecuteQueryOperator(
        task_id="read_pet_table",
        conn_id="postgres_default",
        sql="""
            SELECT * FROM pet;
            """,
    )
read_pet_table