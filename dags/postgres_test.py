from airflow.models.dag import DAG
from datetime import datetime
import os
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 5, 19),
    schedule=None,
    catchup=False,
) as dag:
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
            """,
    )
    populate_pet_table = SQLExecuteQueryOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, owner)
            VALUES
                ('Fluffy', 'cat', '2020-01-01', 'Alice'),
                ('Rover', 'dog', '2019-05-15', 'Bob'),
                ('Whiskers', 'cat', '2021-03-10', 'Charlie');
            """,
    )

create_pet_table >> populate_pet_table