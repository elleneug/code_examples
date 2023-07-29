from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from e_sidorova_plugins.e_sidorova_ram_location import (
    ESidorovaRamLocationOperator,
)

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-sidorova',
    'poke_interval': 600,
    "depends_on_past": False
}

with DAG(
    "e-sidorova_RickAndMorty",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    description="Top-3 locations, Rick and Morty",
    catchup=False,
    tags=["e-sidorova_ds"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"""CREATE TABLE IF NOT EXISTS e_sidorova_ram_location (
            id INT PRIMARY KEY,
            name TEXT,
            type TEXT,
            dimension TEXT,
            resident_cnt INT
        )""",
    )

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"TRUNCATE e_sidorova_ram_location",
    )

    top3_locations = ESidorovaRamLocationOperator(task_id="top3_locations")

    create_table >> truncate_table >> top3_locations