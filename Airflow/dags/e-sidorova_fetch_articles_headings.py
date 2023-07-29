from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-sidorova',
    'poke_interval': 600
}

def fetch_article_heading(**context):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    day_of_week = context['execution_date'].strftime("%w")  # Get the day of the week as an integer (0-6, where Sunday=0)
    cursor.execute("SELECT heading FROM articles WHERE id = %s", (day_of_week,))
    query_res = cursor.fetchall()
    heading = query_res[0][0] if query_res else None
    logging.info(f"Article Heading: {heading}")
    return heading

dag = DAG("e-sidorova_fetch_articles_headings",
          schedule_interval='0 0 * * 1-6',  # Run from Monday to Saturday (0 represents Sunday)
          default_args=DEFAULT_ARGS,
          tags=['e-sidorova_ds'],
          start_date=datetime(2022, 3, 1),
          end_date=datetime(2023, 7, 18)  # Update the end date to a future date
          )

# Configure logging
logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

# Create tasks
fetch_heading = PythonOperator(
    task_id='fetch_article_heading',
    python_callable=fetch_article_heading,
    provide_context=True,
    dag=dag
)

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag
)

# Define task dependencies
dummy_task >> fetch_heading