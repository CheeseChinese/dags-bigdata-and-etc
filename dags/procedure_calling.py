from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
  dag_id="daily2_postgr_monitoring",
  start_date=datetime(2024, 6, 1),
  catchup=False,
  schedule='05 7 * * *',
  max_active_runs=1,
  is_paused_upon_creation = True,
  default_args={
    'retries': 0,
    'retry_delay': timedelta(seconds=60),
  },
)
def do_daily_mon():

  call_function = PostgresOperator(
    task_id='daily_mon',
    postgres_conn_id='conn',
    sql='call gate.daily_monitoring();',
    autocommit = True
  )

  call_function

do_daily_mon()
