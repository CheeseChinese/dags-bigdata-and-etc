from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import sys
from airflow.models import Variable
venv_path = 'path'
sys.path.append(venv_path)

IN_DIR = Variable.get('alfa_indir')


def from_csv_to_db(**kwargs):
    # Проверка наличия файлов в директории IN
	postgres_conn_id = 'con'
	conn = BaseHook.get_connection(postgres_conn_id)
	engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}', echo = True)
	full_paths = [ os.path.join(IN_DIR, f) for f in os.listdir(IN_DIR) if os.path.isfile(os.path.join(IN_DIR, f))
	and f.lower().startswith('dic_card_sta') and f.lower().endswith('.csv')]
	if full_paths:
		filename = max(full_paths, key=os.path.getmtime)
		if filename:
			df = pd.read_csv(filename, sep=';',header=0, names=['crd_stat', 'crd_stat_desc', 'crd_stat_desc_rus'],
                             encoding='cp1251',)[['crd_stat', 'crd_stat_desc', 'crd_stat_desc_rus']]
			with engine.connect() as connection:
				df.to_sql('dic_card_statuses', con=engine,schema='gate', if_exists='append', index=False)
	else:
            # Если файлов нет, помещаем информацию в xcom
		kwargs['ti'].xcom_push('dic_card_statuses_exists', False)
my_dag = DAG(
    dag_id='extract_dic_card_statuses',
    default_args={
        'owner': 'airflow',
        'retries': 0,
    },
    start_date=datetime(2025, 1, 9),
    catchup=False,
    schedule="3 6 * * 3",
    max_active_runs=1,
)
from_csv_to_db_task = PythonOperator(
task_id='card_stts_csv_to_db',
python_callable=from_csv_to_db,
provide_context=True,
dag=my_dag
)
start_log_task = SQLExecuteQueryOperator(
task_id='start_process_log',
conn_id='con',
sql='/sql/log_start.sql',
params={"proc_name" : "'dic_card_statuses_start'", "table_name" : "gate.dic_card_statuses"},
split_statements=True,
autocommit = True,
hook_params ={"enable_log_db_messages" : True},
dag=my_dag
)
finish_log_task = SQLExecuteQueryOperator(
task_id='finish_process_log',
conn_id='con',
sql='/sql/log_end.sql',
params={"proc_name" : "'dic_card_statuses_end'"},
autocommit = True,
hook_params ={"enable_log_db_messages" : True},
dag=my_dag
)
start_log_task >> from_csv_to_db_task >> finish_log_task
