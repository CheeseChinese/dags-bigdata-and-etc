from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import sys
from airflow.models import Variable
import io
og_path = sys.path.copy()
#sys.path.insert(0, '/path/to/packaegs')
from openpyxl import load_workbook
IN_DIR = Variable.get('refusers')

def files_exist(**kwargs):
	full_paths = [os.path.join(IN_DIR, f) for f in os.listdir(IN_DIR) if os.path.isfile(os.path.join(IN_DIR, f))
                  and f.lower().startswith('refusers_do_tel') and f.lower().endswith('.xlsx')]
	if full_paths:
		filename = max(full_paths, key=os.path.getmtime)
		print("This is", filename)
		kwargs['ti'].xcom_push(key='filename', value=filename)
		kwargs['ti'].xcom_push(key='ref_do_tel_exist', value=True)
		return True
	else:
		kwargs['ti'].xcom_push(key='ref_do_tel_exist', value=False)
		return False
def from_xlsx_to_db(**kwargs):
	ti = kwargs['ti']
	filename = ti.xcom_pull(task_ids='check_files_exist', key='filename')
	if filename:
		postgres_conn_id = 'conn'
		conn = BaseHook.get_connection(postgres_conn_id)
		engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}', echo=True)
		try:
			#df = pd.read_excel(filename, sheet_name = 0, header = 0, names=['tel_sms', 'in_list_date'])
			in_mem_file = None
			with open(filename, "rb") as f:
				in_mem_file = io.BytesIO(f.read())
				wb = load_workbook(in_mem_file, read_only=True)
			data = wb.active.values
			columns =[item.lower() for item in next(data)[0:]]
			df = pd.DataFrame(data, columns=columns)
			df['in_list_date'] = pd.to_datetime(df['in_list_date'], format='%d.%m.%Y')
			df['record_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			df['record_created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			with engine.connect() as connection:
				df.to_sql('refusers_do_tel', con=engine,schema='gate', if_exists='append', index=False)
		except Exception as e:
			print(f"Ошибка при загрузке в базу данных: {e}")
			raise
	else:
		print("Файл не найден. Загрузка данных пропущена.")
my_dag = DAG(
    dag_id='load_refusers_do_tel',
    default_args={
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(seconds=60,)
    },
    start_date=datetime(2025, 1, 15),
    catchup=False,
    schedule='10 19 * * *',
    max_active_runs=1,
)
from_xlsx_to_db_task = PythonOperator(
task_id='refusers_xlsx_to_db',
python_callable=from_xlsx_to_db,
provide_context=True,
dag=my_dag
)
start_log_task = SQLExecuteQueryOperator(
task_id='start_process_log',
conn_id='conn',
sql='/sql/log_start.sql',
params={"proc_name" : "'refusers_do_tel_start'", "table_name" : "gate.refusers_do_tel"},
split_statements=True,
autocommit = True,
hook_params ={"enable_log_db_messages" : True},
dag=my_dag
)
finish_log_task = SQLExecuteQueryOperator(
task_id='finish_process_log',
conn_id='conn',
sql='/sql/log_end.sql',
params={"proc_name" : "'refusers_do_tel_end'"},
autocommit = True,
hook_params ={"enable_log_db_messages" : True},
dag=my_dag
)

files_here_task = PythonOperator(
    task_id='check_files_exist',
    python_callable=files_exist,
    provide_context=True,
    dag=my_dag
)

@task.branch(task_id='branch_task')
def branch_func(ti=None):
	xcom_value = ti.xcom_pull(task_ids='check_files_exist', key='ref_do_tel_exist')
	if xcom_value == False:
		return "error_stop"
	else:
		return ["start_process_log", "refusers_xlsx_to_db"]

branch_op = branch_func()

stop_task = DummyOperator(
    task_id='error_stop',
    dag=my_dag
)
files_here_task >> branch_op
branch_op >> stop_task
branch_op >> start_log_task >> from_xlsx_to_db_task >> finish_log_task
sys.path = og_path
