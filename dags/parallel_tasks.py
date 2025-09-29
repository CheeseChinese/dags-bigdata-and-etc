from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
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

postgres_conn_id = 'conn'
conn = BaseHook.get_connection(postgres_conn_id)
engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}', echo = True)

def accounts_csv_to_db(**kwargs):
	# Проверка наличия файлов в директории IN
	full_paths = [os.path.join(IN_DIR, f) for f in os.listdir(IN_DIR)
	if os.path.isfile(os.path.join(IN_DIR, f)) and f.lower().startswith('accounts') and f.lower().endswith('.csv')]
	if full_paths:
		filename = max(full_paths, key=os.path.getmtime)
		print('this is file',filename )
		if filename:
			df = pd.read_csv(filename, sep=';',header=None,
			names=['do', 'account_number','emission_docnum', 'date_open', 'date_close', 'stts', 'val','num', 'num2', 'num3'],
			usecols=['do', 'account_number','emission_docnum', 'date_open', 'date_close', 'stts'],
			encoding='UTF-8',
			dtype={'do': 'str', 'emission_docnum': 'str'})[['do', 'account_number','emission_docnum', 'date_open', 'date_close', 'stts']]
			df['date_open'] = pd.to_datetime(df['date_open'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
			df['date_open'] = df['date_open'].dt.strftime('%Y-%m-%d %H:%M:%S')
			df['date_close'] = pd.to_datetime(df['date_close'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
			df['date_close'] = df['date_close'].dt.strftime('%Y-%m-%d %H:%M:%S')
			with engine.connect() as connection:
				connection.execute("""CREATE table if not exists gate.accounts_tmp (
                                      ...);""")
				df.to_sql('accounts_tmp', con=engine,schema='gate', if_exists='append', index=False)
	else:
            # Если файлов нет, помещаем информацию в XCom
		kwargs['ti'].xcom_push(key='accounts_files_exist', value=False)
def companies_csv_to_db(**kwargs):
	full_paths_c = [os.path.join(IN_DIR, f) for f in os.listdir(IN_DIR)
	if  os.path.isfile(os.path.join(IN_DIR, f)) and f.lower().startswith('iss_arg') and f.lower().endswith('.csv')]
	if full_paths_c:
		filename_c = max(full_paths_c, key=os.path.getmtime)
		if filename_c:
			with open(filename_c,'r', encoding='windows-1251') as f:
				lines = f.readlines()
			processed_lines = []
			for i, line in enumerate(lines):
				parts = line.strip().split(';')
				if len(parts) == 10:
					print(parts)
					parts[8] = parts[9]
					processed_line = ';'.join(parts[:9])
					processed_lines.append(processed_line)
				else:
					processed_lines.append(line.strip())
			temp_file = os.path.join(IN_DIR, 'temp_comp.csv')
			with open(temp_file, 'w', encoding='windows-1251') as f:
				f.write('\n'.join(processed_lines))
			df1 = pd.read_csv(temp_file, sep=';',header=None,
			names=['emission_docnum', 'do' ,'emission_name', 'docnum_type', 'docnum_date_open', 'docnum_date_end', 'docnum_date_close',
			'company_name', 'inn'],
			encoding='windows-1251',
			dtype={'do': 'str', 'emission_docnum': 'str', 'inn': 'str' },
			)[['emission_docnum', 'do' ,'emission_name', 'docnum_type', 'docnum_date_open', 'docnum_date_end', 'docnum_date_close',
			'company_name', 'inn']]
			df1['docnum_date_open'] = pd.to_datetime(df1['docnum_date_open'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
			df1['docnum_date_open'] = df1['docnum_date_open'].dt.strftime('%Y-%m-%d %H:%M:%S')
			df1['docnum_date_end'] = pd.to_datetime(df1['docnum_date_end'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
			df1['docnum_date_end'] = df1['docnum_date_end'].dt.strftime('%Y-%m-%d %H:%M:%S')
			df1['docnum_date_close'] = pd.to_datetime(df1['docnum_date_close'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
			df1['docnum_date_close'] = df1['docnum_date_close'].dt.strftime('%Y-%m-%d %H:%M:%S')
			with engine.connect() as connection:
				connection.execute("""create table if not exists gate.companies_tmp (
                                       ...);""")
				df1.to_sql('companies_tmp', con=engine,schema='gate', if_exists='append', index=False)
			if os.path.exists(temp_file):
				os.remove(temp_file)
	else:
		# Если файлов нет, помещаем информацию в XCom
		kwargs['ti'].xcom_push(key='iss_arg_files_exist', value=False)
my_dag = DAG(
    dag_id='load_alfa_accounts_and_companies',
    default_args={
        'owner': 'airflow',
        'retries': 0,
    },
    start_date=datetime(2025, 1, 24),
    catchup=False,
    schedule='35 8 * * *',
    max_active_runs=1,
)

start_task = DummyOperator(task_id='start', dag=my_dag)

accounts_csv_to_db_task = PythonOperator(
task_id='accounts_csv_to_db',
python_callable=accounts_csv_to_db,
provide_context=True,
dag=my_dag
)
companies_csv_to_db_task = PythonOperator(
task_id='companies_csv_to_db',
python_callable=companies_csv_to_db,
provide_context=True,
dag=my_dag
)

call_function = PostgresOperator(
task_id='make_alfa_tables',
postgres_conn_id='conn',
sql='call gate.make_alfa_tables();',
autocommit = True,
dag=my_dag
)

start_task >> [accounts_csv_to_db_task, companies_csv_to_db_task]
accounts_csv_to_db_task >> call_function
companies_csv_to_db_task >> call_function
