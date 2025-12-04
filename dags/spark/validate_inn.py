from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
from datetime import datetime, timedelta
import logging

def get_postgres_connection(conn_id='rpul_dev'):
    try:
        conn = BaseHook.get_connection(conn_id)
        connection_params = {
            'host': conn.host,
            'port': conn.port,
            'database': conn.schema,
            'user': conn.login,
            'password': conn.password
        }
        connection_params = {k: v for k, v in connection_params.items() if v is not None}
        return psycopg2.connect(**connection_params)
    except Exception as e:
        logging.error(f"Ошибка при подключении к БД: {e}")
        raise

def update_status():
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cursor:
            update_query = """
                update cmdm.spark_data_camp 
                set client_info_status = 'actual'
                where client_info_status IN ('for_request', 'sent_to_spark')
                and inn in ( 
                    select vl.inn
                    from cmdm.v_spark_data_for_dag vl 
                    where (current_date - vl.actual_date) <= interval '3 days'
                )
            """
            cursor.execute(update_query)
            updated_count = cursor.rowcount
            conn.commit()
            logging.info(f"обновлено записей: {updated_count}")
    except Exception as e:
        logging.error(f"Ошибка при обновлении: {e}")
        raise
    finally:
        if conn:
            conn.close()

with DAG(
    dag_id='spark_validate_inn_status',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 11, 16),
        'retries': 0
    },
    description='Обновление статусов в spark_data_camp на основе v_spark_data',
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:  
    update_status_task = PythonOperator(
        task_id='update_status',
        python_callable=update_status
    )

    update_status_task