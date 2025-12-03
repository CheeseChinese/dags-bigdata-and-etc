from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor
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
        return psycopg2.connect(**connection_params, cursor_factory=RealDictCursor)
        
    except Exception as e:
        logging.error(f"Ошибка при подключении к БД: {e}")
        raise


def process_records():
    conn = None
    try:
        conn = get_postgres_connection()
        
        with conn.cursor() as cursor:
            # Получаем записи из таблицы spark_bf_export
            cursor.execute("SELECT inn, camp_id FROM cmdm.spark_bf_export")
            records = cursor.fetchall()
            
            if not records:
                logging.info("В таблице spark_bf_export нет записей для обработки")
                return
                
            logging.info(f"Найдено {len(records)} записей в таблице spark_bf_export")
            
            for record in records:
                check_relevance(conn, record)
            wipe_export_table(conn, records)
            
            conn.commit()
            logging.info("Обработка записей завершена успешно")
            
    except Exception as e:
        logging.error(f"Ошибка при работе с БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def check_relevance(connection, record):
    inn = record['inn']
    camp_id = record['camp_id']
    
    cursor = None
    try:
        cursor = connection.cursor()
        check_query = """
            SELECT COUNT(*) FROM cmdm.spark_data_camp 
            WHERE inn = %s AND camp_id = %s AND LOWER(client_info_status) <> 'done'
        """
        cursor.execute(check_query, (inn, camp_id))
        exists_in_spark_data_camp = cursor.fetchone()['count'] > 0
        
        if exists_in_spark_data_camp:
            logging.info(f"Запись inn={inn}, camp_id={camp_id} уже существует в spark_data_camp с client_info_status <> 'done' - пропускаем")
            return
        check_view_query = """
            SELECT MAX(actual_date) as actual_date FROM cmdm.v_spark_data_for_dag
            WHERE inn = %s
            GROUP BY inn """
        cursor.execute(check_view_query, (inn,))
        view_record = cursor.fetchone()
        
        if view_record and view_record['actual_date']:
            actual_date = view_record['actual_date']
            days_diff = (datetime.now().date() - actual_date.date()).days
            
            if days_diff <= 3:
                client_info_status = 'actual'
                logging.info(f"Запись inn={inn} найдена в cmdm.v_spark_data, разница дней: {days_diff} - статус: {client_info_status}")
            else:
                client_info_status = 'for_request'
                logging.info(f"Запись inn={inn} найдена в cmdm.v_spark_data, разница дней: {days_diff} - статус: {client_info_status}")
        else:
            client_info_status = 'for_request'
            logging.info(f"Запись inn={inn} не найдена в cmdm.v_spark_data - статус: {client_info_status}")
        
        # Вставляем запись в spark_data_camp
        insert_query = """
            INSERT INTO cmdm.spark_data_camp (inn, camp_id, client_info_status)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (inn, camp_id, client_info_status))
        
        logging.info(f"Запись inn={inn}, camp_id={camp_id} добавлена в spark_data_camp со статусом: {client_info_status}")
        
    except Exception as e:
        logging.error(f"Ошибка при обработке записи inn={inn}, camp_id={camp_id}: {e}")
        raise
    finally:
        if cursor:
            cursor.close()


def wipe_export_table(connection, records):
    cursor = None
    try:
        cursor = connection.cursor()
        
        delete_query = """
            DELETE FROM cmdm.spark_bf_export 
            WHERE inn = %s AND camp_id = %s
        """
        
        for record in records:
            cursor.execute(delete_query, (record['inn'], record['camp_id']))
        
        logging.info(f"Удалено {len(records)} записей из таблицы spark_bf_export")
        
    except Exception as e:
        logging.error(f"Ошибка при удалении записей из spark_bf_export: {e}")
        raise
    finally:
        if cursor:
            cursor.close()


with DAG(
    dag_id='spark_process_bf_import',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 11, 12),
        'retries': 0
    },
    description='Обработка записей из таблицы spark_bf_export каждые 15 минут',
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:

    process_task = PythonOperator(
        task_id='process_records',
        python_callable=process_records
    )