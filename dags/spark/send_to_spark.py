from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import logging
import requests
from requests.exceptions import RequestException
import time


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


def get_records_to_process():
    conn = None
    try:
        conn = get_postgres_connection()
        
        with conn.cursor() as cursor:
            query = """
                SELECT id, inn, client_info_status 
                FROM cmdm.spark_data_camp 
                WHERE client_info_status = 'for_request'
            """
            cursor.execute(query)
            records = cursor.fetchall()
            
            logging.info(f"Найдено записей для обработки: {len(records)}")
            return records
            
    except Exception as e:
        logging.error(f"Ошибка при получении записей: {e}")
        return []
    finally:
        if conn:
            conn.close()


def send_api_request(url, inn, max_retries=3):
    headers = {'Content-Type': 'application/json'}
    data = {'sparkId': '', 'inn': str(inn), 'ogrn': ''}
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Попытка {attempt + 1} для INN {inn} на URL {url}")
            response = requests.post(url, json=data, headers=headers, timeout=30, verify=False)
            
            if response.status_code == 200:
                logging.info(f"Успешный запрос для INN {inn}")
                return True
            else:
                logging.warning(f"Неудачный запрос для INN {inn}. Статус: {response.status_code}")
                
        except RequestException as e:
            logging.error(f"Ошибка соединения для INN {inn} (попытка {attempt + 1}): {e}")
        
        if attempt < max_retries - 1:
            time.sleep(2)
    
    return False


def process_records():
    API_12_DIGITS  = "https://sparkgateway.rec-platform.dev.rshbdev.ru/api/v1/redirectquery/getentrepreneurshortreport"
    API_10_DIGITS_1 = "https://sparkgateway.rec-platform.dev.rshbdev.ru/api/v1/redirectquery/getcompanystructure"
    API_10_DIGITS_2 = "https://sparkgateway.rec-platform.dev.rshbdev.ru/api/v1/redirectquery/getcompanyextendedreport"
    API_10_DIGITS_3 = "https://sparkgateway.rec-platform.dev.rshbdev.ru/api/v1/redirectquery/getcompanyshortreport"
    
    records = get_records_to_process()
    
    if not records:
        logging.info("Нет записей для обработки")
        return
    
    successful_updates = []
    
    for record in records:
        record_id = record['id']
        inn = str(record['inn']).strip() if record['inn'] else ""
        
        if not inn:
            logging.warning(f"Пропуск записи {record_id}: пустой INN")
            continue
        
        logging.info(f"Обработка записи {record_id} с INN: {inn}")
        
        if len(inn) > 10:
            success = send_api_request(API_12_DIGITS, inn)
        elif len(inn) == 10:
            success1 = send_api_request(API_10_DIGITS_1, inn)
            success2 = send_api_request(API_10_DIGITS_2, inn)
            success3 = send_api_request(API_10_DIGITS_3, inn)
            success = success1 and success2 and success3
        else:
            logging.error(f"Неверная длина INN {inn}: {len(inn)} символов")
            continue
        
        if success:
            successful_updates.append(record_id)
            logging.info(f"Успешно обработана запись {record_id}")
        else:
            logging.error(f"Не удалось обработать запись {record_id} после всех попыток")
    
    if successful_updates:
        update_records_status(successful_updates)
        logging.info(f"Обновлено статусов: {len(successful_updates)}")
    else:
        logging.info("Нет успешно обработанных записей для обновления")


def update_records_status(record_ids):
    conn = None
    try:
        conn = get_postgres_connection()
        
        with conn.cursor() as cursor:
            id_list = tuple(record_ids)
            if len(id_list) == 1:
                query = """
                    UPDATE cmdm.spark_data_camp 
                    SET client_info_status = 'sent_to_spark' 
                    WHERE id = %s
                """
                cursor.execute(query, (id_list[0],))
            else:
                query = """
                    UPDATE cmdm.spark_data_camp 
                    SET client_info_status = 'sent_to_spark' 
                    WHERE id IN %s
                """
                cursor.execute(query, (id_list,))
            
            conn.commit()
            logging.info(f"Успешно обновлены записи: {record_ids}")
            
    except Exception as e:
        logging.error(f"Ошибка при обновлении статусов: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


with DAG(
    dag_id='spark_inn_to_api',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 11, 12),
        'retries': 0
    },
    description='Обработка записей spark_data_camp и отправка в API',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:
    
    process_records_task = PythonOperator(
        task_id='process_records',
        python_callable=process_records
    )

    process_records_task