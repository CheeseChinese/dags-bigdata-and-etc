from airflow import DAG
from airflow.decorators import task
from airflow.models import DagModel
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagNotFound
from datetime import datetime
import logging
import psycopg2


# Вспомогательная функция для обновления статус
def _update_db_status(camp_id, status):
    try:
        conn_info = BaseHook.get_connection('rpul_dev')
        conn = psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port,
            database=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password
        )
        with conn.cursor() as cursor:
            update_query = """
                UPDATE cmdm.spark_data_camp 
                SET client_info_status = %s 
                WHERE camp_id = %s
            """
            cursor.execute(update_query, (status, camp_id))
            logging.info(f"Camp_id {camp_id}: статус обновлен на '{status}'")
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при обновлении БД для {camp_id}: {e}")
        raise
    finally:
        if conn:
            conn.close()

with DAG(
    dag_id='spark_campaign_trigger2',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    },
    start_date=datetime(2025, 11, 22), 
    description='Динамический запуск DAG обработчиков кампаний',
    schedule_interval='*/15 * * * *',
    catchup=False
) as dag:

    @task(task_id='get_camp_ids')
    def get_camp_ids_to_process():
        """Получает список ID кампаний для обработки."""
        conn = None
        try:
            conn_info = BaseHook.get_connection('rpul_dev')
            conn = psycopg2.connect(
                host=conn_info.host,
                port=conn_info.port,
                database=conn_info.schema,
                user=conn_info.login,
                password=conn_info.password
            )

            with conn.cursor() as cursor:
                query = """
                    with cte as(
                        select camp_id, 
                        count(client_info_status) filter(where lower(client_info_status) ='actual') over (partition by camp_id) as count_actual,
                        count(client_info_status) over (partition by camp_id) as count_all
                        from cmdm.spark_data_camp
                    )
                    select DISTINCT camp_id 
                    from cte 
                    where count_actual = count_all
                """
                cursor.execute(query)
                result = cursor.fetchall()
                camp_ids = [str(row[0]) for row in result]
                
                logging.info(f"Найдено camp_id для обработки: {len(camp_ids)}")
                return camp_ids
                
        except Exception as e:
            logging.error(f"Ошибка при получении camp_id: {e}")
            return []
        finally:
            if conn:
                conn.close()

    @task(task_id='process_campaign_trigger')
    def process_single_campaign(camp_id: str):
        target_dag_id = f"{camp_id.upper()}.from_spark_{camp_id.lower()}"
        logging.info(f"Целевой DAG ID: {target_dag_id}")

        try:
            dag_model = DagModel.get_dagmodel(target_dag_id)
            
            if not dag_model:
                error_msg = "error: dag not found"
                logging.warning(f"DAG {target_dag_id} не найден в Airflow.")
                _update_db_status(camp_id, error_msg)
                return

            # 2. Пытаемся запустить DAG
            trigger_dag(
                dag_id=target_dag_id,
                conf={
                    'camp_id': camp_id,
                    'source': 'campaign_trigger_dag'
                },
                replace_microseconds=False,

            )
            logging.info(f"DAG {target_dag_id} успешно поставлен в очередь.")

            # 3. Если запуск прошел без исключений, обновляем статус на done
            _update_db_status(camp_id, 'done')

        except DagNotFound:
             # Этот блок сработает, если trigger_dag выкинет специфичную ошибку
            error_msg = "{target_dag_id} not found"
            logging.error(f"Не удалось найти DAG {target_dag_id}")
            _update_db_status(camp_id, error_msg)
            
        except Exception as e:
            # Любая другая ошибка при запуске
            logging.error(f"Критическая ошибка при запуске DAG для {camp_id}: {e}")
            raise e

    # 1. Получаем список ID
    ids_list = get_camp_ids_to_process()

    # 2.Dynamic Task Mapping (expand) для запуска функции для каждого элемента списка
    # Это создаст отдельную задачу для каждого camp_id
    process_single_campaign.expand(camp_id=ids_list)