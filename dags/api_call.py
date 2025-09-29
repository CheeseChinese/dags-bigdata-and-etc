from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
from airflow.models import Variable

def make_get_request(endpoint):
    def _get_request():
        print(f"Reauesting GET to {endpoint}")
        response = requests.get(endpoint)
        print(f"Response from {endpoint}: {response.status_code}, {response.text}")
    return _get_request

def create_dag(dag_id, schedule, endpoint):
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 11, 27),
        'catchup': False
    }

    dag = DAG(dag_id, 
              default_args=default_args,
              schedule_interval=schedule)

    with dag:
        task = PythonOperator(
            task_id='make_get_request',
            python_callable=make_get_request(endpoint),
        )

    return dag

# Create different DAGs
dag1 = create_dag('collect_responses_kc_sms_dag',
                  Variable.get('kc_sms_response_collection_schedule'),
                  Variable.get('kc_sms_response_collection_endpoint'))

dag2 = create_dag('collect_responses_atm_dag',
                  Variable.get('atm_response_collection_schedule'),
                  Variable.get('atm_response_collection_endpoint'))

dag3 = create_dag('collect_efr_cancel_responses_atm_dag',
                  Variable.get('efr_deact_response_collection_schedule'),
                  Variable.get('efr_deact_response_collection_endpoint'))

dag4 = create_dag('collect_responses_efr_dag',
                  Variable.get('efr_response_collection_schedule'),
                  Variable.get('efr_response_collection_endpoint'))


globals().update({
    'collect_responses_kc_sms': dag1,
    'collect_responses_atm': dag2,
	'collect_responses_efr_cancel': dag3,
    'collect_responses_efr': dag4
})
