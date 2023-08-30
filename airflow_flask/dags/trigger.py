import pendulum
import requests
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def get_duration():
    sending_group = datetime.now().hour
    sending_group = (sending_group + 8) % 24 # UTF +8
    url = "http://10.1.106.139:8090/get_duration" # flask api url
    json_data = {'traffic_control': 7,'sending_group':sending_group}
    res = requests.post(url, json = json_data, verify=False)  ##Respond format
    dictionary = res.json()
    member_list = dictionary["response_content"]["member_list"]
    logging.info("get_duration",member_list)
    return member_list

def insert_to_sql(**context):
    member_list = context['task_instance'].xcom_pull(task_ids='get_duration')
    now_time = (datetime.now()+ timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S") # UTF + 8
    dictionary = {"member_list":member_list,"trigger_time":now_time}
    logging.info("insert_to_sql",dictionary)
    return dictionary

def write_trigger_record(**context):
    url = "http://10.1.106.139:8090/write_trigger_record" # flask api url
    json_data = context['task_instance'].xcom_pull(task_ids='insert_to_sql')
    res = requests.post(url, json = json_data, verify=False) ##Respond format
    print("done!")
    return True
"""
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * * <command to execute>
"""

with DAG(
    dag_id='self_trigger_v1',
    # schedule_interval='@hourly',
    schedule_interval='*/30 * * * *',
    start_date=pendulum.datetime(2023, 8, 28, 2, 10, tz="UTC"), # using UCT timezone, and the UTC+8 == Taiwan's time
    catchup=False,
    default_args={'owner':'eddie_yen','depends_on_past': True, 'retries': 2,
    'retry_delay': timedelta(minutes=2)},
) as dag:
    # define tasks
    get_duration = PythonOperator(
        task_id='get_duration',
        python_callable=get_duration,
        provide_context=True
    )

    insert_to_sql = PythonOperator(
        task_id='insert_to_sql',
        python_callable=insert_to_sql,
        provide_context=True
    )

    write_trigger_record = PythonOperator(
        task_id='write_trigger_record',
        python_callable=write_trigger_record,
        provide_context=True
    )


    # define workflow
    get_duration >> insert_to_sql >>write_trigger_record 