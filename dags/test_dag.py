import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'Denzel Kinyua',
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime(2025, 8, 27)
}

@dag(dag_id='test_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False)
def test_dag():
    @task
    def say_hello():
        return "Hello, Airflow's working!"

    @task
    def check_env_variables(message):
        sender = os.getenv("SENDER")
        return sender
    
    s = say_hello()
    c = check_env_variables(s)

    s >> c

test = test_dag()