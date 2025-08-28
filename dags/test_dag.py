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
    
    say_hello()


test = test_dag()