import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

print("all packages are imported.")

def first_task(**context):
    print("inside the first function")
    name = "Shahid"
    context['ti'].xcom_push(key="mykey", value=name)

def second_task(**context):
    print("inside second function")
    data = [{"name":"Shahid","title":"Full Stack Software Engineer"}, { "name":"Afridi","title":"Full Stack Software Engineer"},]
    df = pd.DataFrame(data=data)
    print("*"*70)
    print(df.head())
    print("*"*70)
    name = context.get("ti").xcom_pull(key="mykey")
    print(f"Hello world this is {name}")

default_args = {
    "owner":"shahid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024,1,1)
}
with DAG(dag_id="my_first_dag", schedule_interval="@daily", default_args=default_args, catchup=False) as f:
    first_func = PythonOperator(
        task_id = "first_function",
        python_callable = first_task,
        provide_context = True,
        op_kwargs = {"name": "Md Shahid"}
    )

    second_func = PythonOperator(
        task_id = "second_function",
        python_callable = second_task,
        provide_context = True
    )

first_func >> second_func