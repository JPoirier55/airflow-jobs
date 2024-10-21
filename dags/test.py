from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),  # Run once a day
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define the Python function to be run
    def print_hello():
        print("Hello, Airflow!")

    # Create a PythonOperator task
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
        dag=dag,
    )

    # You can add more tasks if needed. For now, just this one.
    hello_task
