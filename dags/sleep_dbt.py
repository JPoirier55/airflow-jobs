from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="sleep_dbt",
    start_date=datetime(2023, 10, 20),
    schedule_interval="0 1 * * *",
    catchup=False
) as dag:

    run_docker_dbt = DockerOperator(
        task_id="sleep_dbt",
        image="sleep-dbt",
        command="dbt run --profiles-dir /app/sleepdbt/sleep/profiles",
        docker_url="unix://var/run/docker.sock", 
        network_mode="shared_network", 
        auto_remove=True,  
    )

    run_docker_dbt
