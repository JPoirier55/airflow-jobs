from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG and the schedule
with DAG(
    dag_id="run_ubuntu_container",
    start_date=datetime(2023, 10, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    # Define the DockerOperator
    run_ubuntu = DockerOperator(
        task_id="run_ubuntu",
        image="ubuntu:latest",
        command="echo 'Hello from Ubuntu!'",
        docker_url="unix://var/run/docker.sock",  # Using the Docker socket
        network_mode="bridge",  # Optional, but common for network access
        auto_remove=True,  # Clean up the container after completion
    )

    run_ubuntu
