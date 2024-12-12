pull all spotify listens for the last 50, every hour? 
mark the time, and make a column for month and year or something

Set variables in airflow for 
FITBIT_CLIENT_ID = Variable.get("FITBIT_CLIENT_ID") 
FITBIT_REFRESH_TOKEN = Variable.get("FITBIT_REFRESH_TOKEN")
FITBIT_DB_PASSWORD = Variable.get("FITBIT_DB_PASSWORD")

Use this tool for getting refresh token initially
Runs every hour so we can keep the same refresh token 
https://dev.fitbit.com/build/reference/web-api/troubleshooting-guide/oauth2-tutorial

connect sleeptrack and airflow containers:
docker network create shared_network
docker network connect shared_network sleeptracking-postgres-1
docker network connect shared_network airflow-airflow-worker-1

If system reboots:

`export AIRFLOW_UID=$(id -u)`

`sudo chmod 666 /var/run/docker.sock`