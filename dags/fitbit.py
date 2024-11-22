from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import requests
import json
import psycopg2
import fitbit
import pytz

FITBIT_CLIENT_ID = Variable.get("FITBIT_CLIENT_ID") 
FITBIT_REFRESH_TOKEN = Variable.get("FITBIT_REFRESH_TOKEN")
FITBIT_DB_PASSWORD = Variable.get("FITBIT_DB_PASSWORD")


@dag(schedule="59 4 * * *", start_date=datetime(2024, 11, 11), catchup=False)
def fitbit_data_pull():
    @task.python(task_id="refresh_access_token")
    def refresh_access_token():        
        url = 'https://api.fitbit.com/oauth2/token'
        payload = {
            'grant_type': 'refresh_token',
            'client_id': Variable.get("FITBIT_CLIENT_ID") ,
            'refresh_token': Variable.get("FITBIT_REFRESH_TOKEN")
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic ' + Variable.get("FITBIT_BASIC_AUTH")
        }
        response = requests.post(url, data=payload, headers=headers)
        print(response.text)
        Variable.set('FITBIT_ACCESS_TOKEN', json.loads(response.text)['access_token'])
        Variable.set('FITBIT_REFRESH_TOKEN', json.loads(response.text)['refresh_token'])
        print('Access token refresh and set')
    
    @task.python(
        task_id="pull_raw_data"
    )
    def pull_raw_data():
        authd_client = fitbit.Fitbit(
            Variable.get("FITBIT_CLIENT_ID"),
            Variable.get("FITBIT_CLIENT_SECRET"),
            access_token=Variable.get("FITBIT_ACCESS_TOKEN"),
            refresh_token=Variable.get("FITBIT_REFRESH_TOKEN")
        )
        eastern_tz = pytz.timezone('US/Eastern')
        eastern_time = datetime.now(eastern_tz)
        today = eastern_time.date().strftime("%Y-%m-%d")

        # today = date.today().strftime("%Y-%m-%d")

        # Pull activity data
        activity_data = authd_client.activities(date=today)
        # Pull sleep data
        sleep_data = authd_client.sleep(date=today)

        connection = psycopg2.connect(
            host="sleeptracking-postgres-1",
            database="fitbit",
            user="fitbit",
            password="password"
        )
        cursor = connection.cursor()

        activity_data_json_str = json.dumps(activity_data)
        sleep_data_json_str = json.dumps(sleep_data)

        sql = """
        INSERT INTO activity_raw (json_blob, load_date)
        VALUES (%s, %s);
        """
        cursor.execute(sql, (activity_data_json_str, today)) 
        connection.commit()

        sql = """
        INSERT INTO sleep_raw (json_blob, load_date)
        VALUES (%s, %s);
        """
        cursor.execute(sql, (sleep_data_json_str, today)) 
        connection.commit()

        cursor.close()
        connection.close()

    refresh_access_token = refresh_access_token()
    pull_raw_data = pull_raw_data()

fitbit_data_pull()
