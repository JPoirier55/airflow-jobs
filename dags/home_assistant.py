from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import requests
import pandas as pd
import psycopg2
import pytz

HA_URL = "http://192.168.1.22:8123"  # e.g., "http://192.168.1.100:8123"
API_ENDPOINT = "/api/history/period"
ACCESS_TOKEN = Variable.get("HA_ACCESS_TOKEN")

def store_data(table, data, entity_type):
    connection = psycopg2.connect(
        host="sleeptracking-postgres-1",
        database="fitbit",
        user="fitbit",
        password="password"
    )
    cursor = connection.cursor()

    sql = f"""
    INSERT INTO {table} (sensor_id, friendly_name, {entity_type}, hour, load_date)
    VALUES (%s, %s, %s, %s, %s);
    """
    cursor.execute(sql, data) 
    connection.commit()

    cursor.close()
    connection.close()

def pull_govee_temp_sensor(entity_id, table):
    eastern_tz = pytz.timezone('US/Eastern')
    eastern_time = datetime.now(eastern_tz)
    today = eastern_time.date().strftime("%Y-%m-%d")

    start_time = (eastern_time - timedelta(hours=1)).isoformat()

    url = f"{HA_URL}{API_ENDPOINT}/{start_time}?filter_entity_id={entity_id}"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers) 
    friendly_name = response.json()[0][0]['attributes']['friendly_name']

    flattened_data = [item for sublist in response.json() for item in sublist]
    df = pd.DataFrame(flattened_data)
    df['state'] = df['state'].astype(float)

    average_state = df['state'].mean()
    hour = eastern_time.hour

    data = (entity_id, friendly_name, average_state, hour, today)
    store_data(table, data, 'temperature' if 'temperature' in entity_id else 'humidity')

    return f"Success: {start_time} : {friendly_name} : {entity_id} : {average_state}"

@dag(schedule="56 * * * *", start_date=datetime(2024, 11, 11), catchup=False)
def home_assistant_data_pull():
    @task.python(task_id="govee_temp_sensor")
    def govee_temp_sensor():   
        sensors = [
            'h5100_3f41',
            'h5100_774b',
            'h5100_3e49',
            'h5100_6c25',
            'h5100_6764',
            'h5100_3587'
        ]
        for sensor in sensors:
            print(pull_govee_temp_sensor(f"sensor.{sensor}_temperature", "govee_temp_sensor_hourly"))
            print(pull_govee_temp_sensor(f"sensor.{sensor}_humidity", "govee_humidity_sensor_hourly"))

    govee_temp_sensor = govee_temp_sensor()

home_assistant_data_pull()
