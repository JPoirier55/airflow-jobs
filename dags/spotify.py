from datetime import datetime
from dotenv import load_dotenv
from airflow.decorators import dag, task
import os

load_dotenv()


@dag(schedule="@daily", start_date=datetime(2024, 9, 1), catchup=False)
def spotify_data_puller():
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["spotipy==2.24.0"], system_site_packages=False
    )
    def callable_virtualenv():
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials
        

        SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID") 
        SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET") 

        # Authentication
        client_credentials_manager = SpotifyClientCredentials(client_id=SPOTIFY_CLIENT_ID, client_secret=SPOTIFY_CLIENT_SECRET)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        # Get recent listening history
        recent_tracks = sp.current_user_recently_played(limit=50)  # limit is the number of tracks to pull, max 50

        # Print the details of each track
        for item in recent_tracks['items']:
            track = item['track']
            print(item)
            # print(f"{track['name']} by {', '.join([artist['name'] for artist in track['artists']])}")

    virtualenv_task = callable_virtualenv()


daaag()

