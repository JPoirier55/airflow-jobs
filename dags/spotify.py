from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

SPOTIFY_CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID") 
SPOTIFY_CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
SPOTIFY_DB_PASSWORD = Variable.get("SPOTIFY_DB_PASSWORD")


@dag(schedule="0 * * * *", start_date=datetime(2024, 9, 1), catchup=False)
def track_puller(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET,SPOTIFY_DB_PASSWORD):
    @task.virtualenv(
        task_id="virtualenv_python", requirements=["spotipy==2.24.0","psycopg2-binary==2.9.10"], system_site_packages=False
    )
    def callable_virtualenv(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET,SPOTIFY_DB_PASSWORD):
        import spotipy
        from spotipy.oauth2 import SpotifyOAuth
        
        import psycopg2
        from psycopg2 import sql
        import json

         
        REDIRECT_URI = 'http://localhost:3000/auth/callback'  # Replace with your redirect URI
        SCOPE = 'user-library-read user-top-read user-read-recently-played'  # Define the scope(s) you need

        # Create an OAuth object with Spotipy, this automatically handles token refresh
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=REDIRECT_URI,
            scope=SCOPE,
            show_dialog=True,  # Set to True if you want to show the login dialog every time
            cache_path=".cache"  # Cache the token and refresh token
        ))

        # Example: Get the current user's top tracks
        results = sp.current_user_top_tracks(limit=10)
        for idx, track in enumerate(results['items']):
            print(f"{idx + 1}: {track['name']} by {track['artists'][0]['name']}")
        
        recent_tracks = sp.current_user_recently_played(limit=4)  # limit is the number of tracks to pull, max 50
        print('recenttracks')
        print(recent_tracks)
        # Print the details of each track
        # for item in recent_tracks['items']:
        #     track = item['track']
        #     print(item)
        
        connection = psycopg2.connect(
            host="localhost",
            database="spotify_db",
            user="admin",
            password=SPOTIFY_DB_PASSWORD
        )

        # Create a cursor object
        cursor = connection.cursor()
        str_json = json.dumps(recent_tracks['items'])
        # SQL Insert Statement
        insert_query = f"""
        INSERT INTO spotify_tracks_raw (
            json_blob
        ) VALUES (
            (%s)
        )
        """
        
        # Execute the insert query
        cursor.execute(insert_query, [str_json])

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

    virtualenv_task = callable_virtualenv(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET,SPOTIFY_DB_PASSWORD)


track_puller(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_DB_PASSWORD)

