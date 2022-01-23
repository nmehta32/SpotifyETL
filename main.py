import sqlalchemy
import pandas as pd
import requests
from datetime import datetime
import datetime
import sqlite3
import sys

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "	qf2b6svqij4i4qhsdddk0qmhv"  # your Spotify username
TOKEN = 'BQBOZEeUCAFGM3gUvGfuFibMfhP81WcOM3AeUsa0_SAUZtovfcjtUJ0QgsWWq4hg3tPT8RKa24iwbBbK5aQwLkosH1nnHyyW3nF0asxXp7uQRw9JmdQq44z4FGTI9BnbLZweAhg81fKdKauX4bnGdmc_b0WRwkN_qofw'

if __name__ == "__main__":
    # Extract part of the ETL process

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=55)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)

    data = r.json()
    for i in data:
        if i == 'error':
            print('ACCESS TOKEN EXPIRED')
            sys.exit()
        else:
            break
    # print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][:10])
    print(timestamps)

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }
colnames = []

for i in song_dict:
    colnames.append(i)

song_df = pd.DataFrame(song_dict, columns=colnames)

print(song_df)


def check_if_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        print('No songs Downloaded')
        return False
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Data corruption. Primary Key is not unique')
    if df.isnull().values.any():
        raise Exception('Null Values in data')
    # check if dates are within 15 days

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=55)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    for time in df['timestamp'].tolist():
        if datetime.datetime.strptime(time, '%Y-%m-%d') < yesterday:
            raise Exception(f'item with timestamp {time} is older than 15 days and invalid ')
    return True


if check_if_valid(song_df):
    print('Data valid can proceed')


engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()
sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)

"""

if cursor.execute(sql_query):
    print('Table created if not already existed')

try:
    song_df.to_sql("my_played_tracks", engine, index=0, if_exists='append')
except:
    print('Data already exists')

conn.close()
print("connection closed")

## SCHEDULING USING AIRFLOW

