import os
import glob
import psycopg2
from psycopg2 import extras
import pandas as pd
import json
from database.sql_queries import *


def get_db_connection():
    """
    Create a database connection to the sparkifydb.
    :return: A connection and cursor object.
    """
    _conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    _conn.set_session(autocommit=True)
    _cur = _conn.cursor()
    return _conn, _cur


def get_files(_filepath):
    """
    Method to get all filenames in a particular path.
    :param _filepath: Directory to begin the search.
    :return: Returns a list of all files found.
    """
    _all_files = []
    for _root, _dirs, _files in os.walk(_filepath):
        _files = glob.glob(os.path.join(_root, '*.json'))
        for f in _files:
            _all_files.append(os.path.abspath(f))
    return _all_files


def insert_values_to_database(_cur, _conn, _insert_stmnt, _data):
    """
    Insert values into the sparkifydb.
    :param _cur: Cursor for the sparkifydb.
    :param _conn: Connection for the sparkifydb.
    :param _insert_stmnt: Prepared insert statement.
    :param _data: Data in the required format of the insert statement.
    """
    extras.execute_values(_cur, _insert_stmnt, _data)
    _conn.commit()


def merge_json_files(_json_files, _out_path):
    """
    Method to loop through a list of json files and combine the data.
    :param _json_files: List of json file names.
    :param _out_path: File name path for the output.
    """
    with open(_out_path, 'w+') as _out_file:
        for _file in _json_files:
            with open(_file, ) as _in_file:
                for _line in _in_file:
                    _data = json.loads(_line)
                    json.dump(_data, _out_file)
                    _out_file.write('\n')
                _in_file.close()
    _out_file.close()


def combine_song_files():
    """
    Method to combine song file data into a single file.
    :return: File path for the combined data.
    """
    print('Combining song files. . .')
    _song_path = 'data/song_data/combined_songs.json'
    _song_files = get_files('data/song_data/A/')
    merge_json_files(_song_files, _song_path)
    return _song_path


def process_song_files(_conn, _cur, _song_path):
    """
    Method to insert the song and artist tables from the songs data.
    :param _cur: Cursor for the sparkifydb.
    :param _conn: Connection for the sparkifydb.
    :param _song_path: Path to file of combined song data.
    """
    with open(_song_path, 'r') as json_file:
        print('Inserting songs and artists. . .')
        for _line in json_file:
            _df_songs = pd.read_json(_line, lines='true')

            insert_values_to_database(_cur, _conn, song_table_insert,
                                      _df_songs[
                                          ['song_id', 'title', 'artist_id',
                                           'year', 'duration']].values)

            insert_values_to_database(_cur, _conn, artist_table_insert,
                                      _df_songs[
                                          ['artist_id', 'artist_name',
                                           'artist_location',
                                           'artist_latitude',
                                           'artist_longitude']].values)
        json_file.close()


def combine_log_files():
    """
    Method to combine log file data into a single file.
    :return: Pandas data frame of the combined log data.
    """
    print('Combining log files. . .')
    _log_path = 'data/log_data/combined_logs.json'
    _log_files = get_files('data/log_data/2018/')
    merge_json_files(_log_files, _log_path)
    _df_logs = pd.read_json(_log_path, lines='true')
    _df_logs['time'] = pd.to_datetime(_df_logs['ts'], unit='ms')

    return _df_logs


def process_time_data(_df_logs, _conn, _cur):
    """
    Method to format the time data from the log file and insert into the
    database.
    :param _df_logs: Pandas dataframe of combined log data.
    :param _cur: Cursor for the sparkifydb.
    :param _conn: Connection for the sparkifydb.
    """
    print('Formatting time data. . .')
    _df_time = _df_logs.query('page == "NextSong"').copy()
    _df_time.drop(
        ['ts', 'artist', 'auth', 'firstName', 'gender', 'itemInSession',
         'lastName', 'length', 'level', 'loation', 'location', 'method',
         'page', 'registration', 'sessionId', 'song', 'status', 'ts',
         'userAgent', 'userId'], axis=1, inplace=True)

    _df_time['hour'] = pd.DatetimeIndex(_df_time['time']).hour
    _df_time['day'] = pd.DatetimeIndex(_df_time['time']).day
    _df_time['week'] = pd.DatetimeIndex(_df_time['time']).week
    _df_time['month'] = pd.DatetimeIndex(_df_time['time']).month
    _df_time['year'] = pd.DatetimeIndex(_df_time['time']).year
    _df_time['weekday'] = pd.DatetimeIndex(_df_time['time']).weekday

    _df_time.sort_values(by=['time'], inplace=True)

    print('Inserting time data. . .')
    for _i, _row in _df_time.iterrows():
        insert_values_to_database(_cur, _conn, time_table_insert,
                                  [_row.values])


def process_user_data(_df_logs, _conn, _cur):
    """
    Method to insert user data into the database.
    :param _df_logs: Pandas data frame of log data.
    :param _cur: Cursor for the sparkifydb.
    :param _conn: Connection for the sparkifydb.
    """
    print('Inserting user data. . .')
    user_df = _df_logs[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for _i, _row in user_df.iterrows():
        insert_values_to_database(_cur, _conn, user_table_insert,
                                  [_row.values])


def process_songplay_data(_df_logs, _conn, _cur):
    """
    Method to find and insert songplay data into the database.
    :param _df_logs: Pandas data frame of combined log data.
    :param _cur: Cursor for the sparkifydb.
    :param _conn: Connection for the sparkifydb.
    """
    print('Inserting songplay data. . .')
    for _index, _row in _df_logs.iterrows():
        _cur.execute(song_select, (_row.artist, _row.song, _row.length))
        _results = _cur.fetchone()

        if _results:
            _artistid, _songid = _results

            insert_values_to_database(_cur, _conn, songplay_table_insert,
                                      [(_row.time, _row.userId,
                                        _row.level, str(_songid),
                                        str(_artistid),
                                        _row.sessionId,
                                        _row.location, _row.userAgent)])


print('Starting script. . .')
conn, cur = get_db_connection()
song_file = combine_song_files()
process_song_files(conn, cur, song_file)
df_logs = combine_log_files()
process_time_data(df_logs, conn, cur)
process_user_data(df_logs, conn, cur)
process_songplay_data(df_logs, conn, cur)
conn.close()
print('Script complete.')
