# Import Python packages
import pandas as pd
import os
import glob
import csv
from data_modeling_apache_cassandra.database import sql_queries, cassandra_db


def get_file_path_list(_filepath):
    """
    Generates a list of file paths for a given directory
    :param _filepath: Folder directory path
    :return: List of file paths
    """
    _file_path_list = []
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(_filepath):
        # join the file path and roots with the subdirectories using glob
        _file_path_list = glob.glob(os.path.join(root, '*'))
    return _file_path_list


def get_full_file_path_data(_file_path_list):
    """
    Create a list of data rows from a list of file paths
    :param _file_path_list: List of file paths
    :return: List of data rows
    """
    # initiating an empty list of rows that will be generated from each file
    _full_data_rows_list = []

    # for every filepath in the file path list
    for _f in _file_path_list:

        # reading csv file
        with open(_f, 'r', encoding='utf8', newline='') as _csvfile:
            # creating a csv reader object
            _csvreader = csv.reader(_csvfile)
            next(_csvreader)

            # extracting each data row one by one and append it
            for _line in _csvreader:
                _full_data_rows_list.append(_line)
    return _full_data_rows_list


def get_event_data_for_import(_full_data_rows_list, _event_data_file_path):
    """
    Parses the event data rows and produces a new list with only the needed
    columns for inserting into cassandra
    :param _full_data_rows_list: List of data rows
    :param _event_data_file_path: File path that the data will be written too
    """
    # creating a smaller event data csv file called event_datafile_full csv
    # that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)
    if os.path.exists(_event_data_file_path):
        print('Removing existing file: ', _event_data_file_path)
        os.remove(_event_data_file_path)

    with open(_event_data_file_path, 'w+', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(
            ['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
             'length', 'level', 'location', 'sessionId', 'song', 'userId'])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow(
                (row[0], row[2], row[3], row[4], row[5], row[6], row[7],
                 row[8], row[12], row[13], row[16]))
        f.close()

    # check the number of rows in your csv file
    with open(_event_data_file_path, 'r', encoding='utf8') as f:
        print('Created file: ', _event_data_file_path, ' Total rows: ',
              sum(1 for line in f))
    f.close()


def parse_data_file_insert_db(_session, _event_data_file):
    """
    Loop through data file and insert records into the database
    :param _session: Session to database
    :param _event_data_file: Data file
    """
    df = pd.read_csv(_event_data_file)
    # df.info()
    for index, row in df.iterrows():
        # insert records into the session_song_play table
        _session_song_play_values = (
            row['sessionId'], row['itemInSession'], row['artist'], row['song'],
            row['length'])
        cassandra_db.insert_record_into_table(
            _session,
            sql_queries.insert_session_song_play,
            _session_song_play_values)

        # insert records into the session_song_play_list table
        _session_song_play_list_values = (
            row['userId'], row['sessionId'], row['itemInSession'],
            row['artist'],
            row['song'], row['firstName'], row['lastName'])
        cassandra_db.insert_record_into_table(
            _session,
            sql_queries.insert_session_song_play_list,
            _session_song_play_list_values)

        # insert records into the session_play_artist table
        _session_play_artist_values = (
            row['song'], row['firstName'], row['lastName'])
        cassandra_db.insert_record_into_table(
            _session,
            sql_queries.insert_session_play_artist,
            _session_play_artist_values)


def query_data(_session, _query, _question):
    _rows = cassandra_db.query_table(_session, _query)

    # print('Printing rows. . . ')
    for _row in _rows:
        if _question == 1:
            print(
                "The song played during sessionId = 338, and \
                itemInSession = 4 was - artist: ",
                _row.artist, " song title: ", _row.song_title,
                "  song length:",
                _row.song_length)
        elif _question == 2:
            print(_row.first_name, _row.last_name, 'played the song:',
                  _row.song_title, 'from artist:', _row.artist,
                  'during session id 182')
        elif _question == 3:
            print(_row.first_name, _row.last_name,
                  " listened to the song 'All Hands Against His Own'.")


# checking your current working directory
print('Starting etl script. . .')
print('\nPreparing data file. . .')
print('Current directory: ', os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/data/raw'

file_path_list = get_file_path_list(filepath)
full_data_rows_list = get_full_file_path_data(file_path_list)

# uncomment the code below if you would like to get total number of rows
print('Data row length: ', len(full_data_rows_list))

event_data_file = 'data/combined/event_datafile_new.csv'
get_event_data_for_import(full_data_rows_list, event_data_file)

print('\nSetting up database connection to sparkifydb')
session = cassandra_db.create_db_connection('sparkifydb')
cassandra_db.drop_tables(session)
cassandra_db.create_tables(session)

print('Parsing data to tables. . .')
parse_data_file_insert_db(session, event_data_file)

print('\nQuery data for questions. . .\n')
query_data(session, sql_queries.select_session_song_play, 1)
print('')
query_data(session, sql_queries.select_session_song_play_list, 2)
print('')
query_data(session, sql_queries.select_session_play_artist, 3)

print('\nScript complete.')
