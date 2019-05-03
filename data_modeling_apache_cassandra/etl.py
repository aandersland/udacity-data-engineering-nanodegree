# Import Python packages
import pandas as pd
import cassandra
from cassandra.cluster import Cluster
import re
import os
import glob
import numpy as np
import json
import csv


def create_db_connection(_db_name):
    _cluster = Cluster(['127.0.0.1'])
    _session = _cluster.connect()
    _session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + _db_name + """WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")

    _session.set_keyspace(_db_name)

    return _session


def create_db_table(_session, _keyspace, _table_name, _columns, _primary_key):
    print('Creating table: ', _keyspace + "." + _table_name)
    _query = "CREATE TABLE IF NOT EXISTS " + _keyspace + "." + _table_name
    _query = _query + "(" + _columns + ", PRIMARY KEY (" + _primary_key + "))"
    session.execute(_query)


def query_table():
    pass


def get_file_path_list(_filepath):
    _file_path_list = []
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(_filepath):
        # join the file path and roots with the subdirectories using glob
        _file_path_list = glob.glob(os.path.join(root, '*'))
        # print(file_path_list)
    return _file_path_list


def get_full_file_path_data(_file_path_list):
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
                # print(line)
                _full_data_rows_list.append(_line)
    return _full_data_rows_list


def get_event_data_for_import(_full_data_rows_list, _event_data_file_path):
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


# checking your current working directory
print('Starting etl script. . .')
print('Current directory: ', os.getcwd())


# Get your current folder and subfolder event data
filepath = os.getcwd() + '/data/raw'

file_path_list = get_file_path_list(filepath)
full_data_rows_list = get_full_file_path_data(file_path_list)

# uncomment the code below if you would like to get total number of rows
print('Data row length: ', len(full_data_rows_list))

# uncomment the code below if you would like to check to see what the list of
# event data rows will look like
# print(full_data_rows_list)
# print('')

event_data_file = 'data/combined/event_datafile_new.csv'
get_event_data_for_import(full_data_rows_list, event_data_file)

# check the number of rows in your csv file
with open(event_data_file, 'r', encoding='utf8') as f:
    print('Total rows: ', sum(1 for line in f))

print('Setting up database connection to sparkifydb')
session = create_db_connection('sparkifydb')

create_db_table(
    session, 'sparkifydb', 'session_artist_song',
    'artist, song_title, song_length', 'session_id, item_in_session')




print('Script complete.')