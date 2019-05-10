from cassandra.cluster import Cluster
from database.sql_queries import create_table_queries, drop_table_queries


def create_db_connection(_db_name):
    _cluster = Cluster(['localhost'])
    _session = _cluster.connect()

    _session.execute("CREATE KEYSPACE IF NOT EXISTS " + _db_name +
                     " WITH REPLICATION = { 'class' : 'SimpleStrategy', \
                     'replication_factor' : 1 }")
    _session.set_keyspace(_db_name)
    return _session


def drop_tables(_session):
    print('Dropping tables . . .')
    for _query in drop_table_queries:
        _session.execute(_query)


def create_tables(_session):
    print('Creating tables. . .')
    for _query in create_table_queries:
        _session.execute(_query)


def query_table(_session, _query):
    _rows = _session.execute(_query)
    return _rows


def insert_record_into_table(_session, _query, _values):
    _session.execute(_query, _values)
