from cassandra.cluster import Cluster
from database.sql_queries import create_table_queries, drop_table_queries


def create_db_connection(_db_name):
    """
    Creates a database connection
    :param _db_name: Name of database to connect too
    :return: Database session
    """
    _cluster = Cluster(['localhost'])
    _session = _cluster.connect()

    _session.execute("CREATE KEYSPACE IF NOT EXISTS " + _db_name +
                     " WITH REPLICATION = { 'class' : 'SimpleStrategy', \
                     'replication_factor' : 1 }")
    _session.set_keyspace(_db_name)
    return _session


def drop_tables(_session):
    """
    Drop pre-defined tables
    :param _session: Session to database
    """
    print('Dropping tables . . .')
    for _query in drop_table_queries:
        _session.execute(_query)


def create_tables(_session):
    """
    Create a pre-defined list of tables
    :param _session: Session to database
    """
    print('Creating tables. . .')
    for _query in create_table_queries:
        _session.execute(_query)


def query_table(_session, _query):
    """
    Method to run a query
    :param _session: Session to database
    :param _query: Query to run
    :return: Database rows from the result set
    """
    _rows = _session.execute(_query)
    return _rows


def insert_record_into_table(_session, _query, _values):
    """
    Method to insert records into a table
    :param _session: Session to database
    :param _query: Query to run
    :param _values: Values to be inserted
    """
    _session.execute(_query, _values)
