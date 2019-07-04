import configparser
import psycopg2
import time
from sql_queries import insert_table_queries, \
    select_table_queries, set_staging_copy_params, create_table_queries, \
    drop_table_queries, drop_staging_table_queries
from aws_setup import create_cluster, delete_cluster
from create_tables import drop_tables, create_tables


def load_staging_tables(cur, conn, query):
    print('Loading tables: ', query)
    cur.execute(query)
    conn.commit()


def insert_tables(cur, conn):
    print('Inserting data into tables: ', insert_table_queries)
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def select_count_tables(cur, conn):
    print('Selecting counts from tables: ', select_table_queries)
    for query in select_table_queries:
        cur.execute(query)
        print(query, cur.fetchall())


def main():
    start_time = time.time()
    print('Starting time: ', start_time)
    config, ec2, s3, iam, redshift, cluster_props, conn, cur = create_cluster()
    drop_tables(cur, conn, drop_table_queries)
    aws_end_time = time.time()
    print('AWS setup elapsed time: ', aws_end_time - start_time)

    create_tables(cur, conn, create_table_queries)
    staging_events_copy, staging_songs_copy = set_staging_copy_params(config)

    # load_staging_tables(cur, conn, staging_events_copy)
    event_load_time = time.time()
    print('Event load elapsed time: ', event_load_time - aws_end_time)

    # load_staging_tables(cur, conn, staging_songs_copy)
    songs_load_time = time.time()
    print('Song load elapsed time: ', songs_load_time - event_load_time)

    insert_tables(cur, conn)
    table_insert_time = time.time()
    print('Table insert elapsed time: ', table_insert_time - songs_load_time)

    select_count_tables(cur, conn)
    drop_tables(cur, conn, drop_table_queries)
    drop_tables(cur, conn, drop_staging_table_queries)
    conn.close()

    delete_cluster(redshift,
                   config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
                   iam, config.get("DWH", "DWH_IAM_ROLE_NAME"),
                   config.get('IAM_ROLE', 'POLICY'))

    print('Total elapsed time: ', time.time() - start_time)


if __name__ == "__main__":
    main()
