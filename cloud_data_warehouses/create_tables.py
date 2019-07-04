import configparser
import psycopg2


def drop_tables(cur, conn, queries):
    print('Dropping tables: ', queries)
    for query in queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, queries):
    print('Creating tables: ', queries)
    for query in queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
