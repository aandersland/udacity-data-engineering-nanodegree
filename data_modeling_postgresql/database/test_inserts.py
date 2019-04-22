import psycopg2
from sql_queries import *

# create db connection
conn = psycopg2.connect(
    "host=127.0.0.1 dbname=sparkifydb user=student password=student")
conn.set_session(autocommit=True)
cur = conn.cursor()

# table counts
cur.execute('SELECT count(*) FROM songplays;')
print('songplays ', cur.fetchall())
cur.execute('SELECT count(*) FROM users;')
print('users ', cur.fetchall())
cur.execute('SELECT count(*) FROM songs;')
print('songs ', cur.fetchall())
cur.execute('SELECT count(*) FROM artists;')
print('artists ', cur.fetchall())
cur.execute('SELECT count(*) FROM time;')
print('time ', cur.fetchall())
print()

# top 5 table selects statements
cur.execute('SELECT * FROM songplays LIMIT 5;')
print('songplays')
print(cur.fetchall())
cur.execute('SELECT * FROM users LIMIT 5;')
print('users')
print(cur.fetchall())
cur.execute('SELECT * FROM songs LIMIT 5;')
print('songs')
print(cur.fetchall())
cur.execute('SELECT * FROM artists LIMIT 5;')
print('artists')
print(cur.fetchall())
cur.execute('SELECT * FROM time LIMIT 5;')
print('time')
print(cur.fetchall())

conn.close()
