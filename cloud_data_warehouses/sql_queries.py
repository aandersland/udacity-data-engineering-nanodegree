import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# STAGING TABLES
staging_events_copy = None
staging_songs_copy = None

# DROP TABLES
staging_events_table_drop = "DROP table IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES
# todo create sortkey dist key (EX Sol 4 table design)
staging_events_table_create = ("CREATE TABLE IF NOT EXISTS "
                               "staging_events_table ("
                               "id int identity(0,1), artist varchar, "
                               "auth varchar, firstName varchar, "
                               "gender varchar, itemInSession int, "
                               "lastName varchar, length float, "
                               "level varchar, location varchar, "
                               "method varchar, page varchar, "
                               "registration varchar, sessionId int, "
                               "song varchar, status int, ts timestamp, "
                               "userAgent varchar, userId int);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs_table "
                              "( id int identity(0,1), "
                              "num_songs int, artist_id varchar, "
                              "artist_latitude float, "
                              "artist_longitude float, "
                              "artist_location varchar, artist_name varchar, "
                              "song_id varchar, title varchar, "
                              "duration float, year int);")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays "
                         "(songplay_id int identity(0,1) primary key,"
                         " start_time timestamp not null sortkey distkey, "
                         "user_id varchar not null, level varchar, "
                         "song_id varchar not null, "
                         "artist_id varchar not null, "
                         "session_id varchar not null, location varchar, "
                         "user_agent varchar);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users "
                     "(user_id varchar sortkey primary key, "
                     "first_name varchar, last_name varchar, gender varchar, "
                     "level varchar);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs "
                     "(song_id varchar sortkey primary key, "
                     "title varchar not null, artist_id varchar not null, "
                     "year int, duration float);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS "
                       "artists (artist_id varchar sortkey primary key"
                       ", name varchar not null, location varchar, "
                       "latitude float, longitude float);")

time_table_create = ("CREATE TABLE IF NOT EXISTS "
                     "time (start_time timestamp sortkey distkey primary key, "
                     "hour int, day int, week int, month int, year int, "
                     "weekday varchar);")

# FINAL TABLES
songplay_table_insert = (
    "INSERT INTO songplays (start_time, user_id, level, "
    " song_id, artist_id, session_id, location, user_agent) "
    "SELECT DISTINCT e.ts AS start_time, "
    "e.userId AS user_id, "
    "e.level, "
    "s.song_id, "
    "s.artist_id, "
    "e.sessionId AS session_id, "
    "e.location, "
    "e.userAgent AS user_agent "
    "FROM staging_events_table AS e "
    "JOIN staging_songs_table AS s ON e.song = s.title "
    "AND e.artist = s.artist_name "
    "AND e.page = 'NextSong';")

user_table_insert = (
    "INSERT INTO users (user_id, first_name, last_name, gender, level) "
    "SELECT DISTINCT userId AS user_id, "
    "firstName AS first_name, "
    "lastName AS last_name, "
    "gender, "
    "level "
    "FROM staging_events_table "
    "WHERE userId is NOT NULL;")

song_table_insert = (
    "INSERT INTO songs (song_id, title, artist_id, year, duration) "
    "SELECT DISTINCT song_id, "
    "title, "
    "artist_id, "
    "year, "
    "duration "
    "FROM staging_songs_table "
    "WHERE song_id IS NOT NULL;")

artist_table_insert = (
    "INSERT INTO artists (artist_id, name, location, latitude, longitude) "
    "SELECT DISTINCT artist_id, "
    "artist_name AS name, "
    "artist_location AS location, "
    "artist_latitude AS latitude, "
    "artist_longitude AS longitude "
    "FROM staging_songs_table "
    "WHERE artist_id IS NOT NULL;")

time_table_insert = (
    "INSERT INTO time (start_time, hour, day, week, month, year, weekday) "
    "SELECT distinct(ts) AS start_time, "
    "extract(hour FROM ts) AS hour, "
    "extract(day FROM ts) AS day, "
    "extract(week FROM ts) AS week, "
    "extract(month FROM ts) AS month, "
    "extract(year FROM ts) AS year, "
    "extract(weekday FROM ts) AS weekday "
    "FROM staging_events_table "
    "WHERE ts IS NOT NULL;")

# SELECT COUNTS
select_staging_events_table = "SELECT COUNT(*) FROM staging_events_table;"
select_staging_songs_table = "SELECT COUNT(*) FROM staging_songs_table;"
select_songplays = "SELECT COUNT(*) FROM songplays;"
select_users = "SELECT COUNT(*) FROM users;"
select_songs = "SELECT COUNT(*) FROM songs;"
select_artists = "SELECT COUNT(*) FROM artists;"
select_time = "SELECT COUNT(*) FROM time;"

# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]

drop_staging_table_queries = [staging_events_table_drop,
                              staging_songs_table_drop]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]

select_table_queries = [select_staging_events_table,
                        select_staging_songs_table, select_songplays,
                        select_users, select_songs, select_artists,
                        select_time]


def set_staging_copy_params(_config):
    """
    Updates the copy statements with the correct ARN
    :param _config: Configuration file
    :return: Two copy statements: events, songs
    """
    _staging_events_copy = ("""
        copy staging_events_table from '{}'
        credentials 'aws_iam_role={}'
        region '{}'
        timeformat as 'epochmillisecs'
        compupdate off
        statupdate off
        format as JSON '{}';""").format(_config.get('S3', 'LOG_DATA'),
                                        _config.get('DWH', 'DWH_ROLE_ARN'),
                                        _config.get('AWS', 'REGION'),
                                        _config.get('S3', 'LOG_JSONPATH'), )

    _staging_songs_copy = ("""
        copy staging_songs_table from '{}'
        credentials 'aws_iam_role={}'
        region '{}'
        compupdate off
        statupdate off
        format as JSON 'auto';""").format(_config.get('S3', 'SONG_DATA'),
                                          _config.get('DWH', 'DWH_ROLE_ARN'),
                                          _config.get('AWS', 'REGION'))

    return _staging_events_copy, _staging_songs_copy
