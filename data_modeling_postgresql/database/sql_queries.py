# DROP TABLES
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES
songplay_table_create = (
    "CREATE TABLE IF NOT EXISTS songplays ("
    "start_time time without time zone, user_id varchar, level varchar, "
    "song_id varchar, artist_id varchar, session_id varchar, "
    "location varchar, user_agent varchar, "
    "primary key (start_time, song_id, artist_id));")

user_table_create = (
    "CREATE TABLE IF NOT EXISTS users (user_id varchar primary key, "
    "first_name varchar, last_name varchar, gender varchar, level varchar);")

song_table_create = (
    "CREATE TABLE IF NOT EXISTS songs (song_id varchar primary key, "
    "title varchar, artist_id varchar, year int, duration numeric(10,5));")

artist_table_create = (
    "CREATE TABLE IF NOT EXISTS artists (artist_id varchar primary key"
    ", name varchar, location varchar, lattiude numeric(10,5), "
    "longitude numeric(10,5), unique(artist_id))")

time_table_create = (
    "CREATE TABLE IF NOT EXISTS time (start_time time without time zone"
    " primary key, "
    "hour int, day int, week int, month int, year int, "
    "weekday varchar);")

# INSERT RECORDS
songplay_table_insert = (
    "INSERT INTO songplays (start_time, user_id, level,"
    " song_id, artist_id, session_id, location, user_agent) VALUES %s"
    "on conflict (start_time, song_id, artist_id) do update set "
    "start_time = songplays.start_time, "
    "song_id = songplays.song_id, "
    "artist_id = songplays.artist_id")

user_table_insert = (
    "INSERT INTO users (user_id, first_name, last_name, gender, level) "
    "VALUES %s on conflict (user_id) do update set user_id = users.user_id;")

song_table_insert = (
    "INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES %s "
    "on conflict (song_id) do update set song_id = songs.song_id;")

artist_table_insert = (
    "INSERT INTO artists (artist_id, name, location, lattiude, longitude) "
    "VALUES %s on conflict (artist_id) do update set "
    "artist_id = artists.artist_id;")

time_table_insert = (
    "INSERT INTO time (start_time, hour, day, week, month, year, weekday) "
    "VALUES %s on conflict (start_time) do update set "
    "start_time = time.start_time;")

# FIND SONGS
song_select = ("select a.artist_id, song_id from artists a join "
               "songs s on a.artist_id = s.artist_id "
               "where a.name = (%s) and s.title = (%s) and s.duration = (%s);")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
