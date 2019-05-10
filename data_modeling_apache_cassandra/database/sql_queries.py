# DROP TABLES
drop_session_song_play = "DROP TABLE IF EXISTS session_song_play"
drop_session_song_play_list = "DROP TABLE IF EXISTS session_song_play_list"
drop_session_play_artist = "DROP TABLE IF EXISTS session_play_artist"

# CREATE TABLES
create_session_song_play = ("CREATE TABLE IF NOT EXISTS session_song_play \
(session_id int, item_in_session int, artist text, song_title text, \
song_length float, PRIMARY KEY (session_id, item_in_session))")

create_session_song_play_list = ("CREATE TABLE IF NOT EXISTS \
session_song_play_list \
(user_id int, session_id int, item_in_session int, artist text, \
song_title text, first_name text, last_name text, \
PRIMARY KEY ((user_id, session_id), item_in_session))")

create_session_play_artist = ("CREATE TABLE IF NOT EXISTS session_play_artist \
(song_title text, first_name text, last_name text, \
PRIMARY KEY (song_title, first_name, last_name))")

# INSERT RECORDS
insert_session_song_play = ("INSERT INTO session_song_play \
(session_id, item_in_session, artist, song_title, song_length) \
VALUES (%s, %s, %s, %s, %s)")

insert_session_song_play_list = ("INSERT INTO session_song_play_list \
(user_id, session_id, item_in_session, artist, song_title, first_name, \
last_name) VALUES (%s, %s, %s, %s, %s, %s, %s)")

insert_session_play_artist = ("INSERT INTO session_play_artist \
(song_title, first_name, last_name) \
VALUES (%s, %s, %s)")

# SELECT RECORDS
select_session_song_play = ("SELECT artist, song_title, song_length \
FROM session_song_play WHERE session_id = 338 and item_in_session = 4;")

select_session_song_play_list = ("SELECT artist, song_title, first_name, \
last_name FROM session_song_play_list WHERE user_id = 10 and \
session_id = 182;")

select_session_play_artist = ("SELECT first_name, last_name \
FROM session_play_artist WHERE song_title = 'All Hands Against His Own';")

# QUERY LISTS
create_table_queries = [create_session_play_artist, create_session_song_play,
                        create_session_song_play_list]
drop_table_queries = [drop_session_play_artist, drop_session_song_play,
                      drop_session_song_play_list]
