import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE staging_songs(
    artist_id VARCHAR, 
    artist_latitude numeric, 
    artist_longitude numeric, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration numeric, 
    year INT
    );
""")

staging_events_table_create = ("""
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR, 
    gender VARCHAR, 
    itemInSession INT, 
    lastName VARCHAR, 
    length numeric, 
    level VARCHAR, 
    location VARCHAR, 
    method VARCHAR, 
    page VARCHAR, 
    registration BIGINT, 
    sessionId INT, 
    song VARCHAR, 
    status VARCHAR,
    ts BIGINT, 
    userAgent TEXT, 
    userId VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id VARCHAR, 
    level VARCHAR, 
    song_id VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    session_id VARCHAR, 
    location VARCHAR, 
    user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY sortkey distkey, 
    title VARCHAR,
    artist_id VARCHAR, 
    year INT, 
    duration numeric
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY sortkey distkey, 
    name VARCHAR, 
    location VARCHAR, 
    latitude numeric, 
    longitude numeric
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY sortkey,
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
json {}
;
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}'
FORMAT AS json 'auto'
region 'us-west-2'
;
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)
SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second' as start_time,  
        staging_events.userId as user_id, 
        staging_events.level as level, 
        staging_songs.song_id as song_id, 
        staging_songs.artist_id as artist_id, 
        staging_events.sessionId as session_id, 
        staging_events.location as location, 
        staging_events.userAgent as user_agent
FROM (staging_events JOIN staging_songs 
ON (staging_events.artist = staging_songs.artist_name AND staging_events.song = staging_songs.title))
WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level)
SELECT DISTINCT staging_events.userId as user_id, 
    staging_events.firstName as first_name, 
    staging_events.lastName as last_name, 
    staging_events.gender as gender, 
    staging_events.level as level
FROM staging_events 
WHERE page = 'NextSong'
AND user_id NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id, 
    title,
    artist_id,
    year, 
    duration)
SELECT DISTINCT staging_songs.song_id as song_id, 
    staging_songs.title as title, 
    staging_songs.artist_id as artist_id, 
    staging_songs.year as year, 
    staging_songs.duration as duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude)
SELECT DISTINCT staging_songs.artist_id as artist_id, 
    staging_songs.artist_name as name, 
    staging_songs.artist_location as location, 
    staging_songs.artist_latitude as latitude, 
    staging_songs.artist_longitude as longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour, 
    day, 
    week,
    month,
    year, 
    weekday)
SELECT 
    a.ts as start_time, 
    EXTRACT(HOUR FROM a.ts) as hour, 
    EXTRACT(DAY FROM a.ts) as day, 
    EXTRACT(WEEK FROM a.ts) as week, 
    EXTRACT(MONTH FROM a.ts) as month, 
    EXTRACT(YEAR FROM a.ts) as year,
    EXTRACT(DOW FROM a.ts) as weekday
FROM (SELECT (TIMESTAMP 'epoch' + staging_events.ts/1000 * INTERVAL '1 second') as ts
    FROM staging_events
    WHERE page = 'NextPage') a
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
