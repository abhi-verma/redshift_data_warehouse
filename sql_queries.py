import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data = config.get("S3", "LOG_DATA")
log_jsonpath = config.get("S3", "LOG_JSONPATH")
song_data = config.get("S3", "SONG_DATA")
iam_role = config.get("IAM_ROLE", "ARN")

# DROP ALL TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE STAGING, FACT AND DIMENSION TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist_name VARCHAR DISTKEY,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR DISTKEY,
    song_id VARCHAR,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INTEGER NOT NULL DISTKEY,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    firstName VARCHAR NOT NULL,
    lastName VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR NOT NULL
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL DISTKEY,
    year INT NOT NULL,
    duration DOUBLE PRECISION NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL DISTKEY,
    weekday INTEGER NOT NULL
);
""")

# COPYING DATA FROM JSON FILES TO STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    IAM_ROLE '{}'
    TIMEFORMAT AS 'epochmillisecs'
    JSON {}
    COMPUPDATE OFF REGION 'us-west-2';
""").format(log_data, iam_role, log_jsonpath)

staging_songs_copy = ("""
COPY staging_songs FROM {}
    IAM_ROLE '{}'
    TIMEFORMAT AS 'epochmillisecs'
    FORMAT JSON AS 'auto'
    COMPUPDATE OFF REGION 'us-west-2';
""").format(song_data, iam_role)

# INSERTING DATA FROM STAGING TO FACT AND DIMENSION TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist_name = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, firstName, lastName, gender, level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs;
""")
    
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH time_extract AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time
    FROM staging_events
)
SELECT
    start_time AS start_time,
    EXTRACT (hour from start_time) AS hour,
    EXTRACT (day from start_time) AS day,
    EXTRACT (week from start_time) AS week,
    EXTRACT (month from start_time) AS month,
    EXTRACT (year from start_time) AS year,
    EXTRACT (dow from start_time) AS weekday
FROM time_extract;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
