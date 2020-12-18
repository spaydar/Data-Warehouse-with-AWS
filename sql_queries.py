import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
    (
        artist          varchar, 
        auth            varchar, 
        firstName       varchar, 
        gender          varchar, 
        itemInSession   int,         
        lastName        varchar, 
        length          numeric, 
        level           varchar, 
        location        varchar, 
        method          varchar, 
        page            varchar, 
        registration    numeric, 
        sessionId       int, 
        song            varchar                distkey, 
        status          int, 
        ts              bigint, 
        userAgent       varchar, 
        userId          int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs 
    (
        artist_id           varchar, 
        artist_latitude     numeric, 
        artist_location     varchar, 
        artist_longitude    numeric, 
        artist_name         varchar, 
        duration            numeric, 
        num_songs           int, 
        song_id             varchar, 
        title               varchar                    distkey, 
        year                int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
    (
        songplay_id     int IDENTITY(0,1)    PRIMARY KEY     distkey, 
        start_time      timestamp            NOT NULL        sortkey, 
        user_id         varchar              NOT NULL, 
        level           varchar, 
        song_id         varchar, 
        artist_id       varchar, 
        session_id      int, 
        location        varchar, 
        user_agent      varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
    (
        user_id       varchar    PRIMARY KEY    sortkey, 
        first_name    varchar, 
        last_name     varchar, 
        gender        varchar, 
        level         varchar
    ); 
    ALTER TABLE songplays ADD FOREIGN KEY (user_id) REFERENCES users(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
    (
        song_id      varchar    PRIMARY KEY    sortkey, 
        title        varchar, 
        artist_id    varchar, 
        year         int, 
        duration     numeric
    ); 
    ALTER TABLE songplays ADD FOREIGN KEY (song_id) REFERENCES songs(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
    (
        artist_id    varchar    PRIMARY KEY    sortkey, 
        name         varchar, 
        location     varchar, 
        latitude     numeric, 
        longitude    numeric
    ); 
    ALTER TABLE songplays ADD FOREIGN KEY (artist_id) REFERENCES artists(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
    (
        start_time    timestamp    PRIMARY KEY    sortkey, 
        hour          int, 
        day           int, 
        week          int, 
        month         int, 
        year          int, 
        weekday       int
    ); 
    ALTER TABLE songplays ADD FOREIGN KEY (start_time) REFERENCES time(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role {}
    json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        timestamp 'epoch' + se.ts/1000 * interval '1 second',
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.song=ss.title
    WHERE se.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId, 
        firstName, 
        lastName, 
        gender, 
        level
    FROM staging_events
    WHERE page='NextSong' AND userId NOT IN (SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
        EXTRACT(HOUR FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
        EXTRACT(DAY FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
        EXTRACT(WEEK FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
        EXTRACT(MONTH FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
        EXTRACT(YEAR FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
        EXTRACT(DOW FROM timestamp 'epoch' + ts/1000 * interval '1 second')
    FROM staging_events
    WHERE page='NextSong' AND start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
