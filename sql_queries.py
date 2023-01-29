import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR  ,    
        auth            VARCHAR  ,
        first_name      VARCHAR  ,
        gender          CHAR(1)  ,
        item_in_session INT      ,
        last_name       VARCHAR  ,
        length          VARCHAR  ,
        level           VARCHAR  ,
        location        VARCHAR  ,
        method          VARCHAR  ,
        page            VARCHAR  ,
        registration    BIGINT   ,
        session_id      INT      ,
        song            VARCHAR  ,
        status          VARCHAR  ,
        ts              TIMESTAMP,
        user_agent      VARCHAR  ,
        user_id         INT
        );
    """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT     ,
        artist_id VARCHAR ,
        latitude  FLOAT   ,
        longitude FLOAT   ,
        location  VARCHAR ,
        artist    VARCHAR ,
        song_id   VARCHAR ,
        title     VARCHAR ,
        duration  NUMERIC , 
        year      INT
        );
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id  INT       IDENTITY(0,1) PRIMARY KEY, 
        start_time   TIMESTAMP NOT NULL DISTKEY SORTKEY , 
        user_id      INT       NOT NULL                 , 
        level        VARCHAR                            , 
        song_id      VARCHAR                            , 
        artist_id    VARCHAR                            ,
        session_id   INT                                , 
        location     VARCHAR                            , 
        user_agent   VARCHAR
    )
    DISTSTYLE KEY;
""") 


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INT     PRIMARY KEY SORTKEY, 
        first_name VARCHAR                    , 
        last_name  VARCHAR                    , 
        gender     CHAR(1) ENCODE BYTEDICT    , 
        level      VARCHAR
        ) ;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id    VARCHAR PRIMARY KEY SORTKEY, 
        title      VARCHAR NOT NULL           , 
        artist_id  VARCHAR NOT NULL           , 
        year       INT                        , 
        duration   NUMERIC NOT NULL
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY, 
        name VARCHAR                         , 
        location VARCHAR                     , 
        latitude FLOAT                       ,  
        longitude FLOAT 
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY DISTKEY SORTKEY,  
    hour       INT                                  , 
    day        INT                                  , 
    week       INT                                  , 
    month      INT                                  , 
    year       INT ENCODE BYTEDICT                  , 
    weekday    INT ENCODE BYTEDICT
    )
    DISTSTYLE KEY;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    format as json {}
    timeformat 'epochmillisecs'
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    format as json 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))




# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
        SELECT DISTINCT e.ts, 
                        e.user_id, 
                        e.level, 
                        s.song_id, 
                        s.artist_id, 
                        e.session_id, 
                        e.location, 
                        e.user_agent
        FROM staging_events e 
        INNER JOIN staging_songs s 
            ON e.song = s.title AND e.artist = s.artist
        WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level
        FROM staging_events
        WHERE user_id IS NOT NULL;
""")


song_table_insert = ("""
  INSERT INTO songs 
        SELECT DISTINCT song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
  INSERT INTO artists 
        SELECT DISTINCT artist_id, 
                        artist, 
                        location,
                        latitude,
                        longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
""")


time_table_insert = ("""
INSERT INTO time (
       start_time, 
       hour, 
       day, 
       week, 
       month, 
       year, 
       weekday)
SELECT DISTINCT ts                    AS start_time,
       EXTRACT(HOUR FROM start_time)  AS hour,
       EXTRACT(DAY FROM start_time)   AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time)  AS year,
       EXTRACT(DOW FROM start_time)   As weekday       
FROM staging_events;
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
