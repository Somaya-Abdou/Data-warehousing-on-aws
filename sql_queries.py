import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events "
staging_songs_table_drop = "Drop table if exists staging_songs"
songplay_table_drop = "Drop table if exists songplays"
user_table_drop = "Drop table if exists users"
song_table_drop = "Drop table if exists song"
artist_table_drop = "Drop table if exists artists"
time_table_drop = "Drop table if exists time"



# CREATE TABLES

staging_events_table_create= ("""Create table if not exists staging_events
                             (
                             artist varchar(max),
                             auth varchar,
                             firstName varchar,
                             gender varchar,
                             itemInSession int,
                             lastName varchar,
                             length float(5),
                             level varchar,
                             location varchar,
                             method varchar,
                             page varchar,
                             registration varchar(max),
                             sessionid int,
                             song varchar(max),
                             status int,
                             ts bigint,
                             userAgent varchar,
                             userId int
                             )""" 
                             )

staging_songs_table_create = ("""Create table if not exists staging_songs
                             (
                             num_songs text,
                             artist_id varchar(max),
                             artist_latitude numeric,
                             artist_longitude numeric,
                             artist_location varchar(max),
                             artist_name varchar(max),
                             song_id varchar(max),
                             title varchar(max),
                             duration float,
                             year int
                             )"""
                             )

songplay_table_create = ("""Create table if not exists songplays
                        (
                        songplay_id int IDENTITY(0,1) Primary Key sortkey,
                        start_time timestamp not null distkey,
                        user_id int not null,
                        level varchar ,
                        song_id varchar(max) not null,
                        artist_id varchar(max) not null,
                        session_id int not null,
                        location varchar,
                        user_agent varchar
                        )"""
                        )

user_table_create = ("""Create table if not exists users
                    (
                    user_id int Primary Key not null ,
                    first_name varchar ,
                    last_name varchar,
                    gender varchar,
                    level varchar 
                    )"""
                    )

song_table_create = ("""Create table if not exists song
                    (
                    song_id varchar Primary Key not null ,
                    title varchar not null,
                    artist_id varchar not null,
                    year int,
                    duration float
                    )"""
                    )

artist_table_create = ("""Create table if not exists artists
                      (
                      artist_id varchar Primary Key not null,
                      name varchar ,
                      location varchar,
                      latitude numeric,
                      longitude numeric
                      )"""
                      )

time_table_create = ("""Create table if not exists time
                    (
                    start_time time Primary Key ,
                    hour int,
                    day int,
                    week int,
                    month int,
                    year int,
                    weekday int
                    )"""
                    ) 

# STAGING TABLES

staging_events_copy = ("""Copy staging_events from {}
                      credentials 'aws_iam_role={}' 
                      region 'us-west-2' 
                      format as JSON {}
                      timeformat as 'epochmillisecs';
                      """).format(config['S3']['LOG_DATA'],config['IAM_ROLE']['RoleArn'],
                                  config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""Copy staging_songs from {}
                      credentials 'aws_iam_role={}' 
                      COMPUPDATE OFF 
                      region 'us-west-2'
                      format as JSON 'auto'
                      TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL 
                      TIMEFORMAT as 'epochmillisecs';
                      """).format(config['S3']['SONG_DATA'],config['IAM_ROLE']['RoleArn'])

# FINAL TABLES          

songplay_table_insert = ("""INSERT INTO songplays (
                            start_time, user_id, level, song_id,
                            artist_id, session_id, location, user_agent)
                            SELECT dateadd(s, convert(bigint, events.ts) / 1000,convert(datetime,'1-1-1970 00:00:00')),
                            events.userID,
                            events.level,
                            songs.song_id,
                            songs.artist_id,
                            events.sessionId,
                            events.location,
                            events.userAgent
                            FROM staging_events AS events
                            JOIN staging_songs AS songs
                            ON (events.artist = songs.artist_name)
                            AND (events.song = songs.title)
                            WHERE events.page = 'NextSong'     
                        """                     
                        )

user_table_insert = ("""INSERT INTO users (
                         user_id,first_name,last_name,
                         gender,level     )
                         select distinct e.userId,
                                         e.firstName,
                                         e.lastName,
                                         e.gender,
                                         e.level
                         From  staging_events as e 
                         where e.userID is not null ; 
                     """                     
                     )

song_table_insert = ("""Insert into song (
                            song_id,title,artist_id,
                            year,duration    )
                        select distinct s.song_id,
                                        s.title,
                                        s.artist_id,
                                        s.year,
                                        s.duration
                        From staging_songs as s
                        where s.song_id is not null and s.title is not null and s.artist_id is not null;
                     """                     
                     )

artist_table_insert = ("""Insert into artists (
                           artist_id,name,location,
                           latitude,longitude    )
                         select distinct s.artist_id,
                                         s.artist_name,
                                         s.artist_location,
                                         s.artist_latitude,
                                         s.artist_longitude
                          From staging_songs as s
                          where s.artist_id is not null
                       """                     
                       )

time_table_insert = ("""Insert into time (
                                          start_time,hour,day,
                                          week,month,year,weekday
                                                                  )
                           Select dateadd(s, convert(bigint, events.ts) / 1000,convert(datetime,'1-1-1970 00:00:00')) as times ,
                                  extract (hour FROM times) as hour,
                                  extract (day FROM times) as day,
                                  extract (week FROM times) as week,
                                  extract (month FROM times)as month,
                                  extract (year FROM times) as year,
                                  extract (weekday FROM times) as weekday
                           From staging_events as events
                           where times is not null;
                     """                      
                     )

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


