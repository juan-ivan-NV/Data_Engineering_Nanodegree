import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["CREDENTIALS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["CREDENTIALS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark Session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function pulls the song_data from S3 and extracts the songs / artists tables
    to push them back to S3
    
    Parameters:
    ► spark: The Spark Session
    ► input_data: The location source from the song_data files
    ► output_data: The storage location where the processed tables will be pushed 
    """
    
    # get filepath to song data file
    # song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create song view
    df.createOrReplaceTempView("song_data_table")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT song_id,
                            title,
                            artist_id,
                            year,
                            duration
                            FROM song_data_table
                            WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                            SELECT artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude 
                            FROM song_data_table
                            WHERE artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    This function pulls the log_data from S3 and extracts the users / songplays / time tables
    to push them back to S3
    
    Parameters:
    ► spark: The Spark Session
    ► input_data: The location source from the log_data files
    ► output_data: The storage location where the processed tables will be pushed 
    """
    
    # get filepath to log data file log_data/*/*/*.json
    # log_data = input_data + 'log_data/*.json'
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # Create a log view
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userId user_id,
                            firstName first_name,
                            lastName last_name,
                            gender,
                            level
                            FROM log_data_table
                            WHERE userId IS NOT NULL
                            """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT T.time_t       AS start_time,
                            hour(T.time_t)        AS hour,
                            dayofmonth(T.time_t)  AS say,
                            weekofyear(T.time_t)  AS week,
                            month(T.time_t)       AS month,
                            year(T.time_t)        AS year,
                            dayofweek(T.time_t)   AS weekday
                            FROM 
                            (SELECT to_timestamp(ldt.ts/1000) as time_t
                            FROM log_data_table ldt
                            WHERE ldt.ts IS NOT NULL) AS T
                            """) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')
    song_df.createOrReplaceTempView("song_data_table")
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() AS songplay_id,
                                to_timestamp(ldt.ts/1000)            AS start_time,
                                month(to_timestamp(ldt.ts/1000))     AS month,
                                year(to_timestamp(ldt.ts/1000))      AS year,
                                ldt.userId                           AS user_id,
                                ldt.level                            AS level,
                                sdt.song_id                          AS song_id,
                                sdt.artist_id                        AS atist_id,
                                ldt.sessionID                        AS session_id,
                                ldt.location                         AS location,
                                ldt.userAgent                        AS user_agent
                                FROM log_data_table AS ldt
                                JOIN song_data_table AS sdt 
                                ON ldt.artist = sdt.artist_name AND ldt.song = sdt.title
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')
    
    print("-----------ETL process finished-------------")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jinb-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
