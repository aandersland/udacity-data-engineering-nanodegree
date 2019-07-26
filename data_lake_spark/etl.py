import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf  # , col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import StructType as R, StructField as Fld, \
    DoubleType as Dbl, StringType as Str, IntegerType as Int, LongType as Lng, \
    DateType as Date

config = configparser.ConfigParser()
# config.read('dl.cfg')
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + '/song-data/A/A/A/TRAAAAK128F9318786.json'

    songs_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int())
    ])

    # read song data file
    # df = spark.read.json(song_data, schema=songs_schema)

    song_columns = ['song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration']

    # extract columns to create songs table
    # songs_table = df.selectExpr(song_columns).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    # songs_table.write.mode('overwrite') \
    #     .option('header', 'true') \
    #     .partitionBy('year', 'artist_id') \
    #     .parquet(output_data + 'songs/')

    artist_columns = ['artist_id',
                      'artist_name as name',
                      'artist_location as location',
                      'artist_latitude as latitude',
                      'artist_longitude as longitude']

    # extract columns to create artists table
    # artists_table = df.selectExpr(artist_columns).dropDuplicates()

    # write artists table to parquet files
    # artists_table.write.mode('overwrite') \
    #     .option('header', 'true') \
    #     .parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + '/log_data/2018/11/2018-11-01-events.json'

    # log_schema = R([
    #     Fld('artist', Str()),
    #     Fld('auth', Str()),
    #     Fld('firstName', Str()),
    #     Fld('gender', Str()),
    #     Fld('itemInSession', Int()),
    #     Fld('lastName', Str()),
    #     Fld('length', Dbl()),
    #     Fld('level', Str()),
    #     Fld('location', Str()),
    #     Fld('method', Str()),
    #     Fld('page', Str()),
    #     Fld('registration', Dbl()),
    #     Fld('sessionId', Int()),
    #     Fld('song', Str()),
    #     Fld('status', Int()),
    #     Fld('ts', Lng()),
    #     Fld('userAgent', Str()),
    #     Fld('userId', Int())
    # ])

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    user_columns = ['userId as user_id', 'firstName as first_name',
                    'lastName as last_name', 'gender', 'level']
    # extract columns for users table
    users_table = df.selectExpr(user_columns).dropDuplicates()

    # # write users table to parquet files
    users_table.write.mode('overwrite') \
        .option('header', 'true') \
        .parquet(output_data + 'users/')

    print(df.show(5, truncate=False))

    # # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn('start_time', get_timestamp(df.ts))

    print(df.show(5, truncate=False))

    # # create datetime column from original timestamp column
    # get_datetime = udf()
    # df =

    # extract columns to create time table
    # time_table =

    # write time table to parquet files partitioned by year and month
    # time_table

    songplay_columns = ['songplay_id', 'start_time', 'user_id', 'level',
                        'song_id', 'artist_id', 'session_id', 'location',
                        'user_agent']

    # read in song data to use for songplays table
    # song_df =

    # extract columns from joined song and log datasets to create songplays table
    # songplays_table =

    # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-sparkify/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
