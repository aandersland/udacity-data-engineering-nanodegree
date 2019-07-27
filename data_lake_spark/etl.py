import configparser
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    to_timestamp, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, \
    DoubleType as Dbl, StringType as Str, IntegerType as Int

config = configparser.ConfigParser()
config.read('dl.cfg')
# config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a spark session for AWS
    :return: Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config('spark.executor.cores', '4') \
        .config('spark.executor.memory', '2g') \
        .config('spark.cores.max', '12') \
        .config('spark.driver.memory', '2g') \
        .getOrCreate()

    return spark


def write_parquet(data, output_location, folder_name, partition_one,
                  partition_two):
    """
    Method to abstract data written to a parquet file
    :param data: Dataframe of data to be written
    :param output_location: Target folder location
    :param folder_name: Name of new folder
    :param partition_one: Optional partition field, Required if partition_two
                          is populated
    :param partition_two: Optional partition field
    """
    if partition_one is not None and partition_two is not None:
        data.write.mode('overwrite') \
            .option('header', 'true') \
            .partitionBy(partition_one, partition_two) \
            .parquet(output_location + folder_name + '/')
    elif partition_one is not None and partition_two is None:
        data.write.mode('overwrite') \
            .option('header', 'true') \
            .partitionBy(partition_one) \
            .parquet(output_location + folder_name + '/')
    else:
        data.write.mode('overwrite') \
            .option('header', 'true') \
            .parquet(output_location + folder_name + '/')


def process_song_data(spark, input_data, output_data):
    """
    Method to process song data and create tables: songs, artists
    :param spark: Spark session
    :param input_data: S3 bucket
    :param output_data: S3 bucket
    :return: Data frame of song data
    """
    # get filepath to song data file
    song_data = input_data + '/song-data/A/A/B/*.json'

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
    print('Reading song data.')
    df = spark.read.json(song_data, schema=songs_schema)

    song_columns = ['song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration']

    # extract columns to create songs table
    songs_table = df.selectExpr(song_columns).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    print('Writing songs to parquet.')
    write_parquet(songs_table, output_data, 'songs', 'year', 'artist_id')

    artist_columns = ['artist_id',
                      'artist_name as name',
                      'artist_location as location',
                      'artist_latitude as latitude',
                      'artist_longitude as longitude']

    # extract columns to create artists table
    artists_table = df.selectExpr(artist_columns).dropDuplicates()

    # write artists table to parquet files
    print('Writing artists to parquet.')
    write_parquet(artists_table, output_data, 'artists', None, None)

    return df


def process_log_data(spark, input_data, output_data, songs):
    """
    Method to process log data and create tables: users, time, songplays
    :param spark: Spark session
    :param input_data: S3 bucket
    :param output_data: S3 bucket
    :param songs: Parsed song data frame
    """
    # get filepath to log data file
    print('Reading log data.')
    log_data = input_data + '/log_data/2018/11/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    user_columns = ['userId as user_id',
                    'firstName as first_name',
                    'lastName as last_name',
                    'gender',
                    'level']

    # extract columns for users table
    users_table = df.selectExpr(user_columns).dropDuplicates()

    # write users table to parquet files
    print('Writing users to parquet.')
    write_parquet(users_table, output_data, 'users', None, None)

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', to_timestamp(df['ts'] / 1000)).withColumn(
        'songplay_id', monotonically_increasing_id())

    # extract columns to create time table
    time_table = df.select('start_time',
                           hour('start_time').alias('hour'),
                           dayofmonth('start_time').alias('day'),
                           weekofyear('start_time').alias('week'),
                           month('start_time').alias('month'),
                           year('start_time').alias('year'),
                           dayofweek('start_time').alias('weekday')) \
        .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    print('Writing time to parquet.')
    write_parquet(time_table, output_data, 'time', 'year', 'month')

    songplay_columns = ['songplay_id',
                        'start_time',
                        'userId as user_id',
                        'level',
                        'song_id',
                        'artist_id',
                        'sessionId as session_id',
                        'location',
                        'userAgent as user_agent',
                        'year',
                        'month']

    # extract columns from joined song and log data to create songplays table
    songplays_table = df.join(songs, (df.song == songs.title) & (
            df.artist == songs.artist_name)) \
        .withColumn('month', month('start_time')) \
        .selectExpr(songplay_columns)\
        .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays to parquet.')
    write_parquet(songplays_table, output_data, 'songplays', 'year', 'month')


def main():
    start_time = time.time()
    print('Starting time: ', start_time)

    spark = create_spark_session()
    # S3 buckets
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-sparkify/"

    # capture song data frame for use later
    songs = process_song_data(spark, input_data, output_data)
    songs_end_time = time.time()
    print('Songs process time: ', songs_end_time - start_time)

    process_log_data(spark, input_data, output_data, songs)
    log_end_time = time.time()
    print('Songs process time: ', log_end_time - songs_end_time)

    print('Total elapsed time: ', time.time() - start_time)


if __name__ == "__main__":
    main()
