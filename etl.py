import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnull
from pyspark.sql.functions import (year, month, dayofmonth,
                                   hour, weekofyear, date_format)
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This create a spark context that is returned.

    Arguments:
        None

    Returns:
        spark: Spark Contexts
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the song data files
    from S3 located at input_data. The data is written to buckets
    located at output_data on S3

    Arguments:
        spark: spark session context
        input_data: root of song data S3 bucket
        output_data: root of the output directory

    Returns:
        spark: Spark Contexts
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title',
                            'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .parquet(F"{output_data}/songs/", mode='overwrite')

    # extract columns to create artists table
    df = df \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude')

    artists_table = df.select(['artist_id',
                               'name',
                               'location',
                               'latitude',
                               'longitude'])
    artist_table = artists_table.dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(F"{output_data}/artists/", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function can be used to read the log data files
    from S3 located at input_data. The data is written to buckets
    located at output_data on S3

    log data and song data are also joined to create songplays data set.

    Arguments:
        spark: spark session context
        input_data: root of song data S3 bucket
        output_data: root of the output directory

    Returns:
        spark: Spark Contexts
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df_log_data = spark.read.json(log_data)

    # filter by actions for song plays
    df_log_data = df_log_data \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('firstName', 'first_name') \
        .withColumnRenamed('lastName', 'last_name')

    # extract columns for users table
    users_table = df_log_data.select(['user_id',
                                      'first_name',
                                      'last_name',
                                      'gender',
                                      'level'])
    users_table = users_table.dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(F"{output_data}/users/", mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000))
    df_log_data = df_log_data .withColumn(
                'timestamp', get_timestamp(df_log_data['ts']))

    # create datetime column from original timestamp column
    get_datetime = udf(
            lambda x: datetime.fromtimestamp(x/1000)
            .strftime('%Y-%m-%d %H:00:00'))

    df_log_data = df_log_data .withColumn(
                'datetime', get_datetime(df_log_data['ts']))

    # extract the day of the week from the timestamp
    get_weekday = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%w'))

    # extract columns to create time table
    df_log_data = df_log_data \
        .withColumn('hour', hour(df_log_data['datetime'])) \
        .withColumn('day', dayofmonth(df_log_data['datetime'])) \
        .withColumn('week', weekofyear(df_log_data['datetime'])) \
        .withColumn('month', month(df_log_data['datetime'])) \
        .withColumn('year', year(df_log_data['datetime'])) \
        .withColumn('weekday', get_weekday(df_log_data['ts']))

    time_table = df_log_data.select(
            ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
            )
    time_table = time_table.dropDuplicates()

    # time_table = time_table.filter(time_table.timestamp != Null)

    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy("year", "month") \
              .parquet(F"{output_data}/time/", mode='overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    df_song = spark.read.json(song_data)

    df_song_data = df_song.withColumnRenamed('year', 'song_year')

    # extract columns from joined song and log to create songplays table
    df_log_data = df_log_data.filter(df_log_data.page == 'NextSong')

    df_joined = df_song_data.join(
                    df_log_data,
                    (df_song_data.artist_name == df_log_data.artist)
                    & (df_song_data.title == df_log_data.song),
                    "inner")

    df_joined = df_joined \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .withColumnRenamed('sessionId', 'session_id') \
        .withColumnRenamed('userAgent', 'user_agent') \
        .withColumnRenamed('timestamp', 'start_time')

    songplays_table = df_joined.select(['month', 'year', 'songplay_id',
                                        'start_time', 'user_id', 'level',
                                        'song_id', 'artist_id',
                                        'session_id', 'location',
                                        'user_agent'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .partitionBy("year", "month") \
                   .parquet(F"{output_data}/songplays/", mode='overwrite')


def main():
    """
    The main function used for orchestrating the ETL process.
    1. Creating a spark session context
    2. Processing the song data
    3. Processing loag data

    Arguments:
        None

    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://rcr-udacity-dend-project3/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
