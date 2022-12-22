import configparser
from datetime import datetime
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import  TimestampType


config = configparser.ConfigParser()

#Normally this file should be in ~/.aws/credentials
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create spark_session and lauch spark in aws
    
    
    """
    spark = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()

    return spark
def process_song_data(spark, input_data, output_data):
    """
    process song_data to extract  correct fields for songs_table and artist_table
    
    and write tables in praquet format 
    
    with three params:
    
    sparks: to connect to saprk_session
    
    input_data: path to read data from the s3 bucket
    
    out_data: path to save data in s3 bucket
    
    """
    # get filepath to song data file
    song_data =input_data+'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =df.select('song_id', 'title', 'artist_id','year', 'duration').dropDuplicates()
                            
    
    # write songs table to parquet files partitioned by year and artist
    songs_table=songs_table.write.partitionBy("year","artist_id").parquet(output_data+"songs_table/", mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name", "artist_location", "artist_latitude", "artist_longitude")\
                    .withColumnRenamed("artist_name","name")\
                     .withColumnRenamed("artist_location","location")\
                      .withColumnRenamed("artist_latitude","lattitude")\
                       .withColumnRenamed("artist_longitude","longitude")\
                        .dropDuplicates()
  
    
    # write artists table to parquet files
    artists_table=artists_table.write.parquet(output_data+"artist_table/", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
    process log_data to extract  correct fields for time_table ,users_table,song_plays table
    
    and write tables in praquet format 
    
    with three params:
    
    sparks: to connect to saprk_session
    
    input_data: path to read data from the s3 bucket
    
    out_data: path to save data in s3 bucket
    """
    # get filepath to log data file
    log_data =input_data+"log_data/2018/11/*.json"

    # read log data file
    df =spark.read.json(log_data)
    
    # filter by actions for song plays
    page_df= df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table =page_df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users_table/", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:str(int(x)/1000))
    page_df = page_df.withColumn('timestamp',get_timestamp(page_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime =udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    page_df= page_df.withColumn("start_time",get_datetime (page_df.ts))
    
    # extract columns to create time table
    time_table =page_df.withColumn("hour",hour("start_time"))\
                .withColumn("day",dayofmonth("start_time"))\
                .withColumn("week",weekofyear("start_time"))\
                .withColumn("month",month("start_time"))\
                .withColumn("year",year("start_time"))\
                 .withColumn("weekday",dayofweek("start_time"))
    time_table=time_table.select("start_time","hour","week","month","year","weekday").dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"time_table/", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/A/A/A/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = page_df.join(song_df ,(page_df.song==song_df .title))\
                        .select(
                                "start_time",
                                col('userId').alias('user_id'),
                                "level",
                                "song_id",
                                "artist_id",
                                col('sessionId').alias('session_id'),
                                "location",
                                col('userAgent').alias('user_agent'))\
                            .withColumn('songplay_id', monotonically_increasing_id())
    songplays_table=songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                                .select("songplay_id",
                                        songplays_table.start_time,
                                        "user_id", 
                                        "level", 
                                        "song_id",
                                        "artist_id",
                                        "session_id",
                                        "location",
                                        "user_agent",
                                        "year",
                                        "month")\
                                    .dropDuplicates()
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays_table/", mode = "overwrite")


def main():
    """
    implement all function   here 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-pyspark95/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
