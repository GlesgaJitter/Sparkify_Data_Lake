import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import IntegerType, StringType, DoubleType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY'] 


def create_spark_session():
    """
    Function to create standard Spark session
    
    INPUTS:
    None
    
    OUTPUTS:
    spark - spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
	
	
def process_song_data(spark, input_data, output_data):
    """
    Loads song data from S3 and creates:
    - song table
    - artist table
    and load them back to S3 in parquet format
    
    INPUTS: 
    spark - spark session
    input_data - s3 location of datasets
    output_data - s3 location for tables
    
    OUTPUTS:
    None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'
        
    schema = StructType() \
	.add("num_songs", IntegerType(), True) \
	.add("artist_id", StringType(), True) \
	.add("artist_latitude", DoubleType(), True) \
	.add("artist_longitude", DoubleType(), True) \
	.add("artist_location", StringType(), True) \
	.add("artist_name", StringType(), True) \
	.add("song_id", StringType(), True) \
	.add("title", StringType(), True) \
	.add("duration", DoubleType(), True) \
	.add("year", IntegerType(), True)

	
    # read song data file
    df = spark.read.schema(schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 
                            'duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'output/songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
			      col('artist_name').alias('name'), 
			      col('artist_location').alias('location'),
			      col('artist_latitude').alias('latitude'), 
			      col('artist_longitude').alias('longitude')) \
                              .dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'output/artists/artists.parquet'), 'overwrite')
	
	
	
def process_log_data(spark, input_data, output_data):
    """
    Loads song data from S3 and creates:
    - users table
    - time table
    - songplays table
    and load them back to S3 in parquet format
    
    INPUTS: 
    spark - spark session
    input_data - s3 location of datasets
    output_data - s3 location for tables
    
    OUTPUTS:
    None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json' 
    #log_data = input_data + 'log_data/2018/11/2018-11-12-events.json'
	
    schema = StructType() \
	.add("artist", StringType(), True) \
	.add("auth", StringType(), True) \
	.add("firstName", StringType(), True) \
	.add("gender", StringType(), True) \
	.add("itemInSession", IntegerType(), True) \
	.add("lastName", StringType(), True) \
	.add("length", DoubleType(), True) \
	.add("level", StringType(), True) \
	.add("location", StringType(), True) \
	.add("method", StringType(), True) \
	.add("page", StringType(), True) \
	.add("registration", IntegerType(), True) \
	.add("sessionId", StringType(), True) \
	.add("song", StringType(), True) \
	.add("status", StringType(), True) \
	.add("ts", IntegerType(), True) \
	.add("userAgent", StringType(), True) \
	.add("userId", StringType(), True)

	
    # read song data file
    df = spark.read.schema(schema).json(log_data)
    
    # filter by actions for song plays
    nextSong_df = df.filter(df.page == 'NextSong') \
                    .select('ts', 'userId', 'level', 'song', 'artist',
                           'sessionId', 'location', 'userAgent')

    # extract columns for users table
    users_table = df.select(col('userId').alias('userId'), 
			    col('firstName').alias('first_name'), 
			    col('lastName').alias('last_name'), 
                            'gender', 'level') \
                            .dropDuplicates()
        
    users_table.createOrReplaceTempView('users')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'output/users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000))
    nextSong_df = nextSong_df.withColumn('timestamp', get_timestamp(nextSong_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    nextSong_df = nextSong_df.withColumn('datetime', get_datetime(nextSong_df.ts))
    
    # extract columns to create time table
    time_table = nextSong_df.select('datetime').dropDuplicates()
    
    time_table = time_table.withColumn('start_time', nextSong_df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime'))# \
                           #.withColumn('weekday', dayofweek('datetime'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'output/time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    #song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df = spark.read.json(input_data + 'song_data/A/B/C/TRABCEI128F424C983.json')

    # extract columns from joined song and log datasets to create songplays table 
    merged_df = nextSong_df.join(song_df, col('artist') == col(
        'artist_name'), 'inner')
    songplays_table = merged_df.select(col('datetime').alias('start_time'),
                                       col('userId').alias('user_id'),
                                       col('level').alias('level'),
                                       col('song_id').alias('song_id'),
                                       col('artist_id').alias('artist_id'),
                                       col('sessionId').alias('session_id'),
                                       col('location').alias('location'), 
                                       col('userAgent').alias('user_agent'),
                                       year('datetime').alias('year'),
                                       month('datetime').alias('month')) \
                                      .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'output/songplays/songplays.parquet'), 'overwrite')
		
		

def main():
    """
    Main function in which S3 source and target locations are defined
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-sparkify/"
        
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
	

if __name__ == "__main__":
    main()
