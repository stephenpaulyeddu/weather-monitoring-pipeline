from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WeatherMonitoring") \
    .getOrCreate()

# dependency in spark submit to run --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

# Define the schema for the Kafka message
schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("weather_id", IntegerType(), True),
    StructField("weather_description", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("created_at", LongType(), True),
    StructField("timezone", LongType(), True)
])

# Define Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "weather-data-1",
    "startingOffsets": "latest"
}

# Connect kafka and read data from the topic
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

# CHANGING DATATYPES OF 2 COLUMNS:
    # 'created_at','timezone'

# Replace 'created_at' with 'time' containing timestamps
df_parsed = df_parsed.withColumn("created_at", from_unixtime("created_at"))
df_parsed = df_parsed.withColumn("created_at",col("created_at").cast("timestamp"))


# Define custom function to get timezone from Unix timestamp
def get_timezone_from_unix(unix_time):

    unix_time = int(unix_time)
    # Convert seconds to hours and minutes
    hours = abs(unix_time) // 3600
    minutes = (abs(unix_time) % 3600) // 60

    # Construct the timezone string
    timezone_str = 'UTC' + ('+' if unix_time >= 0 else '-') + '{:02}:{:02}'.format(hours, minutes)

    return timezone_str

# Register the custom function as a UDF
get_timezone_udf = udf(get_timezone_from_unix, StringType())
df_parsed = df_parsed.withColumn("timezone", get_timezone_udf("timezone"))


# Write the parsed data to HDFS
query = df_parsed \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://127.0.0.1:9000/user/test") \
    .option("checkpointLocation", "/Users/Stephen/Documents/Projects_1/abc/weather-monitoring/weather_data_checkpoint") \
    .start()

# # Wait for the stream to finish
query.awaitTermination()
