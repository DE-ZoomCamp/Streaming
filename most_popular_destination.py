from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MostPopularDestination") \
    .getOrCreate()

# Create streaming dataframe from Kafka topic
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON data and select necessary columns
schema = "green"  # Define your schema here
green_stream_parsed = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*")

# Add a column "timestamp" using current_timestamp function
green_stream_with_timestamp = green_stream_parsed \
    .withColumn("timestamp", F.current_timestamp())

# Group by 5 minutes window based on timestamp and DOLocationID, then count
popular_destinations = green_stream_with_timestamp \
    .groupBy(F.window("timestamp", "5 minutes"), "DOLocationID") \
    .count() \
    .orderBy(F.desc("count"))

# Start the streaming query
query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

