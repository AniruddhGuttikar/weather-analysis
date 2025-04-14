from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

# Define schema of incoming Kafka weather data
schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("timestamp", StringType())

spark = SparkSession.builder \
    .appName("WeatherSparkStreaming") \
    .config("spark.jars", "/mnt/d/weather-analysis-project/spark/libs/postgresql-42.7.1.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read data from Kafka topic 'weather-data'
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "latest") \
    .load()

# Decode and parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Add event time column (using to_timestamp for better conversion)
weather_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

# Tumbling window (15-minute) aggregation
agg_df = weather_df.groupBy(
    window(col("event_time"), "15 minutes"),
    col("city")
).agg(
    {"temperature": "avg", "humidity": "avg"}
).withColumnRenamed("avg(temperature)", "avg_temp") \
 .withColumnRenamed("avg(humidity)", "avg_humidity")

# Transform the window struct into separate timestamp columns
final_df = agg_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("city"),
    col("avg_temp"),
    col("avg_humidity")
)

# Output: Write to PostgreSQL
def write_to_postgres(batch_df, epoch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/weatherdb") \
        .option("dbtable", "streaming_weather") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = final_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()