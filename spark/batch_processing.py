from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WeatherBatchProcessing") \
    .config("spark.jars", "/opt/postgresql-42.2.5.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load entire dataset from PostgreSQL
weather_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/weatherdb") \
    .option("dbtable", "streaming_weather") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Batch analysis - Example: Average temperature by city
result_df = weather_df.groupBy("city").avg("avg_temp", "avg_humidity") \
    .withColumnRenamed("avg(avg_temp)", "overall_avg_temp") \
    .withColumnRenamed("avg(avg_humidity)", "overall_avg_humidity")

# Show or export result
result_df.show()
# result_df.write.csv("batch_result.csv", header=True)
