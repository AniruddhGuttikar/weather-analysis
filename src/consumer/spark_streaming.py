# save this as consumer.py in your src directory
import datetime
import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType
)
from pyspark.sql.functions import from_json, col, window, avg, count, max, min, expr, udf

# --- Configuration Directly Embedded ---

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092" 
WEATHER_RAW_TOPIC = "weather-raw"
WEATHER_ALERTS_TOPIC = "weather-alerts"
AIR_QUALITY_TOPIC = "air-quality"
# Spark settings
SPARK_MASTER = "local[*]"  # Use local mode for easier debugging
# Ensure this path exists and is writable by the Spark process.
# If in Docker, ensure it's inside the container or a mounted volume.
CHECKPOINT_LOCATION_BASE = "/tmp/checkpoint"  # Base checkpoint location
CHECKPOINT_LOCATION = "/tmp/checkpoint"


# PostgreSQL settings
POSTGRES_HOST = "postgres"        # Use your actual Postgres host if different
POSTGRES_PORT = "5432"            # Use your actual Postgres port if different
POSTGRES_DB = "weather_db"      # Use your actual Postgres database name
POSTGRES_USER = "postgres"        # Use your actual Postgres user
POSTGRES_PASSWORD = "postgres"    # Use your actual Postgres password

# Database connection string and properties
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
# --- End of Embedded Configuration ---


# Set up logging
logging.basicConfig(
    level=logging.WARN,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkStreamingConsumer:
    def __init__(self):
        """Initialize Spark session and define schemas for weather data."""
        logger.info(f"Initializing SparkSession with master: {SPARK_MASTER}")
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("WeatherAnalysisStreaming") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"  # Kafka connector
                    "org.postgresql:postgresql:42.6.0") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
            .getOrCreate()

        # Set log level (INFO is good for debugging, WARN or ERROR for production)
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully.")
        # Define schema for raw weather data incoming from Kafka
        self.weather_schema = StructType([
            StructField("city_id", IntegerType()),
            StructField("city_name", StringType()),
            StructField("temperature", FloatType()),
            StructField("humidity", FloatType()),
            StructField("pressure", FloatType()),
            StructField("wind_speed", FloatType()),
            StructField("weather_condition", StringType()),
            StructField("timestamp", TimestampType()) # Assuming Kafka producer sends timestamp correctly
        ])

        self.alert_schema = StructType([
            StructField("city_name", StringType()),
            StructField("alert_type", StringType()),
            StructField("alert_message", StringType()),
            StructField("temperature", FloatType()),
            StructField("timestamp", TimestampType())
        ])

        self.aqi_schema = StructType([
            StructField("city_name", StringType()),
            StructField("aqi", FloatType()),
            StructField("pm2_5", FloatType()),
            StructField("pm10", FloatType()),
            StructField("no2", FloatType()),
            StructField("no", FloatType()),
            StructField("so2", FloatType()),
            StructField("co", FloatType()),
            StructField("o3", FloatType()),
            StructField("nh3", FloatType()),
            StructField("timestamp", TimestampType())
        ])

        logger.info("Spark Streaming Consumer initialized")

    def create_raw_weather_stream(self):
        """Create a streaming DataFrame from Kafka raw weather topic."""
        logger.info(f"Creating Kafka stream from topic: {WEATHER_RAW_TOPIC}")
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", WEATHER_RAW_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load() \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") # Keep key if needed, cast value

            # Parse the JSON data from the 'value' column
            parsed_df = df.select(from_json(col("value"), self.weather_schema).alias("data")) \
                          .select("data.*")

            logger.info("Kafka stream DataFrame created successfully.")
            return parsed_df
        except Exception as e:
            logger.error(f"Error creating Kafka stream: {e}", exc_info=True)
            raise

    def create_alerts_stream(self):
        """Create a streaming DataFrame from Kafka weather alerts topic."""
        logger.info(f"Creating Kafka stream from topic: {WEATHER_ALERTS_TOPIC}")
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", WEATHER_ALERTS_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load() \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            parsed_df = df.select(from_json(col("value"), self.alert_schema).alias("alert")) \
                          .select("alert.*")

            logger.info("Kafka weather alerts stream DataFrame created successfully.")
            return parsed_df
        except Exception as e:
            logger.error(f"Error creating Kafka alerts stream: {e}", exc_info=True)
            raise

    def create_aqi_stream(self):
        """Create a streaming DataFrame from Kafka air quality topic."""
        logger.info(f"Creating Kafka stream from topic: {AIR_QUALITY_TOPIC}")
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", AIR_QUALITY_TOPIC) \
                .option("startingOffsets", "earliest") \
                .option("failOnDataLoss", "false") \
                .load() \
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

            parsed_df = df.select(from_json(col("value"), self.aqi_schema).alias("aqi")) \
                          .select("aqi.*")

            logger.info("Kafka air quality stream DataFrame created successfully.")
            return parsed_df
        except Exception as e:
            logger.error(f"Error creating Kafka air quality stream: {e}", exc_info=True)
            raise

    def run(self):
        """Run the Spark Streaming consumer with data processing operations."""
        try:
            raw_weather_df = self.create_raw_weather_stream()
            weather_alerts_df = self.create_alerts_stream()
            aqi_df = self.create_aqi_stream()
            
            # Define the target tables and checkpoint locations
            raw_weather_table = "weather_raw"
            alerts_table = "weather_alerts"
            aqi_table = "air_quality"
            
            # Add new tables for processed data
            weather_stats_table = "weather_stats"
            alert_counts_table = "alert_counts"
            aqi_summary_table = "aqi_summary"

            checkpoint_loc_base = CHECKPOINT_LOCATION_BASE
            checkpoint_loc_raw = os.path.join(checkpoint_loc_base, raw_weather_table)
            checkpoint_loc_alerts = os.path.join(checkpoint_loc_base, alerts_table)
            checkpoint_loc_aqi = os.path.join(checkpoint_loc_base, aqi_table)
            checkpoint_loc_weather_stats = os.path.join(checkpoint_loc_base, weather_stats_table)
            checkpoint_loc_alert_counts = os.path.join(checkpoint_loc_base, alert_counts_table)
            checkpoint_loc_aqi_summary = os.path.join(checkpoint_loc_base, aqi_summary_table)
            
            # Ensure checkpoint directories exist
            for checkpoint_loc in [checkpoint_loc_raw, checkpoint_loc_alerts, checkpoint_loc_aqi,
                                checkpoint_loc_weather_stats, checkpoint_loc_alert_counts, 
                                checkpoint_loc_aqi_summary]:
                if not os.path.exists(checkpoint_loc):
                    os.makedirs(checkpoint_loc)
                    logger.info(f"Created checkpoint directory: {checkpoint_loc}")

            # --- OPERATION 1: Weather Statistics by City in Tumbling Windows ---
            # Process raw weather data to calculate statistics per city in 1-hour windows
            # TODO: update the time back to 1 hour
            weather_stats_df = raw_weather_df \
                .withWatermark("timestamp", "10 seconds") \
                .groupBy(
                    window(col("timestamp"), "2 minutes"),
                    col("city_name")
                ) \
                .agg(
                    avg("temperature").alias("avg_temperature"),
                    max("temperature").alias("max_temperature"),
                    min("temperature").alias("min_temperature"),
                    avg("humidity").alias("avg_humidity"),
                    avg("pressure").alias("avg_pressure"),
                    avg("wind_speed").alias("avg_wind_speed"),
                    count("*").alias("record_count")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("city_name"),
                    col("avg_temperature"),
                    col("min_temperature"),
                    col("max_temperature"),
                    col("avg_humidity"),
                    col("avg_pressure"),
                    col("avg_wind_speed"),
                    col("record_count")
                )
            
            # --- OPERATION 2: Aggregate Alert Counts by Type ---
            # Process weather alerts to count occurrences by type
            alert_counts_df = weather_alerts_df \
                .withWatermark("timestamp", "10 seconds") \
                .groupBy(
                    window(col("timestamp"), "2 minutes", "30 seconds"),  # 1-hour sliding window with 30min slide
                    col("alert_type")
                ) \
                .agg(
                    count("*").alias("alert_count"),
                    avg("temperature").alias("avg_temperature")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("alert_type"),
                    col("alert_count"),
                    col("avg_temperature")
                )
            
            # --- OPERATION 3: Categorize Air Quality and Calculate City Averages ---
            # Define a UDF to categorize AQI values

            
            # Process air quality data to categorize and calculate city averages
            # .withColumn("aqi_category", categorize_aqi(col("aqi"))) 

            aqi_summary_df = aqi_df \
                .withWatermark("timestamp", "10 seconds") \
                .groupBy(
                    window(col("timestamp"), "2 minutes"),
                    col("city_name"),
                ) \
                .agg(
                    avg("aqi").alias("avg_aqi"),
                    avg("pm2_5").alias("avg_pm2_5"),
                    avg("pm10").alias("avg_pm10"),
                    avg("o3").alias("avg_o3"),
                    count("*").alias("measurement_count")
                ) \
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("city_name"),
                    col("avg_aqi"),
                    col("avg_pm2_5"),
                    col("avg_pm10"),
                    col("avg_o3"),
                    col("measurement_count")
                )



            # Define the batch processing function
            def save_batch_to_postgres(batch_df, batch_id, target_table):
                if batch_df.rdd.isEmpty():
                    logger.info(f"Batch {batch_id} is empty, skipping write to {target_table}.")
                    return
                
                logger.info(f"Processing batch {batch_id} for table {target_table}.")
                try:
                    # Define tables that need upsert logic (aggregated data)
                    upsert_tables = [weather_stats_table, alert_counts_table, aqi_summary_table]
                    
                    # Apply upsert logic only for aggregated tables
                    if target_table in upsert_tables:
                        # Get the primary key columns based on target table
                        if target_table == weather_stats_table:
                            key_columns = ["window_start", "window_end", "city_name"]
                        elif target_table == alert_counts_table:
                            key_columns = ["window_start", "window_end", "alert_type"]
                        elif target_table == aqi_summary_table:
                            key_columns = ["window_start", "window_end", "city_name"]
                            logger.warning(f"Writing for aqi summary table {batch_df}")
                        
                        # Extract key values from this batch
                        key_values = batch_df.select(*key_columns).distinct().collect()
                        
                        if key_values:
                            # Connect to PostgreSQL and delete existing records with these keys
                            
                            conn = psycopg2.connect(
                                host=POSTGRES_HOST,
                                port=POSTGRES_PORT,
                                dbname=POSTGRES_DB,
                                user=POSTGRES_USER,
                                password=POSTGRES_PASSWORD
                            )
                            
                            try:
                                with conn.cursor() as cursor:
                                    # Construct WHERE clause for deletion with proper quoting for timestamps and strings
                                    where_conditions = []
                                    for row in key_values:
                                        conditions = []
                                        for i, col in enumerate(key_columns):
                                            value = row[col]
                                            
                                            # Handle different data types with proper quoting
                                            if value is None:
                                                conditions.append(f"{col} IS NULL")
                                            elif isinstance(value, (datetime.datetime, datetime.date)):
                                                # Format timestamp with quotes
                                                conditions.append(f"{col} = '{value}'")
                                            elif isinstance(value, str):
                                                # Escape single quotes in strings and add quotes
                                                escaped_value = value.replace("'", "''")
                                                conditions.append(f"{col} = '{escaped_value}'")
                                            elif isinstance(value, (int, float, bool)):
                                                # No quotes for numeric/boolean values
                                                conditions.append(f"{col} = {value}")
                                            else:
                                                # Default case - quote as string
                                                conditions.append(f"{col} = '{value}'")
                                        
                                        where_conditions.append(f"({' AND '.join(conditions)})")
                                    
                                    where_clause = " OR ".join(where_conditions)
                                    delete_query = f"DELETE FROM {target_table} WHERE {where_clause}"
                                    
                                    cursor.execute(delete_query)
                                    conn.commit()
                                    
                                    deleted_count = cursor.rowcount
                                    logger.info(f"Deleted {deleted_count} existing records from {target_table}")
                            finally:
                                conn.close()
                    
                    # For all tables (both raw and aggregated), append the new batch data
                    batch_df.write \
                        .jdbc(url=POSTGRES_URL, table=target_table, mode="append", properties=POSTGRES_PROPERTIES)
                    
                    logger.info(f"Successfully saved batch {batch_id} to {target_table}")
                except Exception as e:
                    logger.error(f"Error saving batch {batch_id} to {target_table}: {e}", exc_info=True)
            # Start the streaming queries for raw data storage
            raw_query = raw_weather_df \
                .writeStream \
                .foreachBatch(lambda batch_df, batch_id: save_batch_to_postgres(batch_df, batch_id, raw_weather_table)) \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_loc_raw) \
                .start()

            alerts_query = weather_alerts_df \
                .writeStream \
                .foreachBatch(lambda batch_df, batch_id: save_batch_to_postgres(batch_df, batch_id, alerts_table)) \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_loc_alerts) \
                .start()
            
            aqi_query = aqi_df \
                .writeStream \
                .foreachBatch(lambda df, id: save_batch_to_postgres(df, id, aqi_table)) \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_loc_aqi) \
                .start()

            # Start the streaming queries for the new operations
            weather_stats_query = weather_stats_df \
                .writeStream \
                .foreachBatch(lambda df, id: save_batch_to_postgres(df, id, weather_stats_table)) \
                .outputMode("update") \
                .option("checkpointLocation", checkpoint_loc_weather_stats) \
                .start()
                
            alert_counts_query = alert_counts_df \
                .writeStream \
                .foreachBatch(lambda df, id: save_batch_to_postgres(df, id, alert_counts_table)) \
                .outputMode("update") \
                .option("checkpointLocation", checkpoint_loc_alert_counts) \
                .start()
                
            aqi_summary_query = aqi_summary_df \
                .writeStream \
                .foreachBatch(lambda df, id: save_batch_to_postgres(df, id, aqi_summary_table)) \
                .outputMode("update") \
                .option("checkpointLocation", checkpoint_loc_aqi_summary) \
                .start()

            logger.info("Started all streaming queries for data processing.")
            
            # Wait for all queries to terminate
            for query in [raw_query, alerts_query, aqi_query, 
                        weather_stats_query, alert_counts_query, aqi_summary_query]:
                query.awaitTermination()

        except Exception as e:
            logger.error(f"FATAL ERROR in Spark Streaming Consumer run method: {e}", exc_info=True)
        finally:
            logger.info("Stopping Spark session")
            if hasattr(self, 'spark') and self.spark:
                self.spark.stop()
                logger.info("Spark session stopped.")
            else:
                logger.info("Spark session object not found or already stopped.")

if __name__ == "__main__":
    logger.info("Starting Weather Analysis Spark Consumer...")
    consumer = SparkStreamingConsumer()
    consumer.run()
    logger.info("Weather Analysis Spark Consumer finished.")