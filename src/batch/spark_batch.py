# save this as spark_batch.py
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, max, min

# --- Configuration ---
# Spark settings
SPARK_MASTER = "local[*]"  # Use local mode for easier debugging

# PostgreSQL settings
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "weather_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

# Database connection string and properties
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkBatchProcessor:
    def __init__(self):
        """Initialize Spark session for batch processing."""
        logger.info(f"Initializing SparkSession with master: {SPARK_MASTER}")
        
        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("WeatherAnalysisBatch") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .getOrCreate()

        # Set log level
        self.spark.sparkContext.setLogLevel("INFO")
        logger.info("Spark session created successfully.")

    def read_from_postgres(self, table_name):
        """Read data from PostgreSQL table."""
        logger.info(f"Reading data from {table_name}")
        try:
            df = self.spark.read \
                .jdbc(url=POSTGRES_URL, table=table_name, properties=POSTGRES_PROPERTIES)
            logger.info(f"Successfully read {df.count()} records from {table_name}")
            return df
        except Exception as e:
            logger.error(f"Error reading from {table_name}: {e}", exc_info=True)
            raise

    def write_to_postgres(self, df, table_name):
        """Write DataFrame to PostgreSQL table."""
        logger.info(f"Writing data to {table_name}")
        try:
            df.write \
                .jdbc(url=POSTGRES_URL, table=f"{table_name}_batch", mode="overwrite", properties=POSTGRES_PROPERTIES)
            logger.info(f"Successfully wrote {df.count()} records to {table_name}_batch")
        except Exception as e:
            logger.error(f"Error writing to {table_name}_batch: {e}", exc_info=True)
            raise

    def compare_results(self, table_name):
        """Compare results between streaming and batch processing."""
        logger.info(f"Comparing results for {table_name}")
        
        stream_df = self.read_from_postgres(table_name)
        batch_df = self.read_from_postgres(f"{table_name}_batch")
        
        # Compare record counts
        stream_count = stream_df.count()
        batch_count = batch_df.count()
        
        # Calculate percentage difference
        if stream_count > 0:
            count_diff_pct = abs(stream_count - batch_count) / stream_count * 100
        else:
            count_diff_pct = 0 if batch_count == 0 else 100
            
        logger.info(f"Comparison for {table_name}:")
        logger.info(f"  - Stream record count: {stream_count}")
        logger.info(f"  - Batch record count: {batch_count}")
        logger.info(f"  - Count difference: {abs(stream_count - batch_count)} ({count_diff_pct:.2f}%)")
        
        return {
            "table": table_name,
            "stream_count": stream_count,
            "batch_count": batch_count,
            "count_diff": abs(stream_count - batch_count),
            "count_diff_pct": count_diff_pct
        }

    def run_operation_1(self):
        """Run batch version of Operation 1: Weather Statistics by City."""
        logger.info("Running Operation 1: Weather Statistics by City")
        start_time = time.time()
        
        # Read raw weather data
        raw_weather_df = self.read_from_postgres("weather_raw")
        
        # Apply the same aggregation as in streaming
        weather_stats_df = raw_weather_df \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
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
        
        # Write results to PostgreSQL
        self.write_to_postgres(weather_stats_df, "weather_stats")
        
        end_time = time.time()
        logger.info(f"Operation 1 completed in {end_time - start_time:.2f} seconds")
        
        return end_time - start_time

    def run_operation_2(self):
        """Run batch version of Operation 2: Aggregate Alert Counts by Type."""
        logger.info("Running Operation 2: Aggregate Alert Counts by Type")
        start_time = time.time()
        
        # Read weather alerts data
        alerts_df = self.read_from_postgres("weather_alerts")
        
        # Apply the same aggregation as in streaming
        alert_counts_df = alerts_df \
            .groupBy(
                window(col("timestamp"), "15 minutes", "5 minutes"),
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
        
        # Write results to PostgreSQL
        self.write_to_postgres(alert_counts_df, "alert_counts")
        
        end_time = time.time()
        logger.info(f"Operation 2 completed in {end_time - start_time:.2f} seconds")
        
        return end_time - start_time

    def run_operation_3(self):
        """Run batch version of Operation 3: Categorize Air Quality and Calculate City Averages."""
        logger.info("Running Operation 3: Air Quality Summary")
        start_time = time.time()
        
        # Read air quality data
        aqi_df = self.read_from_postgres("air_quality")
        
        # Apply the same aggregation as in streaming
        aqi_summary_df = aqi_df \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
                col("city_name")
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
        
        # Write results to PostgreSQL
        self.write_to_postgres(aqi_summary_df, "aqi_summary")
        
        end_time = time.time()
        logger.info(f"Operation 3 completed in {end_time - start_time:.2f} seconds")
        
        return end_time - start_time

    def generate_comparison_report(self, timing_results):
        """Generate a report comparing batch and streaming results."""
        logger.info("Generating comparison report")
        
        comparisons = []
        comparisons.append(self.compare_results("weather_stats"))
        comparisons.append(self.compare_results("alert_counts"))
        comparisons.append(self.compare_results("aqi_summary"))
        
        logger.info("\n=== COMPARISON REPORT ===")
        logger.info("Performance Summary:")
        for op, time_taken in timing_results.items():
            logger.info(f"  - {op}: {time_taken:.2f} seconds")
        
        logger.info("\nData Comparison:")
        for comp in comparisons:
            logger.info(f"  - {comp['table']}: {comp['count_diff']} records difference ({comp['count_diff_pct']:.2f}%)")
        
        logger.info("\nRecommendations:")
        for comp in comparisons:
            if comp['count_diff_pct'] > 5:
                logger.info(f"  - Investigate differences in {comp['table']} (>{comp['count_diff_pct']:.2f}% difference)")
            else:
                logger.info(f"  - {comp['table']} results are consistent between streaming and batch processing")
        
        logger.info("========================\n")

    def run(self):
        """Run all batch operations and comparisons."""
        try:
            timing_results = {}
            
            # Run all operations
            timing_results["Operation 1"] = self.run_operation_1()
            timing_results["Operation 2"] = self.run_operation_2()
            timing_results["Operation 3"] = self.run_operation_3()
            
            # Generate comparison report
            self.generate_comparison_report(timing_results)
            
        except Exception as e:
            logger.error(f"FATAL ERROR in Spark Batch Processor: {e}", exc_info=True)
        finally:
            logger.info("Stopping Spark session")
            if hasattr(self, 'spark') and self.spark:
                self.spark.stop()
                logger.info("Spark session stopped.")

if __name__ == "__main__":
    logger.info("Starting Weather Analysis Batch Processor...")
    processor = SparkBatchProcessor()
    processor.run()
    logger.info("Weather Analysis Batch Processor finished.")