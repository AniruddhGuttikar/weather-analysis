import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, max, min, lit, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

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

    def get_common_columns(self, stream_df, batch_df):
        """Get common columns between stream and batch dataframes."""
        stream_columns = set(stream_df.columns)
        batch_columns = set(batch_df.columns)
        # Always include window_start and window_end in the common columns
        common_cols = list(stream_columns.intersection(batch_columns))
        # Ensure required keys are first in the list
        for key in ['window_end', 'window_start']:
            if key in common_cols:
                common_cols.remove(key)
                common_cols.insert(0, key)
        return common_cols

    def get_key_columns(self, table_name):
        """Get key columns for joining based on table name."""
        # Define key columns for each table type to properly join between streaming and batch results
        if table_name == "weather_stats":
            return ["window_start", "window_end", "city_name"]
        elif table_name == "alert_counts":
            return ["window_start", "window_end", "alert_type"]
        elif table_name == "aqi_summary":
            return ["window_start", "window_end", "city_name"]
        else:
            return ["window_start", "window_end"]

    def get_numeric_columns(self, df, exclude_columns):
        """Get numeric columns in the dataframe."""
        return [field.name for field in df.schema.fields 
                if field.dataType.typeName() in ('double', 'integer', 'long', 'float') 
                and field.name not in exclude_columns]

    def compare_results(self, table_name):
        """Compare results between streaming and batch processing with window-based comparison."""
        logger.info(f"Comparing results for {table_name}")
        
        # Read data from both tables
        stream_df = self.read_from_postgres(table_name)
        batch_df = self.read_from_postgres(f"{table_name}_batch")
        
        # Get total counts for overall comparison
        stream_count = stream_df.count()
        batch_count = batch_df.count()
        
        # Calculate percentage difference in counts
        if stream_count > 0:
            count_diff_pct = abs(stream_count - batch_count) / stream_count * 100
        else:
            count_diff_pct = 0 if batch_count == 0 else 100
        
        # Log basic comparison metrics
        logger.info(f"Overall comparison for {table_name}:")
        logger.info(f"  - Stream record count: {stream_count}")
        logger.info(f"  - Batch record count: {batch_count}")
        logger.info(f"  - Count difference: {abs(stream_count - batch_count)} ({count_diff_pct:.2f}%)")
        
        # Get key columns for joining
        key_columns = self.get_key_columns(table_name)
        
        # Get common value columns between stream and batch dataframes
        common_cols = self.get_common_columns(stream_df, batch_df)
        
        # Get only key columns that exist in both dataframes
        actual_key_columns = [col for col in key_columns if col in stream_df.columns and col in batch_df.columns]
        
        # If no proper key columns, return basic comparison
        if not actual_key_columns:
            logger.warning(f"No valid key columns found for {table_name}. Skipping detailed comparison.")
            return {
                "table": table_name,
                "stream_count": stream_count,
                "batch_count": batch_count,
                "count_diff": abs(stream_count - batch_count),
                "count_diff_pct": count_diff_pct,
                "window_comparison": None
            }
        
        # Prepare dataframes for comparison by selecting common columns
        stream_df = stream_df.select(*common_cols)
        batch_df = batch_df.select(*common_cols)
        
        # Add source identifiers to prepare for union and comparison
        stream_df = stream_df.withColumn("source", lit("streaming"))
        batch_df = batch_df.withColumn("source", lit("batch"))
        
        # Union the dataframes to get all records in one
        combined_df = stream_df.union(batch_df)
        
        # Perform window-based statistics
        window_stats = combined_df.groupBy(*actual_key_columns).pivot("source").count()
        
        # Calculate window-based differences
        window_stats = window_stats.withColumn(
            "missing_in_batch", 
            col("streaming").isNotNull() & col("batch").isNull()
        ).withColumn(
            "missing_in_streaming", 
            col("streaming").isNull() & col("batch").isNotNull()
        ).withColumn(
            "record_diff", 
            spark_abs(col("streaming").cast(DoubleType()) - col("batch").cast(DoubleType()))
        )
        
        # Calculate detailed metrics
        missing_in_batch_count = window_stats.filter(col("missing_in_batch") == True).count()
        missing_in_streaming_count = window_stats.filter(col("missing_in_streaming") == True).count()
        matching_windows_count = window_stats.filter(
            (col("missing_in_batch") == False) & (col("missing_in_streaming") == False)
        ).count()
        
        total_windows = window_stats.count()
        
        # Now compare values within matching windows
        value_comparison = None
        
        if matching_windows_count > 0:
            # Get numeric columns for value comparison
            numeric_cols = self.get_numeric_columns(stream_df, actual_key_columns + ["source"])
            
            # Join the dataframes on key columns
            comparison_df = stream_df.alias("stream").join(
                batch_df.alias("batch"),
                on=[stream_df[k] == batch_df[k] for k in actual_key_columns],
                how="inner"
            )
            
            # Calculate differences for each numeric column
            diff_metrics = {}
            for col_name in numeric_cols:
                # Create diff column
                diff_col_name = f"{col_name}_diff"
                comparison_df = comparison_df.withColumn(
                    diff_col_name,
                    spark_abs(col(f"stream.{col_name}") - col(f"batch.{col_name}"))
                )
                
                # Calculate metrics
                stats = comparison_df.agg(
                    avg(diff_col_name).alias("avg_diff"),
                    max(diff_col_name).alias("max_diff")
                ).collect()[0]
                
                # Store metrics
                diff_metrics[col_name] = {
                    "avg_diff": stats["avg_diff"] if stats["avg_diff"] is not None else 0,
                    "max_diff": stats["max_diff"] if stats["max_diff"] is not None else 0
                }
            
            value_comparison = diff_metrics
            
            # Log detailed metrics for values
            logger.info(f"Value comparison for matching windows in {table_name}:")
            for col_name, metrics in diff_metrics.items():
                logger.info(f"  - {col_name}: avg_diff={metrics['avg_diff']:.4f}, max_diff={metrics['max_diff']:.4f}")
        
        # Log detailed metrics for windows
        logger.info(f"Window-based comparison for {table_name}:")
        logger.info(f"  - Total windows: {total_windows}")
        logger.info(f"  - Matching windows: {matching_windows_count} ({matching_windows_count/total_windows*100:.2f}% if total > 0)")
        logger.info(f"  - Windows only in streaming: {missing_in_batch_count}")
        logger.info(f"  - Windows only in batch: {missing_in_streaming_count}")
        
        # Write the differences to a separate table for further analysis
        difference_table_name = f"{table_name}_differences"
        try:
            window_stats.write \
                .jdbc(url=POSTGRES_URL, table=difference_table_name, mode="overwrite", properties=POSTGRES_PROPERTIES)
            logger.info(f"Successfully wrote difference data to {difference_table_name}")
        except Exception as e:
            logger.error(f"Error writing difference data to {difference_table_name}: {e}")
        
        return {
            "table": table_name,
            "stream_count": stream_count,
            "batch_count": batch_count,
            "count_diff": abs(stream_count - batch_count),
            "count_diff_pct": count_diff_pct,
            "window_comparison": {
                "total_windows": total_windows,
                "matching_windows": matching_windows_count,
                "missing_in_batch": missing_in_batch_count,
                "missing_in_streaming": missing_in_streaming_count,
                "value_comparison": value_comparison
            }
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
                window(col("timestamp"), "2 minutes", "1 minute"),
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
                window(col("timestamp"), "2 minutes"),
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
        
        logger.info("\nOverall Data Comparison:")
        for comp in comparisons:
            logger.info(f"  - {comp['table']}: {comp['count_diff']} records difference ({comp['count_diff_pct']:.2f}%)")
        
        logger.info("\nWindow-Based Comparison:")
        for comp in comparisons:
            window_comp = comp.get('window_comparison')
            if window_comp:
                table = comp['table']
                logger.info(f"  {table}:")
                logger.info(f"    - Total windows: {window_comp['total_windows']}")
                logger.info(f"    - Matching windows: {window_comp['matching_windows']} "
                           f"({window_comp['matching_windows']/window_comp['total_windows']*100:.2f}% if total > 0)")
                logger.info(f"    - Windows only in streaming: {window_comp['missing_in_batch']}")
                logger.info(f"    - Windows only in batch: {window_comp['missing_in_streaming']}")
                
                # Report on value differences in matching windows
                if window_comp['value_comparison']:
                    logger.info(f"    - Value differences in matching windows:")
                    for col_name, metrics in window_comp['value_comparison'].items():
                        logger.info(f"      - {col_name}: avg={metrics['avg_diff']:.4f}, max={metrics['max_diff']:.4f}")
        
        logger.info("\nRecommendations:")
        for comp in comparisons:
            window_comp = comp.get('window_comparison')
            if window_comp:
                if window_comp['missing_in_batch'] > 0:
                    logger.info(f"  - Investigate {window_comp['missing_in_batch']} windows missing in batch for {comp['table']}")
                if window_comp['missing_in_streaming'] > 0:
                    logger.info(f"  - Investigate {window_comp['missing_in_streaming']} windows missing in streaming for {comp['table']}")
                
                # Check for significant value differences
                if window_comp['value_comparison']:
                    for col_name, metrics in window_comp['value_comparison'].items():
                        if metrics['max_diff'] > 1.0:  # Threshold for significant difference
                            logger.info(f"  - Investigate significant differences in {col_name} for {comp['table']} (max diff: {metrics['max_diff']:.4f})")
            else:
                if comp['count_diff_pct'] > 5:
                    logger.info(f"  - Investigate differences in {comp['table']} (>{comp['count_diff_pct']:.2f}% difference)")
        
        logger.info("========================\n")
        
        # Write comprehensive comparison report to database
        try:
            report_data = []
            for comp in comparisons:
                report_row = {
                    "table_name": comp['table'],
                    "stream_count": comp['stream_count'],
                    "batch_count": comp['batch_count'],
                    "count_diff": comp['count_diff'],
                    "count_diff_pct": comp['count_diff_pct']
                }
                
                window_comp = comp.get('window_comparison')
                if window_comp:
                    report_row.update({
                        "total_windows": window_comp['total_windows'],
                        "matching_windows": window_comp['matching_windows'],
                        "missing_in_batch": window_comp['missing_in_batch'],
                        "missing_in_streaming": window_comp['missing_in_streaming']
                    })
                
                report_data.append(report_row)
            
            # Create report DataFrame
            report_df = self.spark.createDataFrame(report_data)
            
            # Write report to database
            report_df.write \
                .jdbc(url=POSTGRES_URL, table="comparison_report", mode="overwrite", properties=POSTGRES_PROPERTIES)
            logger.info("Successfully wrote comparison report to database")
        except Exception as e:
            logger.error(f"Error writing comparison report to database: {e}")

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