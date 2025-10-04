# dags/spark_jobs/data_quality_transform.py
"""
Data quality checks and initial transformations for NYC Green Taxi data
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import sys
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName('GreenTaxiDataQualityTransform') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()

def perform_data_quality_checks(df):
    """Perform comprehensive data quality checks"""
    initial_count = df.count()
    logger.info(f"Initial record count: {initial_count:,}")
    
    # Schema validation - cast data types
    df = df.withColumn("payment_type", F.col("payment_type").cast("int")) \
           .withColumn("lpep_pickup_datetime", F.to_timestamp("lpep_pickup_datetime")) \
           .withColumn("lpep_dropoff_datetime", F.to_timestamp("lpep_dropoff_datetime"))
    
    # Remove duplicates
    df = df.dropDuplicates()
    logger.info(f"After removing duplicates: {df.count():,}")
    
    # Remove invalid coordinates
    df = df.filter(
        (F.col("PULatitude").between(-90, 90)) & 
        (F.col("PULongitude").between(-180, 180)) &
        (F.col("DOLatitude").between(-90, 90)) & 
        (F.col("DOLongitude").between(-180, 180))
    )
    
    # Remove invalid datetime records
    df = df.filter(F.col("lpep_pickup_datetime") < F.col("lpep_dropoff_datetime"))
    
    # Remove zero or negative trip distances
    df = df.filter(F.col("trip_distance") > 0)
    
    # Remove nulls
    df = df.dropna()
    
    final_count = df.count()
    logger.info(f"Final record count after quality checks: {final_count:,}")
    logger.info(f"Removed {initial_count - final_count:,} invalid records ({(initial_count - final_count)/initial_count*100:.2f}%)")
    
    return df

def add_derived_features(df):
    """Add calculated features to the dataframe"""
    
    # Add trip duration in seconds
    df = df.withColumn("trip_duration_sec", 
                      F.unix_timestamp("lpep_dropoff_datetime") - F.unix_timestamp("lpep_pickup_datetime"))
    
    # Convert miles to kilometers
    df = df.withColumn("trip_distance_km", F.round(F.col("trip_distance") * 1.60934, 3))
    
    # Create trip_id
    df = df.withColumn("trip_id", 
                      F.concat(F.lit("trip_"),
                      F.date_format("lpep_pickup_datetime", "yyyyMMdd"),
                      F.lit("_"),
                      F.row_number().over(Window.orderBy(F.col("lpep_pickup_datetime")))))
    
    # Add geohash calculation (if geohash library available)
    try:
        import geohash2 as geohash
        
        def latlon_to_geohash(lat, lon, precision=8):
            if lat and lon:
                return geohash.encode(lat, lon, precision=precision)
            return None
        
        geohash_udf = F.udf(latlon_to_geohash, StringType())
        
        df = df.withColumn("start_geo_hash", geohash_udf(F.col("PULatitude"), F.col("PULongitude"))) \
               .withColumn("end_geo_hash", geohash_udf(F.col("DOLatitude"), F.col("DOLongitude")))
    except ImportError:
        logger.warning("Geohash library not available. Using location IDs instead.")
        df = df.withColumn("start_geo_hash", F.concat(F.lit("loc_"), F.col("PULocationID"))) \
               .withColumn("end_geo_hash", F.concat(F.lit("loc_"), F.col("DOLocationID")))
    
    return df

def create_staging_table(df, year_month):
    """Create staging table with selected columns"""
    staging_df = df.select(
        "trip_id",
        F.col("lpep_pickup_datetime").alias("start_time"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("payment_type").alias("payment_id"),
        "start_geo_hash",
        "end_geo_hash",
        "trip_duration_sec",
        "trip_distance_km",
        "total_amount"
    )
    
    # Add year_month partition column
    staging_df = staging_df.withColumn("year_month", F.lit(year_month))
    
    return staging_df

def main(year_month):
    """Main ETL function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read transformed parquet from HDFS
        input_path = f"hdfs://namenode:9000/data/green_taxi/transformed/{year_month}/"
        logger.info(f"Reading data from: {input_path}")
        
        df = spark.read.parquet(input_path)
        logger.info(f"Successfully loaded data for {year_month}")
        
        logger.info(f"Total records available: {df.count():,}")
        df = df.limit(1000)
        logger.info("Limited to 1000 records for testing")

        # Perform data quality checks
        df = perform_data_quality_checks(df)
        
        # Add derived features
        df = add_derived_features(df)
        
        # Create staging table
        staging_df = create_staging_table(df, year_month)
        
        # Write to Hive staging table
        logger.info("Writing to Hive staging table...")
        staging_df.write \
            .mode("overwrite") \
            .partitionBy("year_month") \
            .saveAsTable("taxi_warehouse.staging_rides_geo")
        
        # Also save to HDFS for dimensional modeling
        output_path = f"hdfs://namenode:9000/data/green_taxi/staging/{year_month}"
        staging_df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        logger.info(f"Data quality transformation completed for {year_month}")
        
        # Print summary statistics
        print(f"""
        ========================================
        Data Quality Transformation Summary
        ========================================
        Year-Month: {year_month}
        Records Processed: {staging_df.count():,}
        
        Trip Statistics:
        - Average Distance: {staging_df.select(F.avg("trip_distance_km")).collect()[0][0]:.2f} km
        - Average Duration: {staging_df.select(F.avg("trip_duration_sec")).collect()[0][0]/60:.1f} minutes
        - Total Revenue: ${staging_df.select(F.sum("total_amount")).collect()[0][0]:,.2f}
        ========================================
        """)
        
    except Exception as e:
        logger.error(f"Error in data quality transformation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: data_quality_transform.py <year-month>")
        sys.exit(1)
    
    year_month = sys.argv[1]
    main(year_month) 
