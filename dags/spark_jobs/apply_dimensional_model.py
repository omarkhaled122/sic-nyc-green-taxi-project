# dags/spark_jobs/apply_dimensional_model.py
"""
Apply dimensional modeling to create fact and dimension tables
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName('GreenTaxiDimensionalModel') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def create_dim_location(trips_df, zones_df):
    """Create location dimension table"""
    logger.info("Creating Location dimension...")
    
    # Get unique locations from pickup and dropoff
    pickup_locations = trips_df.select(
        F.col("pu_location_id").alias("location_id"),
        F.col("start_geo_hash").alias("geo_hash")
    ).distinct()
    
    dropoff_locations = trips_df.select(
        F.col("do_location_id").alias("location_id"),
        F.col("end_geo_hash").alias("geo_hash")
    ).distinct()
    
    # Union all unique locations
    all_locations = pickup_locations.union(dropoff_locations).distinct()
    
    # Join with zone lookup to get borough
    dim_location = all_locations.join(
        zones_df,
        all_locations.location_id == zones_df.LocationID,
        "left"
    ).select(
        "location_id",
        "geo_hash",
        F.col("Borough").alias("borough")
    )
    
    # Add center coordinates (if geohash available)
    try:
        import geohash2 as geohash
        
        def decode_lat(gh):
            if gh and gh.startswith("loc_"):
                return None
            return geohash.decode(gh)[0] if gh else None
        
        def decode_lon(gh):
            if gh and gh.startswith("loc_"):
                return None
            return geohash.decode(gh)[1] if gh else None
        
        lat_udf = F.udf(decode_lat, DoubleType())
        lon_udf = F.udf(decode_lon, DoubleType())
        
        dim_location = dim_location.withColumn("center_lat", lat_udf("geo_hash")) \
                                   .withColumn("center_lon", lon_udf("geo_hash"))
    except ImportError:
        logger.warning("Geohash library not available. Setting center coordinates to null.")
        dim_location = dim_location.withColumn("center_lat", F.lit(None).cast(DoubleType())) \
                                   .withColumn("center_lon", F.lit(None).cast(DoubleType()))
    
    return dim_location

def create_dim_time(trips_df):
    """Create time dimension table"""
    logger.info("Creating Time dimension...")
    
    dim_time = trips_df.select(F.col("start_time").alias("full_date")).distinct()
    
    dim_time = dim_time.withColumn("time_id", F.date_format("full_date", "yyyyMMddHHmmss")) \
                       .withColumn("year", F.year("full_date")) \
                       .withColumn("month", F.month("full_date")) \
                       .withColumn("day", F.dayofmonth("full_date")) \
                       .withColumn("hour", F.hour("full_date")) \
                       .withColumn("minute", F.minute("full_date")) \
                       .withColumn("second", F.second("full_date")) \
                       .withColumn("weekday", F.dayofweek("full_date")) \
                       .withColumn("quarter", F.quarter("full_date"))
    
    return dim_time.select("time_id", "full_date", "year", "month", "day", 
                          "hour", "minute", "second", "weekday", "quarter")

def create_dim_payment():
    """Create payment dimension table"""
    logger.info("Creating Payment dimension...")
    
    payment_data = [
        (0, "Flex_Fare_trip"),
        (1, "Credit_card"),
        (2, "Cash"),
        (3, "No_charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided_trip")
    ]
    
    payment_schema = StructType([
        StructField('payment_id', IntegerType(), False),
        StructField('payment_type', StringType(), True),
    ])
    
    spark = SparkSession.builder.getOrCreate()
    return spark.createDataFrame(payment_data, schema=payment_schema)

def create_fact_trip(trips_df, dim_time, dim_location, year_month):
    """Create trip fact table"""
    logger.info("Creating Trip fact table...")
    
    fact_trip = trips_df.join(
        dim_time,
        trips_df.start_time == dim_time.full_date,
        "left"
    ).join(
        dim_location.alias("start_loc"),
        (trips_df.pu_location_id == F.col("start_loc.location_id")) & 
        (trips_df.start_geo_hash == F.col("start_loc.geo_hash")),
        "left"
    ).join(
        dim_location.alias("end_loc"),
        (trips_df.do_location_id == F.col("end_loc.location_id")) & 
        (trips_df.end_geo_hash == F.col("end_loc.geo_hash")),
        "left"
    ).select(
        "trip_id",
        "time_id",
        F.col("start_loc.location_id").alias("start_location_id"),
        F.col("end_loc.location_id").alias("end_location_id"),
        "payment_id",
        "trip_duration_sec",
        "trip_distance_km",
        "total_amount"
    )
    
    # Add year_month partition
    fact_trip = fact_trip.withColumn("year_month", F.lit(year_month))
    
    return fact_trip

def main(year_month):
    """Main dimensional modeling function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Read staging data
        logger.info(f"Reading staging data for {year_month}...")
        trips_df = spark.sql(f"""
            SELECT * FROM taxi_warehouse.staging_rides_geo 
            WHERE year_month = '{year_month}'
        """)
        
        # Read zone lookup from HDFS
        zones_df = spark.read.option("header", "true") \
                             .csv("hdfs://namenode:9000/data/green_taxi/lookup/taxi_zone_lookup.csv")
        
        # Create dimensions
        dim_location = create_dim_location(trips_df, zones_df)
        dim_time = create_dim_time(trips_df)
        dim_payment = create_dim_payment()
        
        # Create fact table
        fact_trip = create_fact_trip(trips_df, dim_time, dim_location, year_month)
        
        # Write dimension tables (overwrite mode for dimensions)
        logger.info("Writing dimension tables...")
        
        # Location dimension - append new locations only
        existing_locations = spark.sql("SELECT DISTINCT location_id, geo_hash FROM taxi_warehouse.DimLocation")
        new_locations = dim_location.join(existing_locations, ["location_id", "geo_hash"], "left_anti")
        if new_locations.count() > 0:
            new_locations.write.mode("append").saveAsTable("taxi_warehouse.DimLocation")
            logger.info(f"Added {new_locations.count()} new locations")
        
        # Time dimension - append new times only
        existing_times = spark.sql("SELECT DISTINCT time_id FROM taxi_warehouse.DimTime")
        new_times = dim_time.join(existing_times, ["time_id"], "left_anti")
        if new_times.count() > 0:
            new_times.write.mode("append").saveAsTable("taxi_warehouse.DimTime")
            logger.info(f"Added {new_times.count()} new time records")
        
        # Payment dimension - overwrite (static data)
        dim_payment.write.mode("overwrite").saveAsTable("taxi_warehouse.DimPayment")
        
        # Write fact table - append with partition
        logger.info("Writing fact table...")
        fact_trip.write \
            .mode("overwrite") \
            .partitionBy("year_month") \
            .saveAsTable("taxi_warehouse.FactTrip")
        
        # Print summary
        fact_count = fact_trip.count()
        print(f"""
        ========================================
        Dimensional Model Creation Summary
        ========================================
        Year-Month: {year_month}
        Fact Records Created: {fact_count:,}
        
        Dimension Counts:
        - Locations: {dim_location.count():,}
        - Time Records: {dim_time.count():,}
        - Payment Types: {dim_payment.count():,}
        
        Top 5 Routes:
        """)
        
        # Show top routes
        top_routes = spark.sql(f"""
            SELECT 
                sl.borough as start_borough,
                el.borough as end_borough,
                COUNT(*) as trip_count,
                AVG(f.trip_distance_km) as avg_distance_km,
                AVG(f.total_amount) as avg_fare
            FROM taxi_warehouse.FactTrip f
            JOIN taxi_warehouse.DimLocation sl ON f.start_location_id = sl.location_id
            JOIN taxi_warehouse.DimLocation el ON f.end_location_id = el.location_id
            WHERE f.year_month = '{year_month}'
            GROUP BY sl.borough, el.borough
            ORDER BY trip_count DESC
            LIMIT 5
        """)
        top_routes.show()
        
        logger.info(f"Dimensional modeling completed for {year_month}")
        
    except Exception as e:
        logger.error(f"Error in dimensional modeling: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: apply_dimensional_model.py <year-month>")
        sys.exit(1)
    
    year_month = sys.argv[1]
    main(year_month)
