# dags/green_taxi_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.apache.hive.operators.hive import HiveOperator
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import os
import logging
from utils.coordinates import nyc_taxi_zone_coordinates, add_coordinates_with_distance_matching_optimized

# Configure logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False  # Process historical data
}

dag = DAG(
    'green_taxi_monthly_pipeline',
    default_args=default_args,
    description='Monthly green taxi data ingestion and processing pipeline',
    schedule_interval='0 2 1 * *',  # Run at 2 AM on the 1st of each month
    max_active_runs=1,
    tags=['taxi', 'monthly', 'ingestion']
)

def download_taxi_zone_lookup(**context):
    """Download taxi zone lookup CSV file"""
    
    logger.info("Downloading taxi zone lookup file...")
    
    # Create directories
    os.makedirs("/tmp/taxi_data", exist_ok=True)
    
    # URL and file paths
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    local_path = "/tmp/taxi_data/taxi_zone_lookup.csv"
    
    try:
        # Download file
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Save CSV file
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"Downloaded taxi zone lookup. File size: {os.path.getsize(local_path) / 1024:.2f} KB")
        
        # Read and validate the CSV
        zones_df = pd.read_csv(local_path)
        logger.info(f"Loaded {len(zones_df)} taxi zones")
        
        # Quick validation
        expected_columns = ['LocationID', 'Borough', 'Zone', 'service_zone']
        if not all(col in zones_df.columns for col in expected_columns):
            raise ValueError(f"Missing expected columns. Found: {zones_df.columns.tolist()}")
        
        # Push path to XCom
        context['task_instance'].xcom_push(key='zone_lookup_path', value=local_path)
        
        # Return summary
        return {
            'total_zones': len(zones_df),
            'boroughs': zones_df['Borough'].nunique(),
            'file_path': local_path
        }
        
    except Exception as e:
        logger.error(f"Error downloading taxi zone lookup: {str(e)}")
        raise

def download_and_light_transform(**context):
    """Download parquet file and add coordinate columns"""
    
    # Get execution date for the previous month
    execution_date = context['execution_date']
    # Process previous month's data
    target_date = execution_date - timedelta(days=1)
    year = target_date.year
    month = target_date.month
    year_month = f"{year}-{month:02d}"
    
    logger.info(f"Processing data for: {year_month}")
    
    # Create directories
    os.makedirs("/tmp/taxi_data", exist_ok=True)
    
    # Build URL and file paths
    file_name = f"green_tripdata_{year_month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    local_raw_path = f"/tmp/taxi_data/raw_{file_name}"
    local_transformed_path = f"/tmp/taxi_data/transformed_{file_name}"
    
    try:
        # Download file
        logger.info(f"Downloading from: {url}")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        if downloaded % (1024 * 1024 * 10) == 0:  # Log every 10MB
                            logger.info(f"Download progress: {progress:.1f}%")
        
        logger.info(f"Download complete. File size: {os.path.getsize(local_raw_path) / (1024*1024):.2f} MB")
        
        # Read parquet file
        logger.info("Reading parquet file...")
        df = pd.read_parquet(local_raw_path)
        logger.info(f"Loaded {len(df):,} records")
        
        # Add processing timestamp
        df['ingestion_timestamp'] = pd.Timestamp.now()
        df['year_month'] = year_month
        
        # Add coordinates
        logger.info("Adding coordinate columns...")
        df = add_coordinates_with_distance_matching_optimized(df)
        
        # Save transformed file
        logger.info(f"Saving transformed data: {len(df):,} records")
        df.to_parquet(local_transformed_path, compression='snappy', index=False)
        
        # Push file paths to XCom for next tasks
        context['task_instance'].xcom_push(key='raw_file_path', value=local_raw_path)
        context['task_instance'].xcom_push(key='transformed_file_path', value=local_transformed_path)
        context['task_instance'].xcom_push(key='year_month', value=year_month)
        context['task_instance'].xcom_push(key='record_count', value=len(df))
        
        return {
            'year_month': year_month,
            'records': len(df),
            'file_size_mb': os.path.getsize(local_transformed_path) / (1024*1024),
            'has_zone_info': 'PUZone' in df.columns
        }
        
    except Exception as e:
        logger.error(f"Error in download_and_light_transform: {str(e)}")
        raise

def validate_data(**context):
    """Validate the downloaded and transformed data"""
    
    transformed_file_path = context['task_instance'].xcom_pull(
        task_ids='download_and_transform', 
        key='transformed_file_path'
    )
    
    logger.info(f"Validating file: {transformed_file_path}")
    
    df = pd.read_parquet(transformed_file_path)
    
    # Validation checks
    validations = {
        'has_records': len(df) > 0,
        'has_coordinates': df[['PULatitude', 'PULongitude', 'DOLatitude', 'DOLongitude']].notna().any().all(),
        'valid_dates': df['lpep_pickup_datetime'].max() > df['lpep_pickup_datetime'].min(),
        'valid_amounts': (df['total_amount'] >= df['fare_amount']).all()
    }
    
    failed_checks = [check for check, passed in validations.items() if not passed]
    
    if failed_checks:
        logger.warning(f"Some validations failed: {failed_checks}")
        # You can decide whether to fail or just warn
        # raise ValueError(f"Data validation failed for checks: {failed_checks}")
    
    logger.info("Validation complete!")
    
    # Log some statistics
    logger.info(f"Total records: {len(df):,}")
    logger.info(f"Date range: {df['lpep_pickup_datetime'].min()} to {df['lpep_pickup_datetime'].max()}")
    logger.info(f"Records with coordinates: {df[['PULatitude', 'PULongitude']].notna().all(axis=1).sum():,}")
    
    return True

# DAG Tasks
download_zone_lookup_task = PythonOperator(
    task_id='download_zone_lookup',
    python_callable=download_taxi_zone_lookup,
    provide_context=True,
    dag=dag
)

download_transform_task = PythonOperator(
    task_id='download_and_transform',
    python_callable=download_and_light_transform,
    provide_context=True,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag
)

upload_to_hdfs_task = BashOperator(
    task_id='upload_to_hdfs',
    bash_command="""
    # Get year_month from XCom
    year_month="{{ ti.xcom_pull(task_ids='download_and_transform', key='year_month') }}"
    
    # Create HDFS directories
    hdfs dfs -mkdir -p /data/green_taxi/raw/${year_month}
    hdfs dfs -mkdir -p /data/green_taxi/transformed/${year_month}
    hdfs dfs -mkdir -p /data/green_taxi/lookup
    
    # Upload main data files
    hdfs dfs -put -f /tmp/taxi_data/raw_green_tripdata_${year_month}.parquet \
        /data/green_taxi/raw/${year_month}/
    
    hdfs dfs -put -f /tmp/taxi_data/transformed_green_tripdata_${year_month}.parquet \
        /data/green_taxi/transformed/${year_month}/
    
    # Upload zone lookup file (overwrite if exists)
    hdfs dfs -put -f /tmp/taxi_data/taxi_zone_lookup.csv \
        /data/green_taxi/lookup/
    
    # Verify uploads
    echo "=== Uploaded Files ==="
    echo "Raw data:"
    hdfs dfs -ls /data/green_taxi/raw/${year_month}/
    echo ""
    echo "Transformed data:"
    hdfs dfs -ls /data/green_taxi/transformed/${year_month}/
    echo ""
    echo "Zone lookup:"
    hdfs dfs -ls /data/green_taxi/lookup/
    
    # Show file sizes
    echo ""
    echo "=== Storage Summary ==="
    hdfs dfs -du -h /data/green_taxi/
    
    # Cleanup local files
    rm -f /tmp/taxi_data/*green_tripdata_${year_month}.parquet
    rm -f /tmp/taxi_data/taxi_zone_lookup.csv
    """,
    dag=dag
)

# Add cleanup task
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command="""
    # Clean up any remaining temp files older than 7 days
    find /tmp/taxi_data -type f -mtime +7 -delete 2>/dev/null || true
    
    # Log cleanup
    echo "Cleanup completed at $(date)"
    
    # Show remaining files (if any)
    echo "Remaining files in /tmp/taxi_data:"
    ls -la /tmp/taxi_data/ 2>/dev/null || echo "Directory is clean"
    """,
    dag=dag,
    trigger_rule='all_done'  # Run regardless of upstream success/failure
)

# Set task dependencies
download_zone_lookup_task >> download_transform_task >> validate_task >> upload_to_hdfs_task >> cleanup_task

