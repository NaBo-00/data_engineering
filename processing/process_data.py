from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col, to_timestamp
import sys
import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def process_data_and_store(file_name):
    # Initialize Spark session
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Process Data and Store in Processed DB") \
        .getOrCreate()

    try:
        raw_postgres_url = "jdbc:postgresql://postgres:5432/taxi_data_raw"
        processed_postgres_url = "jdbc:postgresql://postgres:5432/taxi_data_processed"
        postgres_properties = {
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'driver': 'org.postgresql.Driver'
        }

        # Read data from raw PostgreSQL database
        df = spark.read.jdbc(
            url=raw_postgres_url,
            table=f"{file_name}_raw",
            properties=postgres_properties
        )

        # Data processing
        df_modified = df.drop("store_and_fwd_flag") \
            .withColumn('VendorID', col('VendorID').cast(IntegerType())) \
            .withColumn('tpep_pickup_datetime', to_timestamp(col('tpep_pickup_datetime'), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('tpep_dropoff_datetime', to_timestamp(col('tpep_dropoff_datetime'), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('passenger_count', col('passenger_count').cast(IntegerType())) \
            .withColumn('trip_distance', col('trip_distance').cast(FloatType())) \
            .withColumn('RatecodeID', col('RatecodeID').cast(IntegerType())) \
            .withColumn('PULocationID', col('PULocationID').cast(IntegerType())) \
            .withColumn('DOLocationID', col('DOLocationID').cast(IntegerType())) \
            .withColumn('payment_type', col('payment_type').cast(IntegerType())) \
            .withColumn('fare_amount', col('fare_amount').cast(FloatType())) \
            .withColumn('extra', col('extra').cast(FloatType())) \
            .withColumn('mta_tax', col('mta_tax').cast(FloatType())) \
            .withColumn('tip_amount', col('tip_amount').cast(FloatType())) \
            .withColumn('tolls_amount', col('tolls_amount').cast(FloatType())) \
            .withColumn('improvement_surcharge', col('improvement_surcharge').cast(FloatType())) \
            .withColumn('total_amount', col('total_amount').cast(FloatType())) \
            .withColumn('congestion_surcharge', col('congestion_surcharge').cast(FloatType())) \
            .withColumn('Airport_fee', col('Airport_fee').cast(FloatType()))

        df_cleaned = df_modified.filter(
            (col('passenger_count') > 0) & (col('passenger_count') < 11) &
            (col('trip_distance') > 0) &
            (col('fare_amount') >= 0) &
            (col('extra') >= 0) &
            (col('mta_tax') >= 0) &
            (col('tip_amount') >= 0) &
            (col('tolls_amount') >= 0) &
            (col('improvement_surcharge') >= 0) &
            (col('total_amount') > 0) & (col('total_amount') < 1500) &
            (col('congestion_surcharge') >= 0) &
            (col('Airport_fee') >= 0)
        )

        # Write processed data to processed PostgreSQL database
        df_cleaned.write.jdbc(
            url=processed_postgres_url,
            table=f"{file_name}_processed",
            mode="overwrite",
            properties=postgres_properties
        )

        print(f"Success - Processed data stored in table '{file_name}_processed'.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: process_data.py {file_name}")
        sys.exit(-1)
    file_name = sys.argv[1]
    process_data_and_store(file_name)
    print("Success - Data processing finished")
