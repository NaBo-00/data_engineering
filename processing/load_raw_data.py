from pyspark.sql import SparkSession
import sys
import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def load_csv_to_raw_db(file_name):
    # Initialize Spark session
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Load CSV to Raw DB") \
        .getOrCreate()
    
    try:
        # Load CSV file into DataFrame
        df = spark.read.csv(f"/data/{file_name}.csv", header=True, inferSchema=True)

        # Write DataFrame to raw database PostgreSQL
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/taxi_data_raw") \
            .option("dbtable", f"{file_name}_raw") \
            .option("user", os.getenv('POSTGRES_USER')) \
            .option("password", os.getenv('POSTGRES_PASSWORD')) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print(f"Success - Raw data loaded and stored in table '{file_name}_raw'.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: load_raw_data.py {file_name}")
        sys.exit(-1)
    file_name = sys.argv[1]
    load_csv_to_raw_db(file_name)
    print("Success - Load raw data finished")