import os
from pyspark.sql import SparkSession

def load_latest_data(data_dir: str):
    """
    Load the latest CSV file from the given directory.
    Assumes files are named in the format: data_dd_mm_yyyy.csv
    """
    # List files with the expected format
    files = [f for f in os.listdir(data_dir) if f.startswith("data_") and f.endswith(".csv")]
    # Get the latest file by sorting filenames in reverse order
    latest_file = sorted(files, reverse=True)[0]
    latest_file_path = os.path.join(data_dir, latest_file)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBA Data Cleaning").getOrCreate()
    # Load CSV into DataFrame
    df = spark.read.csv(latest_file_path, header=True, inferSchema=True)
    return df
