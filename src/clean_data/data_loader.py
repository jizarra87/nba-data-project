import os
from pyspark.sql import SparkSession
import re 

def load_latest_data(data_dir: str):
    """
    Load the latest CSV file from the given directory.
    Assumes files are named in the format: data_dd_mm_yyyy.csv
    """
    pattern = re.compile(r"^data_\d{2}_\d{2}_\d{4}\.csv$")
    # List files with the expected format
    files = [f for f in os.listdir(data_dir) if pattern.match(f)]
    # Get the latest file by sorting filenames in reverse order
    latest_file = sorted(files, reverse=True)[0]
    latest_file_path = os.path.join(data_dir, latest_file)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBA Data Cleaning").getOrCreate()
    # Load CSV into DataFrame
    df = spark.read.csv(latest_file_path, header=True, inferSchema=True)
    return df


