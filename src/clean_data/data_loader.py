import os
from pyspark.sql import SparkSession
import re 
from datetime import datetime


def load_latest_data(data_dir: str):
    """
    Load the latest CSV file from the given directory.
    Assumes files are named in the format: data_dd_mm_yyyy.csv
    """
    today_str = datetime.today().strftime("%d_%m_%Y")
    filename = f"data_{today_str}.csv"

    latest_file_path = os.path.join(data_dir, filename)
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("NBA Data Cleaning").getOrCreate()
    # Load CSV into DataFrame
    df = spark.read.csv(latest_file_path, header=True, inferSchema=True)
    return df


