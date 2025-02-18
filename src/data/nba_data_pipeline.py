import json
from pyspark.sql import SparkSession
from fetch_data import fetch_nba_teams, nba_games
from load_data import load_data_to_csv

# Load configuration from JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Extract output path from JSON
output_path = config.get("output_path", "/default/path")  # Fallback to a default if missing

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NBA_Data_Pipeline") \
    .getOrCreate()

# Fetch NBA teams and game data
teams_list = fetch_nba_teams()
games_data = nba_games(teams_list)

# Load data
load_data_to_csv(games_data, output_path, spark)

# Stop Spark session
spark.stop()
