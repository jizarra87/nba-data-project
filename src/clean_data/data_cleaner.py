from pyspark.sql.functions import col, when, avg, lag
from pyspark.sql.window import Window

def clean_data(df):
    """
    Clean and transform the raw DataFrame.
    - Keeps regular season games (SEASON_ID starts with "2") and seasons >= 22014.
    - Drops rows with missing critical values.
    - Converts WL to a binary WIN column.
    - Extracts Home/Away info from MATCHUP.
    - Fills missing performance metrics.
    - Computes rolling averages and rest days.
    """
    # Filter to keep only regular season games (SEASON_ID starting with "2")
    df_cleaned = df.filter(col("SEASON_ID").cast("string").startswith("2"))
    
    # Further filter to include only seasons from 2014 onward
    df_cleaned = df_cleaned.filter(col("SEASON_ID") >= 22014)
    
    # Drop rows with missing critical values
    df_cleaned = df_cleaned.dropna(subset=["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"])
    
    # Convert 'WL' to binary WIN column (1 for Win, 0 for Loss)
    df_cleaned = df_cleaned.withColumn("WIN", when(col("WL") == "W", 1).otherwise(0))
    
    # Extract Home/Away indicator from MATCHUP column
    df_cleaned = df_cleaned.withColumn("HOME_AWAY", when(col("MATCHUP").like("%vs%"), "H").otherwise("A"))
    
    # Fill missing values for performance metrics
    df_cleaned = df_cleaned.fillna({
        "FG_PCT": 0, 
        "FG3_PCT": 0, 
        "FT_PCT": 0, 
        "REB": 0, 
        "AST": 0, 
        "STL": 0, 
        "BLK": 0, 
        "TOV": 0
    })
    
    # Define window for rolling averages (last 5 games per TEAM_ID)
    window_spec = Window.partitionBy("TEAM_ID").orderBy(col("GAME_DATE"))
    
    # Compute rolling averages for key stats (PTS, AST, REB, FG_PCT)
    for stat in ["PTS", "AST", "REB", "FG_PCT"]:
        df_cleaned = df_cleaned.withColumn(f"{stat}_LAST5", avg(col(stat)).over(window_spec.rowsBetween(-5, -1)))
    
    # Compute rest days between consecutive games (casting GAME_DATE to long for subtraction)
    #df_cleaned = df_cleaned.withColumn("REST_DAYS", col("GAME_DATE").cast("long") - lag("GAME_DATE", 1).over(window_spec).cast("long"))
    
    return df_cleaned
