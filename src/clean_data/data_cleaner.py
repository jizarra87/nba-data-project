import pandas as pd
from pyspark.sql import SparkSession

def extract_rival(matchup):
    if pd.isnull(matchup):
        return None
    # For home games with "vs"
    if "vs" in matchup:
        rival = matchup.split("vs")[-1].strip()
        # Remove any leading punctuation (like a period) and extra whitespace
        rival = re.sub(r"^\W+", "", rival)
        return rival
    # For away games with "@"
    elif "@" in matchup:
        rival = matchup.split("@")[-1].strip()
        return rival
    else:
        return None

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
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    df = df[df['GAME_DATE'] >='2014-08-01']

    df = df.dropna()

    df['WL'] = df['WL'].str.strip()

    df['WL'] = df['WL'].map({'W':1,'L':0})

    df['HOME_AWAY'] = df['MATCHUP'].apply(lambda x: 'A' if '@' in x else 'H')

    df["RIVAL"] = df["MATCHUP"].apply(extract_rival)

    # Convert GAME_DATE to datetime and sort by TEAM_ID and GAME_DATE
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    df = df.sort_values(['TEAM_ID', 'GAME_DATE'])

    # List of stat columns for which to compute last 5 games average
    stat_columns = [
        "PTS", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
        "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST",
        "STL", "BLK", "TOV", "PF", "PLUS_MINUS"
    ]

    # For each column, compute the rolling average of the last 5 games (excluding the current game) 
    for col_name in stat_columns:
        new_col = col_name + "_LAST5"
        df[new_col] = (df.groupby("TEAM_ID")[col_name]
                         .apply(lambda x: x.shift(1).rolling(window=5, min_periods=1).mean())
                         .reset_index(level=0, drop=True))

    df = df.dropna()

    df = df[df['SEASON_ID'].astype(str).str.startswith('2')]

    df_away = df[df['HOME_AWAY'] == 'A']
    df_home = df[df['HOME_AWAY'] == 'H']
    
    df_joined = df_away.merge(
    df_home, 
    left_on=["GAME_ID"], 
    right_on=["GAME_ID"],
    suffixes=('_away', '_home')
)
    stat_columns_away = [f"{col}_LAST5_away" for col in stat_columns]

    stat_columns_home = [f"{col}_LAST5_home" for col in stat_columns]

    columns = stat_columns_away + stat_columns_home + ['WL_away']

    df_cleaned = df_joined[columns]

    df_cleaned = df_cleaned.toPandas()


    return df_cleaned
