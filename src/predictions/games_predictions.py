import pandas as pd
import re
import joblib

df = pd.read_csv('/home/ji/NBA_Project/data/data_28_02_2025.csv')
df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])

away = input('away')
home = input('home')
df_away = df[df['TEAM_ABBREVIATION']== away]
# Sort by GAME_DATE in descending order
df_away.sort_values(by='GAME_DATE', ascending=False, inplace = True)
df_away = df_away.head()

df_home = df[df['TEAM_ABBREVIATION']== home]
# Sort by GAME_DATE in descending order
df_home.sort_values(by='GAME_DATE', ascending=False, inplace = True)
df_home = df_home.head()

# Define the numeric columns you want to average
numeric_columns = [
    "PTS", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST",
    "STL", "BLK", "TOV", "PF", "PLUS_MINUS"
]

# Compute the averages for df_away and df_home for the numeric columns
avg_away = df_away[numeric_columns].mean()
avg_home = df_home[numeric_columns].mean()

# Rename the indices (i.e. column names) by adding suffixes
avg_away.index = [f"{col}_LAST5_away" for col in avg_away.index]
avg_home.index = [f"{col}_LAST5_home" for col in avg_home.index]


# Combine the two Series and add the win/loss average as "WL_away"
combined = pd.concat([avg_away, avg_home])

# Create a new DataFrame with just one row from the combined Series
result_df = pd.DataFrame([combined])

# Optionally, display the result
print(result_df)

# Define the path to your saved model (update the filename as needed)
model_filename = "/home/ji/NBA_Project/src/models/model_logistic_25_02_2025.pkl"

# Load the saved model
model = joblib.load(model_filename)
print("Model loaded successfully.")

predictions = model.predict(result_df)
predicted_probs = model.predict_proba(result_df)[:, 1]

print(away, home, predictions, predicted_probs)