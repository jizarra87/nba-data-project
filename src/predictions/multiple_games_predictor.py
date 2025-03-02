import pandas as pd
import re
import joblib
from results_writer import ResultsWriter
from datetime import datetime
import os

today_str = datetime.today().strftime("%d_%m_%Y")
data_results = f"/home/ji/NBA_Project/results/predictions_{today_str}.csv"
data_file = f"/home/ji/NBA_Project/data/data_{today_str}.csv"
writer = ResultsWriter(data_results)


df = pd.read_csv(data_file)
df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])


# Define the numeric columns you want to average
numeric_columns = [
    "PTS", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT", "OREB", "DREB", "REB", "AST",
    "STL", "BLK", "TOV", "PF", "PLUS_MINUS"
]

print('Data imported')

# Define the path to your saved model (update the filename as needed)
model_filename = "/home/ji/NBA_Project/src/models/model_logistic_25_02_2025.pkl"

# Load the saved model
model = joblib.load(model_filename)
print("Model loaded successfully.")

teams_away = []  # List to store game names
teams_home = []
results_list = []
prob_list = []

while True:
    away = input("Enter a away (or type 'escape' to finish): ").strip()
    

    if away.lower() == "escape":
        break
    #games.append(user_input)
    home = input("Enter a home: ").strip()
    teams_away.append(away)
    teams_home.append(home)

print("Games list:", teams_away, teams_home)



for i in range(0,len(teams_away)):
    df_away = df[df['TEAM_ABBREVIATION']== teams_away[i]]
    # Sort by GAME_DATE in descending order
    df_away.sort_values(by='GAME_DATE', ascending=False, inplace = True)
    df_away = df_away.head()

    df_home = df[df['TEAM_ABBREVIATION']== teams_home[i]]
    # Sort by GAME_DATE in descending order
    df_home.sort_values(by='GAME_DATE', ascending=False, inplace = True)
    df_home = df_home.head()


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
    #print(result_df)



    predictions = model.predict(result_df)
    results_list.append(predictions)
    predicted_probs = model.predict_proba(result_df)[:, 1]
    prob_list.append(predicted_probs)
    print(teams_away[i])

for i in range(0, len(teams_away)):
    writer_result = []
    writer_result.append(str(teams_away[i]))
    writer_result.append(str(teams_home[i]))
    writer_result.append(str(results_list[i][0]))  
    writer_result.append(str(prob_list[i][0]))     
    line = ",".join(writer_result)
    writer.write_line(line)
    print(teams_away[i], teams_home[i], results_list[i], prob_list[i])

