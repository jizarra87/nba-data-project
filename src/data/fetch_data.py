import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder

#branch merge validation. 
# Function to fetch NBA teams
def fetch_nba_teams():
    nba_teams = teams.get_teams()
    teams_df = pd.DataFrame(nba_teams)
    return teams_df['id'].tolist()

# Function to fetch NBA games for each team
def nba_games(team_ids):
    dataframes = []
    for team_id in team_ids:
        gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
        games = gamefinder.get_data_frames()[0]  # Get the DataFrame for this team
        dataframes.append(games)

    # Concatenate all DataFrames into a single DataFrame
    final_df = pd.concat(dataframes, ignore_index=True)
    return final_df
