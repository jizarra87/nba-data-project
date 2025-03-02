import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score
from datetime import datetime
import joblib

# Get today's date in dd_mm_yyyy format
today_str = datetime.today().strftime("%d_%m_%Y")

# Construct the filename
filename = "/home/ji/NBA_Project/data/"+f"data_cleaned_{today_str}.csv"

df = pd.read_csv(filename)

columns = [
    "PTS_LAST5_away",
    "FGM_LAST5_away",
    "FGA_LAST5_away",
    "FG_PCT_LAST5_away",
    "FG3M_LAST5_away",
    "FG3A_LAST5_away",
    "FG3_PCT_LAST5_away",
    "FTM_LAST5_away",
    "FTA_LAST5_away",
    "FT_PCT_LAST5_away",
    "OREB_LAST5_away",
    "DREB_LAST5_away",
    "REB_LAST5_away",
    "AST_LAST5_away",
    "STL_LAST5_away",
    "BLK_LAST5_away",
    "TOV_LAST5_away",
    "PF_LAST5_away",
    "PLUS_MINUS_LAST5_away",
    "PTS_LAST5_home",
    "FGM_LAST5_home",
    "FGA_LAST5_home",
    "FG_PCT_LAST5_home",
    "FG3M_LAST5_home",
    "FG3A_LAST5_home",
    "FG3_PCT_LAST5_home",
    "FTM_LAST5_home",
    "FTA_LAST5_home",
    "FT_PCT_LAST5_home",
    "OREB_LAST5_home",
    "DREB_LAST5_home",
    "REB_LAST5_home",
    "AST_LAST5_home",
    "STL_LAST5_home",
    "BLK_LAST5_home",
    "TOV_LAST5_home",
    "PF_LAST5_home",
    "PLUS_MINUS_LAST5_home"
    ]


X = df[columns]
y = df['WL_away']


# Split data into training and testing sets (70/30 split)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Create and train the logistic regression model
model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

# Generate predictions and predicted probabilities on the test set
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

# Evaluate the model using AUC (Area Under the ROC Curve)
auc = roc_auc_score(y_test, y_prob)
print(f"Model AUC: {auc}")

# Optionally, display a few prediction results
results = X_test.copy()
results['WIN'] = y_test
results['Prediction'] = y_pred
results['Probability'] = y_prob
print(results.head())


# Save the trained model to a file
model_filename = "/home/ji/NBA_Project/src/models/model_logistic_" + today_str + ".pkl"
joblib.dump(model, model_filename)
print(f"Model saved to: {model_filename}")