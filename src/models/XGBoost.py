import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_recall_curve, confusion_matrix
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from sklearn.feature_selection import RFE
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from datetime import datetime
import joblib
import numpy as np

# Get today's date in dd_mm_yyyy format
today_str = datetime.today().strftime("%d_%m_%Y")

# Load data
filename = f"/home/ji/NBA_Project/data/data_cleaned_{today_str}.csv"
df = pd.read_csv(filename)

# Define feature columns and target variable
columns = [
    "PTS_LAST5_away", "FGM_LAST5_away", "FGA_LAST5_away", "FG_PCT_LAST5_away",
    "FG3M_LAST5_away", "FG3A_LAST5_away", "FG3_PCT_LAST5_away", "FTM_LAST5_away",
    "FTA_LAST5_away", "FT_PCT_LAST5_away", "OREB_LAST5_away", "DREB_LAST5_away",
    "REB_LAST5_away", "AST_LAST5_away", "STL_LAST5_away", "BLK_LAST5_away",
    "TOV_LAST5_away", "PF_LAST5_away", "PLUS_MINUS_LAST5_away", "PTS_LAST5_home",
    "FGM_LAST5_home", "FGA_LAST5_home", "FG_PCT_LAST5_home", "FG3M_LAST5_home",
    "FG3A_LAST5_home", "FG3_PCT_LAST5_home", "FTM_LAST5_home", "FTA_LAST5_home",
    "FT_PCT_LAST5_home", "OREB_LAST5_home", "DREB_LAST5_home", "REB_LAST5_home",
    "AST_LAST5_home", "STL_LAST5_home", "BLK_LAST5_home", "TOV_LAST5_home",
    "PF_LAST5_home", "PLUS_MINUS_LAST5_home"
]

X = df[columns]
y = df['WL_away']

# Split data (70% training, 30% testing)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Feature Scaling
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Handle Class Imbalance with SMOTE & Dynamic Under-sampling
print("Original class distribution:\n", y_train.value_counts())
smote = SMOTE(sampling_strategy='auto', random_state=42)
X_train_balanced, y_train_balanced = smote.fit_resample(X_train_scaled, y_train)

# Dynamically adjust the undersampling ratio
minority_class_count = sum(y_train_balanced == 1)
majority_class_count = sum(y_train_balanced == 0)
new_ratio = minority_class_count / majority_class_count if majority_class_count > minority_class_count else "auto"

under_sampler = RandomUnderSampler(sampling_strategy=new_ratio, random_state=42)
X_train_balanced, y_train_balanced = under_sampler.fit_resample(X_train_balanced, y_train_balanced)
print("After resampling class distribution:\n", pd.Series(y_train_balanced).value_counts())

# Feature Selection using RFE (Increased to 20 features)
log_model = LogisticRegression(max_iter=1000)
selector = RFE(log_model, n_features_to_select=20, step=1)
selector.fit(X_train_balanced, y_train_balanced)
selected_features = X_train.columns[selector.support_]
print("Selected Features:", selected_features)

# Train Logistic Regression Model
log_model.fit(X_train_balanced[:, selector.support_], y_train_balanced)
y_prob = log_model.predict_proba(X_test_scaled[:, selector.support_])[:, 1]

# Adjust Decision Threshold
precisions, recalls, thresholds = precision_recall_curve(y_test, y_prob)
best_threshold = thresholds[np.argmax(precisions + recalls)]
y_pred_adjusted = (y_prob >= best_threshold).astype(int)

# Evaluate Model Performance
auc = roc_auc_score(y_test, y_prob)
conf_matrix = confusion_matrix(y_test, y_pred_adjusted)
print(f"Logistic Regression AUC: {auc}")
print("Confusion Matrix:\n", conf_matrix)

# Train XGBoost Model for Comparison
xgb_model = XGBClassifier(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
xgb_model.fit(X_train_balanced, y_train_balanced)
y_prob_xgb = xgb_model.predict_proba(X_test_scaled)[:, 1]
y_pred_xgb = (y_prob_xgb >= best_threshold).astype(int)
auc_xgb = roc_auc_score(y_test, y_prob_xgb)
print(f"XGBoost AUC: {auc_xgb}")

# Save Logistic Regression Model
model_filename = f"/home/ji/NBA_Project/src/models/model_logistic_v3_{today_str}.pkl"
joblib.dump(log_model, model_filename)
print(f"Logistic Regression Model saved to: {model_filename}")

# Save XGBoost Model
xgb_model_filename = f"/home/ji/NBA_Project/src/models/model_xgb_{today_str}.pkl"
joblib.dump(xgb_model, xgb_model_filename)
print(f"XGBoost Model saved to: {xgb_model_filename}")
