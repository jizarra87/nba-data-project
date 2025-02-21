import os
from src.data_loader import load_latest_data
from src.data_cleaner import clean_data
from src.data_saver import save_cleaned_data

def main():
    # Define the data directory (adjust if necessary)
    data_dir = "/home/ji/NBA_Project/data"
    
    # Load the latest raw data
    df_raw = load_latest_data(data_dir)
    
    # Clean and transform the data
    df_cleaned = clean_data(df_raw)
    
    # Optionally show some of the cleaned data
    df_cleaned.show(10)
    
    # Save the cleaned data to CSV
    final_path = save_cleaned_data(df_cleaned, data_dir)
    print(f"Cleaned data saved to: {final_path}")

if __name__ == "__main__":
    main()
