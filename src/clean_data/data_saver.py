import os
import shutil
from datetime import datetime

def save_cleaned_data(df, data_dir: str):
    """
    Save the cleaned DataFrame to a single CSV file.
    The file will be named data_cleaned_dd_mm_yyyy.csv in the specified data directory.
    """
    # Coalesce to one partition to generate a single output file
    df_single = df.coalesce(1)
    
    # Generate today's date string in dd_mm_yyyy format
    today_str = datetime.today().strftime("%d_%m_%Y")
    
    # Define temporary directory and final file path
    temp_dir = os.path.join(data_dir, f"temp_data_cleaned_{today_str}")
    final_file = os.path.join(data_dir, f"data_cleaned_{today_str}.csv")
    
    # Write DataFrame to temporary directory with header
    df_single.write.mode("overwrite").option("header", "true").csv(temp_dir)
    
    # Find the CSV part file in the temporary directory
    temp_files = os.listdir(temp_dir)
    csv_file = [f for f in temp_files if f.endswith(".csv")][0]
    temp_csv_path = os.path.join(temp_dir, csv_file)
    
    # Move the CSV file to the final destination with desired name
    shutil.move(temp_csv_path, final_file)
    
    # Remove the temporary directory
    shutil.rmtree(temp_dir)
    
    return final_file
