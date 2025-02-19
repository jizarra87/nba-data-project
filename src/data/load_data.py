from pyspark.sql import SparkSession
from datetime import datetime
import os
import shutil


# Function to load data into CSV
def load_data_to_csv(dataframe, output_dir, spark):
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(dataframe)

    # Generate filename with date format
    date_str = datetime.now().strftime("%d_%m_%Y")
    file_name = f"data_{date_str}.csv"

    # Temporary path to write CSV (since PySpark writes multiple part files)
    temp_output_path = os.path.join(output_dir, "temp_output")

    # Write to CSV
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

    # Rename the output file
    for file in os.listdir(temp_output_path):
        if file.startswith("part-") and file.endswith(".csv"):
            os.rename(os.path.join(temp_output_path, file), os.path.join(output_dir, file_name))

    # Clean up temporary directory
    shutil.rmtree(temp_output_path)

# Example usage
# spark = SparkSession.builder.appName("CSV Export").getOrCreate()
# load_data_to_csv(your_dataframe, "/your/output/path", spark)
