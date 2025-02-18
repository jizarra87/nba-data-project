from pyspark.sql import SparkSession

# Function to load data into CSV
def load_data_to_csv(dataframe, output_path, spark):
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(dataframe)
    # Write to CSV
    spark_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
