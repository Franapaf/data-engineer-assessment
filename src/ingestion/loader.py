

import os
from pyspark.sql import SparkSession

def load_data(directory: str,spark: SparkSession):
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError("No CSV files found in the specified directory.")
    combined_df = None

    for file in csv_files:
        path = os.path.join(directory, file)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    return combined_df