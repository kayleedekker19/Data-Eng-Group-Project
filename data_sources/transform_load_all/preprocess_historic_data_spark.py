# This script pre-processed the data that has been retrieved via scraping
# The first function add the necessary airport code column and transforms the Date column
# The second function processes all the parquet files in this way and saves within our local directory
# The files are saved as parquet files again

# Load libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, to_date


def process_parquet_file(spark, file_path):
    """
    Process a single Parquet file to add airport_code and adjust the Date column.

    Parameters:
    - spark: SparkSession instance.
    - file_path: The path to the Parquet file to process.

    Returns:
    - A Spark DataFrame with the processed data.
    """
    file_name = os.path.basename(file_path)
    file_name_parts = file_name.split("_")  # ['ACK', '2023-1.parquet']
    airport_code = file_name_parts[0]  # Extracted airport code
    year_month = file_name_parts[1].split(".")[0]  # Extracted year and month

    df = spark.read.parquet(file_path)
    df = df.withColumn("airport_code", lit(airport_code))
    df = df.withColumn("Date", to_date(concat(lit(year_month + "-"), col("Date")), "yyyy-M-d"))
    return df


def process_all_files(directory):
    """
    Process all Parquet files in the specified directory.

    Parameters:
    - directory: The directory containing Parquet files to process.

    Returns:
    - A Spark DataFrame containing the appended data from all files.
    """
    spark = SparkSession.builder.appName("ParquetDataAggregation").getOrCreate()
    dataframes = []

    for file in os.listdir(directory):
        if file.endswith(".parquet"):
            file_path = os.path.join(directory, file)
            df = process_parquet_file(spark, file_path)
            dataframes.append(df)

    # Combine all DataFrames into one
    if dataframes:
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = final_df.union(df)
        return final_df
    else:
        return spark.createDataFrame([], schema=None)  # Return empty DataFrame if no files were processed


if __name__ == "__main__":
    directory = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/historic_data"
    combined_df = process_all_files(directory)
    combined_df.show()

    # Print the length of the DataFrame
    print(f"The length of the DataFrame is: {combined_df.count()}")

    # Define the path where you want to save the combined DataFrame
    save_path = "/data_sources/combined_weather_data.parquet"

    # Save the DataFrame as Parquet
    combined_df.write.mode("overwrite").parquet(save_path)
    print(f"DataFrame saved to {save_path}")