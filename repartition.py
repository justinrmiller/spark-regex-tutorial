from pyspark.sql import SparkSession
from pyspark import StorageLevel
from config import parse_spark_config

import math

cfg = parse_spark_config()
repartition_config = cfg['repartition']

# Initialize Spark session
spark_session = SparkSession.builder \
    .master(cfg["master"]) \
    .appName(cfg["app"]) \

for keys in repartition_config["config"].keys():
    spark_session.config(keys, repartition_config["config"][keys])

spark = spark_session.getOrCreate()

# File Size for repartition targets and Input/Output file paths
file_size_gb = 11 # This is the approximate size of the input parquet files, should probably be loaded from disk
file_size_mb = file_size_gb * 1024
target_partition_size_mb = 512

input_path = "parquet/input/wikipedia_20231101.parquet"
output_path = "parquet/input/repartitioned_data"

# Load the input parquet file
df = spark.read.parquet(input_path)
df.persist(StorageLevel.MEMORY_AND_DISK)

# Calculate the number of partitions
num_partitions = math.ceil(file_size_mb / target_partition_size_mb)

# Repartition the dataframe, note this doesn't happen until the write on a subsequent line as dataframes in Spark are lazy
repartitioned_df = df.repartition(num_partitions)

# Write the repartitioned dataframe to parquet files
repartitioned_df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session, this frees resources in the Spark cluster and clearly marks the end of the job
spark.stop()