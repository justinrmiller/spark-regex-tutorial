from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, size
from pyspark.sql.types import StringType, ArrayType
from pyspark import StorageLevel
from utils import regex_patterns
from config import parse_spark_config

import re

cfg = parse_spark_config()
scan_config = cfg['scan']

spark_session = SparkSession.builder \
    .master(scan_config["master"]) \
    .appName(scan_config["app"]) \

for keys in scan_config["config"].keys():
    spark_session.config(keys, scan_config["config"][keys])

spark = spark_session.getOrCreate()

# Compile the combined regex patterns, this significantly speeds up the process and (?i) makes it case insensitive
compiled_patterns = {category: re.compile("(?i)" + "|".join(patterns)) for category, patterns in regex_patterns.items()}

# Define UDF to classify text based on compiled regex patterns
def classify_text(text):
    matched_categories = []
    for category, pattern in compiled_patterns.items():
        if pattern.search(text):
            matched_categories.append(category)
    return matched_categories if matched_categories else None

classify_text_udf = udf(classify_text, ArrayType(StringType()))

# Load the input parquet file
input_df = spark.read.parquet("parquet/input/repartitioned_data/*.parquet")

# Ensure that in limited memory environments the job can run successfully
input_df.persist(StorageLevel.MEMORY_AND_DISK)

# Apply the classification UDF to the text column
output_df = input_df.withColumn("categories", classify_text_udf(col("text")))

# Filter out rows with no categories
filtered_df = output_df.filter(col("categories").isNotNull() & (size(col("categories")) > 0))

# Select the required columns
result_df = filtered_df.select("title", "text", "categories")

# Save the result to a parquet file
result_df.write.mode("overwrite").parquet("parquet/output/results")

# Stop the Spark session
spark.stop()