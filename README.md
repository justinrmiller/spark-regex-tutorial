Offline Feature Extraction with Spark and Regular Expressions
----

This project demonstrates the scalability of Spark using the Wikipedia dataset and it's ability to perform off-line feature extract. This allows the user of the scripts to find articles that are related to the supplied RegExs for use in training machine learning models.

The dataset can be found here:
https://huggingface.co/datasets/wikimedia/wikipedia

Three scripts are provided:

1. `download_wikipedia.py` - This script downloads the English Wikipedia dataset as a single file.
2. `repartition.py` - This script uses Spark to repartition the data into convenient ~512 MB Parquet files w/Snappy compression.
3. `scan.py` - This script uses Spark to add a regular expression column to the data set, filter out rows the lack any matches and saves the data to disk.

Note: 

The configuration of the driver/executors are hardcoded into the repartition and scan scripts. This should likely be driven by a configuration file instead. 

Future Enhancements:

1. Remove unnecessary columns to save on storage space.
2. Replace UDF with a flatMap to reduce time associated with `filter` step.
3. Source a larger dataset and benchmark the performance of the scripts.
4. Load driver/executor settings from a configuration file instead of hard-coding.
