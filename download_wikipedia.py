from datasets import load_dataset
import pandas as pd

# Load the dataset, note this data set is quite large so make sure you have enough free disk space
ds = load_dataset("wikimedia/wikipedia", "20231101.en")

# Convert the dataset to a Pandas DataFrame
df = pd.DataFrame(ds['train'])

# Save the DataFrame as a Parquet file
df.to_parquet('parquet/input/wikipedia_20231101.parquet', engine='fastparquet')
