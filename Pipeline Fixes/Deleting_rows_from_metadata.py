import pandas as pd

# Assuming 'combined_df' has a column named 'batch' with values like 'batch_1001'

# Create a set of batch names to remove
batches_to_remove = {f"batch_{i}" for i in range(1001, 2001)}

# Filter out those rows
combined_df = pd.read_csv("/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv")
combined_df = combined_df[~combined_df["batch"].isin(batches_to_remove)]
combined_df.to_csv("/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv", index = False)