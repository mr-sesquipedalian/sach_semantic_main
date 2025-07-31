import os
import shutil

# Set the directory path where the folders are located
parent_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunking_batches"

# Loop through the desired range
for i in range(1001, 2001):  # 2001 is exclusive
    dir_name = f"Batch_{i}"
    dir_path = os.path.join(parent_path, dir_name)
    
    # Check if the directory exists and delete it
    if os.path.isdir(dir_path):
        shutil.rmtree(dir_path)
        print(f"Deleted directory: {dir_path}")
    else:
        print(f"Directory not found: {dir_path}")