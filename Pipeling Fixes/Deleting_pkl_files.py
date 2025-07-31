import os

# Set the directory path where the files are located
folder_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunked_pickle_files"

# Loop through the desired range
for i in range(1001, 2001):  # start pkl file to end pkl file + 1
    filename = f"Batch_{i}"
    file_path = os.path.join(folder_path, filename)
    
    # Check if the file exists and delete it
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted: {file_path}")
    else:
        print(f"Not found: {file_path}")