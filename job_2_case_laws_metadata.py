import os
import json
import pandas as pd
import time
import uuid
import shutil
import torch
import pickle
import fitz 
import sys
from pathlib import Path
from PyPDF2 import PdfReader, PdfWriter
from typing import List, Tuple, Optional
from collections import defaultdict
from langchain.document_loaders import PyMuPDFLoader
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain.vectorstores import FAISS


def combine_jsons_to_dataframe(raw_data_directory: str):
    """
    Combine all JSON files in subfolders of the test directory into a single pandas DataFrame.

    Args:
        test_dir (str): Path to the root /Test directory.

    Returns:
        pd.DataFrame: A DataFrame containing all data from the JSON files, with an additional
                      column for the source JSON file.
    """
    test_path = Path(raw_data_directory)
    
    if not test_path.exists() or not test_path.is_dir():
        print(f"Error: Directory {raw_data_directory} does not exist or is not a directory")
        #return pd.DataFrame()
    
    # Initialize an empty list to store data from all JSON files
    all_data = []
    checkpoint = ""
    
    # Iterate through all subdirectories in the test directory
    for folder in test_path.iterdir():
        if folder.is_dir():
            print(f"Processing folder: {folder.name}")
            # Iterate through all JSON files in the folder
            for json_file in folder.glob("*.json"):
                try:
                    with open(json_file, "r") as f:
                        data = json.load(f)
                    
                    if not isinstance(data, list):
                        print(f"Error: JSON file {json_file} does not contain a list of dictionaries")
                        continue

                    temp = str(json_file).split('/')
                    timestamp = time.time()
                    # Add source file information to each dictionary
                    for entry in data:
                        entry['source_file'] = str(json_file).replace(".json",".pdf")
                        entry['source_folder'] = temp[-2]
                        case_name = entry["name_abbreviation"].replace(" ","_").replace("/","_")
                        entry['real_path'] = "/projectnb/sachgrp/apgupta/Case Law Data/"+temp[-4]+"/"+temp[-3]+"/"+temp[-2]+"/"+case_name+"_"+str(entry["id"])+".pdf"
                        entry['metadata_insert_timestamp'] = timestamp
                        entry['file_key'] = uuid.uuid4()
                        entry['is_chunked'] = 0
                        entry['case_year'] = entry["decision_date"].split("-")[0]
                        entry["country"] = temp[-4]
                        entry["state"] = temp[-3]
                        entry["file_primary_key"] = case_name+"_"+str(entry["id"])
                    all_data.extend(data)
                    checkpoint = temp[-4]+"_"+temp[-3]+"_"+str(timestamp)
                
                except FileNotFoundError:
                    print(f"Error: File {json_file} not found")
                except json.JSONDecodeError:
                    print(f"Error: Invalid JSON format in {json_file}")
                except Exception as e:
                    print(f"Error processing {json_file}: {str(e)}")
    
    if not all_data:
        print("No valid JSON data found")
        #return pd.DataFrame()
    
    # Create DataFrame from the combined data
    df = pd.DataFrame(all_data)
    print(f"Created DataFrame with {len(df)} rows")
    primary_key = df.pop('file_primary_key')
    df.insert(0, 'file_primary_key', primary_key)
    df.to_csv(f"/projectnb/sachgrp/apgupta/Case Law Data/cases_metadata_checkpoints/{checkpoint}.csv", index = False)
    print("Checkpoint Saved")
    df.to_csv("/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv", mode='a', index=False, header = False)
    print("Combined file updated")
    # return df

if __name__ == "__main__":
    #raw_data_directory = "/projectnb/sachgrp/prathamk/CaseLaw/USA/Maine/"
    combine_jsons_to_dataframe(sys.argv[1])