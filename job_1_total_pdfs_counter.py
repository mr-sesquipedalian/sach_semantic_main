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
#from IPython.display import display, HTML


'''pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)'''

# sys.argv[1] = Raw file folder like "/projectnb/sachgrp/prathamk/CaseLaw/USA/Massachusetts/" 
# sys.argv[2] = Output directory path like "/projectnb/sachgrp/apgupta/Case Law Data/USA/Massachusetts"

# To calculate the total number of PDFs in the after breaking the raw pdf
def count_pdfs_in_folders(test_dir: str):
    """
    Count the number of PDF files in each folder within the test directory.

    Args:
        test_dir (str): Path to the root /Test directory.
    """
    test_path = Path(test_dir)
    
    if not test_path.exists() or not test_path.is_dir():
        print(f"Error: Directory {test_dir} does not exist or is not a directory")
        return

    total = 0
    # Iterate through all subdirectories in the test directory
    for folder in test_path.iterdir():
        if folder.is_dir():
            # Count files with .pdf extension
            pdf_count = len(list(folder.glob("*.pdf")))
            total += pdf_count
            print(f"Folder: {folder.name}, PDF count: {pdf_count}")
    print(f"In total this directory has {total} pdfs")
    

# Example usage
if __name__ == "__main__":
    #test_directory = "/projectnb/sachgrp/apgupta/Case Law Data/USA/Massachusetts"   # Pass the path for which you want to calculate the number of PDFs
    count_pdfs_in_folders(sys.argv[2])