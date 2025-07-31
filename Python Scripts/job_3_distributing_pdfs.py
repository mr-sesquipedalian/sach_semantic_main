import os
import pandas as pd
import shutil
from typing import List
from collections import defaultdict
from langchain.document_loaders import PyMuPDFLoader



'''pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)'''

# sys.argv[1] = Raw file folder like "/projectnb/sachgrp/prathamk/CaseLaw/USA/Massachusetts/" 
# sys.argv[2] = Output directory path like "/projectnb/sachgrp/apgupta/Case Law Data/USA/Massachusetts"

def get_pdf_page_count(pdf_path: str):
    """
    Get the number of pages in a PDF file.

    Args:
        pdf_path (str): Path to the PDF file.

    Returns:
        int: Number of pages in the PDF.
    """
    try:
        loader = PyMuPDFLoader(pdf_path)
        documents = loader.load()
        return len(documents)
    except Exception as e:
        print(f"Error reading page count for {pdf_path}: {str(e)}")
        return 0

def distribute_pdfs(pdf_paths: List[str], bucketNum: int = 10):
    """
    Distribute PDFs into buckets based on page count for balanced processing.

    Args:
        pdf_paths (List[str]): List of PDF file paths.
        bucketNum (int): Number of buckets to distribute PDFs into.

    Returns:
        defaultdict: Dictionary mapping bucket indices to lists of (pdf_path, page_count) tuples.
    """
    pdfs = [(path, get_pdf_page_count(path)) for path in pdf_paths]
    pdfs = [p for p in pdfs if p[1] > 0]  # Filter out PDFs with errors
    pdfs.sort(key=lambda x: x[1], reverse=True)

    buckets = defaultdict(list)
    bucket_sums = [0] * bucketNum  # Track total pages per bucket

    for pdf, pages in pdfs:
        min_bucket = bucket_sums.index(min(bucket_sums))  # Find bucket with least pages
        buckets[min_bucket].append((pdf, pages))
        bucket_sums[min_bucket] += pages

    return buckets

def process_and_chunk_pdfs(df: pd.DataFrame, temp_folder: str, batches_folder: str, output_csv: str, bucket_num: int = 10):
    """
    Process PDFs with is_chunked == 0, copy them to a temporary folder, distribute into batch folders,
    and update the DataFrame with is_chunked and chunking_folder. Save to the original CSV.

    Args:
        df (pd.DataFrame): Input DataFrame with case metadata.
        temp_folder (str): Temporary folder to copy PDFs for chunking.
        batches_folder (str): Folder to store batch folders.
        output_csv (str): Path to the original CSV to update.
        bucket_num (int): Number of buckets for distribution.
    """
    # Ensure temporary and batches folders exist
    os.makedirs(temp_folder, exist_ok=True)
    os.makedirs(batches_folder, exist_ok=True)

    # Filter rows where is_chunked == 0
    chunking_df = df[df['is_chunked'] == 0].copy()
    if chunking_df.empty:
        print("No PDFs to chunk (all is_chunked != 0)")
        return df

    print(f"Found {len(chunking_df)} PDFs with is_chunked == 0")

    # Copy PDFs to temporary folder and update is_chunked
    pdf_files = []
    for index, row in chunking_df.iterrows():
        pdf_path = row['real_path']
        if not os.path.exists(pdf_path):
            print(f"Error: PDF not found at {pdf_path}")
            continue
        
        # Define destination path in temp_folder
        pdf_filename = os.path.basename(pdf_path)
        dest_path = os.path.join(temp_folder, pdf_filename)
        
        try:
            shutil.copy(pdf_path, dest_path)
            pdf_files.append(dest_path)
            # Update is_chunked in the original DataFrame
            df.at[index, 'is_chunked'] = 1
        except Exception as e:
            print(f"Error copying {pdf_path}: {str(e)}")

    if not pdf_files:
        print("No PDFs were copied for chunking")
        return df

    # Distribute PDFs into batch folders
    buckets = distribute_pdfs(pdf_files, bucketNum=bucket_num)

    # Find a value K here such that it gives the last processed batch in the chunking_batches folder so that upcoming batch gets the new name
    folder_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunking_batches"
    batch_folders = [f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
    k = -1

    for batch_name in batch_folders:
        temp_name = batch_name.split("_")
        if int(temp_name[-1]) > k:
            k = int(temp_name[-1])

    # Save initial k
    with open("initial_k.txt", "w") as f:
        f.write(str(k))

    # Process each bucket and update chunking_folder
    for i, bucket in buckets.items():
        batch_folder = os.path.join(batches_folder, f"Batch_{i+k+1}")
        os.makedirs(batch_folder, exist_ok=True)
        
        for pdf, _ in bucket:
            pdf_filename = os.path.basename(pdf)
            dest_path = os.path.join(batch_folder, pdf_filename)
            try:
                shutil.copy(pdf, dest_path)
                # Update chunking_folder in the DataFrame for this PDF
                # Use the original real_path (from temp_folder) to match
                df.loc[df['real_path'] == pdf, 'chunking_folder'] = batch_folder
            except Exception as e:
                print(f"Error copying {pdf} to {dest_path}: {str(e)}")
        
        print(f"Batch {i+1} done")

    print("PDFs distributed into batches successfully.")

    # Delete the temporary folder
    try:
        shutil.rmtree(temp_folder)
        print(f"Deleted temporary folder {temp_folder}")
    except Exception as e:
        print(f"Error deleting temporary folder {temp_folder}: {str(e)}")

    # Save the updated DataFrame to the original CSV
    try:
        root_dir = batches_folder
        data = []

        # Sort the batch folders in increasing order
        batch_folders = sorted(
            [f for f in os.listdir(root_dir) if f.startswith('Batch_')],
            key=lambda x: int(x.split('_')[1])
        )

        # Loop through each batch folder
        for batch in batch_folders:
            batch_path = os.path.join(root_dir, batch)
            for file in os.listdir(batch_path):
                if file.endswith('.pdf'):
                    file_name = os.path.splitext(file)[0]  # remove .pdf extension
                    data.append((file_name, batch))

        # Create DataFrame
        batch_df = pd.DataFrame(data, columns=['file_primary_key', 'batch'])

        # setup the incremental logic for this as batches once made wont be 
        combined_df = df.loc[:, ~df.columns.isin(['batch'])]
        combined_df = pd.merge(combined_df, batch_df, how = 'left', on = 'file_primary_key')
        combined_df.to_csv("/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv", index = False)
        print(f"Updated original DataFrame at {output_csv}")

    except Exception as e:
        print(f"Error saving DataFrame to {output_csv}: {str(e)}")

    #return df

# Example usage
if __name__ == "__main__":
    # Load the DataFrame
    input_csv = "/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv"
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"Error: Input CSV {input_csv} not found")
        df = pd.DataFrame()

    temp_folder = "/projectnb/sachgrp/apgupta/Case Law Data/temp_pdfs"
    batches_folder = "/projectnb/sachgrp/apgupta/Case Law Data/chunking_batches"
    output_csv ="/projectnb/sachgrp/apgupta/Case Law Data/combined_cases_metadata.csv"  

    process_and_chunk_pdfs(df, temp_folder, batches_folder, output_csv, bucket_num=500)