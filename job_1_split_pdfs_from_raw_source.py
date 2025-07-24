import os
import json
import pandas as pd
import sys
from pathlib import Path
from PyPDF2 import PdfReader, PdfWriter


'''pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)'''

# sys.argv[1] = Raw file folder like "/projectnb/sachgrp/prathamk/CaseLaw/USA/Massachusetts/" 
# sys.argv[2] = Output directory path like "/projectnb/sachgrp/apgupta/Case Law Data/USA/Massachusetts"

def extract_page_orders(json_data: str):
    """
    Extract a list of tuples containing first_page_order and last_page_order for each dictionary in the JSON.

    Args:
        json_data (str): Path to the JSON file containing a list of dictionaries.

    Returns:
        List[Tuple[Optional[int], Optional[int]]]: List of tuples, each containing
                                                  (first_page_order, last_page_order) for each dictionary.
    """
    try:
        with open(json_data, "r") as file:
            data = json.load(file)
        
        if not isinstance(data, list) or not data:
            print(f"Error: JSON data in {json_data} must be a non-empty list of dictionaries")
            return []
        
        # Extract first_page_order and last_page_order from each dictionary as a tuple
        result = [
            (d.get('first_page_order', None), d.get('last_page_order', None))
            for d in data
        ]
        
        return result

    except FileNotFoundError:
        print(f"Error: File {json_data} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in {json_data}")
        return []
    except Exception as e:
        print(f"Error processing {json_data}: {str(e)}")
        return []

def split_pdf_by_case(json_file: str, pdf_file: str, output_dir: str):
    """
    Split a PDF into separate PDFs for each case based on page orders from the JSON file.
    If start_page equals end_page, extract only that single page.

    Args:
        json_file (str): Path to the JSON file.
        pdf_file (str): Path to the corresponding PDF file.
        output_dir (str): Directory to save the split PDFs.
    """
    try:
        # Load the JSON data
        with open(json_file, "r") as f:
            name_data = json.load(f)
        
        if not isinstance(name_data, list):
            print(f"Error: JSON file {json_file} does not contain a list of dictionaries")
            return
        
        # Extract page orders
        page_numbers = extract_page_orders(json_file)
        
        if not page_numbers:
            print(f"No valid page orders extracted from {json_file}")
            return
        
        # Load the source PDF
        reader = PdfReader(pdf_file)
        total_pages = len(reader.pages)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Loop over both page_numbers and name_data together
        for (start, end), info in zip(page_numbers, name_data):
            if start is None or end is None:
                print(f"Skipping case {info.get('name_abbreviation', 'unknown')} (ID: {info.get('id', 'unknown')}) due to missing page orders")
                continue
                
            if start < 1 or end > total_pages or start > end:
                print(f"Invalid page range {start}-{end} for case {info.get('name_abbreviation', 'unknown')} (ID: {info.get('id', 'unknown')})")
                continue
                
            writer = PdfWriter()
            if start == end:
                # Extract only the single page (0-based indexing for PyPDF2)
                writer.add_page(reader.pages[start - 1])
                page_range_str = f"{start}"
            else:
                # Extract the range of pages
                for page_num in range(start - 1, end):  # PyPDF2 uses 0-based indexing
                    writer.add_page(reader.pages[page_num])
                page_range_str = f"{start} to {end}"
            
            # Construct output filename using name_abbreviation and id
            name_abbreviation = info.get('name_abbreviation', 'unknown').replace(' ', '_').replace('/', '_')
            case_id = info.get('id', 'unknown')
            output_filename = os.path.join(output_dir, f"{name_abbreviation}_{case_id}.pdf")
            
            with open(output_filename, "wb") as out_file:
                writer.write(out_file)
            print(f"Saved {output_filename} with pages {page_range_str}")
    
    except FileNotFoundError:
        print(f"Error: File not found ({json_file} or {pdf_file})")
    except Exception as e:
        print(f"Error processing {json_file} and {pdf_file}: {str(e)}")

def process_all_folders(test_dir: str, output_base_dir: str):
    """
    Process all folders in the test directory, splitting PDFs based on JSON page orders.

    Args:
        test_dir (str): Path to the root /Test directory.
        output_base_dir (str): Base directory to save the split PDFs.
    """
    test_path = Path(test_dir)
    
    if not test_path.exists() or not test_path.is_dir():
        print(f"Error: Directory {test_dir} does not exist or is not a directory")
        return
    
    # Iterate through all subdirectories in the test directory
    for folder in test_path.iterdir():
        if folder.is_dir():
            print(f"Processing folder: {folder}")
            # Iterate through all JSON files in the folder
            for json_file in folder.glob("*.json"):
                pdf_file = json_file.with_suffix(".pdf")
                if pdf_file.exists():
                    # Define output directory for this folder
                    folder_output_dir = os.path.join(output_base_dir, folder.name)
                    split_pdf_by_case(str(json_file), str(pdf_file), folder_output_dir)
                else:
                    print(f"Error: Corresponding PDF {pdf_file} not found for {json_file}")

if __name__ == "__main__":
    # raw_data_directory = "/projectnb/sachgrp/prathamk/CaseLaw/USA/Massachusetts/"    # put the raw data path here. mostly just have to change the state in the end
    # output_directory = "/projectnb/sachgrp/apgupta/Case Law Data/USA/Massachusetts"  # put the output directory path here. mostly just have to change the state in the end
    process_all_folders(sys.argv[1], sys.argv[2])

