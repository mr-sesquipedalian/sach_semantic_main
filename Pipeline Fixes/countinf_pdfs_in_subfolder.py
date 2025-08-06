import os

def count_pdfs_in_subfolders(root_folder):
    pdf_counts = {}
    total_pdfs = 0

    # Iterate through subdirectories only
    for subfolder in os.listdir(root_folder):
        subfolder_path = os.path.join(root_folder, subfolder)
        if os.path.isdir(subfolder_path):
            pdf_files = [f for f in os.listdir(subfolder_path) if f.lower().endswith('.pdf')]
            count = len(pdf_files)
            pdf_counts[subfolder] = count
            total_pdfs += count

    return pdf_counts, total_pdfs

# Example usage:
folder_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunking_batches"  # <- replace this with your actual path
pdf_counts, total = count_pdfs_in_subfolders(folder_path)

print("PDF count per subfolder:")
for subfolder, count in pdf_counts.items():
    print(f"{subfolder}: {count} PDFs")

print(f"\nTotal number of PDFs: {total}")
