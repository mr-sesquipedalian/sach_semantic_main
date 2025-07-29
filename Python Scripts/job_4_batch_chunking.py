# sge_task_id = os.getenv('SGE_TASK_ID')
import os
import torch
import uuid
from langchain.document_loaders import PyMuPDFLoader
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_core.runnables.config import ContextThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor
import time
from itertools import islice
import pickle

# Helper function to batch items
def batch(iterable, size):
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk

# Load and clean documents
def load_and_clean_documents(folder_path):
    documents = []
    pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
    for file in pdf_files:
        loader = PyMuPDFLoader(os.path.join(folder_path, file))
        doc_pages = loader.load()
        for page in doc_pages:
            page.metadata["document_name"] = file
            page.metadata["page_number"] = page.metadata.get("page", len(documents) + 1)
            documents.append(page)
    return documents

# Process a single document
def process_document(doc):
    chunks = semantic_chunker.create_documents([doc.page_content])
    chunk_offset = 0
    for chunk in chunks:
        chunk_length = len(chunk.page_content)
        chunk.metadata["chunk_key"] = str(uuid.uuid4())
        chunk.metadata["document_name"] = doc.metadata["document_name"]
        chunk.metadata["page_number"] = doc.metadata["page_number"]
        chunk.metadata["offset"] = chunk_offset
        chunk.metadata["length"] = chunk_length
        chunk_offset = chunk_offset+chunk_length+1
        #chunk.metadata["html_keys"] = list of constituent keys that made this chunk from json
    return chunks

# Process documents with limited threads
def process_documents(documents, max_workers, batch_size=50):
    semantic_chunks = []
    for doc_batch in batch(documents, batch_size):
        with ContextThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_document, doc_batch))
        for result in results:
            semantic_chunks.extend(result)
    return semantic_chunks

# Initialize embedding model and SemanticChunker

if __name__ == "__main__":
    device = "cuda" if torch.cuda.is_available() else "cpu"
    nthreads = int(os.getenv("NSLOTS"))
    embed_model = FastEmbedEmbeddings(model_name="BAAI/bge-base-en-v1.5", device=device, threads=nthreads)

    # figure out a way to get this value from the job_3 code
    with open("initial_k.txt", "r") as f:
        k = int(f.read())
    
    sge_task_id = os.getenv('SGE_TASK_ID')
    batch_number = int(sge_task_id) + k

    batch_folder = "/projectnb/sachgrp/apgupta/Case Law Data/chunking_batches/Batch_" + str(batch_number) # Recheck the logic with sir once so that we can 
    documents = load_and_clean_documents(batch_folder)
    print(f"Loaded {len(documents)} pages Batch_{batch_number}")

    start_time = time.time()
    semantic_chunker = SemanticChunker(embed_model, breakpoint_threshold_type="percentile")
    semantic_chunks = process_documents(documents, nthreads)

    end_time = time.time()
    runtime = (end_time - start_time) / 3600
    print(f"Semantic chunking completed in {runtime} hours.")

    with open(os.path.join("/projectnb/sachgrp/apgupta/Case Law Data/chunked_pickle_files", f"test_{batch_number}.pkl"), "wb") as file:
        pickle.dump(semantic_chunks, file)
