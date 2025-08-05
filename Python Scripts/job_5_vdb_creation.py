import time
from langchain_community.vectorstores import FAISS
import pickle
from fastembed import TextEmbedding
import os
import torch
from typing import List, cast
import numpy as np
from datetime import datetime

def setup_environment():
    cuda_ids = [int(i) for i in os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(',')]
    print("Cuda IDs", cuda_ids)
    use_cuda = torch.cuda.is_available()
    print("Use cuda", use_cuda)

    nslots = int(os.environ.get("NSLOTS", 1))
    print("Cores", nslots)

    tmpdir = os.environ.get("TMPDIR", "/tmp")
    tmpdir += '/sl'
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)

    return cuda_ids, use_cuda, nslots, tmpdir

def combine_pickle_files(folder_path):
    combined_data = []
    batch_num_list = []

    start_batch = int()

    with open("/projectnb/sachgrp/apgupta/case_law_semantic_search_pipeline/sach_semantic_main/initial_k.txt", "r") as f:   
        start_batch = int(f.read())

    start_batch += 1
    end_batch = start_batch + 499

    folder_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunked_pickle_files"
    for filename in os.listdir(folder_path):
        if filename.endswith('.pkl'):
            # Extract batch number from filename (e.g., test_1.pkl -> 1)
            try:
                batch_num = int(filename.replace(".pkl", "").split("_")[-1])
                # Check if batch number is within the specified range
                if start_batch <= batch_num <= end_batch:
                    file_path = os.path.join(folder_path, filename)
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                        combined_data.extend(data)
                    batch_num_list.append(batch_num)
            except (ValueError, IndexError):
                # Skip files that don't match the expected naming pattern
                continue

    output_file = f"/projectnb/sachgrp/apgupta/Case Law Data/combined_chunking_files/all_cases_combined_batch_{start_batch}_batch_{end_batch}.pkl"  
    # put the final chunked file for PDFs 
    with open(output_file, 'wb') as f:
        pass  # This just creates or truncates the file

    with open(output_file, 'wb') as f:
        pickle.dump(combined_data, f)

    print(f"Combined {len(batch_num_list)} pickle files into {output_file}")
    print(f"Total items in combined list: {len(combined_data)}")
    print(start_batch, end_batch)
    return output_file, start_batch, end_batch

class SachTextEmbedding(TextEmbedding):
    def __init__(self, model_name, cuda, device_ids, threads, cache_dir, providers, lazy_load, parallel):
        self.parallel = parallel
        super().__init__(model_name=model_name, 
                         cache_dir=cache_dir, 
                         cuda=cuda, 
                         device_ids=device_ids, 
                         threads=threads,
                         providers=providers,
                         lazy_load=lazy_load)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        embeddings = self.model.embed(texts, parallel=self.parallel)
        return [cast(List[float], e.tolist()) for e in embeddings]

def main():
    # Setup environment
    cuda_ids, use_cuda, nslots, tmpdir = setup_environment()

    # Combine pickle files
    folder_path = "/projectnb/sachgrp/apgupta/Case Law Data/chunked_pickle_files"  # put the semantic chunks folder path here
    output_file, start_batch, end_batch = combine_pickle_files(folder_path)

    # Load combined data
    with open(output_file, "rb") as file:
        semantic_chunks = pickle.load(file)

    # Create embedding model
    print("Creating embed model")
    embed_model = SachTextEmbedding(
        model_name="BAAI/bge-base-en-v1.5",
        cuda=use_cuda,
        device_ids=cuda_ids,
        threads=nslots,
        cache_dir=tmpdir,
        providers=["CUDAExecutionProvider"],
        lazy_load=True,
        parallel=len(cuda_ids)
    )
    print("Created embed model")

    # Create and save vector store
    print("Creating vector store")
    start_time = time.time()
    vectorstore = FAISS.from_documents(semantic_chunks, embed_model)

    print("Saving vector store")
    current_datetime = datetime.now()
    datetime_string = current_datetime.strftime("%Y-%m-%d_%H")
    vectorstore.save_local(f"/projectnb/sachgrp/apgupta/Case Law Data/vector_databases/faiss_index_vdb_batch_{start_batch}_batch_{end_batch}_{datetime_string}")
    end_time = time.time()
    runtime = (end_time - start_time) / 3600
    print(f"All embeddings stored in a single FAISS vector database in {runtime} hours")

if __name__ == '__main__':
    main()