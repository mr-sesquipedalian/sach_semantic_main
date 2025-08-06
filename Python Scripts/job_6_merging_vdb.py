import faiss
import pickle
from langchain.docstore import InMemoryDocstore

# Load the first FAISS index and metadata

def merge_vector_database():
    faiss_path_base = faiss.read_index("/projectnb/sachgrp/apgupta/Case Law Data/vector_databases/main_vdb/index.faiss")
    with open("/projectnb/sachgrp/apgupta/Case Law Data/vector_databases/main_vdb/index.pkl", "rb") as f:
        pkl_path_base = pickle.load(f)
    
    # Load the second FAISS index and metadata
    with open ("/projectnb/sachgrp/apgupta/case_law_semantic_search_pipeline/sach_semantic_main/latest_incremental.txt", "r") as f:
        latest_vdb_path = f.read()
        
    faiss_path_incremental = faiss.read_index(os.path.join(latest_vdb_path, "index.pkl"))
    with open(os.path.join(latest_vdb_path, "index.pkl"), "rb") as f:
        pkl_path_incremental = pickle.load(f)
    
    # Check if both indices are of the same type/dimensions
    assert faiss_path_base.d == faiss_path_incremental.d, "Index dimensions do not match!"
    assert len(pkl_path_base) == len(pkl_path_incremental)
    
    docstore_base, ids_base = pkl_path_base
    docstore_incremental, ids_incremental = pkl_path_incremental
    
    # Access the internal dicts
    dict_base = docstore_base._dict
    dict_incremental = docstore_incremental._dict
    
    # Merge the docstores and ID mappings
    merged_dict = {**dict_base, **dict_incremental}
    merged_docstore = InMemoryDocstore(merged_dict)
    
    # Extract doc IDs in vector order from both dicts (sorted by key)
    ordered_ids_base = [ids_base[i] for i in sorted(ids_base.keys())]
    ordered_ids_incremental = [ids_incremental[i] for i in sorted(ids_incremental.keys())]
    
    # Combine them
    all_doc_ids = ordered_ids_base + ordered_ids_incremental
    
    # Reassign correct indices for merged FAISS index
    merged_ids = {i: doc_id for i, doc_id in enumerate(all_doc_ids)}
    
    # Merge the FAISS indices
    faiss_path_base.merge_from(faiss_path_incremental)  # faiss_path_base
    
    # Validate
    assert faiss_path_base.ntotal == len(merged_ids), "Mismatch after reindexing!"
    
    # Final merged metadata tuple
    merged_metadata = (merged_docstore, merged_ids)
    
    # Save merged index and metadata
    faiss.write_index(faiss_path_base, "/projectnb/sachgrp/apgupta/Case Law Data/vector_databases/main_vdb/index.faiss")
    with open("/projectnb/sachgrp/apgupta/Case Law Data/vector_databases/main_vdb/index.pkl", "wb") as f:
        pickle.dump(merged_metadata, f)
    
    print("Merged FAISS index and metadata saved.")

if __name__ == "__main__":
    merge_vector_database()