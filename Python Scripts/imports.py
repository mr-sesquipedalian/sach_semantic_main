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