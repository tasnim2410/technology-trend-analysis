import os
import requests
from urllib.parse import quote
from dotenv import load_dotenv
import json
import pandas as pd
import time

import random
import logging
import threading
from functools import lru_cache

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
load_dotenv()
CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()

TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
BASE_URL = "https://ops.epo.org/3.2/rest-services"

# === Functions ===

def get_access_token() -> str:
    """
    Obtain an OAuth access token from OPS.
    """
    data = {
        "grant_type": "client_credentials",
        "client_id": CONSUMER_KEY,
        "client_secret": CONSUMER_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=15)
    response.raise_for_status()
    return response.json()["access_token"]

def get_abstract_json(publication_number: str) -> dict:
    """
    Retrieve the abstract for a given publication number as JSON.
    
    For example, for publication_number = 'KR102511398B1', the endpoint URL becomes:
      https://ops.epo.org/3.2/rest-services/published-data/publication/docdb/KR102511398B1/abstract
    """
    token = get_access_token()
    url = f"{BASE_URL}/published-data/publication/docdb/{quote(publication_number)}/abstract"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers, timeout=15)
    print(f"Fetching {publication_number} - Status code: {response.status_code}")
    response.raise_for_status()
    return response.json()

def extract_english_abstract_from_json(json_data: dict) -> str:
    """
    Given the JSON response from the OPS abstract endpoint, extract and return the English abstract text.
    Returns the abstract text as a string or None if not found.
    """
    try:
        world_data = json_data.get("ops:world-patent-data", {})
        exch_docs = world_data.get("exchange-documents", {})
        doc = exch_docs.get("exchange-document", {})
        abstract_section = doc.get("abstract", [])
        # If abstract is a dict (only one present), convert it to a list
        if isinstance(abstract_section, dict):
            abstract_section = [abstract_section]
        for abstract in abstract_section:
            if abstract.get("@lang") == "en":
                # The abstract text is in the "p" field as a dict with key "$"
                p = abstract.get("p")
                if isinstance(p, dict):
                    return p.get("$", "").strip()
                elif isinstance(p, list):
                    # In case of multiple paragraphs
                    return " ".join(item.get("$", "").strip() for item in p if isinstance(item, dict))
                elif isinstance(p, str):
                    return p.strip()
        return None
    except Exception as e:
        print("Error extracting English abstract:", e)
        return None

def add_abstracts_to_dataframe(df: pd.DataFrame, patent_col: str) -> pd.DataFrame:
    """
    Enrich the DataFrame by adding an 'english_abstract' column.
    For each patent number in the patent_col, this function:
      1. Retrieves the abstract JSON from the OPS API.
      2. Extracts the English abstract using extract_english_abstract_from_json().
      3. Stores the result in a new column.
      
    A delay is added between requests to comply with rate limits.
    """
    if patent_col not in df.columns:
        raise ValueError(f"Column '{patent_col}' not found in DataFrame")
    
    abstracts = []
    total = len(df)
    for idx, pub_number in enumerate(df[patent_col]):
        print(f"Processing {idx+1}/{total}: {pub_number}")
        try:
            json_data = get_abstract_json(pub_number)
            abstract_text = extract_english_abstract_from_json(json_data)
        except Exception as e:
            print(f"Error processing {pub_number}: {e}")
            abstract_text = None
        abstracts.append(abstract_text)
       
        time.sleep(1.2)
    df = df.copy()
    df["abstract"] = abstracts
    return df


if __name__ == "__main__":

    enriched_df = add_abstracts_to_dataframe(df, "first publication number")
    

    print(enriched_df[["first publication number", "abstract"]])

