# technology-trend-analysis/backend/patent_api.py
import os
import time
import requests
import pandas as pd
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

import os
import requests
import time
from urllib.parse import quote
import pandas as pd
from dotenv import load_dotenv

# Global token cache
TOKEN = None
TOKEN_EXPIRY = 0

# Constants for API endpoints
TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
BASE_URL = "https://ops.epo.org/3.2/rest-services"

# Load credentials from .env file
load_dotenv()
CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()

def get_access_token() -> str:
    """Get or refresh the OAuth access token."""
    global TOKEN, TOKEN_EXPIRY
    if TOKEN and time.time() < TOKEN_EXPIRY:
        return TOKEN
    data = {
        "grant_type": "client_credentials",
        "client_id": CONSUMER_KEY,
        "client_secret": CONSUMER_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=15)
    response.raise_for_status()
    TOKEN = response.json()["access_token"]
    TOKEN_EXPIRY = time.time() + 3500  # approximately 58 minutes
    return TOKEN

def validate_patent_number(patent: str) -> bool:
    """Perform a basic validation for the patent number format."""
    if not patent or len(patent.strip()) < 4:
        return False
    return True

def extract_jurisdictions_and_members(data: dict) -> dict:
    """
    Extract jurisdictions (as a sorted list) and family member publication numbers
    (formatted as country+doc-number+kind) from the JSON response.
    """
    try:
        jurisdictions = set()
        family_members = []
        world_data = data.get('ops:world-patent-data', {})
        patent_family = world_data.get('ops:patent-family', {})
        members = patent_family.get('ops:family-member', [])
        if isinstance(members, dict):
            members = [members]

        for member in members:
            pub_ref = member.get('publication-reference', {})
            docs = pub_ref.get('document-id', [])
            if isinstance(docs, dict):
                docs = [docs]

            for doc in docs:
                if doc.get('@document-id-type') == 'docdb':
                    country = doc.get('country')
                    if isinstance(country, dict):
                        country = country.get('$')
                    doc_number = doc.get('doc-number')
                    if isinstance(doc_number, dict):
                        doc_number = doc_number.get('$')
                    kind = doc.get('kind')
                    if isinstance(kind, dict):
                        kind = kind.get('$')

                    if country and doc_number and kind:
                        jurisdictions.add(country)
                        family_members.append(f"{country}{doc_number}{kind}")

        return {
            'jurisdictions': sorted(jurisdictions),
            'family_members': sorted(set(family_members))
        }

    except Exception as e:
        print(f"Error parsing response: {e}")
        return {'jurisdictions': None, 'family_members': None}

def process_patent(patent: str) -> dict:
    """
    Process a single patent by sending a request to the patent family endpoint,
    then extract family jurisdictions and family member publication numbers.
    Returns a dict with two keys: 'jurisdictions' and 'family_members'.
    """
    if not validate_patent_number(patent):
        print(f"Invalid patent number: {patent}")
        return {'jurisdictions': None, 'family_members': None}
    try:
        token = get_access_token()
        url = f"{BASE_URL}/family/publication/docdb/{quote(patent)}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 403:
            print(f"Access forbidden for patent {patent}")
            return {'jurisdictions': None, 'family_members': None}
        if response.status_code == 404:
            print(f"Patent {patent} not found")
            return {'jurisdictions': None, 'family_members': None}
        response.raise_for_status()
        data = response.json()
        return extract_jurisdictions_and_members(data)
    except Exception as e:
        print(f"Error processing patent {patent}: {e}")
        return {'jurisdictions': None, 'family_members': None}

def process_dataframe(df: pd.DataFrame, patent_col: str) -> pd.DataFrame:
    """
    For a DataFrame containing a column of patent numbers,
    process each patent (in batches) and add two new columns:
      - 'family_jurisdictions': sorted list of jurisdictions for the patent's family
      - 'family_members': sorted list of publication numbers for family members
    """
    if patent_col not in df.columns:
        raise ValueError(f"Column '{patent_col}' not found in DataFrame")
    result_df = df.copy()
    patents = result_df[patent_col].tolist()
    total = len(patents)
    batch_size = 100
    request_delay = 1.2  # seconds delay between requests
    results = {}

    for i in range(0, total, batch_size):
        batch = patents[i:i + batch_size]
        print(f"\nProcessing batch {i//batch_size + 1}/{(total - 1)//batch_size + 1}")
        for patent in batch:
            results[patent] = process_patent(patent)
            time.sleep(request_delay)
        if i + batch_size < total:
            print("Pausing between batches...")
            time.sleep(1)
            
    # Map the processed results to new DataFrame columns
    result_df['family_jurisdictions'] = result_df[patent_col].map(
        lambda p: results.get(p, {}).get('jurisdictions')
    )
    result_df['family_members'] = result_df[patent_col].map(
        lambda p: results.get(p, {}).get('family_members')
    )
    return result_df

# ======= USAGE EXAMPLE =======
# if __name__ == "__main__":

#     try:
#         processed_df = process_dataframe(df, 'first publication number')
#         print("\nFinal Results:")
#         print(processed_df[['first publication number', 'family_jurisdictions', 'family_members']])
#         # Optionally, export the results to CSV
#         #processed_df.to_csv('patent_jurisdictions.csv', index=False)
#     except Exception as e:
#         print(f"Processing failed: {e}")
