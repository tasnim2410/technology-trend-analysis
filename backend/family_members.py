#family_members.py
import concurrent.futures
import os
import requests
import time
from urllib.parse import quote
import pandas as pd
from dotenv import load_dotenv
import random
import logging
import threading
from functools import lru_cache
import pandas as pd
import sqlalchemy
import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import threading
# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)





load_dotenv()

# Get the connection URL
db_url = os.getenv("DATABASE_URL")

# Check if the URL was loaded correctly
if db_url is None:
    raise ValueError("DATABASE_URL not found. Please check your .env file.")

# Create the SQLAlchemy engine
engine = sqlalchemy.create_engine(db_url)

# Test the connection
with engine.connect() as connection:
    print("Database connection successful!")
table_name = "raw_patents"      # Replace with your table
column_name = "Publication number"  # Column containing patent numbers

    # Create DB engine


    # Load data into DataFrame
logger.info("Fetching publication numbers from DB...")

query = 'SELECT * FROM raw_patents'

# Execute the query using pandas and SQLAlchemy
df = pd.read_sql(query, engine)
df = pd.read_sql(query, engine)
#df.rename(columns={column_name: "Publication number"}, inplace=True)
import pandas as pd

#renaming columns
df.rename(columns={
    'Titre': 'Title',
    'Inventeurs': 'Inventors',
    'Demandeurs': 'Applicants',
    'Numéro de publication': 'Publication number',
    'Priorité la plus ancienne': 'Earliest priority',
    'CIB': 'IPC',
    'CPC': 'CPC',
    'Date de publication': 'Publication date',
    'Publication la plus ancienne': 'Earliest publication',
    'Numéro de famille': 'Family number'
}, inplace=True)
#cleaning the date columns
df[['first publication date','second publication date']] = df['Publication date'].str.split(' ' , n=1 , expand= True)
#df['second publication date'] = df['second publication date'].str.strip('\n')
df['second publication date'] = df['second publication date'].str.strip('\r')
df['second publication date'] = df['second publication date'].str.strip('\n')

#first filing country 
df[['first publication number', 'second publication number']] = df['Publication number'].str.split(' ' , n=1 , expand=True)
#df['second publication number'] = df['second publication number'].str.strip('\n')
df['second publication number'] = df['second publication number'].str.strip('\r')
df['second publication number'] = df['second publication number'].str.strip('\n')


df.rename(columns={'No': 'id'}, inplace=True)
if 'Unnamed: 11' in df.columns:
    df.drop(columns=['Unnamed: 11','Publication date'], inplace=True)

df['Family number'] = pd.to_numeric(df['Family number'], errors='coerce')
df.rename(columns={'Family number': 'family number'}, inplace=True)
    # Process the DataFrame
# Calculate the number of rows for each part
n = len(df) // 3

# Split the DataFrame into three parts
df1 = df.iloc[:n].copy()       # First part
df2 = df.iloc[n:2*n].copy()    # Second part
df3 = df.iloc[2*n:].copy()     # Third part

import concurrent.futures
import os
import requests
import time
from urllib.parse import quote
import pandas as pd
from dotenv import load_dotenv

# Global token cache for multiple credentials
TOKENS = {}
TOKENS_EXPIRY = {}

# Constants for API endpoints
TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
BASE_URL = "https://ops.epo.org/3.2/rest-services"

# Load credentials from .env file
load_dotenv()
CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()
CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_2").strip()
CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_2").strip()

CREDENTIALS = [
    {"key": CONSUMER_KEY, "secret": CONSUMER_SECRET},
    {"key": CONSUMER_KEY1, "secret": CONSUMER_SECRET1}
]

def get_access_token(key_index: int = 0) -> str:
    if key_index not in TOKENS or time.time() >= TOKENS_EXPIRY[key_index]:
        creds = CREDENTIALS[key_index]
        data = {
            "grant_type": "client_credentials",
            "client_id": creds["key"],
            "client_secret": creds["secret"]
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=15)
        response.raise_for_status()
        TOKENS[key_index] = response.json()["access_token"]
        TOKENS_EXPIRY[key_index] = time.time() + 3500
    return TOKENS[key_index]

def validate_patent_number(patent: str) -> bool:
    return bool(patent and len(patent.strip()) >= 4)

def extract_jurisdictions_and_members(data: dict) -> dict:
    jurisdictions = set()
    family_members = []
    world_data = data.get('ops:world-patent-data', {})
    patent_family = world_data.get('ops:patent-family', {})
    members = patent_family.get('ops:family-member', []) or []
    if isinstance(members, dict):
        members = [members]

    for member in members:
        pub_ref = member.get('publication-reference', {})
        docs = pub_ref.get('document-id', []) or []
        if isinstance(docs, dict):
            docs = [docs]
        for doc in docs:
            if doc.get('@document-id-type') == 'docdb':
                country = doc.get('country')
                country = country.get('$') if isinstance(country, dict) else country
                doc_number = doc.get('doc-number')
                doc_number = doc_number.get('$') if isinstance(doc_number, dict) else doc_number
                kind = doc.get('kind')
                kind = kind.get('$') if isinstance(kind, dict) else kind
                if country and doc_number and kind:
                    jurisdictions.add(country)
                    family_members.append(f"{country}{doc_number}{kind}")
    return {
        'jurisdictions': sorted(jurisdictions),
        'family_members': sorted(set(family_members))
    }

def process_patent(patent: str, key_index: int) -> dict:
    if not validate_patent_number(patent):
        return {'jurisdictions': None, 'family_members': None, 'error': 'Invalid patent number'}
    try:
        token = get_access_token(key_index)
        url = f"{BASE_URL}/family/publication/docdb/{quote(patent)}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code in (403, 404):
            return {'jurisdictions': None, 'family_members': None, 'error': f'API returned {response.status_code}'}
        response.raise_for_status()
        data = extract_jurisdictions_and_members(response.json())
        data['error'] = None
        return data
    except Exception as e:
        return {'jurisdictions': None, 'family_members': None, 'error': str(e)}

def process_dataframe_parallel(df: pd.DataFrame, patent_col: str, max_workers: int = 10) -> pd.DataFrame:
    if patent_col not in df.columns:
        raise ValueError(f"Column '{patent_col}' not found in DataFrame")
    patents = df[patent_col].tolist()
    N = len(patents)
    mid = N // 2
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_patent = {}
        for i, patent in enumerate(patents):
            key_index = 0 if i < mid else 1
            future = executor.submit(process_patent, patent, key_index)
            future_to_patent[future] = patent
        for future in concurrent.futures.as_completed(future_to_patent):
            patent = future_to_patent[future]
            result = future.result()
            results[patent] = result
            if result['error'] is not None:
                print(f"Error for patent {patent}: {result['error']}")
            time.sleep(0.1)

    df['family_jurisdictions'] = df[patent_col].map(lambda p: results[p]['jurisdictions'])
    df['family_members'] = df[patent_col].map(lambda p: results[p]['family_members'])
    return df

# Example usage
if __name__ == "__main__":
    # Create a sample DataFrame (fixing the original issue where df2 was used before definition)
    #df2 = pd.DataFrame({'first publication number': ['US1234567A', 'INVALID', 'EP1234567A']})
    df1 = process_dataframe_parallel(df2, 'first publication number', max_workers=4)
    empty_arrays_count = df1['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
    print('Number of rows with empty arrays in df1:', empty_arrays_count,'\n','number of empty rows:',df1['family_jurisdictions'].isnull().sum())
####processing df2
#3 threads


class PatentsSearch:
    def __init__(self, headless=True):
        """Initialize the scraper with enhanced compatibility options."""
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        
        try:
            self.driver = uc.Chrome(
                options=options,
                use_subprocess=True,
                version_main=None,
                suppress_welcome=True,
                debug=False
            )
            self.driver.set_page_load_timeout(30)
            self.driver.set_window_size(1920, 1080)
        except Exception as e:
            print(f"Failed to initialize ChromeDriver: {e}")
            print("Trying alternative initialization method...")
            self.driver = uc.Chrome(
                options=options,
                driver_executable_path=None
            )

    def add_random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to mimic human behavior."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def get_page_html(self, url):
        """Navigate to the given URL and return the page HTML."""
        try:
            print(f"Navigating to: {url}")
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "h5"))
            )
            self.add_random_delay(3, 5)
            return self.driver.page_source
        except TimeoutException:
            print("Timed out waiting for the page to load.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def parse_html(self, html):
        """Parse the HTML and extract all span elements inside the 'Published as' content."""
        soup = BeautifulSoup(html, 'html.parser')
        published_as_element = soup.find(lambda tag: tag.name == "h5" and ("Publié en tant que" in tag.text or "Published as" in tag.text))
        if published_as_element:
            content_element = published_as_element.find_next_sibling("span")
            if content_element:
                spans = content_element.find_all('span')
                return [span.get_text(strip=True) for span in spans]
        return []

    def close(self):
        """Close the browser when done."""
        if self.driver:
            self.driver.quit()

def process_rows(df, indices):
    """Process a subset of DataFrame rows using a dedicated PatentsSearch instance."""
    scraper = PatentsSearch(headless=False)  # Set to False as per task requirement for visible windows
    try:
        for index in indices:
            row = df.loc[index]
            url = f"https://worldwide.espacenet.com/patent/search/family/{row['family number']}/publication/{row['first publication number']}?q=hydrogen%20battery"
            html = scraper.get_page_html(url)
            if html:
                family_members = scraper.parse_html(html)
                df.at[index, 'family_members'] = family_members
            else:
                print(f"Failed to retrieve the page HTML for {row['first publication number']}.")
    finally:
        scraper.close()

if __name__ == "__main__":
    # Assuming df is defined elsewhere with 'family number' and 'first publication number' columns
    
    df2['family_members'] = None

    # Split the DataFrame indices into three parts
    indices = df2.index.tolist()
    n = len(indices)
    part_size = n // 4
    remainder = n % 4
    parts = []
    start = 0
    for i in range(4):
        if i < remainder:
            end = start + part_size + 1
        else:
            end = start + part_size
        parts.append(indices[start:end])
        start = end

    # Create three threads, each with its own PatentsSearch instance
    threads = []
    for part in parts:
        thread = threading.Thread(target=process_rows, args=(df2, part))
        threads.append(thread)

    # Start all three threads to run three browser windows concurrently
    for thread in threads:
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    print("All threads finished.")
    empty_arrays_count = df2['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
    print('Number of rows with empty arrays in df1:', empty_arrays_count,'\n','number of empty rows:',df2['family_jurisdictions'].isnull().sum())

#processing df3


# Global token cache for multiple credentials
TOKENS = {}
TOKENS_EXPIRY = {}

# Constants for API endpoints
TOKEN_URL = "https://ops.epo.org/3.2/auth/accesstoken"
BASE_URL = "https://ops.epo.org/3.2/rest-services"

# Load credentials from .env file
load_dotenv()
CONSUMER_KEY = os.getenv("CONSUMER_KEY_3").strip()
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET_3").strip()
CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_1").strip()
CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_1").strip()

CREDENTIALS = [
    {"key": CONSUMER_KEY, "secret": CONSUMER_SECRET},
    {"key": CONSUMER_KEY1, "secret": CONSUMER_SECRET1}
]

def get_access_token(key_index: int = 0) -> str:
    if key_index not in TOKENS or time.time() >= TOKENS_EXPIRY[key_index]:
        creds = CREDENTIALS[key_index]
        data = {
            "grant_type": "client_credentials",
            "client_id": creds["key"],
            "client_secret": creds["secret"]
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=15)
        response.raise_for_status()
        TOKENS[key_index] = response.json()["access_token"]
        TOKENS_EXPIRY[key_index] = time.time() + 3500
    return TOKENS[key_index]

def validate_patent_number(patent: str) -> bool:
    return bool(patent and len(patent.strip()) >= 4)

def extract_jurisdictions_and_members(data: dict) -> dict:
    jurisdictions = set()
    family_members = []
    world_data = data.get('ops:world-patent-data', {})
    patent_family = world_data.get('ops:patent-family', {})
    members = patent_family.get('ops:family-member', []) or []
    if isinstance(members, dict):
        members = [members]

    for member in members:
        pub_ref = member.get('publication-reference', {})
        docs = pub_ref.get('document-id', []) or []
        if isinstance(docs, dict):
            docs = [docs]
        for doc in docs:
            if doc.get('@document-id-type') == 'docdb':
                country = doc.get('country')
                country = country.get('$') if isinstance(country, dict) else country
                doc_number = doc.get('doc-number')
                doc_number = doc_number.get('$') if isinstance(doc_number, dict) else doc_number
                kind = doc.get('kind')
                kind = kind.get('$') if isinstance(kind, dict) else kind
                if country and doc_number and kind:
                    jurisdictions.add(country)
                    family_members.append(f"{country}{doc_number}{kind}")
    return {
        'jurisdictions': sorted(jurisdictions),
        'family_members': sorted(set(family_members))
    }

def process_patent(patent: str, key_index: int) -> dict:
    if not validate_patent_number(patent):
        return {'jurisdictions': None, 'family_members': None, 'error': 'Invalid patent number'}
    try:
        token = get_access_token(key_index)
        url = f"{BASE_URL}/family/publication/docdb/{quote(patent)}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code in (403, 404):
            return {'jurisdictions': None, 'family_members': None, 'error': f'API returned {response.status_code}'}
        response.raise_for_status()
        data = extract_jurisdictions_and_members(response.json())
        data['error'] = None
        return data
    except Exception as e:
        return {'jurisdictions': None, 'family_members': None, 'error': str(e)}

def process_dataframe_parallel(df: pd.DataFrame, patent_col: str, max_workers: int = 10) -> pd.DataFrame:
    if patent_col not in df.columns:
        raise ValueError(f"Column '{patent_col}' not found in DataFrame")
    patents = df[patent_col].tolist()
    N = len(patents)
    mid = N // 2
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_patent = {}
        for i, patent in enumerate(patents):
            key_index = 0 if i < mid else 1
            future = executor.submit(process_patent, patent, key_index)
            future_to_patent[future] = patent
        for future in concurrent.futures.as_completed(future_to_patent):
            patent = future_to_patent[future]
            result = future.result()
            results[patent] = result
            if result['error'] is not None:
                print(f"Error for patent {patent}: {result['error']}")
            time.sleep(0.1)

    df['family_jurisdictions'] = df[patent_col].map(lambda p: results[p]['jurisdictions'])
    df['family_members'] = df[patent_col].map(lambda p: results[p]['family_members'])
    return df

def ensure_columns_exist(engine):
    columns_to_add = ['family_jurisdictions', 'family_members']
    for column in columns_to_add:
        result = engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_patents' AND column_name = '{column}'")
        if not result.fetchone():
            engine.execute(f"ALTER TABLE raw_patents ADD COLUMN {column} JSONB")


# Example usage
if __name__ == "__main__":
    # Create a sample DataFrame (fixing the original issue where df2 was used before definition)
    #df2 = pd.DataFrame({'first publication number': ['US1234567A', 'INVALID', 'EP1234567A']})
    df3 = process_dataframe_parallel(df3, 'first publication number', max_workers=4)
    empty_arrays_count = df3['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
    print('Number of rows with empty arrays in df1:', empty_arrays_count,'\n','number of empty rows:',df3['family_jurisdictions'].isnull().sum())


logger.info("Processing complete.")
