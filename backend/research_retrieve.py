import requests
import pandas as pd
import time
from db import db
from db import db, ResearchData
from datetime import datetime
# Function to retrieve research data from the API
def fetch_research_data(query):
    url = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
    params = {
        'query': query,
        'fields': 'paperId,title,authors,venue,publicationVenue,year,publicationDate,citationCount,abstract,influentialCitationCount,fieldsOfStudy,publicationTypes',
        'limit': 500
    }
    max_retries = 5
    retry_delay = 60
    for attempt in range(max_retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('data', [])
        elif response.status_code == 429:
            time.sleep(retry_delay)
        else:
            break
    return []

# Function to process the retrieved data into a DataFrame
def process_research_data(papers):
    rows = []
    for paper in papers:
        pub_venue = paper.get('publicationVenue') or {}
        
        # Handle venue type with default value
        if isinstance(pub_venue, dict):
            pub_venue_type = pub_venue.get('type', 'unknown')  # Default to 'unknown'
        else:
            pub_venue_type = 'unknown'

        # Safely truncate to 50 characters
        pub_venue_type = str(pub_venue_type)[:50] if pub_venue_type else 'unknown'
        pub_venue_name = pub_venue.get('name') if isinstance(pub_venue, dict) else paper.get('venue')
        pub_venue_type = pub_venue.get('type') if isinstance(pub_venue, dict) else 'unknown'
        authors = paper.get('authors', [])
        author_names = [author.get('name', '') for author in authors]
        pub_date_str = paper.get('publicationDate', None)
        pub_date = None
        if pub_date_str:
            try:
                pub_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
            except:
                pass
        
        row = {
            'paper_id': paper.get('paperId', '')[:255],  # Truncate to 255
            'title': paper.get('title', '')[:255],  # Truncate to 255
            'abstract': paper.get('abstract', ''),
            'publication_venue_name': (pub_venue_name or '')[:255],  # Truncate to 255
            'publication_venue_type': pub_venue_type,  # Truncate to 50
            'year': paper.get('year', None),
            'reference_count': paper.get('referenceCount', None),
            'citation_count': paper.get('citationCount', None),
            'influential_citation_count': paper.get('influentialCitationCount', None),
            'fields_of_study': paper.get('fieldsOfStudy', []),
            'publication_types': paper.get('publicationTypes', []),
            'publication_date': pub_date if pd.notna(pub_date) else None,
            'authors': (', '.join(author_names)[:255] if author_names else None)  # Truncate to 255
        }
        rows.append(row)
        df= pd.DataFrame(rows)
        df['year'] = df['year'].astype('Int64')
        df = df[~((df['publication_venue_name'] == '') | (df['year'].isnull()))]
        #df.reset_index(drop=True, inplace=True)
    return df

# Function to store the DataFrame into the PostgreSQL database
# In research_retrieve.py
#from db import ResearchData

def store_research_data(df):
    # Clear existing data
    db.session.query(ResearchData).delete()
    # Convert DataFrame to list of dictionaries
    data = df.to_dict('records')
    for record in data:
        # Convert list fields to Python lists (if stored as JSON/ARRAY)
        # Handle any other data type conversions if necessary
        research_entry = ResearchData(**record)
        db.session.add(research_entry)
    db.session.commit()