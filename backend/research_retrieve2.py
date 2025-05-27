import requests
import pandas as pd
import time
import traceback
from datetime import datetime
from sqlalchemy import or_
from db import db, ResearchData2, ImpactFactor
from impact_factor_processor import clean_and_process_data , store_processed_data
from flask import current_app
# Field-specific JIF thresholds (match your impact factor processor)
FIELD_THRESHOLDS = {
    'Medicine': 1.0,
    'Biology': 1.0,
    'Materials Science': 1.5,
    'Computer Science': 0.8,
    'Chemistry': 1.5,
    'Mathematics': 0.7,
    'Geology': 0.8,
    'Physics': 1.0,
    'Engineering': 1.5,
    'Environmental Science': 0.5
}

def fetch_research_data2(query):
    url = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
    params = {
        'query': query,
        'fields': 'paperId,title,authors,venue,publicationVenue,year,publicationDate,citationCount,abstract,influentialCitationCount,fieldsOfStudy,publicationTypes',
        'limit': 500
    }
    for attempt in range(5):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json().get('data', [])
        elif response.status_code == 429:
            time.sleep(60)
    return []

"""working code"""
def process_research_data2(papers , impact_factors=None):
    rows = []
    for paper in papers:
        pub_venue = paper.get('publicationVenue') or {}
        pub_venue_type = str(pub_venue.get('type', 'unknown'))[:50] if isinstance(pub_venue, dict) else 'unknown'
        pub_venue_name = pub_venue.get('name') if isinstance(pub_venue, dict) else paper.get('venue', '')
        
        authors = paper.get('authors', [])
        author_names = [author.get('name', '') for author in authors]
        pub_date = parse_date(paper.get('publicationDate'))

        row = {
            'paper_id': paper.get('paperId', '')[:255],
            'title': paper.get('title', ''),
            'abstract': paper.get('abstract', ''),
            'publication_venue_name': pub_venue_name,
            'publication_venue_type': pub_venue_type,
            'year': paper.get('year'),
            'citation_count': paper.get('citationCount'),
            'influential_citation_count': paper.get('influentialCitationCount'),
            'fields_of_study': paper.get('fieldsOfStudy', []),
            'publication_types': paper.get('publicationTypes', []),
            'publication_date': pub_date,
            'authors': ', '.join(author_names)[:255] if author_names else None
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    
    # Clean and filter data
    df = clean_base_data(df)
    
    # Apply impact factor filtering
    df = filter_by_impact_factor(df , impact_factors)
    
    df = df.rename(columns={'Field': 'field', 'JIF5Years': 'jif_5years'})
    
    return df


"""working code"""

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None
    except:
        return None

def clean_base_data(df):
    # Convert and filter year
    df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
    df = df[~((df['publication_venue_name'] == '') | (df['year'].isna()))]
    
    # Clean publication types
    # df['publication_types'] = df['publication_types'].apply(
    #     lambda x: x if isinstance(x, list) else []
    # )
    return df.reset_index(drop=True)
  
def normalize_name(col):
    return col.str.lower().str.strip().str.replace(r'\s*&\s*', ' and ', regex=True)

def filter_by_impact_factor(df,impact_factors=None):
  # Normalize publication venue name
  df = df.copy()
  df['venue_norm'] = normalize_name(df['publication_venue_name'])

  # Get the ImpactFactor data from the database
  
  final_data = pd.DataFrame([{
    'id': j.id,
    'Name': j.name,
    'Abbr Name': j.abbrev_name,
    'issn': j.issn,
    'JIF5Years': j.jif_5yr,
    'subcategory': j.subcategory,
    'Field': j.field,
    
  } for j in impact_factors])

  # Normalize Name and Abbr Name in final_data
  final_data['name_norm'] = normalize_name(final_data['Name'])
  final_data['abbr_norm'] = normalize_name(final_data['Abbr Name'])

  # Merge on the normalized columns (first on full name, then on abbr)
  merged = df.merge(
    final_data[['name_norm', 'subcategory', 'Field', 'JIF5Years']],
    how='left',
    left_on='venue_norm',
    right_on='name_norm'
  ).merge(
    final_data[['abbr_norm', 'subcategory', 'Field', 'JIF5Years']],
    how='left',
    left_on='venue_norm',
    right_on='abbr_norm',
    suffixes=('', '_abbr')
  )

  # Coalesce the two matches
  for col in ('subcategory', 'Field', 'JIF5Years'):
    if f"{col}_abbr" in merged.columns:
      merged[col] = merged[col].combine_first(merged[f"{col}_abbr"])

  lookup = final_data.set_index(['name_norm','abbr_norm'])[['subcategory','Field','JIF5Years']]
  mask = merged['subcategory'].isna()
  for idx in merged.index[mask]:
    venue = merged.at[idx, 'venue_norm']
    match = None
    venue = str(venue) if venue is not None else ""

    # 3a) look for any normalized name or abbr inside venue
    for (nm, ab), row in lookup.iterrows():
        if nm in venue or ab in venue:
            match = row
            break

    if match is not None:
        # ← here’s the fix: use .loc instead of .at
        merged.loc[idx, ['subcategory','Field','JIF5Years']] = match.values
  # Drop helper columns
  merged = merged.drop(columns=[
    'name_norm', 'abbr_norm', 'venue_norm',
    'subcategory_abbr', 'Field_abbr', 'JIF5Years_abbr'
  ], errors='ignore')
  keep_types = {'JournalArticle', 'Review'}
  mask = merged['publication_types'].apply(lambda types: isinstance(types, list) and bool(set(types) & keep_types))
  clean_df = merged[mask].reset_index(drop=True)
  mask_conf_only = merged['publication_types'].apply(lambda x: x == ['conference'])
  clean_df = merged.loc[~mask_conf_only].reset_index(drop=True)
  df=clean_df
  df = df[df['subcategory'].notna()].reset_index(drop=True)

  return df


def store_research_data2(df):
    # Clear existing data
    db.session.query(ResearchData2).delete()
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    for record in records:
        # Ensure array types are handled for PostgreSQL
        record['fields_of_study'] = record.get('fields_of_study', [])
        record['publication_types'] = record.get('publication_types', [])
        # Create and add ResearchData instance
        db.session.add(ResearchData2(**record))
    
    # Commit changes to the database
    db.session.commit()
# def store_research_data2(df):
#     db.session.query(ResearchData).delete()
    
#     # Handle array types for PostgreSQL
#     records = df.to_dict('records')
#     for record in records:
#         record['fields_of_study'] = record.get('fields_of_study', [])
#         record['publication_types'] = record.get('publication_types', [])
#         record['subcategory'] = record.get('subcategory')
#         record['field'] = record.get('Field')  # Watch case here!
#         record['jif_5years'] = record.get('JIF5Years')
#         db.session.add(ResearchData(**record))

    
#     db.session.commit()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    """not so working code"""
# def process_research_data2(papers):
#     try: 
#         rows = []
#         for paper in papers:
#             pub_venue = paper.get('publicationVenue') or {}
        
#             # Handle venue type with default value
#             if isinstance(pub_venue, dict):
#                 pub_venue_type = pub_venue.get('type', 'unknown')  # Default to 'unknown'
#             else:
#                 pub_venue_type = 'unknown'

#             # Safely truncate to 50 characters
#             pub_venue_type = str(pub_venue_type)[:50] if pub_venue_type else 'unknown'
#             pub_venue_name = (
#                 pub_venue.get('name', '') or paper.get('venue', '') or ''
#             )
#             pub_venue_type = pub_venue.get('type') if isinstance(pub_venue, dict) else 'unknown'
#             authors = paper.get('authors', [])
#             author_names = [author.get('name', '') for author in authors]
#             pub_date_str = paper.get('publicationDate', None)
#             pub_date = None
#             if pub_date_str:
#                 try:
#                     pub_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
#                 except:
#                     pass
#             row = {
#                 'paper_id': paper.get('paperId', '')[:255],
#                 'title': paper.get('title', '')[:255],
#                 'abstract': paper.get('abstract', ''),
#                 'publication_venue_name': pub_venue_name[:255],
#                 'publication_venue_type': pub_venue_type,
#                 'year': paper.get('year'),
#                 'citation_count': paper.get('citationCount'),
#                 'influential_citation_count': paper.get('influentialCitationCount'),
#                 'fields_of_study': paper.get('fieldsOfStudy', []),
#                 'publication_types': paper.get('publicationTypes', []),
#                 'publication_date': pub_date,
#                 'authors': ', '.join(author_names)[:255] if author_names else None
#             }
#             rows.append(row)

#     # Build the initial DataFrame
#         df_initial = pd.DataFrame(rows)
#         before_clean = len(df_initial)

#     # Clean the base data
#         df_cleaned = clean_base_data(df_initial)
#         after_clean = len(df_cleaned)

#     # Filter by impact factor
#         df_filtered = filter_by_impact_factor(df_cleaned)
#         after_filter = len(df_filtered)

#     # (Optional) log to console for debugging
#         print(f"Rows before cleaning: {before_clean}")
#         print(f"Rows after clean_base_data: {after_clean}")
#         print(f"Rows after filter_by_impact_factor: {after_filter}")

#     # Pack stats into a dict
#         stats = {
#             'before_clean': before_clean,
#             'after_clean': after_clean,
#             'after_filter': after_filter
#         }

#         print("Returning from try block:", type((df_filtered, stats)), len((df_filtered, stats)))
#         return df_filtered, stats
#     except Exception as e:
#             print(f"Error processing research data: {e}")
#             traceback.print_exc()
#             result = (pd.DataFrame(), {'before_clean': 0, 'after_clean': 0, 'after_filter': 0})
#             print("Returning from except block:", type(result), len(result))
#             return result

# def parse_date(date_str):
#     try:
#         return datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None
#     except:
#         return None

# def clean_base_data(df):
#     # Convert and filter year
#     df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
#     df = df[~((df['publication_venue_name'] == '') | (df['year'].isna()))]
    
#     # Clean publication types
#     df['publication_types'] = df['publication_types'].apply(
#         lambda x: x if isinstance(x, list) else []
#     )
#     return df.reset_index(drop=True)
  
# def normalize_name(col):
#     return col.str.lower().str.strip().str.replace(r'\s*&\s*', ' and ', regex=True)

# def filter_by_impact_factor(df):
#   # Normalize publication venue name
#   df = df.copy()
#   df['venue_norm'] = normalize_name(df['publication_venue_name'])

#   # Get the ImpactFactor data from the database
#   impact_factors = ImpactFactor.query.all()
#   final_data = pd.DataFrame([{
#     'Name': j.name,
#     'Abbr Name': j.abbrev_name,
#     'issn': j.issn,
#     'subcategory': j.subcategory,
#     'Field': j.field,
#     'JIF5Years': j.jif_5yr
#   } for j in impact_factors])

#   # Normalize Name and Abbr Name in final_data
#   final_data['name_norm'] = normalize_name(final_data['Name'])
#   final_data['abbr_norm'] = normalize_name(final_data['Abbr Name'])

#   # Merge on the normalized columns (first on full name, then on abbr)
#   merged = df.merge(
#     final_data[['name_norm', 'subcategory', 'Field', 'JIF5Years']],
#     how='left',
#     left_on='venue_norm',
#     right_on='name_norm'
#   ).merge(
#     final_data[['abbr_norm', 'subcategory', 'Field', 'JIF5Years']],
#     how='left',
#     left_on='venue_norm',
#     right_on='abbr_norm',
#     suffixes=('', '_abbr')
#   )

#   # Coalesce the two matches
#   for col in ('subcategory', 'Field', 'JIF5Years'):
#     if f"{col}_abbr" in merged.columns:
#       merged[col] = merged[col].combine_first(merged[f"{col}_abbr"])

#   # Drop helper columns
#   merged = merged.drop(columns=[
#     'name_norm', 'abbr_norm', 'venue_norm',
#     'subcategory_abbr', 'Field_abbr', 'JIF5Years_abbr'
#   ], errors='ignore')
#   keep_types = {'JournalArticle', 'Review'}
#   mask = merged['publication_types'].apply(lambda types: isinstance(types, list) and bool(set(types) & keep_types))
#   clean_df = merged[mask].reset_index(drop=True)
#   mask_conf_only = merged['publication_venue_type'].apply(lambda x: x == 'conference')
#   clean_df = merged.loc[~mask_conf_only].reset_index(drop=True)
#   df=clean_df
#   return df

# def store_research_data2(df):
#     db.session.query(ResearchData).delete()
    
#     # Handle array types for PostgreSQL
#     records = df.to_dict('records')
#     for record in records:
#         record['fields_of_study'] = record.get('fields_of_study', [])
#         record['publication_types'] = record.get('publication_types', [])
#         db.session.add(ResearchData(**record))
    
#     db.session.commit()
