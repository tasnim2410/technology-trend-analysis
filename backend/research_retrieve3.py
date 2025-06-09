# import requests
# import pandas as pd
# import time
# import traceback
# from datetime import datetime
# from sqlalchemy import or_
# from db import db, ResearchData2, ImpactFactor ,ResearchData3
# from impact_factor_processor import clean_and_process_data , store_processed_data
# from flask import current_app
# import re, pandas as pd
# from datetime import date
# import pandas as pd
# from thefuzz import fuzz
# # Field-specific JIF thresholds (match your impact factor processor)
# FIELD_THRESHOLDS = {
#     'Medicine': 1.0,
#     'Biology': 1.0,
#     'Materials Science': 1.5,
#     'Computer Science': 0.8,
#     'Chemistry': 1.5,
#     'Mathematics': 0.7,
#     'Geology': 0.8,
#     'Physics': 1.0,
#     'Engineering': 1.5,
#     'Environmental Science': 0.5
# }

# def fetch_research_data2(query):
#     url = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
#     params = {
#         'query': query,
#         'fields': 'paperId,title,authors,venue,publicationVenue,year,publicationDate,citationCount,abstract,influentialCitationCount,fieldsOfStudy,publicationTypes',
#         'limit': 500
#     }
#     for attempt in range(5):
#         response = requests.get(url, params=params)
#         if response.status_code == 200:
#             return response.json().get('data', [])
#         elif response.status_code == 429:
#             time.sleep(60)
#     return []

# """working code"""
# def process_research_data2(papers , impact_factors=None):
#     rows = []
#     for paper in papers:
#         pub_venue = paper.get('publicationVenue') or {}
#         pub_venue_type = str(pub_venue.get('type', 'unknown'))[:50] if isinstance(pub_venue, dict) else 'unknown'
#         pub_venue_name = pub_venue.get('name') if isinstance(pub_venue, dict) else paper.get('venue', '')
#         issn = pub_venue.get('issn') if isinstance(pub_venue, dict) else None

#         authors = paper.get('authors', [])
#         author_names = [author.get('name', '') for author in authors]
#         pub_date = parse_date(paper.get('publicationDate'))
        
#         doi = None
#         disclaimer = paper.get('openAccessPdf', {}).get('disclaimer', '')
#         doi_match = re.search(r'https?://doi\.org/([^\s]+)', disclaimer)
#         if doi_match:
#           doi = doi_match.group(1).rstrip('.')


#         row = {
#             'paper_id': paper.get('paperId', '')[:255],
#             'title': paper.get('title', ''),
#             'abstract': paper.get('abstract', ''),
#             'publication_venue_name': pub_venue_name,
#             'publication_venue_type': pub_venue_type,
#             'year': paper.get('year'),
#             'citation_count': paper.get('citationCount'),
#             'influential_citation_count': paper.get('influentialCitationCount'),
#             'fields_of_study': paper.get('fieldsOfStudy', []),
#             'publication_types': paper.get('publicationTypes', []),
#             'publication_date': pub_date,
#             'authors': ', '.join(author_names)[:255] if author_names else None,
#             'DOI': doi,
#             'ISSN': issn
#         }
#         rows.append(row)

#     df = pd.DataFrame(rows)
    
#     # Clean and filter data
#     df = clean_base_data(df)
    
#     # Apply impact factor filtering
#     df = filter_by_impact_factor(df , impact_factors)
    
#     df = df.rename(columns={'Field': 'field', 'JIF5Years': 'jif_5years'})
    
#     return df


# """working code"""

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
#     # df['publication_types'] = df['publication_types'].apply(
#     #     lambda x: x if isinstance(x, list) else []
#     # )
#     return df.reset_index(drop=True)
  
# def normalize_name(col):
#     return col.str.lower().str.strip().str.replace(r'\s*&\s*', ' and ', regex=True)

# def filter_by_impact_factor(df,impact_factors=None):
#   # Normalize publication venue name
#   df = df.copy()
#   df['venue_norm'] = normalize_name(df['publication_venue_name'])

#   # Get the ImpactFactor data from the database
  
#   final_data = pd.DataFrame([{
#     'id': j.id,
#     'Name': j.name,
#     'Abbr Name': j.abbrev_name,
#     'issn': j.issn,
#     'JIF5Years': j.jif_5yr,
#     'subcategory': j.subcategory,
#     'Field': j.field,
    
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

#   lookup = final_data.set_index(['name_norm','abbr_norm'])[['subcategory','Field','JIF5Years']]
#   mask = merged['subcategory'].isna()
#   for idx in merged.index[mask]:
#     venue = merged.at[idx, 'venue_norm']
#     match = None
#     venue = str(venue) if venue is not None else ""

#     # 3a) look for any normalized name or abbr inside venue
#     for (nm, ab), row in lookup.iterrows():
#         if nm in venue or ab in venue:
#             match = row
#             break

#     if match is not None:
#         # ← here’s the fix: use .loc instead of .at
#         merged.loc[idx, ['subcategory','Field','JIF5Years']] = match.values
#   # Drop helper columns
#   merged = merged.drop(columns=[
#     'name_norm', 'abbr_norm', 'venue_norm',
#     'subcategory_abbr', 'Field_abbr', 'JIF5Years_abbr'
#   ], errors='ignore')
#   keep_types = {'JournalArticle', 'Review'}
#   mask = merged['publication_types'].apply(lambda types: isinstance(types, list) and bool(set(types) & keep_types))
#   clean_df = merged[mask].reset_index(drop=True)
#   mask_conf_only = merged['publication_types'].apply(lambda x: x == ['conference'])
#   clean_df = merged.loc[~mask_conf_only].reset_index(drop=True)
#   df=clean_df
#   df = df[df['subcategory'].notna()].reset_index(drop=True)

#   return df


# # def store_research_data2(df):
# #     # Clear existing data
# #     db.session.query(ResearchData2).delete()
    
# #     # Convert DataFrame to list of dictionaries
# #     records = df.to_dict('records')
# #     for record in records:
# #         # Ensure array types are handled for PostgreSQL
# #         record['fields_of_study'] = record.get('fields_of_study', [])
# #         record['publication_types'] = record.get('publication_types', [])
# #         # Create and add ResearchData instance
# #         db.session.add(ResearchData2(**record))
    
# #     # Commit changes to the database
# #     db.session.commit()


#   """openalex works retrieval """ 
    
# def fetch_openalex_works(max_docs=300, per_page=200):
#     # Validate per_page (OpenAlex allows 1-200 per request)
#     per_page = min(max(per_page, 1), 200)
    
#     filters = {
#         "title_and_abstract.search": "quantum",
#         "fulltext.search": "tunneling"  # Fixed: separate filter for fulltext
#     }

#     url = "https://api.openalex.org/works"
#     params = {
#         "filter": ",".join([f"{k}:{v}" for k, v in filters.items()]),
#         "per_page": per_page,
#         "cursor": "*"  # Initialize cursor for pagination
#     }

#     all_works = []
#     total_docs_retrieved = 0
    
#     while total_docs_retrieved < max_docs:
#         response = requests.get(url, params=params)
#         results = response.json()
        
#         # Add the current page results
#         current_works = results.get("results", [])
#         all_works.extend(current_works)
        
#         # Update counters
#         total_docs_retrieved += len(current_works)
        
#         # Check if we've reached our limit or end of results
#         if not current_works or total_docs_retrieved >= max_docs:
#             break
            
#         # Get next cursor for pagination
#         params["cursor"] = results.get("meta", {}).get("next_cursor")

#     return all_works[:max_docs]  # Return exactly max_docs

# import pandas as pd

# def process_documents(documents):
#     rows = []
    
#     for doc in documents:
#         # Handle nested structures safely with fallback defaults
#         primary_location = doc.get('primary_location') or {}
#         source = primary_location.get('source') or {}
#         primary_topic = doc.get('primary_topic') or {}
#         field = primary_topic.get('field') or {}
#         domain = primary_topic.get('domain') or {}
        
#         # Process keywords
#         keywords = doc.get('keywords', []) or []
#         keyword_names = [k.get('display_name', '') for k in keywords]
#         keyword_scores = [k.get('score', '') for k in keywords]
        
#         # Process locations with None handling
#         locations = doc.get('locations', []) or []
#         location_sources = [
#             (loc.get('source') or {}).get('display_name', '')  
#             for loc in locations if loc
#         ]
        
#         # Extract DOI (preferred top-level, fallback to ids)
#         raw_doi = doc.get('doi') or (doc.get('ids') or {}).get('doi') or ''
#         doi = raw_doi.split('doi.org/')[-1].strip() if raw_doi else ''

#         # Process authorships countries: each authorship dict has a 'countries' key
#         authorships = doc.get('authorships', []) or []
#         # collect all non-empty country values
#         countries = [auth.get('countries') for auth in authorships if auth.get('countries')]
#         # flatten if any entries are lists
#         flat_countries = []
#         for c in countries:
#             if isinstance(c, list):
#                 flat_countries.extend(c)
#             else:
#                 flat_countries.append(c)
#         # dedupe preserving order
#         unique_countries = list(dict.fromkeys(flat_countries))
#         countries_str = ", ".join(unique_countries)
        
#         row = {
#             'doi': doi,
#             'title': doc.get('title'),
#             'display_name': doc.get('display_name'),
#             'relevance_score': doc.get('relevance_score'),
#             'publication_year': doc.get('publication_year'),
#             'type_crossref': doc.get('type_crossref'),
#             'fwci': doc.get('fwci'),
            
#             # Primary location information
#             'primary_location.source.display_name': source.get('display_name'),
#             'primary_location.source.issn_l': source.get('issn_l'),
#             'primary_location.source.host_organization_name': source.get('host_organization_name'),
#             'primary_location.source.host_organization_lineage_names': " | ".join(
#                 source.get('host_organization_lineage_names', [])
#             ),
#             'primary_location.source.type': source.get('type'),
            
#             # Primary topic information
#             'primary_topic.display_name': primary_topic.get('display_name'),
#             'primary_topic.field.display_name': field.get('display_name'),
#             'primary_topic.domain.display_name': domain.get('display_name'),
            
#             # Keywords information
#             'keywords.display_name': ", ".join(filter(None, keyword_names)),
#             'keywords.score': ", ".join(map(str, keyword_scores)),
            
#             # Locations information
#             'locations.source.display_name': ", ".join(filter(None, location_sources)),

#             # Authorships countries extracted directly from each authorship dict
#             'authorships.countries': countries_str
#         }
        
#         rows.append(row)
#         df = pd.DataFrame(rows)
#         column_rename_map = {
#       'title': 'title',
#       'display_name': 'official_title',
#       'relevance_score': 'relevance',
#       'publication_year': 'publication_year',
#       'type_crossref': 'document_type',
#       'fwci': 'field_weighted_citation_impact',
#       'doi': 'DOI',
    
#       # Primary location information
#       'primary_location.source.display_name': 'journal_name',
#       'primary_location.source.issn_l': 'journal_issn_l',
#       'primary_location.source.host_organization_name': 'publisher',
#       'primary_location.source.host_organization_lineage_names': 'publisher_hierarchy',
#       'primary_location.source.type': 'source_type',
    
#       # Primary topic information
#       'primary_topic.display_name': 'main_topic',
#       'primary_topic.field.display_name': 'research_field',
#       'primary_topic.domain.display_name': 'academic_domain',
    
#       # Keywords information
#       'keywords.display_name': 'keywords',
#       'keywords.score': 'keyword_relevance_score',
    
#       # Locations information
#       'locations.source.display_name': 'all_journal_sources',
#       'authorships.countries': 'authorships_countries'

#       } 
#         df = df.rename(columns=column_rename_map)
#     return df
  


# def clean_issn(issn: str) -> str | None:
#     """
#     Remove hyphens and whitespace from an ISSN, convert to uppercase.
#     Returns None for NaN or empty values.
#     """
#     if pd.isna(issn):
#         return None
#     s = str(issn).replace("-", "").strip().upper()
#     return s if s else None

# def match_papers_by_issn(
#     journal_df: pd.DataFrame,
#     paper_df: pd.DataFrame,
#     issn_col_journal: str = 'ISSN',
#     issn_col_paper: str = 'journal_issn_l',
#     field_col: str = 'Field'
# ) -> tuple[pd.DataFrame, pd.DataFrame, dict]:

#     # 1) Clean ISSN columns
#     jdf = journal_df.copy()
#     jdf['ISSN_clean'] = jdf[issn_col_journal].apply(clean_issn)

#     pdf = paper_df.copy()
#     pdf['ISSN_clean'] = pdf[issn_col_paper].apply(clean_issn)

#     # 2) Build ISSN → field lookup
#     issn_to_field: dict[str, str] = {}
#     for _, row in jdf.iterrows():
#         issn = row['ISSN_clean']
#         if issn:
#             # if multiple ISSNs per row, you could split on commas here
#             issn_to_field[issn] = row[field_col]

#     # 3) Map field over papers
#     pdf['Field'] = pdf['ISSN_clean'].map(issn_to_field)

#     # 4) Partition matched / unmatched
#     matched_mask = pdf['Field'].notna()
#     matched_df   = pdf[matched_mask].reset_index(drop=True)
#     unmatched_df = pdf[~matched_mask].reset_index(drop=True)

#     # 5) Build summary
#     summary = {
#         'total':    len(pdf),
#         'matched':  matched_mask.sum(),
#         'unmatched': len(pdf) - matched_mask.sum()
#     }

#     return matched_df, unmatched_df, summary

# def renaming_columns(
#     sem_df: pd.DataFrame,
#     AIex_df: pd.DataFrame
# ) -> tuple[pd.DataFrame, pd.DataFrame]:

#     sem_df.rename(
#         columns={
#             'PublicationVenueName': 'journal_name',
#             'Title': 'title',
#         },
#         inplace=True
#     )
    
#     AIex_df.rename(
#         columns={
#             'document_type':   'PublicationTypes',
#             'Journal_issn_l':  'ISSN',
#             'research_field':  'FieldsOfStudy',
#             'publication_year':'Year',
#         },
#         inplace=True
#     )
    
#     return sem_df, AIex_df
# def clean_doi(raw):
#     if pd.isna(raw): return None
#     s = str(raw).strip().lower()
#     s = re.sub(r'^https?://(dx\.)?doi\.org/', '', s)
#     s = re.sub(r'^doi:\s*', '', s)
#     s = re.split(r'[\?#]', s)[0]
#     return s.rstrip('/') or None
#   #sem_df['doi_clean']   = sem_df['DOI'].apply(clean_doi)
#   #AIex_df['doi_clean']  = AIex_df['DOI'].apply(clean_doi)


# def merge_unique_by_doi(
#     sem_df: pd.DataFrame,
#     AIex_df: pd.DataFrame,
#     doi_col: str = 'doi_clean',
#     title_col: str = 'title',
#     fuzz_threshold: int = 85
# ) -> pd.DataFrame:

#     # 1) Exact matches on DOI
#     sem_dois = sem_df[[doi_col]].dropna().drop_duplicates()
#     aiex_dois = AIex_df[[doi_col]].dropna().drop_duplicates()
#     exact_matches = pd.merge(sem_dois, aiex_dois, on=doi_col, how='inner')

#     # 2) Filter sem_df to those not in exact matches
#     sem_non = sem_df[~sem_df[doi_col].isin(exact_matches[doi_col])].copy()

#     # 3) Prepare AIex pool for fuzzy matching (exclude exact matches & null DOIs)
#     aiex_pool = AIex_df[~AIex_df[doi_col].isin(exact_matches[doi_col])].copy()
#     aiex_pool = aiex_pool[aiex_pool[doi_col].notna()]

#     # 4) Fuzzy-match by title
#     fuzzy_dois = set()
#     for _, s_row in sem_non.iterrows():
#         s_doi = s_row[doi_col]
#         if pd.isna(s_doi):
#             continue
#         s_title = str(s_row[title_col]).lower().strip()
#         best_score = 0
#         best_doi = None
#         for _, a_row in aiex_pool.iterrows():
#             score = fuzz.token_set_ratio(s_title, str(a_row[title_col]).lower().strip())
#             if score > best_score:
#                 best_score = score
#                 best_doi = a_row[doi_col]
#         if best_score >= fuzz_threshold:
#             fuzzy_dois.add(s_doi)

#     # 5) Drop any sem_df rows with DOIs in exact or fuzzy matches
#     to_drop = set(exact_matches[doi_col]).union(fuzzy_dois)
#     sem_filtered = sem_df[~sem_df[doi_col].isin(to_drop)].copy()

#     # 6) Concatenate AIex_df + filtered sem_df
#     final_df = pd.concat([AIex_df, sem_filtered], ignore_index=True)
#     return final_df


# # def store_research_data3(df):
# #     """
# #     Wipe and reload research_data2 from a pandas DataFrame.
# #     Assumes df columns roughly mirror the model attributes:
# #       - 'doi_clean' → paper_id
# #       - 'DOI' → doi
# #       - 'Year' → year
# #       - 'PublicationTypes' → publication_types (list)
# #       - 'FieldsOfStudy' → fields_of_study (list)
# #       - 'publisher_hierarchy', 'keywords', 'all_journal_sources', 'authorships_countries' are lists
# #       - 'PublicationDate' convertible to datetime.date
# #     """
# #     # 1) Clear existing table
# #     db.session.query(ResearchData3).delete()

# #     # 2) Prepare and insert new records
# #     records = df.to_dict('records')
# #     for rec in records:
# #         # map/rename keys
# #         rec_mapped = {
# #             'paper_id': rec.get('doi_clean'),
# #             'doi': rec.get('DOI'),
# #             'title': rec.get('title'),
# #             'official_title': rec.get('official_title'),
# #             'abstract': rec.get('abstract'),
# #             'relevance': rec.get('relevance'),
# #             'year': rec.get('Year'),
# #             'publication_date': rec.get('PublicationDate'),
# #             'publication_types': rec.get('PublicationTypes') or [],
# #             'source_type': rec.get('source_type'),
# #             'publication_venue_name': rec.get('journal_name'),
# #             'publication_venue_type': rec.get('PublicationVenueType'),
# #             'journal_name': rec.get('journal_name'),
# #             'journal_issn_l': rec.get('journal_issn_l'),
# #             'journal_issn_l_clean': rec.get('journal_issn_l_clean'),
# #             'issn': rec.get('ISSN'),
# #             'publisher': rec.get('publisher'),
# #             'publisher_hierarchy': rec.get('publisher_hierarchy') or [],
# #             'main_topic': rec.get('main_topic'),
# #             'fields_of_study': rec.get('FieldsOfStudy') or [],
# #             'academic_domain': rec.get('academic_domain'),
# #             'subcategory': rec.get('subcategory'),
# #             'keyword_relevance_score': rec.get('keyword_relevance_score'),
# #             'keywords': rec.get('keywords') or [],
# #             'all_journal_sources': rec.get('all_journal_sources') or [],
# #             'authorships_countries': rec.get('authorships_countries') or [],
# #             'reference_count': rec.get('ReferenceCount'), 
# #             'influential_citation_count': rec.get('InfluentialCitationCount'),
# #             'field_weighted_citation_impact': rec.get('field_weighted_citation_impact'),
# #             'jif_5years': rec.get('JIF5Years'),
# #         }

# #         # parse date if it’s a string
# #         pd_date = rec_mapped.get('publication_date')
# #         if isinstance(pd_date, str):
# #             rec_mapped['publication_date'] = date.fromisoformat(pd_date)

# #         db.session.add(ResearchData3(**rec_mapped))

# #     # 3) Commit
# #     db.session.commit()

def store_research_data3(df):
    records = df.to_dict('records')
    array_fields = [
        'publication_types', 'fields_of_study', 'publisher_hierarchy',
        'keywords', 'all_journal_sources', 'authorships_countries'
    ]
    float_fields = [
        'relevance', 'keyword_relevance_score', 'field_weighted_citation_impact', 'jif_5years'
    ]
    int_fields = ['year', 'citation_count', 'influential_citation_count', 'reference_count']

    for rec in records:
        # Ensure array fields are lists
        for field in array_fields:
            value = rec.get(field)
            if not isinstance(value, list):
                rec[field] = []
                
        for field in float_fields:
            value = rec.get(field)
            if value is not None and not isinstance(value, (float, int)):
                rec[field] = None
                
        for field in int_fields:
            value = rec.get(field)
            if value is not None and not isinstance(value, int):
                rec[field] = None

#         # Create model instance
#         research_entry = ResearchData3(
#             paper_id=rec.get('paper_id', ''),
#             doi=rec.get('doi'),
#             title=rec.get('title', ''),
#             official_title=rec.get('official_title'),
#             abstract=rec.get('abstract'),
#             relevance=rec.get('relevance'),
#             year=rec.get('year'),
#             publication_date=rec.get('publication_date'),
#             publication_types=rec.get('publication_types', []),
#             source_type=rec.get('source_type'),
#             publication_venue_name=rec.get('publication_venue_name'),
#             publication_venue_type=rec.get('publication_venue_type'),
#             journal_name=rec.get('journal_name'),
#             journal_issn_l=rec.get('journal_issn_l'),
#             journal_issn_l_clean=rec.get('journal_issn_l_clean'),
#             issn=rec.get('issn'),
#             publisher=rec.get('publisher'),
#             publisher_hierarchy=rec.get('publisher_hierarchy', []),
#             main_topic=rec.get('main_topic'),
#             fields_of_study=rec.get('fields_of_study', []),
#             academic_domain=rec.get('academic_domain'),
#             subcategory=rec.get('subcategory'),
#             keyword_relevance_score=rec.get('keyword_relevance_score'),
#             keywords=rec.get('keywords', []),
#             all_journal_sources=rec.get('all_journal_sources', []),
#             authorships_countries=rec.get('authorships_countries', []),
#             reference_count=rec.get('reference_count'),
#             citation_count=rec.get('citation_count'),
#             influential_citation_count=rec.get('influential_citation_count'),
#             field_weighted_citation_impact=rec.get('field_weighted_citation_impact'),
#             jif_5years=rec.get('jif_5years')
#         )
#         db.session.add(research_entry)
#     db.session.commit()




""""claude code """



import requests
import pandas as pd
import time
import traceback
from datetime import datetime
from sqlalchemy import or_
from db import db, ResearchData2, ImpactFactor ,ResearchData3
from impact_factor_processor import clean_and_process_data , store_processed_data
from flask import current_app
import re, pandas as pd
from datetime import date
import pandas as pd
from thefuzz import fuzz
import numpy as np

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

def fetch_research_data3(query):
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

def safe_float_convert(value):
    """Safely convert value to float, handling NaN, None, and invalid strings"""
    if value is None or value == '' or pd.isna(value):
        return None
    
    if isinstance(value, str):
        # Check if it's a valid float string
        try:
            float_val = float(value)
            # Check for NaN or infinity
            if np.isnan(float_val) or np.isinf(float_val):
                return None
            return float_val
        except (ValueError, TypeError):
            return None
    
    if isinstance(value, (int, float)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)
    
    return None

def safe_int_convert(value):
    """Safely convert value to int, handling None and invalid values"""
    if value is None or value == '' or pd.isna(value):
        return None
    
    try:
        if isinstance(value, str):
            # Remove any non-numeric characters except minus sign
            cleaned = re.sub(r'[^\d-]', '', value)
            if not cleaned or cleaned == '-':
                return None
            return int(cleaned)
        return int(value)
    except (ValueError, TypeError):
        return None

def process_research_data3(papers, impact_factors=None):
    rows = []
    for paper in papers:
        pub_venue = paper.get('publicationVenue') or {}
        pub_venue_type = str(pub_venue.get('type', 'unknown'))[:50] if isinstance(pub_venue, dict) else 'unknown'
        pub_venue_name = pub_venue.get('name') if isinstance(pub_venue, dict) else paper.get('venue', '')
        issn = pub_venue.get('issn') if isinstance(pub_venue, dict) else None

        authors = paper.get('authors', [])
        author_names = [author.get('name', '') for author in authors]
        pub_date = parse_date(paper.get('publicationDate'))
        
        doi = None
        disclaimer = paper.get('openAccessPdf', {}).get('disclaimer', '')
        doi_match = re.search(r'https?://doi\.org/([^\s]+)', disclaimer)
        if doi_match:
            doi = doi_match.group(1).rstrip('.')

        row = {
            'paper_id': paper.get('paperId', '')[:255],
            'title': paper.get('title', ''),
            'abstract': paper.get('abstract', ''),
            'publication_venue_name': pub_venue_name,
            'publication_venue_type': pub_venue_type,
            'year': safe_int_convert(paper.get('year')),
            'citation_count': safe_int_convert(paper.get('citationCount')),
            'influential_citation_count': safe_int_convert(paper.get('influentialCitationCount')),
            'fields_of_study': paper.get('fieldsOfStudy', []),
            'publication_types': paper.get('publicationTypes', []),
            'publication_date': pub_date,
            'authors': ', '.join(author_names)[:255] if author_names else None,
            'DOI': doi,
            'ISSN': issn
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    
    # Clean and filter data
    df = clean_base_data(df)
    
    # Apply impact factor filtering
    df = filter_by_impact_factor(df, impact_factors)
    
    df = df.rename(columns={'Field': 'field', 'JIF5Years': 'jif_5years'})
    
    return df

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date() if date_str else None
    except:
        return None

def clean_base_data(df):
    # Convert and filter year - using safe conversion
    df['year'] = df['year'].apply(safe_int_convert)
    df = df[~((df['publication_venue_name'] == '') | (df['year'].isna()))]
    
    return df.reset_index(drop=True)
  
def normalize_name(col):
    return col.str.lower().str.strip().str.replace(r'\s*&\s*', ' and ', regex=True)

def filter_by_impact_factor(df, impact_factors=None):
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

        # Look for any normalized name or abbr inside venue
        for (nm, ab), row in lookup.iterrows():
            if nm in venue or ab in venue:
                match = row
                break

        if match is not None:
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
    df = clean_df
    df = df[df['subcategory'].notna()].reset_index(drop=True)

    return df

def fetch_openalex_works(max_docs=300, per_page=200):
    # Validate per_page (OpenAlex allows 1-200 per request)
    per_page = min(max(per_page, 1), 200)
    
    filters = {
        "title_and_abstract.search": "quantum",
        "fulltext.search": "tunneling"
    }

    url = "https://api.openalex.org/works"
    params = {
        "filter": ",".join([f"{k}:{v}" for k, v in filters.items()]),
        "per_page": per_page,
        "cursor": "*"
    }

    all_works = []
    total_docs_retrieved = 0
    
    while total_docs_retrieved < max_docs:
        response = requests.get(url, params=params)
        results = response.json()
        
        current_works = results.get("results", [])
        all_works.extend(current_works)
        
        total_docs_retrieved += len(current_works)
        
        if not current_works or total_docs_retrieved >= max_docs:
            break
            
        params["cursor"] = results.get("meta", {}).get("next_cursor")

    return all_works[:max_docs]
  
def clean_issn(issn: str) -> str | None:
    """
    Clean ISSN string but preserve hyphens.
    Only removes whitespace and converts to uppercase.
    Returns None for NaN or empty values.
    """
    if pd.isna(issn):
        return None
    s = str(issn).strip().upper()
    return s if s else None

def process_documents(documents, journals_df):

    
    # Clean journals_df ISSN and create mapping
    journal_data = [{
        'issn': j.issn,
        'field': j.field
    } for j in journals_df]
    
    # Create ISSN to field mapping
    issn_to_field = {}
    for journal in journal_data:
        if journal['issn']:
            clean_issn_val = clean_issn(journal['issn'])
            if clean_issn_val:
                issn_to_field[clean_issn_val] = journal['field']
    
    rows = []
    for doc in documents:
        # Handle nested structures safely with fallback defaults
        primary_location = doc.get('primary_location') or {}
        source = primary_location.get('source') or {}
        primary_topic = doc.get('primary_topic') or {}
        field = primary_topic.get('field') or {}
        domain = primary_topic.get('domain') or {}
        
        # Clean ISSN from source
        journal_issn = clean_issn(source.get('issn_l'))
        
        # Process keywords
        keywords = doc.get('keywords', []) or []
        keyword_names = [k.get('display_name', '') for k in keywords]
        keyword_scores = [k.get('score', '') for k in keywords]
        
        # Process locations
        locations = doc.get('locations', []) or []
        location_sources = [
            (loc.get('source') or {}).get('display_name', '')  
            for loc in locations if loc
        ]
        
        # Extract DOI
        raw_doi = doc.get('doi') or (doc.get('ids') or {}).get('doi') or ''
        doi = raw_doi.split('doi.org/')[-1].strip() if raw_doi else ''
        
        # Process authorships countries
        authorships = doc.get('authorships', []) or []
        countries = [auth.get('countries') for auth in authorships if auth.get('countries')]
        flat_countries = []
        for c in countries:
            if isinstance(c, list):
                flat_countries.extend(c)
            else:
                flat_countries.append(c)
        unique_countries = list(dict.fromkeys(flat_countries))
        
        row = {
            'doi': doi,
            'title': doc.get('title', ''),
            'official_title': doc.get('display_name', ''),
            'abstract': doc.get('abstract', ''),
            'relevance': safe_float_convert(doc.get('relevance_score')),
            'year': safe_int_convert(doc.get('publication_year')),
            'publication_date': None,  # OpenAlex doesn't provide this
            'publication_types': [doc.get('type', '')] if doc.get('type') else [],
            'source_type': source.get('type'),
            'publication_venue_name': source.get('display_name'),
            'journal_name': source.get('display_name'),
            'journal_issn_l': source.get('issn_l'),
            'journal_issn_l_clean': journal_issn,
            'issn': source.get('issn_l'),
            'publisher': source.get('host_organization_name'),
            'publisher_hierarchy': source.get('host_organization_lineage_names', []),
            'main_topic': primary_topic.get('display_name'),
            'fields_of_study': [field.get('display_name')] if field.get('display_name') else [],
            'academic_domain': domain.get('display_name'),
            'subcategory': None,  # Will be filled from journals mapping
            'keyword_relevance_score': safe_float_convert(keyword_scores[0]) if keyword_scores else None,
            'keywords': keyword_names,
            'all_journal_sources': [
                loc.get('source', {}).get('display_name')
                for loc in doc.get('locations', []) if isinstance(loc, dict) and isinstance(loc.get('source', {}), dict)
            ],
            'authorships_countries': unique_countries,
            'reference_count': safe_int_convert(doc.get('referenced_works_count')),
            'citation_count': safe_int_convert(doc.get('cited_by_count')),
            'influential_citation_count': None,  # OpenAlex doesn't provide this
            'field_weighted_citation_impact': safe_float_convert(doc.get('works_count')),
            'jif_5years': None,  # Will be filled from journals mapping
            'Field': issn_to_field.get(journal_issn)  # Map the field from journals_df
        }
        
        rows.append(row)
    
    return pd.DataFrame(rows)


# def process_documents(documents):
#     rows = []
    
#     for doc in documents:
#         # Handle nested structures safely with fallback defaults
#         primary_location = doc.get('primary_location') or {}
#         source = primary_location.get('source') or {}
#         primary_topic = doc.get('primary_topic') or {}
#         field = primary_topic.get('field') or {}
#         domain = primary_topic.get('domain') or {}
        
#         # Process keywords
#         keywords = doc.get('keywords', []) or []
#         keyword_names = [k.get('display_name', '') for k in keywords]
#         keyword_scores = [k.get('score', '') for k in keywords]
        
#         # Process locations with None handling
#         locations = doc.get('locations', []) or []
#         location_sources = [
#             (loc.get('source') or {}).get('display_name', '')  
#             for loc in locations if loc
#         ]
        
#         # Extract DOI (preferred top-level, fallback to ids)
#         raw_doi = doc.get('doi') or (doc.get('ids') or {}).get('doi') or ''
#         doi = raw_doi.split('doi.org/')[-1].strip() if raw_doi else ''

#         # Process authorships countries
#         authorships = doc.get('authorships', []) or []
#         countries = [auth.get('countries') for auth in authorships if auth.get('countries')]
#         flat_countries = []
#         for c in countries:
#             if isinstance(c, list):
#                 flat_countries.extend(c)
#             else:
#                 flat_countries.append(c)
#         unique_countries = list(dict.fromkeys(flat_countries))
#         countries_str = ", ".join(unique_countries)
        
#         row = {
#             'doi': doi,
#             'title': doc.get('title'),
#             'display_name': doc.get('display_name'),
#             'relevance_score': safe_float_convert(doc.get('relevance_score')),
#             'publication_year': safe_int_convert(doc.get('publication_year')),
#             'type_crossref': doc.get('type_crossref'),
#             'fwci': safe_float_convert(doc.get('fwci')),
            
#             # Primary location information
#             'primary_location.source.display_name': source.get('display_name'),
#             'primary_location.source.issn_l': source.get('issn_l'),
#             'primary_location.source.host_organization_name': source.get('host_organization_name'),
#             'primary_location.source.host_organization_lineage_names': " | ".join(
#                 source.get('host_organization_lineage_names', [])
#             ),
#             'primary_location.source.type': source.get('type'),
            
#             # Primary topic information
#             'primary_topic.display_name': primary_topic.get('display_name'),
#             'primary_topic.field.display_name': field.get('display_name'),
#             'primary_topic.domain.display_name': domain.get('display_name'),
            
#             # Keywords information
#             'keywords.display_name': ", ".join(filter(None, keyword_names)),
#             'keywords.score': ", ".join(map(str, keyword_scores)),
            
#             # Locations information
#             'locations.source.display_name': ", ".join(filter(None, location_sources)),
#             'authorships.countries': countries_str
#         }
        
#         rows.append(row)
    
#     df = pd.DataFrame(rows)
#     column_rename_map = {
#         'title': 'title',
#         'display_name': 'official_title',
#         'relevance_score': 'relevance',
#         'publication_year': 'publication_year',
#         'type_crossref': 'document_type',
#         'fwci': 'field_weighted_citation_impact',
#         'doi': 'DOI',
        
#         # Primary location information
#         'primary_location.source.display_name': 'journal_name',
#         'primary_location.source.issn_l': 'journal_issn_l',
#         'primary_location.source.host_organization_name': 'publisher',
#         'primary_location.source.host_organization_lineage_names': 'publisher_hierarchy',
#         'primary_location.source.type': 'source_type',
        
#         # Primary topic information
#         'primary_topic.display_name': 'main_topic',
#         'primary_topic.field.display_name': 'research_field',
#         'primary_topic.domain.display_name': 'academic_domain',
        
#         # Keywords information
#         'keywords.display_name': 'keywords',
#         'keywords.score': 'keyword_relevance_score',
        
#         # Locations information
#         'locations.source.display_name': 'all_journal_sources',
#         'authorships.countries': 'authorships_countries'
#     } 
#     df = df.rename(columns=column_rename_map)
#     return df

# def clean_issn(issn: str) -> str | None:
#     """Remove hyphens and whitespace from an ISSN, convert to uppercase."""
#     if pd.isna(issn):
#         return None
#     s = str(issn).replace("-", "").strip().upper()
#     return s if s else None

def match_papers_by_issn(
    journal_df: pd.DataFrame,
    paper_df: pd.DataFrame,
    issn_col_journal: str = 'ISSN',
    issn_col_paper: str = 'journal_issn_l',
    field_col: str = 'Field'
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:

    # Clean ISSN columns
    jdf = journal_df.copy()
    jdf['ISSN_clean'] = jdf[issn_col_journal].apply(clean_issn)

    pdf = paper_df.copy()
    pdf['ISSN_clean'] = pdf[issn_col_paper].apply(clean_issn)

    # Build ISSN → field lookup
    issn_to_field: dict[str, str] = {}
    for _, row in jdf.iterrows():
        issn = row['ISSN_clean']
        if issn:
            issn_to_field[issn] = row[field_col]

    # Map field over papers
    pdf['Field'] = pdf['ISSN_clean'].map(issn_to_field)

    # Partition matched / unmatched
    matched_mask = pdf['Field'].notna()
    matched_df   = pdf[matched_mask].reset_index(drop=True)
    unmatched_df = pdf[~matched_mask].reset_index(drop=True)

    # Build summary
    summary = {
        'total':    len(pdf),
        'matched':  matched_mask.sum(),
        'unmatched': len(pdf) - matched_mask.sum()
    }

    return matched_df, unmatched_df, summary

def renaming_columns(
    sem_df: pd.DataFrame,
    AIex_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:

    sem_df.rename(
        columns={
            'PublicationVenueName': 'journal_name',
            'Title': 'title',
        },
        inplace=True
    )
    
    AIex_df.rename(
        columns={
            'document_type':   'PublicationTypes',
            'Journal_issn_l':  'ISSN',
            'research_field':  'FieldsOfStudy',
            'publication_year':'Year',
            
        },
        inplace=True
    )
    
    return sem_df, AIex_df
def clean_doi(raw):
    if pd.isna(raw): return None
    s = str(raw).strip().lower()
    s = re.sub(r'^https?://(dx\.)?doi\.org/', '', s)
    s = re.sub(r'^doi:\s*', '', s)
    s = re.split(r'[\?#]', s)[0]
    return s.rstrip('/') or None
  #sem_df['doi_clean']   = sem_df['DOI'].apply(clean_doi)
  #AIex_df['doi_clean']  = AIex_df['DOI'].apply(clean_doi)


def merge_unique_by_doi(
    sem_df: pd.DataFrame,
    AIex_df: pd.DataFrame,
    doi_col: str = 'doi_clean',
    title_col: str = 'title',
    fuzz_threshold: int = 85
) -> pd.DataFrame:

    # Exact matches on DOI
    sem_dois = sem_df[[doi_col]].dropna().drop_duplicates()
    aiex_dois = AIex_df[[doi_col]].dropna().drop_duplicates()
    exact_matches = pd.merge(sem_dois, aiex_dois, on=doi_col, how='inner')

    # Filter sem_df to those not in exact matches
    sem_non = sem_df[~sem_df[doi_col].isin(exact_matches[doi_col])].copy()

    # Prepare AIex pool for fuzzy matching
    aiex_pool = AIex_df[AIex_df[doi_col].isin(exact_matches[doi_col])].copy()
    aiex_pool = aiex_pool[aiex_pool[doi_col].notna()]

    # Fuzzy-match by title
    fuzzy_dois = set()
    for _, s_row in sem_non.iterrows():
        s_doi = s_row[doi_col]
        if pd.isna(s_doi):
            continue
        s_title = str(s_row[title_col]).lower().strip()
        best_score = 0
        best_doi = None
        for _, a_row in aiex_pool.iterrows():
            score = fuzz.token_set_ratio(s_title, str(a_row[title_col]).lower().strip())
            if score > best_score:
                best_score = score
                best_doi = a_row[doi_col]
        if best_score >= fuzz_threshold:
            fuzzy_dois.add(s_doi)

    # Drop any sem_df rows with DOIs in exact or fuzzy matches
    to_drop = set(exact_matches[doi_col]).union(fuzzy_dois)
    sem_filtered = sem_df[~sem_df[doi_col].isin(to_drop)].copy()

    # Concatenate AIex_df + filtered sem_df
    final_df = pd.concat([AIex_df, sem_filtered], ignore_index=True)
    final_df['doi'] = final_df['doi'].combine_first(final_df['doi'])
    final_df['year'] = final_df['year'].combine_first(final_df['year'])
    final_df.drop(columns=['DOI', 'Year'], inplace=True, errors='ignore')
    return final_df

def store_research_data3(df):
    """Store research data with proper type conversion and validation"""
    
    # Clear existing data
    db.session.query(ResearchData3).delete()
    
    records = df.to_dict('records')
    array_fields = [
        'publication_types', 'fields_of_study', 'publisher_hierarchy',
        'keywords', 'all_journal_sources', 'authorships_countries'
    ]
    float_fields = [
        'relevance', 'keyword_relevance_score', 'field_weighted_citation_impact', 'jif_5years'
    ]
    int_fields = ['year', 'citation_count', 'influential_citation_count', 'reference_count']

    for rec in records:
        # Ensure array fields are lists
        for field in array_fields:
            value = rec.get(field)
            if not isinstance(value, list):
                if isinstance(value, str) and value:
                    # Split comma-separated strings into lists
                    rec[field] = [item.strip() for item in value.split(',') if item.strip()]
                else:
                    rec[field] = []
                
        # Safe conversion of float fields
        for field in float_fields:
            rec[field] = safe_float_convert(rec.get(field))
                
        # Safe conversion of int fields
        for field in int_fields:
            rec[field] = safe_int_convert(rec.get(field))

        # Handle publication_date
        pub_date = rec.get('publication_date')
        if isinstance(pub_date, str):
            try:
                rec['publication_date'] = date.fromisoformat(pub_date)
            except (ValueError, TypeError):
                rec['publication_date'] = None
        elif not isinstance(pub_date, date):
            rec['publication_date'] = None

        # Create model instance with safe values
        research_entry = ResearchData3(
            paper_id=str(rec.get('paper_id', ''))[:255],  # Ensure string and limit length
            doi=str(rec.get('doi') or rec.get('DOI', ''))[:255] if rec.get('doi') or rec.get('DOI') else None,            title=str(rec.get('title', ''))[:5000],  # Limit title length
            official_title=str(rec.get('official_title', ''))[:5000] if rec.get('official_title') else None,
            abstract=str(rec.get('abstract', ''))[:10000] if rec.get('abstract') else None,
            relevance=rec.get('relevance'),
            year=rec.get('year') if 'year' in rec else rec.get('Year'),            publication_date=rec.get('publication_date'),
            publication_types=rec.get('publication_types', []),
            source_type=str(rec.get('source_type', ''))[:50] if rec.get('source_type') else None,
            publication_venue_name=str(rec.get('publication_venue_name', ''))[:500] if rec.get('publication_venue_name') else None,
            publication_venue_type=str(rec.get('publication_venue_type', ''))[:100] if rec.get('publication_venue_type') else None,
            journal_name=str(rec.get('journal_name', ''))[:255] if rec.get('journal_name') else None,
            journal_issn_l=str(rec.get('journal_issn_l', ''))[:50] if rec.get('journal_issn_l') else None,
            journal_issn_l_clean=str(rec.get('journal_issn_l_clean', ''))[:50] if rec.get('journal_issn_l_clean') else None,
            issn=str(rec.get('issn', ''))[:50] if rec.get('issn') else None,
            publisher=str(rec.get('publisher', ''))[:255] if rec.get('publisher') else None,
            publisher_hierarchy=rec.get('publisher_hierarchy', []),
            main_topic=str(rec.get('main_topic', ''))[:100] if rec.get('main_topic') else None,
            fields_of_study=rec.get('fields_of_study', []),
            academic_domain=str(rec.get('academic_domain', ''))[:100] if rec.get('academic_domain') else None,
            subcategory=str(rec.get('subcategory', ''))[:100] if rec.get('subcategory') else None,
            keyword_relevance_score=rec.get('keyword_relevance_score'),
            keywords=rec.get('keywords', []),
            all_journal_sources=rec.get('all_journal_sources', []),
            authorships_countries=rec.get('authorships_countries', []),
            reference_count=rec.get('reference_count'),
            citation_count=rec.get('citation_count'),
            influential_citation_count=rec.get('influential_citation_count'),
            field_weighted_citation_impact=rec.get('field_weighted_citation_impact'),
            jif_5years=rec.get('jif_5years')
        )
        db.session.add(research_entry)
    
    db.session.commit()