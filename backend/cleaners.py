# technology-trend-analysis/backend/cleaners.py
import pandas as pd
import re
import ast

def clean_espacenet_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Espacenet CSV data."""
    df = df.dropna(how='all')
    if 'Unnamed: 11' in df.columns:
        df = df.drop('Unnamed: 11', axis=1)
    # Rename columns (French → English)
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
    
    # Drop empty rows and unwanted columns


   #cleaning the data columns
    df[['first publication date','second publication date']] = df['Publication date'].str.split(' ' , n=1 , expand= True)
    df['second publication date'] = df['second publication date'].str.strip('\n')
    df['second publication date'] = df['second publication date'].str.strip('\r')
    df['second publication date'] = df['second publication date'].str.strip('\n')
        #changing the format to datetime
    df['first publication date'] = pd.to_datetime(
    df['first publication date'].str.strip(), 
        format='mixed'
          )
        
    df['first filing year'] = df['first publication date'].dt.year
    df[['first publication number', 'second publication number']] = df['Publication number'].str.split(' ' , n=1 , expand=True)
    df['second publication number']=df['second publication number'].str.strip('\n')
    df['first publication country'] = df['first publication number'].str[:2]
    df['second publication country'] = df['second publication number'].str[:2]
    
    
    df['Earliest priority'] = pd.to_datetime(df['Earliest priority'])
    df['earliest priority year'] = df['Earliest priority'].dt.year
    df['applicant country'] = df['Applicants'].str.extract(r'\[([A-Z]{2})\]')
    df['Applicants'] = df['Applicants'].fillna(df['Inventors'])
        #filling missing CPC values 
    df['CPC'] = df['CPC'].fillna('unkown')
    df['IPC'] = df['IPC'].str.split(r'\s+')
    
    cpc_expanded = []
    for classification in df['CPC']:
        parts = re.split(r'\)\s+', classification)
        # Ensure each part ends with ')'
        parts = [p + ')' if not p.endswith(')') else p for p in parts]
        cpc_expanded.append(parts)
    df['CPC'] = cpc_expanded
 
    # Rename columns to use underscores
    df.rename(columns={
        'first publication date': 'first_publication_date',
        'Earliest publication': 'earliest_publication',
        'second publication date': 'second_publication_date',
        'first filing year': 'first_filing_year',
        'earliest priority year': 'earliest_priority_year',
        'applicant country': 'applicant_country',
        'first publication number': 'first_publication_number',
        'second publication number': 'second_publication_number',
        'first publication country': 'first_publication_country',
        'second publication country': 'second_publication_country'
    }, inplace=True)
    df = df.drop('Publication date', axis=1)
    return df
    
    # if 'Publication number' in df.columns:
    #   df = process_dataframe(df, patent_col='Publication number')


def clean_family_members(family_data):
        """
        Clean the family_members column:
        - Convert string representations of lists to actual lists.
        - Remove semicolons and empty strings.
        """
        # If the input is a string, try to evaluate it as a list
        if isinstance(family_data, str):
            try:
                family_data = ast.literal_eval(family_data)  # Convert string to list
            except (ValueError, SyntaxError):
                # If evaluation fails, treat it as a single-item list
                family_data = [family_data]
    
        # If the input is already a list, proceed
        if isinstance(family_data, list):
            # Remove semicolons and empty strings
            cleaned_list = [item.replace(';', '') for item in family_data if item.replace(';', '') != '']
            return cleaned_list
    
        # If the input is neither a string nor a list, return an empty list
        return []
    


def extract_country_codes(family_data):
    """
    Extract country codes from the family_members column.
    Handles both string representations of lists and actual lists.
    """
    # If the input is a string representation of a list
    if isinstance(family_data, str):
        # Check if it's an empty list string
        if family_data == '[]':
            return []
        # Use the string-based extraction function
        return extract_country_codes_from_str(family_data)
    
    # If the input is an actual list
    elif isinstance(family_data, list):
        # Use the list-based extraction function
        return extract_country_codes_from_list(family_data)
    
    # If the input is neither, return an empty list
    return []

def extract_country_codes_from_str(family_str):
    """
    Extract country codes from a string representation of a list.
    """
    if not isinstance(family_str, str) or family_str == '[]':
        return []
    
    # This pattern finds anything that looks like a patent number within quotes
    pattern = r"'([A-Z]{2}[A-Z0-9]+)'"
    matches = re.findall(pattern, family_str)
    
    # Extract the first two characters for country codes
    country_codes = [match[:2] for match in matches]
    return country_codes

def extract_country_codes_from_list(family_members):
    """
    Extract country codes from an actual list of patent numbers.
    """
    if not isinstance(family_members, list) or not family_members:
        return []
    
    # This pattern extracts the 2-letter country code from patent numbers
    country_codes = []
    for item in family_members:
        if isinstance(item, str):  # Ensure the item is a string
            match = re.match(r'^([A-Z]{2})[A-Z0-9]+', item)  # Match the first 2 letters
            if match:
                country_codes.append(match.group(1))  # Append the country code
    return country_codes