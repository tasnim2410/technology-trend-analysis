# technology-trend-analysis/backend/cleaners.py
import pandas as pd
import re
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
        
    df['Earliest priority'] = pd.to_datetime(df['Earliest priority'])
    df['earliest priority year'] = df['Earliest priority'].dt.year
    df['applicant country'] = df['Applicants'].str.extract(r'\[([A-Z]{2})\]')
    df['Applicants'] = df['Applicants'].fillna(df['Inventors'])
        #filling missing CPC values 
    df['CPC'] = df['CPC'].fillna('unkown')
    df['IPC'] = df['IPC'].str.split(r'\s+')


    # if 'Publication number' in df.columns:
    #   df = process_dataframe(df, patent_col='Publication number')

    def split_cpc(classification):
    # Split only at ") " but keep the ")"
      parts = re.split(r'\)\s+', classification)  
      return [p + ')' if not p.endswith(')') else p for p in parts]  # Ensure each part ends with ')'
    df['CPC'] = df['CPC'].apply(split_cpc)

    return df