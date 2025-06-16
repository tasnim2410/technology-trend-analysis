import pandas as pd
#classifications_df = pd.read_csv("C:\Users\tasni\OneDrive\Documents\PFE\code\technology-trend-analysis\backend\classification_df.csv", sep=';')
def get_ipc_meaning(ipc_list, classification_df):
    """
    Extract the first three characters from the first IPC code in the list and
    return the corresponding Meaning from classification_df.
    
    Parameters:
      ipc_list: list of IPC code strings.
      classification_df: DataFrame with index 'ipc_code' and column 'meaning'.
    """
    if not ipc_list or not isinstance(ipc_list, list):
        return None

    
    code_extracted = ipc_list[0][:3]

    # Look for a row where the ipc_code starts with the extracted code
    match = classification_df[classification_df.index.str.startswith(code_extracted)]
    if not match.empty:
        return match.iloc[0]['classification_title']
    else:
        # Fallback: use the section letter (first character)
        section_letter = code_extracted[0]
        if section_letter in classification_df.index:
            return classification_df.loc[section_letter, 'classification_title']
    return None

def normalize_engineering(field_list):
    seen = set()
    out = []
    for f in field_list:
        if 'engineering' in f.lower():
            norm = 'Engineering'
        else:
            norm = f
        # preserve order & drop duplicates
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out

def get_patent_fields(ipc_list,fieldOfStudy_df):
    try:
        fieldOfStudy_df['ipc'] = fieldOfStudy_df['ipc'].str[:3].str.strip().str.upper()
        ipc_mapping = fieldOfStudy_df.set_index('ipc')['fields'].to_dict()

        if not isinstance(ipc_list, list) or len(ipc_list) == 0:
            return ['Unclassified']
        
        # Get first IPC code and clean it
        first_code = str(ipc_list[0]).strip().upper()
        ipc_prefix = first_code[:3]
        
        return ipc_mapping.get(ipc_prefix, ['General Technology'])
        
    except Exception as e:
        print(f"Error processing: {ipc_list} - {str(e)}")
        return ['Unclassified']