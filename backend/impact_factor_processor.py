# # impact_factor_processor.py
# import pandas as pd
# import numpy as np
# from typing import Tuple
# from db import db, ImpactFactor
# from dotenv import load_dotenv
# import os
# from db import db, ImpactFactor, ProcessedImpactFactor

# load_dotenv()  # Loads variables from .env into environment

# IMPACT_FACTOR_FILE = os.getenv("IMPACT_FACTOR_FILE")
# def store_journals_from_excel(file_path: str = IMPACT_FACTOR_FILE):
#     df = pd.read_excel(file_path, sheet_name="2024最新完整版IF")
#     for _, row in df.iterrows():
#         name = row.get('Journal Name') or row.get('Title') or row.iloc[0]
#         abbrev_name = row.get('Abbreviated Title') or row.get('Abbrev Name')
#         issn = str(row.get('ISSN')).strip() if row.get('ISSN') else None
#         eissn = str(row.get('EISSN')).strip() if row.get('EISSN') else None
#         jif = row.get('Impact Factor') or row.get('IF') or row.get('JIF')
#         jif_5yr = row.get('5-Year IF') or row.get('JIF 5yr') or row.get('5-Year Impact Factor')
#         subcategory = row.get('Category') or row.get('Subcategory')
#         field = row.get('Field')
#         if pd.isna(name):
#             continue
#         impact_factor_record = ImpactFactor(
#             name=name,
#             abbrev_name=abbrev_name,
#             issn=issn,            
#             eissn=eissn,
#             jif=jif,
#             jif_5yr=jif_5yr,
#             subcategory=subcategory,
#             field=field
#         )
#         db.session.add(impact_factor_record)
#     db.session.commit()
    

# def clean_and_process_data():


#     # Retrieve impact factor data from the database
#     if_data = pd.read_sql(ImpactFactor.query.statement, db.session.bind)

#     # Clean the 'subcategory' column by taking the first part before '|'
#     if_data['subcategory'] = if_data['subcategory'].apply(
#         lambda x: x.split('|')[0].strip() if isinstance(x, str) else None
#     )

#     # Load the journal subcategories to categories mapping from CSV
#     MAPPING_CSV_PATH = os.getenv("MAPPING_CSV_PATH")
#     journal_subcategories_to_categories = pd.read_csv(MAPPING_CSV_PATH)

#     # Remove rows with NaN values and skip the first row if it duplicates headers
#     journal_subcategories_to_categories = journal_subcategories_to_categories.dropna()
#     journal_subcategories_to_categories = journal_subcategories_to_categories.iloc[1:]

#     # Define missing mappings to be added
#     missing_to_field = {
#         'ENGINEERING, ELECTRICAL & ELECTRONIC': 'Engineering',
#         'PHYSICS, APPLIED': 'Physics',
#         'MATERIALS SCIENCE, PAPER & WOOD': 'Materials Science',
#         'PSYCHOLOGY, PSYCHOANALYSIS': 'Medicine',
#         'ENGINEERING, BIOMEDICAL': 'Engineering',
#         'COMPUTER SCIENCE, INTERDISCIPLINARY APPLICATIONS': 'Computer Science',
#         'CHEMISTRY, MULTIDISCIPLINARY': 'Chemistry',
#         'CHEMISTRY, PHYSICAL': 'Chemistry',
#         'CHEMISTRY, ANALYTICAL': 'Chemistry',
#         'MATHEMATICS, INTERDISCIPLINARY APPLICATIONS': 'Mathematics',
#         'RADIOLOGY, NUCLEAR MEDICINE & MEDICAL IMAGING': 'Medicine',
#         'PHYSICS, MULTIDISCIPLINARY': 'Physics',
#         'FILM, RADIO, TELEVISION': 'Computer Science',
#         'ENGINEERING, PETROLEUM': 'Engineering',
#         'ENGINEERING, MARINE': 'Engineering',
#         'ENGINEERING, CHEMICAL': 'Engineering',
#         'PSYCHOLOGY, CLINICAL': 'Medicine',
#         'PSYCHOLOGY, APPLIED': 'Medicine',
#         'LITERATURE, SLAVIC': 'Environmental Science',
#         'AGRICULTURE, DAIRY & ANIMAL SCIENCE': 'Biology',
#         'ENGINEERING, ENVIRONMENTAL': 'Engineering',
#         'MATERIALS SCIENCE, BIOMATERIALS': 'Materials Science',
#         'LITERATURE, AFRICAN, AUSTRALIAN, CANADIAN': 'Environmental Science',
#         'PSYCHOLOGY, EXPERIMENTAL': 'Medicine',
#         'PSYCHOLOGY, EDUCATIONAL': 'Medicine',
#         'EDUCATION, SPECIAL': 'Medicine',
#         'MATERIALS SCIENCE, COATINGS & FILMS': 'Materials Science',
#         'MATERIALS SCIENCE, MULTIDISCIPLINARY': 'Materials Science',
#         'COMPUTER SCIENCE, HARDWARE & ARCHITECTURE': 'Computer Science',
#         'PHYSICS, FLUIDS & PLASMAS': 'Physics',
#         'SOCIAL SCIENCES, INTERDISCIPLINARY': 'Mathematics',
#         'PSYCHOLOGY, MULTIDISCIPLINARY': 'Medicine',
#         'ENGINEERING, GEOLOGICAL': 'Engineering',
#         'GEOGRAPHY, PHYSICAL': 'Geology',
#         'MATHEMATICS, APPLIED': 'Mathematics',
#         'PSYCHOLOGY, DEVELOPMENTAL': 'Medicine',
#         'PHYSICS, CONDENSED MATTER': 'Physics',
#         'ENGINEERING, MANUFACTURING': 'Engineering',
#         'MEDICINE, RESEARCH & EXPERIMENTAL': 'Medicine',
#         'ENGINEERING, OCEAN': 'Engineering',
#         'ENGINEERING, AEROSPACE': 'Engineering',
#         'COMPUTER SCIENCE, SOFTWARE ENGINEERING': 'Computer Science',
#         'PHYSICS, PARTICLES & FIELDS': 'Physics',
#         'PSYCHOLOGY, MATHEMATICAL': 'Mathematics',
#         'PUBLIC, ENVIRONMENTAL & OCCUPATIONAL HEALTH': 'Medicine',
#         'EDUCATION, SCIENTIFIC DISCIPLINES': 'Mathematics',
#         'PHYSICS, MATHEMATICAL': 'Physics',
#         'HOSPITALITY, LEISURE, SPORT & TOURISM': 'Environmental Science',
#         'GEOSCIENCES, MULTIDISCIPLINARY': 'Geology',
#         'ENGINEERING, INDUSTRIAL': 'Engineering',
#         'MATERIALS SCIENCE, CHARACTERIZATION & TESTING': 'Materials Science',
#         'CHEMISTRY, ORGANIC': 'Chemistry',
#         'DENTISTRY, ORAL SURGERY & MEDICINE': 'Medicine',
#         'MEDICINE, GENERAL & INTERNAL': 'Medicine',
#         'COMPUTER SCIENCE, INFORMATION SYSTEMS': 'Computer Science',
#         'COMPUTER SCIENCE, THEORY & METHODS': 'Computer Science',
#         'LITERATURE, BRITISH ISLES': 'Environmental Science',
#         'PHYSICS, NUCLEAR': 'Physics',
#         'MEDICINE, LEGAL': 'Medicine',
#         'PHYSICS, ATOMIC, MOLECULAR & CHEMICAL': 'Physics',
#         'LITERATURE, GERMAN, DUTCH, SCANDINAVIAN': 'Environmental Science',
#         'SOCIAL SCIENCES, MATHEMATICAL METHODS': 'Mathematics',
#         'CHEMISTRY, APPLIED': 'Chemistry',
#         'HUMANITIES, MULTIDISCIPLINARY': 'Environmental Science',
#         'SOCIAL SCIENCES, BIOMEDICAL': 'Medicine',
#         'LITERATURE, AMERICAN': 'Environmental Science',
#         'BUSINESS, FINANCE': 'Mathematics',
#         'LITERATURE, ROMANCE': 'Environmental Science',
#         'CHEMISTRY, INORGANIC & NUCLEAR': 'Chemistry',
#         'CHEMISTRY, MEDICINAL': 'Chemistry',
#         'PSYCHOLOGY, SOCIAL': 'Medicine',
#         'MATERIALS SCIENCE, CERAMICS': 'Materials Science',
#         'MATERIALS SCIENCE, COMPOSITES': 'Materials Science',
#         'COMPUTER SCIENCE, ARTIFICIAL INTELLIGENCE': 'Computer Science',
#         'PSYCHOLOGY, BIOLOGICAL': 'Medicine',
#         'COMPUTER SCIENCE, CYBERNETICS': 'Computer Science',
#         'MATERIALS SCIENCE, TEXTILES': 'Materials Science',
#         'ENGINEERING, CIVIL': 'Engineering',
#         'ENGINEERING, MULTIDISCIPLINARY': 'Engineering',
#         'AGRICULTURE, MULTIDISCIPLINARY': 'Biology',
#         'ENGINEERING, MECHANICAL': 'Engineering'
#     }

#     # Create a dataframe from missing mappings
#     new_rows = pd.DataFrame.from_dict(
#         missing_to_field, orient='index', columns=['Field']
#     ).reset_index().rename(columns={'index': 'Discipline'})

#     # Concatenate the original mapping with missing mappings
#     df_journals_complete = pd.concat(
#         [journal_subcategories_to_categories, new_rows], ignore_index=True
#     )

#     # Remove duplicates based on 'Discipline'
#     df_journals_complete = df_journals_complete.drop_duplicates(
#         subset=['Discipline'], keep='first'
#     )

#     # Merge impact factor data with the complete journal mapping
#     merged_data = pd.merge(
#         if_data,
#         df_journals_complete,
#         left_on='subcategory',
#         right_on='Discipline',
#         how='left'
#     )

#     # Fill missing 'Field' values with 'Unknown'
#     merged_data['Field'] = merged_data['Field'].fillna('Unknown')

#     # Select and rename columns for the final dataframe
#     final_data = merged_data[['name', 'abbrev_name', 'issn', 'subcategory', 'Field', 'jif_5yr']].rename(
#         columns={'Field': 'field'}
#     )

#     return final_data
# def store_processed_data():
#     """
#     Calls clean_and_process_data to get the processed DataFrame and stores it
#     in the ProcessedImpactFactor table.
#     """
#     # Get the final processed DataFrame
#     final_df = clean_and_process_data()
    
#     # Clear existing records in ProcessedImpactFactor table
#     db.session.query(ProcessedImpactFactor).delete()
    
#     # Convert DataFrame to a list of dictionaries
#     records = final_df.to_dict(orient='records')
    
#     # Bulk insert the records into the table
#     db.session.bulk_insert_mappings(ProcessedImpactFactor, records)
    
#     # Commit the transaction
#     db.session.commit()


# if __name__ == "__main__":
#     from backend.app import app  # Make sure this is the correct import path for your Flask app
#     with app.app_context():
#         store_processed_data()
import pandas as pd
import numpy as np
from db import db, ImpactFactor
from dotenv import load_dotenv
import os
from sqlalchemy import insert
from sqlalchemy import text
load_dotenv()  # Loads variables from .env into environment

IMPACT_FACTOR_FILE = os.getenv("IMPACT_FACTOR_FILE")
MAPPING_CSV_PATH = os.getenv("MAPPING_CSV_FILE")

def clean_and_process_data():
    """
    Reads the Excel file directly, cleans and processes the data, and returns a DataFrame
    with the required columns for storage.
    """
    # Read the Excel file
    df = pd.read_excel(IMPACT_FACTOR_FILE, sheet_name="2024最新完整版IF")
    
    print(f"IMPACT_FACTOR_FILE is: {IMPACT_FACTOR_FILE}")
    if IMPACT_FACTOR_FILE is None:
      raise ValueError("IMPACT_FACTOR_FILE environment variable is not set.")
    
    # Define possible column names for mapping
    column_mapping = {
        'Name': 'name',
        'Abbr Name': 'abbrev_name',
        'ISSN': 'issn',
        'Category': 'subcategory',
        'JIF5Years': 'jif_5yr',

    }
    
    # Select the first available column for each standard column
    standard_columns = ['name', 'abbrev_name', 'issn', 'subcategory', 'jif_5yr']
    selected_columns = {}
    for std_col in standard_columns:
        for possible_col in [k for k, v in column_mapping.items() if v == std_col]:
            if possible_col in df.columns:
                selected_columns[possible_col] = std_col
                break
    
    # Create a new DataFrame with the selected columns
    if_data = df[list(selected_columns.keys())].copy()
    if_data.columns = [selected_columns[col] for col in if_data.columns]
    
    # If 'name' is not found, use the first column
    if 'name' not in if_data.columns:
        if_data['name'] = df.iloc[:, 0]
    
    # Drop rows where 'name' is NaN
    if_data = if_data.dropna(subset=['name'])
    
    # Clean the 'subcategory' column by taking the first part before '|'
    if 'subcategory' in if_data.columns:
        if_data['subcategory'] = if_data['subcategory'].apply(
            lambda x: x.split('|')[0].strip() if isinstance(x, str) else None
        )
    else:
        if_data['subcategory'] = None
    
    # Load the mapping CSV
    journal_subcategories_to_categories = pd.read_csv(MAPPING_CSV_PATH)
    journal_subcategories_to_categories = journal_subcategories_to_categories.dropna()
    journal_subcategories_to_categories = journal_subcategories_to_categories.iloc[1:]
    
    # Define missing mappings to be added
    missing_to_field = {
        'ENGINEERING, ELECTRICAL & ELECTRONIC': 'Engineering',
        'PHYSICS, APPLIED': 'Physics',
        'MATERIALS SCIENCE, PAPER & WOOD': 'Materials Science',
        'PSYCHOLOGY, PSYCHOANALYSIS': 'Medicine',
        'ENGINEERING, BIOMEDICAL': 'Engineering',
        'COMPUTER SCIENCE, INTERDISCIPLINARY APPLICATIONS': 'Computer Science',
        'CHEMISTRY, MULTIDISCIPLINARY': 'Chemistry',
        'CHEMISTRY, PHYSICAL': 'Chemistry',
        'CHEMISTRY, ANALYTICAL': 'Chemistry',
        'MATHEMATICS, INTERDISCIPLINARY APPLICATIONS': 'Mathematics',
        'RADIOLOGY, NUCLEAR MEDICINE & MEDICAL IMAGING': 'Medicine',
        'PHYSICS, MULTIDISCIPLINARY': 'Physics',
        'FILM, RADIO, TELEVISION': 'Computer Science',
        'ENGINEERING, PETROLEUM': 'Engineering',
        'ENGINEERING, MARINE': 'Engineering',
        'ENGINEERING, CHEMICAL': 'Engineering',
        'PSYCHOLOGY, CLINICAL': 'Medicine',
        'PSYCHOLOGY, APPLIED': 'Medicine',
        'LITERATURE, SLAVIC': 'Environmental Science',
        'AGRICULTURE, DAIRY & ANIMAL SCIENCE': 'Biology',
        'ENGINEERING, ENVIRONMENTAL': 'Engineering',
        'MATERIALS SCIENCE, BIOMATERIALS': 'Materials Science',
        'LITERATURE, AFRICAN, AUSTRALIAN, CANADIAN': 'Environmental Science',
        'PSYCHOLOGY, EXPERIMENTAL': 'Medicine',
        'PSYCHOLOGY, EDUCATIONAL': 'Medicine',
        'EDUCATION, SPECIAL': 'Medicine',
        'MATERIALS SCIENCE, COATINGS & FILMS': 'Materials Science',
        'MATERIALS SCIENCE, MULTIDISCIPLINARY': 'Materials Science',
        'COMPUTER SCIENCE, HARDWARE & ARCHITECTURE': 'Computer Science',
        'PHYSICS, FLUIDS & PLASMAS': 'Physics',
        'SOCIAL SCIENCES, INTERDISCIPLINARY': 'Mathematics',
        'PSYCHOLOGY, MULTIDISCIPLINARY': 'Medicine',
        'ENGINEERING, GEOLOGICAL': 'Engineering',
        'GEOGRAPHY, PHYSICAL': 'Geology',
        'MATHEMATICS, APPLIED': 'Mathematics',
        'PSYCHOLOGY, DEVELOPMENTAL': 'Medicine',
        'PHYSICS, CONDENSED MATTER': 'Physics',
        'ENGINEERING, MANUFACTURING': 'Engineering',
        'MEDICINE, RESEARCH & EXPERIMENTAL': 'Medicine',
        'ENGINEERING, OCEAN': 'Engineering',
        'ENGINEERING, AEROSPACE': 'Engineering',
        'COMPUTER SCIENCE, SOFTWARE ENGINEERING': 'Computer Science',
        'PHYSICS, PARTICLES & FIELDS': 'Physics',
        'PSYCHOLOGY, MATHEMATICAL': 'Mathematics',
        'PUBLIC, ENVIRONMENTAL & OCCUPATIONAL HEALTH': 'Medicine',
        'EDUCATION, SCIENTIFIC DISCIPLINES': 'Mathematics',
        'PHYSICS, MATHEMATICAL': 'Physics',
        'HOSPITALITY, LEISURE, SPORT & TOURISM': 'Environmental Science',
        'GEOSCIENCES, MULTIDISCIPLINARY': 'Geology',
        'ENGINEERING, INDUSTRIAL': 'Engineering',
        'MATERIALS SCIENCE, CHARACTERIZATION & TESTING': 'Materials Science',
        'CHEMISTRY, ORGANIC': 'Chemistry',
        'DENTISTRY, ORAL SURGERY & MEDICINE': 'Medicine',
        'MEDICINE, GENERAL & INTERNAL': 'Medicine',
        'COMPUTER SCIENCE, INFORMATION SYSTEMS': 'Computer Science',
        'COMPUTER SCIENCE, THEORY & METHODS': 'Computer Science',
        'LITERATURE, BRITISH ISLES': 'Environmental Science',
        'PHYSICS, NUCLEAR': 'Physics',
        'MEDICINE, LEGAL': 'Medicine',
        'PHYSICS, ATOMIC, MOLECULAR & CHEMICAL': 'Physics',
        'LITERATURE, GERMAN, DUTCH, SCANDINAVIAN': 'Environmental Science',
        'SOCIAL SCIENCES, MATHEMATICAL METHODS': 'Mathematics',
        'CHEMISTRY, APPLIED': 'Chemistry',
        'HUMANITIES, MULTIDISCIPLINARY': 'Environmental Science',
        'SOCIAL SCIENCES, BIOMEDICAL': 'Medicine',
        'LITERATURE, AMERICAN': 'Environmental Science',
        'BUSINESS, FINANCE': 'Mathematics',
        'LITERATURE, ROMANCE': 'Environmental Science',
        'CHEMISTRY, INORGANIC & NUCLEAR': 'Chemistry',
        'CHEMISTRY, MEDICINAL': 'Chemistry',
        'PSYCHOLOGY, SOCIAL': 'Medicine',
        'MATERIALS SCIENCE, CERAMICS': 'Materials Science',
        'MATERIALS SCIENCE, COMPOSITES': 'Materials Science',
        'COMPUTER SCIENCE, ARTIFICIAL INTELLIGENCE': 'Computer Science',
        'PSYCHOLOGY, BIOLOGICAL': 'Medicine',
        'COMPUTER SCIENCE, CYBERNETICS': 'Computer Science',
        'MATERIALS SCIENCE, TEXTILES': 'Materials Science',
        'ENGINEERING, CIVIL': 'Engineering',
        'ENGINEERING, MULTIDISCIPLINARY': 'Engineering',
        'AGRICULTURE, MULTIDISCIPLINARY': 'Biology',
        'ENGINEERING, MECHANICAL': 'Engineering'
    }
    
    # Create a DataFrame from missing mappings
    new_rows = pd.DataFrame.from_dict(
        missing_to_field, orient='index', columns=['Field']
    ).reset_index().rename(columns={'index': 'Discipline'})
    
    # Concatenate the original mapping with missing mappings
    df_journals_complete = pd.concat(
        [journal_subcategories_to_categories, new_rows], ignore_index=True
    )
    
    # Remove duplicates based on 'Discipline'
    df_journals_complete = df_journals_complete.drop_duplicates(
        subset=['Discipline'], keep='first'
    )
    
    # Merge impact factor data with the complete journal mapping
    merged_data = pd.merge(
        if_data,
        df_journals_complete,
        left_on='subcategory',
        right_on='Discipline',
        how='left'
    )
    
    # Fill missing 'Field' values with 'Unknown'
    merged_data['Field'] = merged_data['Field'].fillna('Unknown')
    
    # Select and rename columns for the final DataFrame
    final_data = merged_data[['name', 'abbrev_name', 'issn', 'subcategory', 'Field', 'jif_5yr']].rename(
        columns={'Field': 'field'}
    )
    
    return final_data

# def store_processed_data(): 
#     """
#     Calls clean_and_process_data to get the processed DataFrame and stores it
#     in the ProcessedImpactFactor table.
#     """
#     # Get the final processed DataFrame
#     final_data = clean_and_process_data()
#     # (1) First—make sure your JIF5Years is numeric:
#     final_data['jif_5yr'] = pd.to_numeric(final_data['jif_5yr'], errors='coerce')

# # (2) Define your thresholds
#     thresholds = {
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
#   }

# # (3) Map thresholds onto a new column:
#     final_data['Threshold'] = final_data['field'].map(thresholds)

# # (4) Drop any fields that didn’t have a threshold defined (optional)
#     final_data = final_data.dropna(subset=['Threshold'])

# # (5) Now filter safely:
#     filtered_df = final_data[final_data['jif_5yr'] >= final_data['Threshold']] \
#                       .drop(columns='Threshold')
#     print(f"Records after filtering: {len(filtered_df)}")
    
#     if filtered_df.empty:
#         return {"message": "No data to store after processing"}
    
#     table_name = ImpactFactor.__tablename__

#     # Get the primary key columns (e.g., 'id')
#     primary_key_columns = [c.name for c in ImpactFactor.__table__.primary_key.columns]

#     # Get the columns to insert, excluding the primary key
#     insert_columns = [c.name for c in ImpactFactor.__table__.columns if c.name not in primary_key_columns]

#     # Ensure filtered_df only includes the columns to insert (excludes 'id')
#     filtered_df = filtered_df[insert_columns]
    
#     with db.engine.begin() as connection:
#         # Check if there are records in the table
#         count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
#         if count > 0:
#             # Clear existing records only if there are any
#             connection.execute(text(f"DELETE FROM {table_name}"))
        
#         # Construct the raw SQL insert query
#         columns_str = ", ".join(insert_columns)
#         placeholders = ", ".join([f":{col}" for col in insert_columns])
#         sql = text(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})")
        
#         # Execute the insert with the DataFrame records
#         connection.execute(sql, filtered_df.to_dict(orient='records'))
    
#     return {"message": "Data processed and stored successfully", "records_stored": len(filtered_df)}
  

from sqlalchemy import text, inspect

def store_processed_data(): 
    """
    Calls clean_and_process_data to get the processed DataFrame and stores it
    in the impact_factors table—after verifying the table exists and cleaning
    up duplicate / NaN ISSN values so as not to violate the uq_issn_pair constraint.
    """
    # 1) Get the final processed DataFrame
    final_data = clean_and_process_data()

    # 2) Ensure jif_5yr is numeric
    final_data['jif_5yr'] = pd.to_numeric(final_data['jif_5yr'], errors='coerce')

    # 3) Define thresholds
    thresholds = {
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

    # 4) Map thresholds onto a new column
    final_data['Threshold'] = final_data['field'].map(thresholds)

    # 5) Drop rows without a defined threshold
    final_data = final_data.dropna(subset=['Threshold'])

    # 6) Filter by threshold
    filtered_df = final_data[final_data['jif_5yr'] >= final_data['Threshold']] \
                          .drop(columns='Threshold')
    print(f"Records after filtering: {len(filtered_df)}")

    if filtered_df.empty:
        return {"message": "No data to store after processing"}

    # 7) Clean up the ISSN column so that NaN → None, then drop duplicates on issn
    #    (Postgres allows multiple NULLs in a UNIQUE constraint, but not multiple literal 'NaN'.)
    filtered_df['issn'] = filtered_df['issn'].where(filtered_df['issn'].notnull(), None)
    filtered_df = filtered_df.drop_duplicates(subset=['issn'], keep='first')

    # 8) Prepare for insertion
    table_name = ImpactFactor.__tablename__  # 'impact_factors'
    primary_key_columns = [c.name for c in ImpactFactor.__table__.primary_key.columns]
    insert_columns = [c.name for c in ImpactFactor.__table__.columns if c.name not in primary_key_columns]
    filtered_df = filtered_df[insert_columns]

    # 9) Check if the table exists; if it does, clear it; otherwise, raise an error
    inspector = inspect(db.engine)
    with db.engine.begin() as connection:
        if inspector.has_table(table_name):
            count = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            if count > 0:
                connection.execute(text(f"DELETE FROM {table_name}"))
        else:
            # Option A: create all tables automatically
            #    Uncomment the next line if you want SQLAlchemy to create missing tables:
            #	   db.create_all()
            #    Otherwise, raise so you know to run your migrations first.
            raise RuntimeError(f"Table '{table_name}' does not exist in the database.")

        # 10) Construct and execute the INSERT query
        columns_str = ", ".join(insert_columns)
        placeholders = ", ".join([f":{col}" for col in insert_columns])
        sql = text(f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})")
        connection.execute(sql, filtered_df.to_dict(orient='records'))

    return {"message": "Data processed and stored successfully", "records_stored": len(filtered_df)}
