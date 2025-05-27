# backendTry1/app.py
import os
import time
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import pandas as pd
# import your scraper & post‐download processor
from scraping_raw_data import EspacenetScraper, process_downloaded_data ,DatabaseManager
#from family_members import ensure_columns_exist
from db import db, RawPatent ,ImpactFactor
#from family_members import process_dataframe_parallel
from flask import Flask, request, jsonify
import concurrent.futures
#from family_members import process_patent , PatentsSearch , process_rows
from flask import Flask, request, jsonify
import pandas as pd
import logging
import os
import concurrent.futures
import time
from urllib.parse import quote
import requests
import sqlalchemy
from dotenv import load_dotenv
import threading
import json
from sqlalchemy import text
import uuid
from family_members2 import PatentsSearch , build_espacenet_url
import ast
from cleaners import clean_family_members , extract_country_codes  # Import from your module
from research_retrieve import fetch_research_data , process_research_data, store_research_data
from research_retrieve2 import fetch_research_data2 , process_research_data2, store_research_data2
from impact_factor_processor import clean_and_process_data , store_processed_data
from sqlalchemy.exc import SQLAlchemyError
from pandas.errors import ParserError
from research_retrieve2 import fetch_research_data2, process_research_data2, store_research_data2

load_dotenv()

def create_app():
    
    app = Flask(__name__)

    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)    
    
# Set up logging
    # Database connection
    db_url = os.getenv("DATABASE_URL")
    if db_url is None:
        raise ValueError("DATABASE_URL not found. Please check your .env file.")

# Create the SQLAlchemy engine
    engine = sqlalchemy.create_engine(db_url)

    
    @app.route('/')
    def home():
      return '👋 Hello! Your app is running!', 200
  
    @app.route('/api/last_search_keywords', methods=['GET'])
    def get_last_search_keywords():
        try:
            with engine.connect() as connection: 
                result = connection.execute(
                    text("SELECT search_id FROM search_keywords WHERE id =(SELECT MAX(id) FROM search_keywords)")
                )
                row = result.fetchone()
                if not row: 
                    return jsonify({"message":"No search keywords found"}), 404
                search_id = row[0]

                result = connection.execute(
                    text("SELECT keyword , field FROM search_keywords WHERE search_id = :search_id"),
                    {"search_id": search_id}
                )
                rows = result.mappings().all()
                search_query = {row["keyword"]:row["field"] for row in rows}
                return jsonify(search_query), 200
        except Exception as e:
            return jsonify({"error": f"Failed to fetch last search keywords: {str(e)}"}), 500

    #search keywords with field mapping
    
    @app.route('/api/search', methods=['GET'])
    def search_patents():
    # 1) Parse complex query parameters
      raw_query = request.args.get('keywords', '')
      if not raw_query:
        return jsonify({"error": "Use ?keywords=field:keyword,field:keyword (e.g., title:cloud,abstract:security)"}), 400
      search_id = str(uuid.uuid4())
    # Split into field:keyword pairs
      search_map = {}
      for pair in raw_query.split(','):
        if ':' not in pair:
            return jsonify({"error": f"Invalid format for '{pair}'. Use field:keyword."}), 400
        field, keyword = pair.split(':', 1)
        search_map[keyword.strip()] = field.strip().lower()  # Normalize field name
          
    # 2) Build scraper with the parsed query
        db_manager = DatabaseManager()
      scraper = EspacenetScraper(
        search_map,
        headless=True,
        options_args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--remote-debugging-port=9222",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        ]
    )
      try:
        # Load the Espacenet page
            if not scraper.get_page_html():
                return jsonify({"error": "Failed to load Espacenet page"}), 500
        
        # Download the CSV file
            if not scraper.download_csv(max_results=500):
                return jsonify({"error": "Failed to download CSV"}), 500
        
        # Wait longer to ensure file download completes
            time.sleep(10)
        
        # Process the downloaded CSV
            df = process_downloaded_data(os.path.expanduser("~/Downloads"))
            if df is None or df.empty:
                return jsonify({"error": "Couldn’t parse downloaded CSV or DataFrame is empty"}), 500
        
            print(f"DataFrame contains {len(df)} rows")
        
        # Store data in the database
            if not db_manager.store_patents(df):
                return jsonify({"error": "Failed to store data in database"}), 500
        
            print("Data stored successfully")
    
      finally:
            if scraper:
                scraper.close()
    
    # 3) Insert search keywords into the database
      try:
            with db_manager.engine.connect() as connection:
                keyword_params = [
                    {"search_id": search_id, "field": field, "keyword": keyword}
                    for keyword, field in search_map.items()
                ]
                connection.execute(
                    text("INSERT INTO search_keywords (search_id, field, keyword) VALUES (:search_id, :field, :keyword)"),
                    keyword_params
                )
                connection.commit()
                print("Keyword data inserted successfully")
      except Exception as e:
            print(f"Error inserting keywords into database: {e}")
            return jsonify({"error": f"Failed to insert search keywords: {str(e)}"}), 500
    
    # 4) Return the response
      response_data = {
            "search_id": search_id,
            "results": df.to_dict(orient='records')
        }
                

        # 3) return your cleaned DataFrame as JSON
      return jsonify(response_data), 200
    

    def fetch_last_search_keywords_from_db():
        """
        Returns a dict mapping keyword→field for the most
        recent search_id, or raises ValueError if none found.
        """
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT search_id FROM search_keywords ORDER BY id DESC LIMIT 1")
            ).fetchone()
            if not row:
                raise ValueError("No search keywords found")

            search_id = row[0]
            mappings = conn.execute(
                text("SELECT keyword, field FROM search_keywords WHERE search_id = :sid"),
                {"sid": search_id}
            ).mappings().all()

        # build and return the dict
            return {m["keyword"]: m["field"] for m in mappings}
        

    @app.route('/api/family_members/scraping', methods=['POST'])
    def get_family_members():
    # Fetch data from database
        field_mapping = {
            "title": "ti",
            "abstract": "ab",
            "claims": "cl",
            "title,abstract or claims": "ctxt",
            "all text fields": "ftxt",
            "title or abstract": "ta",
            "description": "desc",
            "all text fields or names": "nftxt",
            "title , abstract or names": "ntxt"
        }
        keywords = fetch_last_search_keywords_from_db()
    
        # Load existing data
        query = 'SELECT *, "No" as id FROM raw_patents'
        df = pd.read_sql(query, engine)
    
        scraper = PatentsSearch(headless=False)
    
        try:
            # Process each patent record
            for index, row in df.iterrows():
                print(f"Processing patent {index+1}/{len(df)}")
            
            # 1. Scrape family members
                url = build_espacenet_url(row, keywords, field_mapping)
                html = scraper.get_page_html(url)
            
                if not html:
                    print(f"Skipping {row['first_publication_number']} - failed to retrieve page")
                    df.at[index, 'family_members'] = []
                    df.at[index, 'family_jurisdictions'] = []
                    continue
                
            # 2. Get raw family members from HTML
                raw_members = scraper.parse_html(html)
            
            # 3. Clean the scraped data
                cleaned_members = clean_family_members(raw_members)
            
            # 4. Extract country codes from CLEANED members
                jurisdictions = extract_country_codes(cleaned_members)
            
            # 5. Update both columns
                df.at[index, 'family_members'] = cleaned_members
                df.at[index, 'family_jurisdictions'] = jurisdictions
            
                print(f"Updated {row['first_publication_number']} with {len(cleaned_members)} members")

            # Prepare database updates
            updates = []
            for _, row in df.iterrows():
                updates.append({
                    'id': row['id'],
                    'members': row['family_members'],
                    'jurisdictions': row['family_jurisdictions']
                })

            # Update database in a single transaction
            with db.engine.begin() as connection:  # Automatically commits/rolls back
                connection.execute(
                    text("""
                        UPDATE raw_patents 
                        SET family_members = :members,
                            family_jurisdictions = :jurisdictions
                        WHERE "No" = :id
                    """),
                    updates
                )

            # Prepare response statistics
            total_members = sum(len(m) for m in df['family_members'])
            unique_countries = list({cc for codes in df['family_jurisdictions'] for cc in codes})
        
            return jsonify({
                "success": True,
                "updated_records": len(df),
                "total_family_members": total_members,
                "unique_jurisdictions": sorted(unique_countries),
                "sample_entry": {
                    "publication_number": df.iloc[0]['first_publication_number'],
                    "members": df.iloc[0]['family_members'],
                    "jurisdictions": df.iloc[0]['family_jurisdictions']
                }
            })

        except Exception as e:
            return jsonify({
                "success": False,
                "error": str(e),
                "failed_at": f"Failed at record {index}: {row['first_publication_number']}" if 'index' in locals() else "Initialization"
            }), 500

        finally:
            if 'scraper' in locals():
                scraper.close()

    @app.route('/api/research', methods=['POST'])
    def store_research():
        query = request.json.get('query')
        if not query:
            return jsonify({"error": "Query parameter is required"}), 400
    
        papers = fetch_research_data(query)
        if not papers:
            return jsonify({"error": "Failed to fetch data from API"}), 500
    
        df = process_research_data(papers)
        try:
            store_research_data(df)
            return jsonify({"message": "Data stored successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
        
        
    @app.route('/api/research_retrieve_and_filtering', methods=['POST'])
    def store_research2():
        query = request.json.get('query')
        papers = fetch_research_data2(query)
        impact_factors = ImpactFactor.query.all()
    
        if not papers:
            return jsonify({"error": "No papers found"}), 404
    
        df = process_research_data2(papers , impact_factors)
    
        try:
            store_research_data2(df)
            return jsonify({
                "message": f"Stored {len(df)} papers",
                "stats": {
                    
                    "top_journal": df['publication_venue_name'].mode()[0]
                }
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
        
    """enpoint to fetch and store research data that's supposed to show the number of papers before and after filtering but not working yet"""
    # @app.route('/api/research_retrieve', methods=['POST'])
    # def store_research2():
    #     query = request.json.get('query')
    #     papers = fetch_research_data(query)

    #     if not papers:
    #         return jsonify({"error": "No papers found"}), 404

    # # Must match the two‐value return from process_research_data:
    #     result = process_research_data(papers)
    #     print(f"Received result type: {type(result)}, length: {len(result) if hasattr(result, '__len__') else 'N/A'}")
    #     if len(result) != 2:
    #         return jsonify({"error": "Unexpected data format from processing"}), 500
    #     df, stats = result
    #     if df.empty:
    #         return jsonify({"error": "No papers survived processing filters"}), 40

    #     try:
    #         store_research_data(df)
    #         return jsonify({
    #             "message": f"Stored {len(df)} papers",
    #             "stats": {
    #                 "before_clean": stats['before_clean'],
    #                 "after_clean": stats['after_clean'],
    #                 "after_filter": stats['after_filter'],
    #                 "top_journal": df['publication_venue_name'].mode()[0] if len(df) else None
    #             }
    #         }), 200

    #     except Exception as e:
    #         return jsonify({"error": str(e)}), 500





        
    @app.route('/process_and_store', methods=['GET'])
    def process_and_store():
        """
        Endpoint to check if store_processed_data works.
        Requires ?confirm=true query parameter to proceed.
        Returns JSON response indicating success or failure.
        """
        confirm = request.args.get('confirm', 'false').lower() == 'true'
        if not confirm:
            return jsonify({"message": "Operation not confirmed. Add ?confirm=true to proceed."}), 400
    
        try:
            logging.info("Starting data processing and storage.")
            message = store_processed_data()
            logging.info("Data processed and stored successfully.")
            return jsonify({"message": message}), 200
        except ValueError as e:
            logging.error(f"Configuration error: {str(e)}")
            return jsonify({"error": f"Configuration error: {str(e)}"}), 400
        except FileNotFoundError as e:
            logging.error(f"File not found: {str(e)}")
            return jsonify({"error": f"File not found: {str(e)}"}), 404
        except ParserError as e:
            logging.error(f"Error parsing file: {str(e)}")
            return jsonify({"error": f"Error parsing file: {str(e)}"}), 400
        except SQLAlchemyError as e:
            logging.error(f"Database error: {str(e)}")
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}")
            return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


#@app.route('/api/family_members/API', methods=['POST']) 

    
    # @app.route('/api/family', methods=['POST'])
    # def populate_family_members():
    # # Get JSON data from the request
    #     CONSUMER_KEY = os.getenv("CONSUMER_KEY_3").strip()
    #     CONSUMER_SECRET = os.getenv("CONSUMER_SECRET_3").strip()
    #     CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_2").strip()
    #     CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_2").strip()

    #     try:
    #     # Fetch data from database
    #         query = 'SELECT * FROM raw_patents'
    #         df = pd.read_sql(query, engine)

    #     # Data cleaning
    #         df.rename(columns={
    #             'Titre': 'Title',
    #             'Inventeurs': 'Inventors',
    #             'Demandeurs': 'Applicants',
    #             'Numéro de publication': 'Publication number',
    #             'Priorité la plus ancienne': 'Earliest priority',
    #             'CIB': 'IPC',
    #             'CPC': 'CPC',
    #             'Date de publication': 'Publication date',
    #             'Publication la plus ancienne': 'Earliest publication',
    #             'Numéro de famille': 'Family number'
    #         }, inplace=True)

    #         # Split 'Publication date' into two columns using regex
    #         df['first publication date'] = df['Publication date'].str.extract(r'^(\S+)', expand=False)
    #         df['second publication date'] = df['Publication date'].str.extract(r'^\S+\s+(.*)', expand=False)


    #         df['second publication date'] = df['second publication date'].str.strip('\r\n')
        
    #         df[['first publication number', 'second publication number']] = df['Publication number'].str.split(' ', n=1, expand=True)
    #         df['second publication number'] = df['second publication number'].str.strip('\r\n')
        
    #         if 'Unnamed: 11' in df.columns:
    #             df.drop(columns=['Unnamed: 11', 'Publication date'], inplace=True)
        
    #         df['family number'] = pd.to_numeric(df['Family number'], errors='coerce')
    #         df.rename(columns={'Family number': 'family number'}, inplace=True)
            
    #         # Calculate the number of rows for each part
    #         n = len(df) // 3

    #     # Split the DataFrame into three parts
    #         df1 = df.iloc[:n].copy()       # First part
    #         df2 = df.iloc[n:2*n].copy()    # Second part
    #         df3 = df.iloc[2*n:].copy()     # Third part

    #     # Process df1
    #         df1 = process_dataframe_parallel(df1, 'first publication number', max_workers=4)
    #         print('num of null values df1 :', df1['family_members'].isnull().sum(), 'number of empty arrays : ', df1['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
    #     #process df2
    #         search_keywords = fetch_last_search_keywords_from_db()
    #         if 'family_members' not in df2.columns: 
    #             df['family_members'] = None
    #         #split the dataframe into 3 parts 
    #         indices = df2.index.tolist()
    #         n = len(indices)
    #         part_size = n // 3
    #         remainder = n % 3
    #         parts= []
    #         start = 0
    #         for i in range(3) : 
    #             if i < remainder:
    #                 end = start + part_size + 1
    #             else:
    #                 end = start + part_size 
    #             parts.append(indices[start:end])
    #             start = end 
    #         #create three threads , each with ist own patentsSearch instance 
    #         threads = []
    #         for part in parts : 
    #             thread = threading.Thread(target=process_rows, args=(df2, part,search_keywords,False))
    #             threads.append(thread)
    #         for thread in threads:
    #             thread.start()
    #         for thread in threads:
    #             thread.join()
    #         print('num of null values df2 :', df2['family_members'].isnull().sum(), 'number of empty arrays : ', df2['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
    #         #add the processing of the rows of family members here

    #     #process df3
    #         CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
    #         CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()
    #         CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_1").strip()
    #         CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_1").strip()
            
    #         df3 = process_dataframe_parallel(df3, 'first publication number', max_workers=4)
    #         print('num of null values df3 :', df3['family_members'].isnull().sum(), 'number of empty arrays : ', df3['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
        
    #     #merging the dataframes
    #         df = pd.concat([df1, df2, df3], ignore_index=True)
    #         df['family_members'] = df['family_members'].apply(lambda x: x if isinstance(x, list) else [])
    #         df['family_jurisdictions'] = df['family_jurisdictions'].apply(lambda x: x if isinstance(x, list) else [])
    #         # Ensure the columns exist in the DataFrame
    #         ensure_columns_exist(df, ['family_members', 'family_jurisdictions'])
    #         print('num of null values df :', df['family_members'].isnull().sum(), 'number of empty arrays : ', df['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
            
    #     # prepare updates for the database 
    #         updates = [
    #             {'id':row['id'],'first publication number' : row['first publication number'], 'jurisdictions' : json.dumps(row['family_jurisdictions']), 'members' : json.dumps(row['family_members'])
    #              } for index, row in df.iterrows()
    #         ]
    #         #update the database with the new columns 
    #         try:
    #             engine.execute(
    #                 text("UPDATE raw_patents SET family_jurisdictions = :jurisdictions, family_members = :members WHERE id = :id"),
    #                 updates
    #             )
    #         except Exception as e:
    #             return jsonify({"error": f"failed to update database: {str(e)}"}), 500


    #     # Prepare response
    #         results = df[['first publication number', 'family_jurisdictions', 'family_members']].to_dict(orient='records')
    #         empty_arrays_count = df['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
    #         null_count = df['family_jurisdictions'].isnull().sum()

    #         return jsonify({
    #             "results": results,
    #             "statistics": {
    #             "empty_jurisdictions_count": int(empty_arrays_count),
    #             "null_jurisdictions_count": int(null_count),
    #             "total_processed": len(df)
    #         }
    #     })

    #     except sqlalchemy.exc.SQLAlchemyError as e:
    #         return jsonify({"error": f"Database error: {str(e)}"}), 500
    #     except Exception as e:
    #         return jsonify({"error": f"Processing failed: {str(e)}"}), 500



    
    return app


if __name__ == '__main__':
    app = create_app() 
    try:
        with app.app_context():
            db.engine.connect()
            print('database conection successful!')
            db.create_all()
            print('database tables created successfully!')
    except Exception as e:
        print(f"Database connection failed: {e}")
        
    app.run(debug=True, port=5001)
