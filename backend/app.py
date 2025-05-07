# backendTry1/app.py
import os
import time
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import pandas as pd
# import your scraper & post‐download processor
from scraping_raw_data import EspacenetScraper, process_downloaded_data
from family_members import ensure_columns_exist
from db import db, RawPatent
from family_members import process_dataframe_parallel
from flask import Flask, request, jsonify
import concurrent.futures
from family_members import process_patent , PatentsSearch , process_rows
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
load_dotenv()
def create_app():
    
    app = Flask(__name__)

# Load environment variables
    
    
    # … your existing config & db.init_app(app) …
        # … your existing config …
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
    
    @app.route('/api/search', methods=['GET'])
    def search_patents():
        # 1) read & validate the comma‑separated keywords
        raw_kw = request.args.get('keywords', '')
        keywords = [k.strip() for k in raw_kw.split(',') if k.strip()]
        if not keywords:
            return jsonify({"error": "Please pass ?keywords=cloud,security"}), 400

        # 2) build the EspacenetScraper, run it, process the CSV
        search_map = { kw: "title,abstract or claims" for kw in keywords }
        # In app.py, modify the scraper initialization:
        scraper = EspacenetScraper(
            search_map,
            headless=True,  # Keep headless but add extra options
           options_args=[
              "--disable-blink-features=AutomationControlled",
              "--no-sandbox",
              "--disable-dev-shm-usage",
              "--remote-debugging-port=9222",
              "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
                       ]      
                  )
        try:
            if not scraper.get_page_html():
                return jsonify({"error": "Failed to load Espacenet page"}), 500
            if not scraper.download_csv(max_results=500):
                return jsonify({"error": "Failed to download CSV"}), 500
            # give the file a sec to land
            time.sleep(5)
            df = process_downloaded_data(os.path.expanduser("~/Downloads"))
        finally:
            scraper.close()

        if df is None:
            return jsonify({"error": "Couldn’t parse downloaded CSV"}), 500

        # 3) return your cleaned DataFrame as JSON
        return jsonify(df.to_dict(orient='records')), 200
    
    

    #search keywords with field mapping
    
    @app.route('/api/search2', methods=['GET'])
    def search_patents2():
    # 1) Parse complex query parameters
      raw_query = request.args.get('keywords', '')
      if not raw_query:
        return jsonify({"error": "Use ?keywords=field:keyword,field:keyword (e.g., title:cloud,abstract:security)"}), 400

    # Split into field:keyword pairs
      search_map = {}
      for pair in raw_query.split(','):
        if ':' not in pair:
            return jsonify({"error": f"Invalid format for '{pair}'. Use field:keyword."}), 400
        field, keyword = pair.split(':', 1)
        search_map[keyword.strip()] = field.strip().lower()  # Normalize field name

    # 2) Build scraper with the parsed query
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
            if not scraper.get_page_html():
                return jsonify({"error": "Failed to load Espacenet page"}), 500
            if not scraper.download_csv(max_results=500):
                return jsonify({"error": "Failed to download CSV"}), 500
            # give the file a sec to land
            time.sleep(5)
            df = process_downloaded_data(os.path.expanduser("~/Downloads"))
      finally:
            scraper.close()

      if df is None:
            return jsonify({"error": "Couldn’t parse downloaded CSV"}), 500

        # 3) return your cleaned DataFrame as JSON
      return jsonify(df.to_dict(orient='records')), 200
    


     # Import the process_patent function from family_members.py



    @app.route('/api/family', methods=['POST'])
    def populate_family_members():
    # Get JSON data from the request
        CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
        CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()
        CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_2").strip()
        CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_2").strip()

        try:
        # Fetch data from database
            query = 'SELECT * FROM raw_patents'
            df = pd.read_sql(query, engine)

        # Data cleaning
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

            df[['first publication date', 'second publication date']] = df['Publication date'].str.split(' ', n=1, expand=True)
            df['second publication date'] = df['second publication date'].str.strip('\r\n')
        
            df[['first publication number', 'second publication number']] = df['Publication number'].str.split(' ', n=1, expand=True)
            df['second publication number'] = df['second publication number'].str.strip('\r\n')
        
            if 'Unnamed: 11' in df.columns:
                df.drop(columns=['Unnamed: 11', 'Publication date'], inplace=True)
        
            df['family number'] = pd.to_numeric(df['Family number'], errors='coerce')
            df.rename(columns={'Family number': 'family number'}, inplace=True)
            
            # Calculate the number of rows for each part
            n = len(df) // 3

        # Split the DataFrame into three parts
            df1 = df.iloc[:n].copy()       # First part
            df2 = df.iloc[n:2*n].copy()    # Second part
            df3 = df.iloc[2*n:].copy()     # Third part

        # Process df1
            df1 = process_dataframe_parallel(df1, 'first publication number', max_workers=4)
            print('num of null values df1 :', df1['family_members'].isnull().sum(), 'number of empty arrays : ', df1['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
        #process df2
            df['family_members'] = None
            #split the dataframe into 3 parts 
            indices = df2.index.tolist()
            n = len(indices)
            part_size = n // 3
            remainder = n % 3
            parts= []
            start = 0
            for i in range(3) : 
                if i < remainder:
                    end = start + part_size + 1
                else:
                    end = start + part_size 
                parts.append(indices[start:end])
                start = end 
            #create three threads , each with ist own patentsSearch instance 
            threads = []
            for part in parts : 
                thread = threading.Thread(target=process_rows, args=(df2,part))
                threads.append(thread)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            print('num of null values df2 :', df2['family_members'].isnull().sum(), 'number of empty arrays : ', df2['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
            #add the processing of the rows of family members here

        #process df3
            CONSUMER_KEY = os.getenv("CONSUMER_KEY").strip()
            CONSUMER_SECRET = os.getenv("CONSUMER_SECRET").strip()
            CONSUMER_KEY1 = os.getenv("CONSUMER_KEY_1").strip()
            CONSUMER_SECRET1 = os.getenv("CONSUMER_SECRET_1").strip()
            
            df3 = process_dataframe_parallel(df3, 'first publication number', max_workers=4)
            print('num of null values df3 :', df3['family_members'].isnull().sum(), 'number of empty arrays : ', df3['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
        
        #merging the dataframes
            df = pd.concat([df1, df2, df3], ignore_index=True)
            df['family_members'] = df['family_members'].apply(lambda x: x if isinstance(x, list) else [])
            df['family_jurisdictions'] = df['family_jurisdictions'].apply(lambda x: x if isinstance(x, list) else [])
            # Ensure the columns exist in the DataFrame
            ensure_columns_exist(df, ['family_members', 'family_jurisdictions'])
            print('num of null values df :', df['family_members'].isnull().sum(), 'number of empty arrays : ', df['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum())
            
        # prepare updates for the database 
            updates = [
                {'first publication number' : row['first publication number'], 'jurisdictions' : json.dumps(row['family_jurisdictions']), 'members' : json.dumps(row['family_members'])
                 } for index, row in df.iterrows()
            ]
            #update the database with the new columns 
            try:
                engine.execute(
                    text("UPDATE raw_patents SET family_jurisdictions = :jurisdictions, family_members = :members WHERE id = :id"),
                    updates
                )
            except Exception as e:
                return jsonify({"error": f"failed to update database: {str(e)}"}), 500


        # Prepare response
            results = df[['first publication number', 'family_jurisdictions', 'family_members']].to_dict(orient='records')
            empty_arrays_count = df['family_jurisdictions'].apply(lambda x: isinstance(x, list) and len(x) == 0).sum()
            null_count = df['family_jurisdictions'].isnull().sum()

            return jsonify({
                "results": results,
                "statistics": {
                "empty_jurisdictions_count": int(empty_arrays_count),
                "null_jurisdictions_count": int(null_count),
                "total_processed": len(df)
            }
        })

        except sqlalchemy.exc.SQLAlchemyError as e:
            return jsonify({"error": f"Database error: {str(e)}"}), 500
        except Exception as e:
            return jsonify({"error": f"Processing failed: {str(e)}"}), 500



    
    return app


if __name__ == '__main__':
    app = create_app()
    
    try:
        with app.app_context():
            db.engine.connect()
            print('database conection successful!')
    except Exception as e:
        print(f"Database connection failed: {e}")
        
    app.run(debug=True, port=5001)
