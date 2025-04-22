# backendTry1/app.py
import os
import time
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# import your scraper & post‐download processor
from scraping_raw_data import EspacenetScraper, process_downloaded_data

load_dotenv()

def create_app():
    app = Flask(__name__)
    # … your existing config & db.init_app(app) …

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
    @app.route('/')
    def home():
      return '👋 Hello! Your app is running!', 200
    
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

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, port=5001)
