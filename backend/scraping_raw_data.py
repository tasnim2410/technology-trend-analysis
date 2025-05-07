# technology-trend-analysis/backend/scraping_raw_data.py
import os
import glob
import time
import random
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from cleaners import clean_espacenet_data 

# Load environment variables
load_dotenv()

class DatabaseManager:
    """Handles database connection and operations"""
    def __init__(self):
        self.engine = create_engine(os.getenv('DATABASE_URL'))
        
    def store_patents(self, df):
        """Store patent data in PostgreSQL"""
        try:
            df.to_sql('raw_patents', self.engine, 
                      if_exists='append', 
                      index=False,
                      method='multi')
            print(f"Successfully stored {len(df)} patents in database")
            return True
        except Exception as e:
            print(f"Error storing data: {e}")
            return False



class EspacenetScraper:
    def __init__(self, search_keywords, headless=True,options_args=None):
        """Initialize the scraper with configurable options and search keywords."""
        self.search_keywords = search_keywords
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless')  # Run in headless mode
        if options_args:
          for arg in options_args:
            options.add_argument(arg)

        options.add_argument('--disable-blink-features=AutomationControlled')
        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(30)
        self.driver.set_window_size(1600, 1300)

    def construct_search_url(self):
        """Construct the search URL based on the provided keywords and their search fields."""
        base_url = 'https://worldwide.espacenet.com/patent/search?q='
        
        # Mapping of search fields to Espacenet query parameters
        field_mapping = {
            'title': 'ti',
            'abstract': 'ab',
            'claims': 'cl',
            'title,abstract or claims': 'ctxt' ,
            'all text fields' : 'ftxt',
            'title or abstract' : 'ta',
            'description' : 'desc',
            'all text fields or names' : 'nftxt',
            'title , abstract or names' : 'ntxt'
              # Full text search
        }
        
        query_parts = []
        for keyword, field in self.search_keywords.items():
            field_param = field_mapping.get(field, 'ctxt')  # Default to 'ctxt' if field is unknown
            query_parts.append(f'{field_param} = "{keyword}"')
        
        query = ' AND '.join(query_parts)
        query += '&queryLang=en%3Ade%3Afr'
        
        return base_url + query

    def add_random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to mimic human behavior."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def get_page_html(self, retries=3):
        """
        Navigate to the constructed URL and return the page HTML.
        Retry the operation if a timeout occurs.

        Args:
            retries (int): Number of retry attempts.

        Returns:
            str: The page HTML, or None if all retries fail.
        """
        url = self.construct_search_url()
        for attempt in range(retries):
            try:
                print(f"Navigating to: {url} (Attempt {attempt + 1})")
                self.driver.get(url)
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Add a random delay to mimic human behavior
                self.add_random_delay(3, 5)

                # Return the page HTML
                return self.driver.page_source

            except TimeoutException:
                print(f"Timed out waiting for the page to load. Retrying ({attempt + 1}/{retries})...")
                if attempt == retries - 1:
                    print("Max retries reached. Unable to load the page.")
                    return None
            except Exception as e:
                print(f"An error occurred: {e}")
                return None

    def download_csv(self, retries=3, max_results=500):
        """
        Complete the sequence of clicking:
        1. More Options button
        2. Download dropdown
        3. List (CSV) option
        4. Handle download dialog by:
           - Setting the "To" value to max_results (e.g., 500)
           - Clicking the Download button
        
        Args:
            retries (int): Number of retry attempts for the entire sequence.
            max_results (int): Maximum number of results to download (1-500).

        Returns:
            bool: True if the download sequence was successful, False otherwise.
        """
        for attempt in range(retries):
            try:
                print(f"Attempting download sequence (Attempt {attempt + 1})...")
                
                # Step 1: Click "More Options" button
                print("Looking for More Options button...")
                more_options_selector = "#more-options-selector--publication-list-header"
                more_options_button = WebDriverWait(self.driver, 30).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, more_options_selector))
                )
                
                # Try to click, but handle intercepted clicks
                try:
                    print("More Options button found. Clicking...")
                    more_options_button.click()
                except ElementClickInterceptedException:
                    print("Click intercepted, trying JavaScript click...")
                    self.driver.execute_script('document.querySelector("#more-options-selector--publication-list-header").click()', more_options_button)
                    
                self.add_random_delay(2, 3)
                print('More Options clicked successfully')
                
                # Step 2: Click "Download" section in the dropdown
                print("Looking for Download section...")
                # Use a more general selector to find the Download section
                # This uses contains() to match the text rather than a fixed CSS path
                download_section_xpath = "/html/body/div[2]/div[3]/ul/section[1]"
                download_section = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, download_section_xpath))
                )
                
                try:
                    print("Download section found. Clicking...")
                    download_section.click()
                except ElementClickInterceptedException:
                    print("Click intercepted, trying JavaScript click...")
                    self.driver.execute_script('document.querySelector("#simple-dropdown > div.prod-jss1034.prod-jss966.prod-jss969.prod-jss1045 > ul > section:nth-child(1)").click()', download_section)
                    
                self.add_random_delay(1, 2)
                print('Download section clicked successfully')
                
                # Step 3: Click "List (CSV)" option
                print("Looking for List (CSV) option...")
                # Use contains() with the XPATH to find the CSV option based on text
                csv_option_xpath = "/html/body/div[2]/div[3]/ul/li[2]"
                csv_option = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, csv_option_xpath))
                )
                
                try:
                    print("List (CSV) option found. Clicking...")
                    csv_option.click()
                except ElementClickInterceptedException:
                    print("Click intercepted, trying JavaScript click...")
                    self.driver.execute_script('document.querySelector("#simple-dropdown > div.prod-jss1034.prod-jss966.prod-jss969.prod-jss1045 > ul > li:nth-child(3)").click()', csv_option)
                    
                self.add_random_delay(2, 3)
                print('List (CSV) option clicked successfully')
                
                # Step 4: Handle the download dialog
                print("Waiting for download dialog to appear...")
                
                # Wait for the dialog to appear
                download_dialog_xpath = "/html/body/div[2]/div[3]/div/div"
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, download_dialog_xpath))
                )
                print("Download dialog appeared")
                
                # Find the "To" input field
                to_input_xpath = "/html/body/div[2]/div[3]/div/div/div/div[1]/input[2]"
                to_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, to_input_xpath))
                )
                
                # Clear the input and set it to max_results
                print(f"Setting maximum results to {max_results}...")
                to_input.clear()
                to_input.send_keys(str(max_results))
                self.add_random_delay(1, 2)
                
                # Click the Download button in the dialog
                download_button_xpath = "/html/body/div[2]/div[3]/div/div/div/button"
                download_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, download_button_xpath))
                )
                
                try:
                    print("Download button found. Clicking...")
                    download_button.click()
                except ElementClickInterceptedException:
                    print("Click intercepted, trying JavaScript click...")
                    self.driver.execute_script('document.querySelector("body > div.prod-jss12 > div.prod-jss15.prod-jss13 > div > div > div > button").click()', download_button)
                
                print("Download button clicked")
                
                # Wait for a moment to ensure the download starts
                self.add_random_delay(3, 5)
                
                # Check if there are any error messages
                try:
                    error_message = self.driver.find_element(By.XPATH, "//div[contains(@class, 'download-modal__validation')]//span")
                    if error_message.is_displayed() and error_message.text.strip():
                        print(f"Error in download dialog: {error_message.text}")
                        return False
                except:
                    # No error message found, continue
                    pass
                
                print("Download sequence completed successfully")
                return True
                
            except TimeoutException as e:
                print(f"Timeout during download sequence: {e}")
                if attempt == retries - 1:
                    print("Max retries reached. Download sequence failed.")
                    return False
            except Exception as e:
                print(f"Error during download sequence: {e}")
                if attempt == retries - 1:
                    print("Max retries reached. Download sequence failed.")
                    return False
                
            # If we reach here, there was an error and we need to try again
            # Refresh the page before the next attempt
            try:
                self.driver.refresh()
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                self.add_random_delay(3, 5)
            except Exception as e:
                print(f"Error refreshing page: {e}")

        return False

    def close(self):
        """Close the browser when done."""
        if self.driver:
            self.driver.quit()




def process_downloaded_data(downloads_path):
    """Process latest downloaded CSV file"""
    try:
        # Find latest CSV
        list_of_files = glob.glob(os.path.join(downloads_path, "*.csv"))
        if not list_of_files:
            return None
            
        latest_file = max(list_of_files, key=os.path.getmtime)
        print(f"Processing file: {latest_file}")

        # Read and clean data
        df = pd.read_csv(latest_file, delimiter=';', skiprows=7)
        df = clean_espacenet_data(df)
        # if 'family_jurisdictions' not in df.columns:
        #     df['family_jurisdictions'] = None
        # if 'family_members' not in df.columns:
        #     df['family_members'] = None

    

          
        return df
    
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def main(search_keywords, max_results=500):
    # Initialize components
    scraper = EspacenetScraper(search_keywords, headless=False)
    db_manager = DatabaseManager()
    
    try:
        # Execute scraping
        if scraper.get_page_html(retries=3):
            if scraper.download_csv(max_results=max_results):
                # Process downloaded data
                time.sleep(10)  # Wait for download completion
                df = process_downloaded_data(os.path.expanduser("~/Downloads"))
                
                if df is not None:
                    # Store in database
                    if db_manager.store_patents(df):
                        print("Data pipeline completed successfully")
                        return True
        return False
    finally:
      if scraper: 
        scraper.close()
        

if __name__ == '__main__':
    # Configuration
    SEARCH_KEYWORDS = {
        "cloud": "title,abstract or claims",
        "security": "title,abstract or claims"
    }
    
    # Run pipeline
    success = main(SEARCH_KEYWORDS, max_results=500)
    
    if success:
        print("✅ Data scraped and stored successfully")
    else:
        print("❌ Pipeline failed")