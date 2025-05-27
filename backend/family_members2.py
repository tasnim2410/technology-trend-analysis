import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import urllib.parse

class PatentsSearch:
    def __init__(self, headless=True):
        """Initialize the scraper with enhanced compatibility options."""
        
        options = uc.ChromeOptions()
        
        
        if headless:
            options.add_argument('--headless')
        
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-extensions')
        
        try:
            
            self.driver = uc.Chrome(
                options=options, 
                use_subprocess=True,  
                version_main=None,    
                suppress_welcome=True,
                debug=False
            )
            
            
            self.driver.set_page_load_timeout(30)
            self.driver.set_window_size(1920, 1080)
        
        except Exception as e:
            print(f"Failed to initialize ChromeDriver: {e}")
            print("Trying alternative initialization method...")
            
            # Alternative initialization method
            self.driver = uc.Chrome(
                options=options,
                driver_executable_path=None  
            )

    def add_random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to mimic human behavior."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def get_page_html(self, url):
        """Navigate to the given URL and return the page HTML."""
        try:
            print(f"Navigating to: {url}")
            self.driver.get(url)

            
            WebDriverWait(self.driver, 25).until(
                EC.presence_of_element_located((By.TAG_NAME, "h5"))
            )

            
            self.add_random_delay(3, 5)

            
            return self.driver.page_source

        except TimeoutException:
            print("Timed out waiting for the page to load.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def parse_html(self, html):
        """Parse the HTML and extract all span elements inside the 'Published as' content."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for the element containing "Publié en tant que" or "Published as"
        published_as_element = soup.find(lambda tag: tag.name == "h5" and ("Publié en tant que" in tag.text or "Published as" in tag.text))
        
        if published_as_element:
            # Get the next sibling span that contains the relevant content
            content_element = published_as_element.find_next_sibling("span")
            if content_element:
                # Extract all span elements within the content
                spans = content_element.find_all('span')
                return [span.get_text(strip=True) for span in spans]
        return []

    def close(self):
        """Close the browser when done."""
        if self.driver:
            self.driver.quit()



def build_espacenet_url(row: dict,
                        keywords: dict[str, str],
                        field_mapping: dict[str, str]) -> str:
    """
    Constructs an Espacenet search-by-family URL for a given row,
    replacing the `q=` parameter with your mapped keywords.

    row must have:
      - 'Family number'
      - 'first publication number'
    keywords maps terms → field-names (e.g. "quantum": "title")
    field_mapping maps field-names → codes (e.g. "title": "ti")
    """
    # 1) translate each keyword into its field-code
    clauses = []
    for term, field in keywords.items():
        if field not in field_mapping:
            raise KeyError(f"No code for field '{field}'")
        code = field_mapping[field]
        # each clause: code="term"
        clauses.append(f'{code}="{term}"')
    
    # 2) join clauses with AND
    query = " AND ".join(clauses)
    
    # 3) URL-encode the query, keeping quotes so they remain literal
    #    this encodes '=' to %3D and spaces to %20, but leaves " unencoded
    q_param = urllib.parse.quote(query, safe='"')
    
    # 4) build the full URL
    base = "https://worldwide.espacenet.com/patent/search/family"
    fam = row["Family number"]
    pub = row["first_publication_number"]
    url = f"{base}/{fam}/publication/{pub}?q={q_param}"
    return url


# if __name__ == '__main__':
    
#     scraper = PatentsSearch(headless=False)  # Set headless to False to see the browser in action

    
  
    
#     df['family_members'] = None

#     try:
#         for index, row in df.iterrows():
            
#             url = f"https://worldwide.espacenet.com/patent/search/family/{row['Family number']}/publication/{row['first publication number']}?q=hydrogen%20battery"

            
#             html = scraper.get_page_html(url)
#             if html:
#                 print(f"Page HTML retrieved successfully for {row['first publication number']}.")
                
#                 family_members = scraper.parse_html(html)
#                 df.at[index, 'family_members'] = family_members  
#             else:
#                 print(f"Failed to retrieve the page HTML for {row['first publication number']}.")

#     finally:
        
#         scraper.close()
#         print("Scraper closed.")

    
    