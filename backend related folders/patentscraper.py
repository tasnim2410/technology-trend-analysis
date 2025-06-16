import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

class PatentsSearch:
    def __init__(self, headless=True, proxy=None):
        """Initialize the scraper with proxy support and optional headless mode."""
        options = uc.ChromeOptions()
        if headless:
            options.add_argument('--headless')
        else:
            options.add_argument('--start-maximized')  # Maximize window for visibility
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        if proxy:
            options.add_argument(f'--proxy-server={proxy}')
        
        try:
            print("Initializing Chrome browser...")
            self.driver = uc.Chrome(options=options)
            print("Browser initialized successfully")
        except Exception as e:
            print(f"Failed to initialize browser: {e}")
            self.driver = None

    def get_page_html(self, url):
        """Fetch the HTML content of the page."""
        if self.driver is None:
            print("Driver not initialized")
            return None
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.XPATH, "//h5[contains(text(), 'Published as')]"))
            )
            time.sleep(5)  # Delay to allow page to fully load and for visibility
            return self.driver.page_source
        except Exception as e:
            print(f"Error loading page: {e}")
            return None

    def parse_html(self, html):
        """Parse HTML to extract family members."""
        if not html:
            return []
        soup = BeautifulSoup(html, 'html.parser')
        published_as = soup.find("h5", string=lambda text: "Published as" in text if text else False)
        if published_as:
            content = published_as.find_next_sibling("span")
            if content:
                return [span.get_text(strip=True) for span in content.find_all("span") if span.get_text(strip=True)]
        return []

    def close(self):
        """Close the browser safely."""
        if self.driver:
            try:
                self.driver.quit()
                print("Browser closed")
            except Exception as e:
                print(f"Error closing browser: {e}")