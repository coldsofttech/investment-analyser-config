import re
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scraper_retry import retry


class ScraperUtils:
    @staticmethod
    def retry_click(wait, xpath):
        @retry(max_retries=5, delay=1)
        def click():
            elem = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            elem.click()
            return elem

        return click()

    @staticmethod
    def wait_for_presence(wait, xpath):
        return wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

    @staticmethod
    def send_keys_to_element(element, keys):
        element.clear()
        element.send_keys(keys)
        time.sleep(0.3)

    @staticmethod
    def extract_number_from_text(text, pattern=r'of\s+([\d,]+)'):
        match = re.search(pattern, text)
        return int(match.group(1).replace(',', '')) if match else 0
