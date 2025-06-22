import random
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from scraper_retry import retry
from scraper_utils import ScraperUtils


class Scraper:
    def __init__(self, ticker_type, region, headless=True):
        self.ticker_type = ticker_type
        self.region = region
        self.driver = self._init_driver(headless)
        self.wait = WebDriverWait(self.driver, 10)
        self.base_url = f"https://finance.yahoo.com/research-hub/screener/{self.ticker_type.lower()}/"

    def _init_driver(self, headless):
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        print("🚀 Launching Chrome browser...")
        return webdriver.Chrome(options=options)

    def _wait(self, a, b):
        time.sleep(random.uniform(a, b))

    def visit(self):
        self._wait(0.1, 0.5)
        print(f"🌐 Navigating to {self.base_url}")
        self.driver.get(self.base_url)
        self._accept_all()

    def _accept_all(self):
        print("🛡️ Checking for consent dialog...")
        try:
            ScraperUtils.retry_click(
                self.wait, '//*[@id="consent-page"]//button[text()="Accept all"]'
            )
            print("✅ Consent accepted.")
        except:
            print("ℹ️ Consent dialog not found or already accepted.")

    def _select_dropdown_option(self, label, value):
        self._wait(1, 2)
        print(f"🔍 Selecting '{value}' in dropdown labeled '{label}'...")
        ScraperUtils.retry_click(
            self.wait, f'//button[contains(@class, "menuBtn") and .//div[text()="{label}"]]'
        )
        ScraperUtils.retry_click(self.wait, '//div[contains(@class, "reset")]//button')
        search_input = ScraperUtils.wait_for_presence(
            self.wait, '//input[@placeholder="Search..."]'
        )
        ScraperUtils.send_keys_to_element(search_input, value)
        search_input.send_keys(Keys.ENTER)
        time.sleep(0.5)
        checkbox_xpath = f'//label[@title="{value}"]//input[@type="checkbox"]'
        checkbox = ScraperUtils.wait_for_presence(self.wait, checkbox_xpath)
        if not checkbox.is_selected():
            checkbox.click()
        ScraperUtils.retry_click(self.wait, '//div[contains(@class, "submit")]//button')
        print(f"✅ Selected '{value}' in '{label}' dropdown.")

    def apply_filters(self):
        self._wait(1, 2)
        if self.ticker_type == "EQUITY":
            self._select_dropdown_option("Region", self.region)
        elif self.ticker_type == "ETF":
            self._select_dropdown_option("Exchange", self.region)
        else:
            print(f"ℹ️ No filters applied for ticker type: {self.ticker_type}")

    @retry(max_retries=5, delay=2)
    def _extract_total_rows(self):
        total_text_xpath = '//div[contains(@class,"total")]'
        total_elem = ScraperUtils.wait_for_presence(self.wait, total_text_xpath)
        total_rows = ScraperUtils.extract_number_from_text(total_elem.text)
        print(f"🔢 Total rows found: {total_rows}")
        return total_rows

    @retry(max_retries=5, delay=2)
    def _get_table_rows(self):
        return self.driver.find_elements(By.XPATH, '//table//tbody/tr')

    @staticmethod
    @retry(max_retries=10, delay=2)
    def _click_next_page(driver, prev_first):
        next_btn = driver.find_element(
            By.XPATH, '//button[@aria-label="Goto next page"]'
        )
        if next_btn.get_attribute("disabled"):
            raise RuntimeError("⏹️ Next button is disabled.")

        next_btn.click()
        WebDriverWait(driver, 10).until(
            EC.staleness_of(driver.find_element(
                By.XPATH, '//tbody/tr[1]//span[contains(@class, "symbol")]'
            ))
        )
        WebDriverWait(driver, 10).until(lambda d: d.find_element(
            By.XPATH, '//tbody/tr[1]//span[contains(@class, "symbol")]'
        ).text.strip() != prev_first)

    def scrape_tickers(self):
        self._wait(1, 2)
        print("📋 Starting ticker extraction...")
        tickers = []
        total_rows = self._extract_total_rows()
        seen_first = None

        pbar = tqdm(total=total_rows, desc="Scrapping tickers", unit="tickers")

        while True:
            rows = self._get_table_rows()
            if not rows:
                print("⚠️ No rows found on page, ending scrape.")
                break

            page_tickers = []
            for row in rows:
                try:
                    symbol = row.find_element(
                        By.XPATH, './/span[contains(@class, "symbol")]'
                    ).text.strip()
                    if symbol:
                        page_tickers.append(symbol)
                except:
                    continue

            if not page_tickers:
                print("⚠️ No tickers found on current page, stopping.")
                break

            tickers.extend(page_tickers)
            pbar.update(len(page_tickers))

            if seen_first == page_tickers[0]:
                print("🏁 First ticker same as previous page, assuming last page reached.")
                break
            seen_first = page_tickers[0]

            try:
                prev_first = self.driver.find_element(
                    By.XPATH, '//tbody/tr[1]//span[contains(@class, "symbol")]'
                ).text.strip()

                self._click_next_page(self.driver, prev_first)
                time.sleep(0.5)
            except Exception as e:
                print(f"⚠️ Could not go to next page: {e}")
                break

        pbar.close()
        print(f"🎉 Finished scraping. Total tickers extracted: {len(tickers)}")
        return tickers

    def run(self):
        try:
            self.visit()
            self.apply_filters()
            return self.scrape_tickers()
        finally:
            print("🧹 Closing browser...")
            self.driver.quit()
