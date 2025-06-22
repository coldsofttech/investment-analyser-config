import argparse
import json
import os
import re
import time
from functools import wraps

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm


def retry(max_retries=5, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Warning: {func.__name__} failed: {e} (attempt: {attempt + 1}).")
                    time.sleep(delay)
            raise RuntimeError(f"❌ {func.__name__} failed after {max_retries} retries.")

        return wrapper

    return decorator


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

    def visit(self):
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
        if self.ticker_type == "EQUITY":
            self._select_dropdown_option("Region", self.region)
        elif self.ticker_type == "ETF":
            self._select_dropdown_option("Exchange", self.region)
        else:
            print(f"ℹ️ No filters applied for ticker type: {self.ticker_type}")

    def _extract_total_rows(self):
        total_text_xpath = '//div[contains(@class,"total")]'
        total_elem = ScraperUtils.wait_for_presence(self.wait, total_text_xpath)
        total_rows = ScraperUtils.extract_number_from_text(total_elem.text)
        print(f"🔢 Total rows found: {total_rows}")
        return total_rows

    @staticmethod
    @retry(max_retries=5, delay=2)
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
        print("📋 Starting ticker extraction...")
        tickers = []
        total_rows = self._extract_total_rows()
        seen_first = None

        pbar = tqdm(total=total_rows, desc="Scrapping tickers", unit="tickers")

        while True:
            rows = self.driver.find_elements(By.XPATH, '//table//tbody/tr')
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


def save_to_file(data, filename, output_dir="output"):
    with open(os.path.join(output_dir, filename), "w") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved {len(data)} tickers to '{filename}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--country",
        required=True,
        help="Country or region filter (e.g., 'India')"
    )
    parser.add_argument(
        "--type",
        required=True,
        choices=["EQUITY", "ETF"],
        default="EQUITY",
        help="Ticker type"
    )
    parser.add_argument(
        "--disable-headless",
        action="store_true",
        help="Disable headless mode for browser"
    )
    args = parser.parse_args()

    scraper = Scraper(
        ticker_type=args.type,
        region=args.country,
        headless=not args.disable_headless
    )
    tickers = scraper.run()
    if tickers:
        save_to_file(tickers, f"{args.type}_{args.country}.json")
