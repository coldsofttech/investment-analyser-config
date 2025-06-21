import argparse
import json
import random
import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_waiter(driver, timeout=1):
    return WebDriverWait(driver, timeout)


def accept_all(driver):
    wait = get_waiter(driver, 10)
    print(f"⏳ Awaiting for 'Accept All' button")
    accept_all_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//*[@id="consent-page"]//button[text()="Accept all"]')
    ))
    accept_all_button.click()
    print(f"✅ Accept All button clicked.")


def select_country(driver, country):
    wait = get_waiter(driver, 10)
    print(f"⏳ Awaiting for 'Region' dropdown")
    region_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//button[contains(@class, "menuBtn") and .//div[text()="Region"]]')
    ))
    region_button.click()
    print(f"✅ Region dropdown clicked.")
    wait.until(EC.presence_of_element_located(
        (By.XPATH, '//div[contains(@class, "dialog-container")]')
    ))
    print(f"⏳ Awaiting for 'Reset' button")
    reset_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//div[contains(@class, "reset")]//button')
    ))
    reset_button.click()
    print(f"✅ Reset button clicked.")
    search_text = wait.until(EC.presence_of_element_located(
        (By.XPATH, '//div[contains(@class, "search")]//input[@placeholder="Search..."]')
    ))
    search_text.send_keys(country)
    search_text.send_keys(Keys.ENTER)
    country_checkbox = wait.until(EC.presence_of_element_located(
        (By.XPATH,
         f'//div[contains(@class, "options")]//label[contains(@class, "input") and @title="{country}"]//input[@type="checkbox"]')
    ))
    if not country_checkbox.is_selected():
        country_checkbox.click()
        print(f"✅ {country} option selected.")
    else:
        print(f"✅ {country} option already selected.")
    apply_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//div[contains(@class, "submit")]//button')
    ))
    apply_button.click()
    print(f"✅ Apply button clicked.")


def get_rows_per_page(driver):
    wait = get_waiter(driver)
    pagination_dropdown = wait.until(EC.presence_of_element_located(
        (By.XPATH, '//div[contains(@class, "screener-table")]//div[contains(@class, "paginationContainer")]//div[contains(@class, "container")]//span[contains(@class, "textSelect")]')
    ))
    rows = pagination_dropdown.text.strip()

    return int(rows)


def get_total_rows(driver):
    wait = get_waiter(driver)
    pagination_dropdown = wait.until(EC.presence_of_element_located((
        By.XPATH,
        '//div[contains(@class, "screener-table")]//div[contains(@class, "paginationContainer")]//div[contains(@class, "total")]'
    )))
    pag_text = pagination_dropdown.text.strip()
    match = re.search(r'of\s+([\d,]+)', pag_text)
    if match:
        rows = int(match.group(1).replace(',', ''))
    else:
        rows = 0

    return rows


def export_equity(driver):
    tickers = []
    wait = get_waiter(driver, 10)
    last_seen_first_ticker = None
    page_number = 0
    rows_per_age = get_rows_per_page(driver)
    total_rows = get_total_rows(driver)

    while True:
        page_number += 1
        completion_per = (len(tickers) / total_rows) * 100
        print(f"ℹ️ Loaded page: {page_number} ({completion_per:.2f} %).")
        time.sleep(random.uniform(0.1, 0.5))
        wait.until(EC.presence_of_element_located((
            By.XPATH,
            '//div[contains(@class, "screener-table")]//div[contains(@class, "table-container")]//table//tbody'
        )))
        equity_table = driver.find_element(
            By.XPATH,
            '//div[contains(@class, "screener-table")]//div[contains(@class, "table-container")]//table//tbody'
        )
        rows = equity_table.find_elements(By.XPATH, './tr')

        page_tickers = []
        for i, row in enumerate(rows):
            try:
                ticker_element = row.find_element(
                    By.XPATH,
                    './/span[contains(@class, "ticker-wrapper")]//a[contains(@class, "ticker")]//span[contains(@class, "symbol")]'
                )
                ticker_text = ticker_element.text.strip()
                if ticker_text:
                    page_tickers.append(ticker_text)
            except Exception as e:
                print(f"⚠️ Skipping row {i} due to error: {e}")

        if not page_tickers:
            print(f"⚠️ No tickers found on page. Ending.")
            break

        tickers.extend(page_tickers)

        try:
            next_button = driver.find_element(
                By.XPATH,
                '//div[contains(@class, "screener-table")]//div[contains(@class, "paginationContainer")]//div[contains(@class, "buttons")]//button[@aria-label="Goto next page"]'
            )
            if next_button.get_attribute("disabled") is not None:
                print(f"✅ Last page reached.")
                break

            last_seen_first_ticker = page_tickers[0]
            next_button.click()
            wait.until(lambda d: d.find_element(
                By.XPATH,
                '//div[contains(@class, "screener-table")]//div[contains(@class, "table-container")]//table//tbody/tr[1]//span[contains(@class, "symbol")]'
            ).text.strip() != last_seen_first_ticker)
        except Exception as e:
            print(f"❌ Pagination error: {e}")
            break

    print(f"\n✅ Total tickers collected: {len(tickers)}")
    return tickers


def export_tickers(
        country,
        ticker_type="EQUITY",
        disable_headless=False
):
    chrome_options = Options()
    if not disable_headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=chrome_options)
    file_name = f"{ticker_type}_{country}.json"
    if ticker_type == "EQUITY":
        url = "https://finance.yahoo.com/research-hub/screener/equity/"
        driver.get(url)
        accept_all(driver)
        select_country(driver, country)
        equities = export_equity(driver)

        with open(file_name, "w") as eq_file:
            json.dump(equities, eq_file, indent=4, sort_keys=True)

        print(f"✅ Exported to {file_name}")

    input()
    driver.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock tickers using scraper")
    parser.add_argument(
        "--country",
        required=True,
        type=str,
        help="Name of the Country. For example, United States, United Kingdom, etc."
    )
    parser.add_argument(
        "--type",
        required=True,
        default="EQUITY",
        type=str,
        help="Ticker Type. For example, EQUITY, ETF, etc."
    )
    parser.add_argument(
        "--disable-headless",
        action="store_true",
        help="True or False for headless Chrome"
    )
    args = parser.parse_args()
    export_tickers(args.country, args.type, args.disable_headless)
