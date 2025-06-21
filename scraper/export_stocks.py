import argparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_waiter(driver, timeout=1):
    return WebDriverWait(driver, timeout)


def accept_all(driver):
    wait = get_waiter(driver, 10)
    print(f"⏳ Awaiting for 'Accept All' button")
    accept_all_button = wait.until(EC.element_to_be_clickable(
        (By.XPATH, '//*[@id="consent-page"]/div/div/div/form/div[2]/div[2]/button[1]')
    ))
    accept_all_button.click()
    print(f"✅ Accept All button clicked.")


def export_tickers(
        country,
        ticker_type="EQUITY",
        url="https://finance.yahoo.com/research-hub/screener/"
):
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Chrome(options=chrome_options)
    if ticker_type == "EQUITY":
        url = "https://finance.yahoo.com/research-hub/screener/equity/"

    driver.get(url)
    accept_all(driver)
    input()


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
    args = parser.parse_args()
    export_tickers(args.country, args.type)
