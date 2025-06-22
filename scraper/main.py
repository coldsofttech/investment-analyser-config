import argparse
import io
import json
import os
import sys

from scraper import Scraper

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


def save_to_file(data, filename, output_dir="output"):
    os.makedirs(output_dir, exist_ok=True)
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
