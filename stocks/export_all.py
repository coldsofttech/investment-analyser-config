import argparse
import concurrent
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from stock_fetcher import StockFetcher
from stock_utils import StockUtils


def export_ticker(tickers, output_dir="output", error_log="error.log", max_workers=10):
    os.makedirs(output_dir, exist_ok=True)
    results = []
    errors = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(StockFetcher.fetch_ticker, ticker): ticker for ticker in tickers}

        for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(tickers),
                desc="Fetching tickers",
                unit="tickers"
        ):
            ticker = futures[future]
            try:
                data = future.result()
                results.append(data)
            except Exception as e:
                error_msg = StockUtils.get_root_error_message(e)
                errors[ticker] = error_msg

    output_path = os.path.join(output_dir, f"ticker_{str(uuid.uuid4())}.json")
    with open(output_path, "w") as jf:
        json.dump(results, jf, indent=4, sort_keys=True)

    if errors:
        with open(error_log, "w") as ef:
            for tkr, err in errors.items():
                ef.write(f"{tkr}: {err}\n")

    print(f"\n✅ Exported {len(results)} tickers to {output_path}")
    if errors:
        print(f"⚠️ {len(errors)} errors logged to {error_log}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch high-level stock/ETF data using yfinance.")
    parser.add_argument(
        "--chunk-id",
        required=True,
        type=int,
        help="Id of the chunk file to process."
    )
    parser.add_argument(
        "--max-workers",
        default=20,
        type=int,
        help="Maximum number of parallel threads."
    )
    args = parser.parse_args()
    chunk_file = os.path.join("chunks", f"chunk_{args.chunk_id}.json")
    with open(chunk_file, "r") as c_file:
        tickers_raw = json.load(c_file)

    ticker_list = [
        t.strip().upper()
        for t in tickers_raw
        if t.strip()
    ]
    export_ticker(ticker_list, max_workers=args.max_workers)
