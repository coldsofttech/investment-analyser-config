import argparse
import concurrent
import json
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm

from stock_fetcher import StockFetcher
from stock_utils import StockUtils


def export_ticker(tickers, output_dir="output", error_log="error.log", max_workers=10, max_global_retries=5):
    os.makedirs(output_dir, exist_ok=True)
    remaining = set(tickers)
    min_workers = 10
    decay_rate = 0.2
    results = {}
    errors = {}

    for global_attempt in range(0, max_global_retries):
        factor = (1 - decay_rate) ** (global_attempt - 1)
        current_workers = max(int(max_workers * factor), min_workers)
        print(f"🔁 Attempt: {global_attempt} with {current_workers} workers.")
        with concurrent.futures.ThreadPoolExecutor(max_workers=current_workers) as executor:
            futures = {executor.submit(StockFetcher.fetch_ticker, t, global_attempt): t for t in remaining}

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(remaining),
                desc="Fetching tickers",
                unit="tickers"
            ):
                ticker = futures[future]
                results[ticker] = future.result()

        failed = {
            t for t, r in results.items()
            if r.get("error", None) is not None
        }
        if not failed:
            break

        print(f"🔁 Retrying {len(failed)} failed tickers: {failed}")
        remaining = failed
        time.sleep(random.uniform(5, 10))

    output_path = os.path.join(output_dir, f"ticker_{str(uuid.uuid4())}.json")
    final_result = list(results.values())
    with open(output_path, "w") as jf:
        json.dump(final_result, jf, indent=4, sort_keys=True)

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
    parser.add_argument(
        "--max-global-retries",
        default=5,
        type=int,
        help="Maximum number of global retries. Defaults to 5"
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
    export_ticker(
        ticker_list,
        max_workers=args.max_workers,
        max_global_retries=args.max_global_retries
    )
