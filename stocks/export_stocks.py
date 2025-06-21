import argparse
import concurrent.futures
import json
import os
import random
import time
from datetime import datetime

import yfinance as yf
from tenacity import RetryError
from tqdm import tqdm

import stock_utils


def fetch_ticker_data(ticker, output_dir="output"):
    time.sleep(random.uniform(0.1, 0.5))

    try:
        print(f"📥 Fetching data for {ticker}...")
        timestamp = datetime.now().isoformat()
        yf_ticker = yf.Ticker(ticker)

        raw_data = stock_utils.fetch_history(yf_ticker)
        csv_data, price_col = stock_utils.download_stock_info(raw_data.copy())
        info = stock_utils.fetch_info(yf_ticker)

        try:
            calendar = stock_utils.fetch_calendar(yf_ticker)
        except RetryError as re:
            calendar = {}
            print(f"⚠️ Skipping calendar for {ticker} after retries: {re.last_attempt.exception()}")

        dividends = stock_utils.fetch_dividends(yf_ticker)
        valid_data = stock_utils.process_index(csv_data)
        valid_div_data = stock_utils.process_index(dividends)
        upcoming_div_date, upcoming_div_amount = stock_utils.calculate_upcoming_dividend(
            calendar, csv_data.copy(), price_col, stock_utils.safe_get(info, "dividendYield", 0)
        )
        ticker_type = stock_utils.safe_get(info, "quoteType", "").upper()

        result_dict = {}
        metadata_dict = {"lastUpdatedTimestamp": timestamp}
        ticker_dict = {
            "tickerCode": ticker.upper(),
            "info": {
                "companyName": stock_utils.safe_get(info, "longName", ""),
                "companyDescription": stock_utils.safe_get(info, "longBusinessSummary", ""),
                "type": ticker_type,
                "exchange": stock_utils.safe_get(info, "exchange", ""),
                "industry": stock_utils.safe_get(info, "industry", ""),
                "sector": stock_utils.safe_get(info, "sector", ""),
                "website": stock_utils.safe_get(info, "website", ""),
                "currency": stock_utils.safe_get(info, "currency", ""),
                "beta": stock_utils.safe_get(info, "beta", ""),
                "payoutRatio": stock_utils.safe_get(info, "payoutRatio", ""),
                "dividendYield": stock_utils.safe_get(info, "dividendYield", ""),
                "dividendFrequency": stock_utils.calculate_dividend_frequency(valid_div_data),
                "volatility": stock_utils.calculate_volatility(raw_data.copy(), price_col),
                "maxDrawdown": stock_utils.calculate_max_drawdown(csv_data.copy(), price_col),
                "sharpeRatio": stock_utils.calculate_sharpe_ratio(raw_data.copy(), price_col)
            },
            "data": [
                {"date": d.strftime('%Y-%m-%d'), "price": float(p)}
                for d, p in zip(valid_data.index, valid_data[price_col])
            ],
            "dividends": [
                {"date": d.strftime('%Y-%m-%d'), "price": float(p)}
                for d, p in zip(valid_div_data.index, valid_div_data.tolist())
            ],
            "events": {
                "dividends": {
                    "date": upcoming_div_date.strftime('%Y-%m-%d') if upcoming_div_date else None,
                    "price": float(upcoming_div_amount) if upcoming_div_amount else None
                }
            },
            "priceInfo": {
                "currentPrice": float(stock_utils.safe_get(info, "currentPrice", csv_data.iloc[-1][price_col]))
            }
        }

        if ticker_type.lower() == "etf":
            ticker_dict["holdings"] = {
                "topHoldings": stock_utils.fetch_top_holdings(yf_ticker),
                "sectorWeights": stock_utils.fetch_sector_weightings(yf_ticker)
            }
            ticker_dict["info"]["marketCap"] = stock_utils.safe_get(info, "totalAssets", "")
        else:
            ticker_dict["info"]["marketCap"] = stock_utils.safe_get(info, "marketCap", "")

        result_dict[ticker] = ticker_dict
        result_dict["metadata"] = metadata_dict

        output_path = os.path.join(output_dir, f"{ticker}.json")
        with open(output_path, "w") as f:
            json.dump(result_dict, f, indent=4, sort_keys=True)

        print(f"✅ Saved: {output_path}.")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {ticker}: {str(e)}")


def export_ticker_data(tickers, output_dir="output", error_log="error.log", max_workers=10):
    os.makedirs(output_dir, exist_ok=True)
    results = []
    errors = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ticker_data, ticker, output_dir): ticker for ticker in tickers}

        for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(tickers),
                desc="Fetching tickers"
        ):
            ticker = futures[future]
            try:
                future.result()
                results.append(ticker)
            except Exception as e:
                error_msg = stock_utils.get_root_error_message(e)
                errors[ticker] = error_msg

    if errors:
        with open(error_log, "w") as ef:
            for tkr, err in errors.items():
                ef.write(f"{tkr}: {err}\n")

    print(f"\n✅ Exported {len(results)} tickers to {output_dir}.")
    if errors:
        print(f"⚠️ {len(errors)} errors logged to {error_log}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock/ETF data using yfinance.")
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
    export_ticker_data(ticker_list, max_workers=args.max_workers)
