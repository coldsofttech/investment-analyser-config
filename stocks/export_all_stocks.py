import argparse
import concurrent
import json
import os
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf
from tqdm import tqdm

import stock_utils


def fetch_ticker_data(ticker):
    try:
        print(f"📥 Fetching data for {ticker}...")
        yf_ticker = yf.Ticker(ticker)

        raw_data = stock_utils.fetch_history(yf_ticker)
        csv_data, price_col = stock_utils.download_stock_info(raw_data.copy())
        info = stock_utils.fetch_info(yf_ticker)
        dividends = stock_utils.fetch_dividends(yf_ticker)

        valid_data = stock_utils.process_index(csv_data)
        valid_div_data = stock_utils.process_index(dividends)

        ticker_type = stock_utils.safe_get(info, "quoteType", "")
        industry = stock_utils.safe_get(info, "industry", "")
        sector = stock_utils.safe_get(info, "sector", "")

        result = {
            "ticker": ticker,
            "companyName": stock_utils.safe_get(info, "longName", ""),
            "industry": industry,
            "sector": sector,
            "exchange": stock_utils.safe_get(info, "exchange", ""),
            "type": ticker_type,
            "country": stock_utils.safe_get(info, "region", ""),
            "currency": stock_utils.safe_get(info, "currency", ""),
            "beta": stock_utils.safe_get(info, "beta", ""),
            "volatility": stock_utils.calculate_volatility(raw_data.copy(), price_col),
            "dividendYield": stock_utils.safe_get(info, "dividendYield", ""),
            "dividendFrequency": stock_utils.calculate_dividend_frequency(valid_div_data),
            "website": stock_utils.safe_get(info, "website", ""),
            "companyDescription": stock_utils.safe_get(info, "longBusinessSummary", ""),
            "currentPrice": stock_utils.safe_get(info, "currentPrice", "")
        }

        cagr = stock_utils.calculate_short_and_long_term_cagr(valid_data, price_col)
        result["shortTermCagr"] = cagr.get("shortTermCagr", None)
        result["longTermCagr"] = cagr.get("longTermCagr", None)

        if ticker_type.lower() == "etf":
            result["marketCap"] = stock_utils.safe_get(info, "totalAssets", "")
            result["industry"] = industry if industry else "Exchange-Traded Fund"
            result["sector"] = sector if sector else "Exchange-Traded Fund"
        elif ticker_type.lower() == "mutualfund":
            result["marketCap"] = stock_utils.safe_get(info, "marketCap", "")
            result["industry"] = industry if industry else "Mutual Fund"
            result["sector"] = sector if sector else "Mutual Fund"
        else:
            result["marketCap"] = stock_utils.safe_get(info, "marketCap", "")

        return result
    except Exception as e:
        raise RuntimeError(f"Failed to fetch {ticker}: {str(e)}")


def export_ticker_data(tickers, output_dir="output", error_log="error.log", max_workers=10):
    os.makedirs(output_dir, exist_ok=True)
    results = []
    errors = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_ticker_data, ticker): ticker for ticker in tickers}

        for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(tickers),
                desc="Fetching tickers"
        ):
            ticker = futures[future]
            try:
                data = future.result()
                # if "error" in data:
                #     errors[data["ticker"]] = data["error"]
                # else:
                results.append(data)
            except Exception as e:
                # ticker = futures[future]
                errors[ticker] = str(e)

    all_path = os.path.join(output_dir, "all.json")
    with open(all_path, "w") as jf:
        json.dump(results, jf, indent=4, sort_keys=True)

    if errors:
        with open(error_log, "w") as ef:
            for tkr, err in errors.items():
                ef.write(f"{tkr}: {err}\n")

    print(f"\n✅ Exported {len(results)} tickers to {all_path}")
    if errors:
        print(f"⚠️ {len(errors)} errors logged to {error_log}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch high-level stock/ETF data using yfinance.")
    parser.add_argument(
        "--input",
        required=True,
        type=str,
        help="Path of the input JSON file. JSON file should be a list of tickers."
    )
    parser.add_argument(
        "--max-workers",
        default=10,
        type=int,
        help="Maximum number of parallel threads."
    )
    args = parser.parse_args()

    with open(args.input, "r") as f:
        ticker_list = json.load(f)

    export_ticker_data(ticker_list, max_workers=args.max_workers)
