import argparse
import json
import os
import sys
from datetime import datetime

import yfinance as yf
from tenacity import RetryError

import stock_utils


def export_ticker_data(tickers, output_dir="output", error_log="error.log"):
    os.makedirs(output_dir, exist_ok=True)
    total = len(tickers)
    any_errors = False

    with open(error_log, "a") as log:
        for i, ticker in enumerate(tickers, 1):
            timestamp = datetime.now().isoformat()
            try:
                print(f"📥 [{i}/{total}] Fetching data for {ticker}...")
                yf_ticker = yf.Ticker(ticker)

                raw_data = stock_utils.fetch_history(yf_ticker)
                csv_data, price_col = stock_utils.download_stock_info(raw_data.copy())
                info = stock_utils.fetch_info(yf_ticker)
                try:
                    calendar = stock_utils.fetch_calendar(yf_ticker)
                except RetryError as e:
                    calendar = {}
                    print(f"⚠️ Skipping calendar for {ticker} after retries: {e.last_attempt.exception()}")

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

                print(f"✅ Saved: {output_path} | Progress: {100 * i / total:.2f}%")
            except Exception as e:
                error_msg = f"[{timestamp}] Error fetching data for {ticker}: {str(e)}\n"
                log.write(error_msg)
                print(f"❌ {error_msg}", file=sys.stderr)
                any_errors = True

    print("✅ Export complete. All files processed.")
    if any_errors:
        print("⚠️ Some tickers failed. See 'error.log' for details.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock/ETF data using yfinance.")
    parser.add_argument(
        "--tickers",
        required=True,
        type=str,
        help="Comma-separated ticker symbols (e.g. AAPL, MSFT, VOO)."
    )
    args = parser.parse_args()
    ticker_list = [
        t.strip().upper()
        for t in args.tickers.split(",")
        if t.strip()
    ]
    export_ticker_data(ticker_list)
