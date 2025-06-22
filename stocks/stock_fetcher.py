import json
import os
import random
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import yfinance as yf

from stock_calculator import StockCalculator
from stock_retry import retry
from stock_utils import StockUtils


class StockFetcher:
    @staticmethod
    @retry(max_retries=5, delay=2, backoff=2, jitter=True)
    def download_stock_info(raw_data):
        csv_data = pd.read_csv(
            StringIO(raw_data.to_csv(index=True)),
            index_col='Date',
            parse_dates=True
        )
        price_col = 'Adj Close' if 'Adj Close' in csv_data.columns else 'Close'
        return csv_data, price_col

    @staticmethod
    @retry(max_retries=5, delay=2, backoff=2, jitter=True)
    def fetch_history(ticker_obj, period='max'):
        data = ticker_obj.history(period=period)

        if data.empty and period == 'max':
            print(f"⚠️ 'max' period returned no data for {ticker_obj.ticker}, retrying with '20y'")
            data = ticker_obj.history(period='20y')

        if data.empty:
            raise ValueError(f"No historical data found for ticker.")

        return data

    @staticmethod
    @retry(max_retries=5, delay=2, backoff=2, jitter=True)
    def fetch_info(ticker_obj):
        info = ticker_obj.info
        if not info or len(info) < 5:
            print(f"🔁 Retrying by forcing re-fetch for {ticker_obj.ticker}")
            info = ticker_obj.get_info()

        if not info or len(info) < 5:
            raise ValueError(f"fetch_info failed for {ticker_obj.ticker}: info is empty or invalid.")

        return info

    @staticmethod
    @retry(max_retries=5, delay=2, backoff=2, jitter=True)
    def fetch_dividends(ticker_obj):
        return ticker_obj.dividends.copy()

    @staticmethod
    @retry(max_retries=5, delay=2, backoff=2, jitter=True)
    def fetch_calendar(ticker_obj):
        return ticker_obj.calendar or {}

    @staticmethod
    def fetch_top_holdings(ticker_obj):
        try:
            holdings = ticker_obj.funds_data.top_holdings
            if isinstance(holdings, pd.DataFrame):
                holdings = holdings.reset_index()
                columns = holdings.columns.tolist()
                symbol_col = "Symbol" if "Symbol" in columns else holdings.columns[0]
                name_col = "Holding" if "Holding" in columns else holdings.columns[1]
                weight_col = "Holding %" if "Holding %" in columns else holdings.columns[-1]

                return [
                    {
                        "tickerCode": row[symbol_col],
                        "companyName": row[name_col],
                        "weight": round(float(row[weight_col]), 6)
                    }
                    for _, row in holdings.iterrows()
                ]
            return []
        except:
            return []

    @staticmethod
    def fetch_sector_weightings(ticker_obj):
        try:
            weights = ticker_obj.funds_data.sector_weightings
            r_weights = []
            for key, value in weights.items():
                if float(value) > 0:
                    r_weights.append({
                        "sector": key,
                        "value": float(value)
                    })
            return r_weights
        except:
            return []

    @staticmethod
    def safe_get(info, key, default=None):
        try:
            return info.get(key, default)
        except:
            return default

    @staticmethod
    def fetch_ticker(ticker):
        time.sleep(random.uniform(0.5, 2.5))

        try:
            print(f"📥 Fetching data for {ticker}...")
            yf_ticker = yf.Ticker(ticker)

            raw_data = StockFetcher.fetch_history(yf_ticker)
            csv_data, price_col = StockFetcher.download_stock_info(raw_data.copy())
            info = StockFetcher.fetch_info(yf_ticker)
            dividends = StockFetcher.fetch_dividends(yf_ticker)

            valid_data = StockUtils.process_index(csv_data)
            valid_div_data = StockUtils.process_index(dividends)

            ticker_type = StockFetcher.safe_get(info, "quoteType", "")
            industry = StockFetcher.safe_get(info, "industry", "")
            sector = StockFetcher.safe_get(info, "sector", "")

            result = {
                "ticker": ticker,
                "companyName": StockFetcher.safe_get(info, "longName", ""),
                "industry": industry,
                "sector": sector,
                "exchange": StockFetcher.safe_get(info, "exchange", ""),
                "type": ticker_type,
                "country": StockFetcher.safe_get(info, "region", ""),
                "currency": StockFetcher.safe_get(info, "currency", ""),
                "beta": StockFetcher.safe_get(info, "beta", ""),
                "volatility": StockCalculator.calculate_volatility(raw_data.copy(), price_col),
                "dividendYield": StockFetcher.safe_get(info, "dividendYield", ""),
                "dividendFrequency": StockCalculator.calculate_dividend_frequency(valid_div_data),
                "website": StockFetcher.safe_get(info, "website", ""),
                "currentPrice": float(StockFetcher.safe_get(info, "currentPrice", csv_data.iloc[-1][price_col])),
                "isDowngrading": StockUtils.is_downgrading(valid_data, price_col)
            }

            historical_cagr = StockCalculator.calculate_historical_short_and_long_term_cagr(valid_data, price_col)
            result["shortTermCagr"] = historical_cagr.get("shortTermCagr", None)
            result["longTermCagr"] = historical_cagr.get("longTermCagr", None)

            if ticker_type.lower() == "etf":
                result["marketCap"] = StockFetcher.safe_get(info, "totalAssets", "")
                result["industry"] = industry if industry else "Exchange-Traded Fund"
                result["sector"] = sector if sector else "Exchange-Traded Fund"
            elif ticker_type.lower() == "mutualfund":
                result["marketCap"] = StockFetcher.safe_get(info, "marketCap", "")
                result["industry"] = industry if industry else "Mutual Fund"
                result["sector"] = sector if sector else "Mutual Fund"
            else:
                result["marketCap"] = StockFetcher.safe_get(info, "marketCap", "")

            return result
        except Exception as e:
            raise RuntimeError(f"Failed to fetch {ticker}: {str(e)}")

    @staticmethod
    def fetch_ticker_detailed(ticker, output_dir="output"):
        time.sleep(random.uniform(0.1, 0.5))

        try:
            print(f"📥 Fetching data for {ticker}...")
            timestamp = datetime.now().isoformat()
            yf_ticker = yf.Ticker(ticker)

            raw_data = StockFetcher.fetch_history(yf_ticker)
            csv_data, price_col = StockFetcher.download_stock_info(raw_data.copy())
            info = StockFetcher.fetch_info(yf_ticker)

            try:
                calendar = StockFetcher.fetch_calendar(yf_ticker)
            except Exception as e:
                calendar = {}
                print(f"⚠️ Skipping calendar for {ticker} after retries: {e}")

            dividends = StockFetcher.fetch_dividends(yf_ticker)
            valid_data = StockUtils.process_index(csv_data)
            valid_div_data = StockUtils.process_index(dividends)
            upcoming_div_date, upcoming_div_amount = StockCalculator.calculate_upcoming_dividend(
                calendar, csv_data.copy(), price_col, StockFetcher.safe_get(info, "dividendYield", 0)
            )
            ticker_type = StockFetcher.safe_get(info, "quoteType", "").upper()

            result_dict = {}
            metadata_dict = {"lastUpdatedTimestamp": timestamp}
            ticker_dict = {
                "tickerCode": ticker.upper(),
                "info": {
                    "companyName": StockFetcher.safe_get(info, "longName", ""),
                    "companyDescription": StockFetcher.safe_get(info, "longBusinessSummary", ""),
                    "type": ticker_type,
                    "exchange": StockFetcher.safe_get(info, "exchange", ""),
                    "industry": StockFetcher.safe_get(info, "industry", ""),
                    "sector": StockFetcher.safe_get(info, "sector", ""),
                    "website": StockFetcher.safe_get(info, "website", ""),
                    "currency": StockFetcher.safe_get(info, "currency", ""),
                    "beta": StockFetcher.safe_get(info, "beta", ""),
                    "payoutRatio": StockFetcher.safe_get(info, "payoutRatio", ""),
                    "dividendYield": StockFetcher.safe_get(info, "dividendYield", ""),
                    "dividendFrequency": StockCalculator.calculate_dividend_frequency(valid_div_data),
                    "volatility": StockCalculator.calculate_volatility(raw_data.copy(), price_col),
                    "maxDrawdown": StockCalculator.calculate_max_drawdown(csv_data.copy(), price_col),
                    "sharpeRatio": StockCalculator.calculate_sharpe_ratio(raw_data.copy(), price_col)
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
                    "currentPrice": float(StockFetcher.safe_get(info, "currentPrice", csv_data.iloc[-1][price_col]))
                }
            }

            if ticker_type.lower() == "etf":
                ticker_dict["holdings"] = {
                    "topHoldings": StockFetcher.fetch_top_holdings(yf_ticker),
                    "sectorWeights": StockFetcher.fetch_sector_weightings(yf_ticker)
                }
                ticker_dict["info"]["marketCap"] = StockFetcher.safe_get(info, "totalAssets", "")
            else:
                ticker_dict["info"]["marketCap"] = StockFetcher.safe_get(info, "marketCap", "")

            result_dict[ticker] = ticker_dict
            result_dict["metadata"] = metadata_dict

            output_path = os.path.join(output_dir, f"{ticker}.json")
            with open(output_path, "w") as f:
                json.dump(result_dict, f, indent=4, sort_keys=True)

            print(f"✅ Saved: {output_path}.")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch {ticker}: {str(e)}")
