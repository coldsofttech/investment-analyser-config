import argparse
import json
import os
import sys
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_random_exponential, RetryError


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def _fetch_info(ticker_obj):
    info = ticker_obj.info
    if not info or len(info) < 5:
        print(f"🔁 Retrying with scrape=True for {ticker_obj.ticker}")
        info = ticker_obj.get_info(scrape=True)
    if not info or len(info) < 5:
        raise ValueError(f"_fetch_info failed for {ticker_obj.ticker}: info is empty or invalid")
    return info


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def _fetch_calendar(ticker_obj):
    return ticker_obj.calendar or {}


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def _fetch_dividends(ticker_obj):
    return ticker_obj.dividends.copy()


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def _fetch_history(ticker_obj):
    data = ticker_obj.history(period='max')
    if data.empty:
        raise ValueError(f"No historical data found for ticker.")
    return data


def _download_stock_info(raw_data):
    csv_data = pd.read_csv(
        StringIO(raw_data.to_csv(index=True)),
        index_col='Date',
        parse_dates=True
    )
    price_col = 'Adj Close' if 'Adj Close' in csv_data.columns else 'Close'
    return csv_data, price_col


def _calculate_volatility(raw_data, price_col):
    returns = raw_data[price_col].pct_change()
    daily_vol = returns.std()
    annual_vol = daily_vol * np.sqrt(252)
    return round(annual_vol * 100, 2)


def _calculate_max_drawdown(csv_data, price_col):
    prices = csv_data[price_col]
    cum_max = prices.cummax()
    drawdowns = (prices - cum_max) / cum_max
    return round(drawdowns.min() * 100, 2)


def _calculate_sharpe_ratio(raw_data, price_col, risk_free_rate=0.01):
    returns = raw_data[price_col].pct_change().dropna()
    avg_daily_return = returns.mean()
    std_daily_return = returns.std()
    if std_daily_return == 0:
        return 0.0
    daily_risk_free = risk_free_rate / 252
    daily_sharpe = (avg_daily_return - daily_risk_free) / std_daily_return
    return round(daily_sharpe * np.sqrt(252), 2)


def _calculate_dividend_frequency(divs):
    if not divs.empty and len(divs) >= 2:
        divs_per_year = divs.groupby(divs.index.year).count()
        avg_div_freq = divs_per_year.mean()
        if avg_div_freq >= 11:
            return 'Monthly'
        elif avg_div_freq >= 3.5:
            return 'Quarterly'
        elif avg_div_freq >= 2:
            return 'Semi-Annually'
        elif avg_div_freq >= 1:
            return 'Annually'
        else:
            return 'Irregular'
    return 'N/A'


def _process_index(data):
    data.index = pd.to_datetime(data.index, errors='coerce', utc=True)
    return data[data.index.tz_localize(None) >= pd.Timestamp('1970-01-01')]


def _calculate_upcoming_dividend(events, csv_data, price_col, div_yield):
    try:
        div_date = events.get("Dividend Date", None)
        if div_date:
            if hasattr(div_date, "date"):
                div_date = div_date.date()
            last_csv_date = (
                csv_data.index[-1].date()
                if hasattr(csv_data.index[-1], 'date')
                else csv_data.index[-1]
            )
            if div_date >= last_csv_date and div_yield > 0:
                div_yield /= 100
                last_price = csv_data[price_col].iloc[-1]
                return div_date, (div_yield * last_price) / 4
    except Exception:
        pass
    return None, None


def _safe_get(info, key, default=None):
    try:
        return info.get(key, default)
    except Exception:
        return default


def _fetch_top_holdings(ticker_obj):
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
    except Exception:
        return []


def _fetch_sector_weightings(ticker_obj):
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
    except Exception:
        return []


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

                raw_data = _fetch_history(yf_ticker)
                csv_data, price_col = _download_stock_info(raw_data.copy())
                info = _fetch_info(yf_ticker)
                try:
                    calendar = _fetch_calendar(yf_ticker)
                except RetryError as e:
                    calendar = {}
                    print(f"⚠️ Skipping calendar for {ticker} after retries: {e.last_attempt.exception()}")

                dividends = _fetch_dividends(yf_ticker)

                valid_data = _process_index(csv_data)
                valid_div_data = _process_index(dividends)

                upcoming_div_date, upcoming_div_amount = _calculate_upcoming_dividend(
                    calendar, csv_data.copy(), price_col, _safe_get(info, "dividendYield", 0)
                )

                ticker_type = _safe_get(info, "quoteType", "").upper()

                result_dict = {}
                metadata_dict = {"lastUpdatedTimestamp": timestamp}
                ticker_dict = {
                    "tickerCode": ticker.upper(),
                    "info": {
                        "companyName": _safe_get(info, "longName", ""),
                        "companyDescription": _safe_get(info, "longBusinessSummary", ""),
                        "type": ticker_type,
                        "exchange": _safe_get(info, "exchange", ""),
                        "industry": _safe_get(info, "industry", ""),
                        "sector": _safe_get(info, "sector", ""),
                        "website": _safe_get(info, "website", ""),
                        "currency": _safe_get(info, "currency", ""),
                        "beta": _safe_get(info, "beta", ""),
                        "payoutRatio": _safe_get(info, "payoutRatio", ""),
                        "dividendYield": _safe_get(info, "dividendYield", ""),
                        "dividendFrequency": _calculate_dividend_frequency(valid_div_data),
                        "volatility": _calculate_volatility(raw_data.copy(), price_col),
                        "maxDrawdown": _calculate_max_drawdown(csv_data.copy(), price_col),
                        "sharpeRatio": _calculate_sharpe_ratio(raw_data.copy(), price_col)
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
                        "currentPrice": float(_safe_get(info, "currentPrice", csv_data.iloc[-1][price_col]))
                    }
                }

                if ticker_type == "ETF":
                    ticker_dict["holdings"] = {
                        "topHoldings": _fetch_top_holdings(yf_ticker),
                        "sectorWeights": _fetch_sector_weightings(yf_ticker)
                    }
                    ticker_dict["info"]["marketCap"] = _safe_get(info, "totalAssets", "")
                else:
                    ticker_dict["info"]["marketCap"] = _safe_get(info, "marketCap", "")

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
