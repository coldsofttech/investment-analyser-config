import argparse
import json
import os
import sys
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import yfinance as yf


def _download_stock_info(raw_data):
    csv_data = pd.read_csv(StringIO(raw_data.to_csv(index=True)), index_col='Date', parse_dates=True)
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
    max_drawdown = drawdowns.min()

    return round(max_drawdown * 100, 2)


def _calculate_sharpe_ratio(raw_data, price_col, risk_free_rate=0.01):
    returns = raw_data[price_col].pct_change().dropna()
    avg_daily_return = returns.mean()
    std_daily_return = returns.std()

    if std_daily_return == 0:
        return 0.0

    daily_risk_free = risk_free_rate / 252
    daily_sharpe = (avg_daily_return - daily_risk_free) / std_daily_return
    annualised_sharpe = daily_sharpe * np.sqrt(252)

    return round(annualised_sharpe, 2)


def _calculate_dividend_frequency(divs):
    div_freq = 'N/A'
    if not divs.empty and len(divs) >= 2:
        divs_per_year = divs.groupby(divs.index.year).count()
        avg_div_freq = divs_per_year.mean()

        if avg_div_freq >= 11:
            div_freq = 'Monthly'
        elif avg_div_freq >= 3.5:
            div_freq = 'Quarterly'
        elif avg_div_freq >= 2:
            div_freq = 'Semi-Annually'
        elif avg_div_freq >= 1:
            div_freq = 'Annually'
        else:
            div_freq = 'Irregular'

    return div_freq


def _process_index(data):
    data.index = pd.to_datetime(data.index, errors='coerce', utc=True)

    return data[data.index.tz_localize(None) >= pd.Timestamp('1970-01-01')]


def _calculate_upcoming_dividend(events, csv_data, price_col, div_yield):
    div_date = events.get("Dividend Date", None)

    if div_date:
        if hasattr(div_date, "date"):
            div_date = div_date.date()

        last_csv_date = csv_data.index[-1].date() if hasattr(csv_data.index[-1], 'date') else csv_data.index[-1]
        if div_date >= last_csv_date:
            if div_yield > 0:
                div_yield = div_yield / 100
                last_price = csv_data[price_col].iloc[-1]
                div_amount = (div_yield * last_price) / 4
                return div_date, div_amount

    return None, None


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
                raw_data = yf_ticker.history(period='max')
                if raw_data.empty:
                    raise ValueError(f'No historical data found for ticker "{ticker}". It may be invalid.')

                csv_data, price_col = _download_stock_info(raw_data.copy())
                info = yf_ticker.info
                valid_data = _process_index(csv_data)
                valid_div_data = _process_index(yf_ticker.dividends.copy())
                upcoming_div_date, upcoming_div_price = _calculate_upcoming_dividend(yf_ticker.calendar,
                                                                                     csv_data.copy(), price_col,
                                                                                     info.get("dividendYield", ""))
                result_dict = {}
                metadata_dict = {"lastUpdatedTimestamp": timestamp}
                ticker_dict = {
                    "tickerCode": ticker.upper(),
                    "info": {
                        "companyName": info.get("longName", ""),
                        "companyDescription": info.get("longBusinessSummary", ""),
                        "type": info.get("typeDisp", ""),
                        "exchange": info.get("exchange", ""),
                        "industry": info.get("industry", ""),
                        "sector": info.get("sector", ""),
                        "website": info.get("website", ""),
                        "currency": info.get("currency", ""),
                        "beta": info.get("beta", ""),
                        "marketCap": info.get("marketCap", ""),
                        "payoutRatio": info.get("payoutRatio", ""),
                        "dividendYield": info.get("dividendYield", ""),
                        "dividendFrequency": _calculate_dividend_frequency(yf_ticker.dividends.copy()),
                        "volatility": _calculate_volatility(raw_data.copy(), price_col),
                        "maxDrawdown": _calculate_max_drawdown(csv_data.copy(), price_col),
                        "sharpeRatio": _calculate_sharpe_ratio(raw_data.copy(), price_col)
                    },
                    "data": [
                        {"date": d.strftime('%Y-%m-%d'), "price": p}
                        for d, p in zip(valid_data.index, valid_data[price_col])
                    ],
                    "dividends": [
                        {"date": d.strftime('%Y-%m-%d'), "price": p}
                        for d, p in zip(valid_div_data.index, valid_div_data.tolist())
                    ],
                    "events": {
                        "dividends": {
                            "date": upcoming_div_date.strftime('%Y-%m-%d') if upcoming_div_date else None,
                            "price": upcoming_div_price
                        }
                    },
                    "priceInfo": {
                        "currentPrice": info.get("currentPrice", "")
                    }
                }
                result_dict[ticker] = ticker_dict
                result_dict["metadata"] = metadata_dict

                output_path = os.path.join(output_dir, f"{ticker}.json")
                with open(output_path, "w") as f:
                    json.dump(result_dict, f, indent=4, sort_keys=True)

                percent = int((i / total) * 100)
                print(f"✅ Saved: {output_path} | Progress: {percent:.2f}%")
            except Exception as e:
                error_msg = f"[{timestamp}] Error fetching data for {ticker}: {str(e)}\n"
                log.write(error_msg)
                print(f"❌ {error_msg}", file=sys.stderr)
                any_errors = True

    print("✅ Export complete. All files processed.")
    if any_errors:
        print("⚠️ Some tickers failed. See 'errors.log' for details.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock data using yfinance.")
    parser.add_argument("--tickers", required=True, type=str,
                        help="Comma-separated list of ticker symbols (e.g. AAPL, MSFT, etc.)")
    args = parser.parse_args()
    ticker_list = [
        t.strip().upper()
        for t in args.tickers.split(",")
        if t.strip()
    ]
    export_ticker_data(ticker_list)
