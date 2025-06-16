from datetime import datetime, timedelta, timezone
from io import StringIO

import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_random_exponential


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def fetch_history(ticker_obj, period='max'):
    data = ticker_obj.history(period=period)
    if data.empty:
        raise ValueError(f"No historical data found for ticker.")
    return data


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def fetch_info(ticker_obj):
    info = ticker_obj.info
    if not info or len(info) < 5:
        print(f"🔁 Retrying with scrape=True for {ticker_obj.ticker}")
        info = ticker_obj.get_info(scrape=True)
    if not info or len(info) < 5:
        raise ValueError(f"_fetch_info failed for {ticker_obj.ticker}: info is empty or invalid")
    return info


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def fetch_dividends(ticker_obj):
    return ticker_obj.dividends.copy()


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def fetch_calendar(ticker_obj):
    return ticker_obj.calendar or {}


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
    except Exception:
        return []


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
    except Exception:
        return []


@retry(stop=stop_after_attempt(10), wait=wait_random_exponential(min=2, max=5))
def download_stock_info(raw_data):
    csv_data = pd.read_csv(
        StringIO(raw_data.to_csv(index=True)),
        index_col='Date',
        parse_dates=True
    )
    price_col = 'Adj Close' if 'Adj Close' in csv_data.columns else 'Close'
    return csv_data, price_col


def process_index(data):
    data.index = pd.to_datetime(data.index, errors='coerce', utc=True)
    return data[data.index.tz_localize(None) >= pd.Timestamp('1970-01-01')]


def safe_get(info, key, default=None):
    try:
        return info.get(key, default)
    except Exception:
        return default


def calculate_volatility(raw_data, price_col):
    price_series = raw_data[price_col].dropna()
    if len(price_series) < 2:
        return 0.0
    returns = price_series.pct_change().dropna()
    if returns.empty:
        return 0.0
    daily_vol = returns.std()
    annual_vol = daily_vol * np.sqrt(252)
    return round(annual_vol * 100, 2)


def calculate_max_drawdown(csv_data, price_col):
    prices = csv_data[price_col]
    cum_max = prices.cummax()
    drawdowns = (prices - cum_max) / cum_max
    return round(drawdowns.min() * 100, 2)


def calculate_sharpe_ratio(raw_data, price_col, risk_free_rate=0.01):
    returns = raw_data[price_col].pct_change().dropna()
    avg_daily_return = returns.mean()
    std_daily_return = returns.std()
    if std_daily_return == 0:
        return 0.0
    daily_risk_free = risk_free_rate / 252
    daily_sharpe = (avg_daily_return - daily_risk_free) / std_daily_return
    return round(daily_sharpe * np.sqrt(252), 2)


def calculate_upcoming_dividend(events, csv_data, price_col, div_yield):
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


def calculate_dividend_frequency(divs):
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


def calculate_short_and_long_term_cagr(data, price_col, today=None):
    if data.empty:
        return {
            "shortTermCagr": None,
            "longTermCagr": None
        }

    if today is None:
        today = datetime.today()

    periods = {
        "1y": 1, "2y": 2, "5y": 5, "10y": 10, "15y": 15, "20y": 20
    }

    results = {}

    for label, years in periods.items():
        start_date = today - timedelta(days=365 * years)
        start_date = start_date.replace(tzinfo=timezone.utc)
        historical = data[data.index <= start_date]

        if historical.empty:
            results[label] = None
            continue

        start_price = historical[price_col].iloc[-1]
        end_price = data[price_col].iloc[-1]
        cagr = (end_price / start_price) ** (1 / years) - 1
        results[label] = round(cagr * 100, 2)

    def combine(labels):
        values = [
            results[k] for k in labels
            if results[k] is not None
        ]
        return round(sum(values) / len(values), 2) if values else None

    return {
        "shortTermCagr": combine(["1y", "2y"]),
        "longTermCagr": combine(["5y", "10y", "15y", "20y"])
    }
