import glob
import json
import os

import pandas as pd


class StockUtils:
    @staticmethod
    def process_index(data):
        data.index = pd.to_datetime(data.index, errors='coerce', utc=True)
        return data[data.index.tz_localize(None) >= pd.Timestamp('1970-01-01')]

    @staticmethod
    def get_root_error_message(exc):
        current = exc
        last_message = str(current)

        while True:
            cause = getattr(current, "__cause__", None)
            context = getattr(current, "__context__", None)

            if cause is not None:
                last_message = str(cause)
                current = cause
            elif context is not None:
                last_message = str(context)
                current = context
            else:
                break

        return last_message

    @staticmethod
    def clean_json(input_file='stocks.json', output_file='stocks.json'):
        try:
            with open(input_file, 'r') as in_file:
                stocks = json.load(in_file)

            if not isinstance(stocks, list):
                raise ValueError("Input JSON is not an array.")

            unique_sorted = sorted(set(stocks))
            with open(output_file, 'w') as out_file:
                json.dump(unique_sorted, out_file, indent=4, sort_keys=True)
        except Exception as e:
            raise e

    @staticmethod
    def split_tickers(input_file, preferred_chunk_size, max_chunks=256, output_dir="chunks"):
        #  max_chunks is to align with GitHub Actions
        os.makedirs(output_dir, exist_ok=True)

        with open(input_file, "r") as in_file:
            tickers = json.load(in_file)

        seen = set()
        unique_tickers = []
        print("🔍 Processing Unique Tickers")
        for ticker in tickers:
            if ticker not in seen:
                seen.add(ticker)
                unique_tickers.append(ticker)

        total_tickers = len(unique_tickers)
        chunk_size = preferred_chunk_size
        num_chunks = (total_tickers + chunk_size - 1) // chunk_size
        if num_chunks > max_chunks:
            chunk_size = (total_tickers + max_chunks - 1) // max_chunks
            num_chunks = (total_tickers + chunk_size - 1) // chunk_size
            print(f"⚠️ Too many chunks ({num_chunks}) for preferred chunk size {preferred_chunk_size}.")
            print(f"➡️ Increasing chunk size to {chunk_size} to keep chunks <= {max_chunks}.")

        chunks = [
            unique_tickers[i:i + chunk_size]
            for i in range(0, len(unique_tickers), chunk_size)
        ]

        for idx, chunk in enumerate(chunks):
            output_file_path = os.path.join(output_dir, f"chunk_{idx + 1}.json")
            with open(output_file_path, "w") as out_file:
                json.dump(chunk, out_file, indent=4, sort_keys=True)

            print(f"✅ Saved chunk {idx + 1} with {len(chunk)} tickers to {output_file_path}.")

        print("✅ Processed all tickers.")

        chunk_ids = list(range(1, len(chunks) + 1))
        with open("chunk_ids.json", "w") as c_file:
            json.dump(chunk_ids, c_file)

    @staticmethod
    def merge_tickers(input_dir, output="output"):
        os.makedirs(output, exist_ok=True)
        ticker_map = {}
        json_files = glob.glob(os.path.join(input_dir, "**/ticker_*.json"), recursive=True)

        for file in json_files:
            try:
                with open(file, "r") as in_file:
                    data = json.load(in_file)
                    for entry in data:
                        ticker = entry.get("ticker")
                        if ticker and ticker not in ticker_map:
                            ticker_map[ticker] = entry
            except Exception as e:
                print(f"⚠️ Failed to read {file}: {e}")

        merged_data = [ticker_map[t] for t in sorted(ticker_map)]
        file_name = os.path.join(output, "all.json")
        with open(file_name, "w") as out_file:
            json.dump(merged_data, out_file, indent=4, sort_keys=True)

        print(f"✅ Merged {len(merged_data)} unique tickers into {file_name}.")

    @staticmethod
    def is_downgrading(data, price_col):
        if data.empty:
            return True

        if data.shape[0] < 200 or price_col not in data.columns or 'Volume' not in data.columns:
            return False

        if data.shape[0] < 5:
            return False

        data = data.copy()
        data['50ma'] = data[price_col].rolling(window=50).mean()
        data['200ma'] = data[price_col].rolling(window=200).mean()
        data['volume_avg'] = data['Volume'].rolling(window=20).mean()

        if data[['50ma', '200ma', 'volume_avg']].iloc[-1].isnull().any():
            return False

        latest = data.iloc[-1]
        recent_price = data[price_col].iloc[-5]
        price_drop = latest[price_col] < recent_price * 0.95
        volume_spike = latest['Volume'] > 1.5 * latest['volume_avg']
        below_ma = (latest[price_col] < latest['50ma']) and (latest[price_col] < latest['200ma'])

        return bool(price_drop and volume_spike and below_ma)
