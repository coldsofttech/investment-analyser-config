import argparse
import glob
import json
import os


def merge_ticker_data(input_dir, output="output"):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge high-level stock/ETF information to 'all.json'.")
    parser.add_argument(
        "--input-dir",
        required=True,
        type=str,
        help="Path of input directory where all ticker_*.json exists."
    )
    args = parser.parse_args()
    merge_ticker_data(args.input_dir)
