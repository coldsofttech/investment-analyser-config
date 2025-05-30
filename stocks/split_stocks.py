import json
import sys


def split_tickers():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "stocks.json"
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    with open(input_file, "r") as f:
        tickers = json.load(f)

    seen = set()
    unique_tickers = []
    for ticker in tickers:
        if ticker not in seen:
            seen.add(ticker)
            unique_tickers.append(ticker)

    chunks = [
        unique_tickers[i:i + chunk_size]
        for i in range(0, len(unique_tickers), chunk_size)
    ]
    matrix = {"include": [{"chunk": chunk} for chunk in chunks]}
    print(json.dumps(matrix))


if __name__ == "__main__":
    split_tickers()
