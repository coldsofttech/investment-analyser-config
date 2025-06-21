import argparse
import json
import os


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split tickers into multiple chunks.")
    parser.add_argument(
        "--input",
        required=True,
        type=str,
        default="stocks.json",
        help="Path of the input file. Defaults to 'stocks.json'."
    )
    parser.add_argument(
        "--chunk-size",
        default=20,
        type=int,
        help="Size of chunk. Defaults to 20."
    )
    args = parser.parse_args()
    split_tickers(args.input, args.chunk_size)
