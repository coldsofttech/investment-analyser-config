import argparse

from stock_utils import StockUtils

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
    StockUtils.split_tickers(
        input_file=args.input,
        preferred_chunk_size=args.chunk_size
    )
