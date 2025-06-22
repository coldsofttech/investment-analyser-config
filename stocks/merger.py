import argparse

from stock_utils import StockUtils

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge high-level stock/ETF information to 'all.json'.")
    parser.add_argument(
        "--input-dir",
        required=True,
        type=str,
        help="Path of input directory where all ticker_*.json exists."
    )
    args = parser.parse_args()
    StockUtils.merge_tickers(input_dir=args.input_dir)
