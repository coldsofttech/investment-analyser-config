import argparse

from fxrate_utils import FXRateUtils

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split fx pairs into multiple chunks.")
    parser.add_argument(
        "--input",
        required=True,
        type=str,
        default="fxpairs.json",
        help="Path of the input file. Defaults to 'fxpairs.json'."
    )
    parser.add_argument(
        "--chunk-size",
        default=20,
        type=int,
        help="Size of chunk. Defaults to 20."
    )
    args = parser.parse_args()
    FXRateUtils.split_fxpairs(
        input_file=args.input,
        chunk_size=args.chunk_size
    )
