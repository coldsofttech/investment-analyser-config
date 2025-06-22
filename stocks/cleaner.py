from stock_utils import StockUtils

if __name__ == "__main__":
    try:
        file = "stocks.json"
        StockUtils.clean_json(input_file=file, output_file=file)
        print(f"✅ Stocks JSON cleaned and saved to {file}.")
    except Exception as e:
        print(f"❌ Error: {e}")
