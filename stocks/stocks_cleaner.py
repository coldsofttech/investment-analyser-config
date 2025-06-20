import json


def clean_json(input_file='stocks.json', output_file='stocks.json'):
    try:
        with open(input_file, 'r', encoding='utf-8') as in_file:
            stocks = json.load(in_file)

        if not isinstance(stocks, list):
            raise ValueError("Input JSON is not an array.")

        unique_sorted = sorted(set(stocks))
        with open(output_file, 'w', encoding='utf-8') as out_file:
            json.dump(unique_sorted, out_file, indent=4, sort_keys=True, ensure_ascii=False)

        print(f"✅ Stocks JSON cleaned and saved to {output_file}.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    clean_json()
