import argparse
import json
import os
import sys
from datetime import datetime

import yfinance as yf


def get_fx_conversion_rate(from_currency, to_currency):
    conv_rate = 1.0
    div_flag = False

    if from_currency == 'GBp' and to_currency == 'GBP':
        return 0.01
    elif from_currency == 'GBP' and to_currency == 'GBp':
        return 100
    elif from_currency == 'GBp':
        from_currency = 'GBP'
        div_flag = True
    elif to_currency == 'GBp':
        to_currency = 'GBP'
        div_flag = True

    if from_currency.lower() != to_currency.lower():
        conv_pair = f"{from_currency}{to_currency}=X" if from_currency != "USD" else f"{to_currency}=X"
        fx_data = yf.Ticker(conv_pair).history(period="1d")
        if fx_data.empty:
            raise ValueError(f"No FX data found for {conv_pair}.")

        conv_rate = fx_data["Close"].iloc[-1]

        if div_flag:
            conv_rate = conv_rate / 100

    return conv_rate


def export_fx_data(from_currency, to_currency, output_dir="output", error_log="error.log"):
    os.makedirs(output_dir, exist_ok=True)

    with open(error_log, "a") as log:
        timestamp = datetime.now().isoformat()
        try:
            print(f"📥 Fetching data for from: {from_currency} & to: {to_currency}...")
            result_dict = {
                "fxRate": {
                    "from": from_currency,
                    "to": to_currency,
                    "conversionRate": get_fx_conversion_rate(from_currency, to_currency)
                },
                "metadata": {
                    "lastUpdatedTimestamp": timestamp
                }
            }

            output_path = os.path.join(output_dir, f"{from_currency}{to_currency}=X.json")
            with open(output_path, "w") as f:
                json.dump(result_dict, f, indent=4, sort_keys=True)

            print(f"✅ Saved: {output_path}")
        except Exception as ex:
            error_msg = f"[{timestamp}] Error fetching data for from: {from_currency} & to: {to_currency}: {str(ex)}\n"
            log.write(error_msg)
            print(f"❌ {error_msg}", file=sys.stderr)

    print("✅ Export complete. All files processed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FX Conversion Rate using yfinance.")
    parser.add_argument("--fxpairs", required=True,help="JSON string of currency pairs.")
    args = parser.parse_args()

    try:
        pairs_obj = json.loads(args.fxpairs)
        pairs = pairs_obj.get("chunk", pairs_obj) if isinstance(pairs_obj, dict) else pairs_obj
        for pair in pairs:
            from_cur = pair.get("from", "")
            to_cur = pair.get("to", "")
            if not from_cur or not to_cur:
                raise ValueError("Missing 'from' or 'to' in one of the pairs.")

            export_fx_data(from_cur, to_cur)
    except Exception as e:
        print(f"❌ Failed to parse fxpairs: {e}", file=sys.stderr)
        sys.exit(1)
