import argparse
import json
import os
from datetime import datetime

from fxrate_utils import FXRateUtils


def export_fx(from_currency, to_currency, output_dir="output", error_log="error.log"):
    os.makedirs(output_dir, exist_ok=True)

    with open(error_log, "a") as log:
        timestamp = datetime.now().isoformat()
        try:
            print(f"📥 Fetching data for from: {from_currency} & to: {to_currency}...")
            result_dict = {
                "fxRate": {
                    "from": from_currency,
                    "to": to_currency,
                    "conversionRate": FXRateUtils.get_fx_conversion_rate(from_currency, to_currency)
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
            print(f"❌ {error_msg}")

    print("✅ Export complete. All files processed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FX Conversion Rate using yfinance.")
    parser.add_argument(
        "--fxpairs",
        required=True,
        help="JSON string of currency pairs."
    )
    args = parser.parse_args()

    try:
        pairs_obj = json.loads(args.fxpairs)
        pairs = pairs_obj.get("chunk", pairs_obj) if isinstance(pairs_obj, dict) else pairs_obj
        for pair in pairs:
            from_cur = pair.get("from", "")
            to_cur = pair.get("to", "")
            if not from_cur or not to_cur:
                raise ValueError("Missing 'from' or 'to' in one of the pairs.")

            export_fx(from_cur, to_cur)
    except Exception as e:
        print(f"❌ Failed to parse fxpairs: {e}")
