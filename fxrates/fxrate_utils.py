import json

import yfinance as yf


class FXRateUtils:
    @staticmethod
    def split_fxpairs(input_file="fxpairs.json", chunk_size=6):
        with open(input_file, "r") as f:
            fxpairs = json.load(f)

        seen = set()
        unique_fxpairs = []
        for pair in fxpairs:
            key = (pair.get("from"), pair.get("to"))
            if key not in seen:
                seen.add(key)
                unique_fxpairs.append(pair)

        chunks = [
            unique_fxpairs[i:i + chunk_size]
            for i in range(0, len(unique_fxpairs), chunk_size)
        ]
        matrix = [{"chunk": chunk} for chunk in chunks]
        print(json.dumps(matrix))

    @staticmethod
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
