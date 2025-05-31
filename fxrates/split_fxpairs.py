import json
import sys


def split_fxpairs():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "fxpairs.json"
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 6

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


if __name__ == "__main__":
    split_fxpairs()
