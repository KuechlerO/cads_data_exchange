from argparse import ArgumentParser

import json

import requests

with open(".baserow_token") as tokenfile:
    TOKEN = tokenfile.readline().strip()


def _get_data(url):
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Token {TOKEN}"
        }
    )
    data = resp.json()
    if "results" in data:
        if data["next"]:
            return data["results"] + _get_data(data["next"])
        return data["results"]

    raise RuntimeError


def get_data(table_id):
    """Check a given table for empty keys.
    """
    return _get_data(f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true")


def main():
    parser = ArgumentParser()
    parser.add_argument("output_file")
    parser.add_argument("--table_id", type=int, default=582)
    args = parser.parse_args()

    data = get_data(args.table_id)

    with open(args.output_file, "w") as outfile:
        json.dump(data, outfile)

if __name__ == "__main__":
    main()
