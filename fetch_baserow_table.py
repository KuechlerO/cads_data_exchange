from argparse import ArgumentParser

import json

import requests

from baserow_utils import BaserowApi

BR = BaserowApi(token_path=".baserow_token")


def main():
    parser = ArgumentParser()
    parser.add_argument("output_file")
    parser.add_argument("--table_id", type=int, default=582)
    args = parser.parse_args()

    data = BR.get_data(args.table_id)

    with open(args.output_file, "w") as outfile:
        json.dump(data, outfile)

if __name__ == "__main__":
    main()
