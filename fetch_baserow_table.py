from argparse import ArgumentParser

import json

from python_baserow_simple import BaserowApi
import pandas as pd

from data_exchange.config import settings

BR = BaserowApi(database_url=settings.baserow.url, token=settings.baserow_token)


def main():
    parser = ArgumentParser()
    parser.add_argument("output_file")
    parser.add_argument("--table_id", type=int, default=582)
    args = parser.parse_args()

    data = BR.get_data(args.table_id)

    if args.output_file.lower().endswith(".json"):
        with open(args.output_file, "w") as outfile:
            json.dump(data, outfile)
    elif args.output_file.lower().endswith(".csv"):
        records = [{"BaserowID": k, **v} for k, v in data.items()]
        df = pd.DataFrame.from_records(records)
        df.to_csv(args.output_file, index=False)
    elif args.output_file.lower().endswith(".xlsx"):
        records = [{"BaserowID": k, **v} for k, v in data.items()]
        df = pd.DataFrame.from_records(records)
        df.to_excel(args.output_file, index=False)
    else:
        raise RuntimeError(f"Unknown file format in output path: {args.output_file}")


if __name__ == "__main__":
    main()
