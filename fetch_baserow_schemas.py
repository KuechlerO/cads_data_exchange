import requests
import yaml
from pathlib import Path
from argparse import ArgumentParser

with open("./.baserow_token") as tokenfile:
    token = tokenfile.readline().strip()


def get_fields(table_id):
    resp = requests.get(
        f"https://phenotips.charite.de/api/database/fields/table/{table_id}/",
        headers={
            "Authorization": f"Token {token}"
        }
    )
    return resp.json()

tables = {
    "Cases": 579,
    "Findings": 581,
    "Personnel": 582,
}

parser = ArgumentParser()

parser.add_argument("schema_path", type=Path, default="schemas")

for tname, tid in tables.items():
    mapped_name = f"{tname}_id".lower()
    parser.add_argument(f"--{mapped_name}", default=tid, type=int)

args = parser.parse_args()

vargs = vars(args)

schema_output_dir = args.schema_path
schema_output_dir.mkdir(exist_ok=True)

for tname in tables:
    tables[tname] = vargs[f"{tname}_id".lower()]

for table_name, table_id in tables.items():
    fields = get_fields(table_id)
    schema = {
        "name": table_name,
        "id": table_id,
        "fields": []
    }
    for field in fields:
        info = {
            "to": field["name"],
            "type": field["type"],
        }
        if info["type"] == "link_row":
            table_ids = [k for k, v in tables.items() if v == field["link_row_table"]]
            if len(table_ids) == 1:
                info["link_table"], = [k for k, v in tables.items() if v == field["link_row_table"]]
            else:
                print(f"Ignoring link to non-specified table {info['to']}")
                continue

        if info["type"] == "single_select" or info["type"] == "multiple_select":
            info["select_options"] = [
                {"id": s["id"], "value": s["value"]}
                for s in field["select_options"]
            ]
        schema["fields"].append(info)

    outpath = schema_output_dir / f"{table_name}.yaml"
    with outpath.open("w") as sfile:
        yaml.dump(schema, sfile, allow_unicode=True)
