import requests
import yaml

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
    "Cases": 386,
    "PatientsRelatives": 387,
    "Findings": 389,
    "Personnel": 390,
}

from pathlib import Path
schema_output_dir = Path("schemas")
schema_output_dir.mkdir(exist_ok=True)

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
            info["link_table"], = [k for k, v in tables.items() if v == field["link_row_table"]]
        if info["type"] == "single_select" or info["type"] == "multiple_select":
            info["select_options"] = [
                {"id": s["id"], "value": s["value"]}
                for s in field["select_options"]
            ]
        schema["fields"].append(info)

    outpath = schema_output_dir / f"{table_name}.yaml"
    with outpath.open("w") as sfile:
        yaml.dump(schema, sfile, allow_unicode=True)
