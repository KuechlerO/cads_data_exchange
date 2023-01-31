"""Script to keep our baserow database clean.
"""
import requests
from collections import defaultdict


with open(".baserow_token") as tokenfile:
    TOKEN = tokenfile.readline().strip()


def get_fields(table_id):
    resp = requests.get(
        f"https://phenotips.charite.de/api/database/fields/table/{table_id}/",
        headers={
            "Authorization": f"Token {TOKEN}"
        }
    )

    resp.raise_for_status()
    data = resp.json()
    return data


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


def delete_entry(table_id, row_id):
    resp = requests.delete(
        f"https://phenotips.charite.de/api/database/rows/table/{table_id}/{row_id}/",
        headers={
            "Authorization": f"Token {TOKEN}",
        },
    )


def delete_entries(table_id, row_ids):
    resp = requests.post(
        f"https://phenotips.charite.de/api/database/rows/table/{table_id}/batch-delete/",
        headers={
            "Authorization": f"Token {TOKEN}",
            "Content-Type": "application/json"
        },
        json={
            "items": row_ids
        }
    )


def check_table_empty(table_id):
    data = get_data(table_id)
    fields = get_fields(table_id)
    writable_fields = [
        f["name"] for f in fields if not f["read_only"]
    ]

    IGNORED_FIELDS = [
        "ResultType",
        "Cases",
    ]

    NONE_VALUES = [
        "None",
        "",
        None
    ]
    if data:
        none_ids = []
        for entry in data:
            if all(v in NONE_VALUES for k, v in entry.items() if k in writable_fields and k not in IGNORED_FIELDS):
                none_ids.append(entry["id"])
            else:
                print(entry)
        delete_entries(table_id, none_ids)


def check_table_duplicates(table_id, identifying_keys):
    """Check a given table for duplicate keys.
    """

    resp = requests.get(
        f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true",
        headers={
            "Authorization": f"Token {TOKEN}"
        }
    )

    resp.raise_for_status()
    data = resp.json()

    if "results" in data:
        results = data["results"]

        data_by_keys = defaultdict(list)
        for entry in results:
            identifier = tuple(entry[k] for k in identifying_keys)
            data_by_keys[identifier].append(entry)

        for identifier, values in data_by_keys.items():
            if len(values) > 1:
                print(identifier, "is duplicated")
        else:
            print("All other entries are fine.")
    else:
        print(data)

if __name__ == "__main__":
    check_table_duplicates(387, ["Lastname", "Firstname", "Birthdate"])
    check_table_empty(389)
