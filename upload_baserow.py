import json
import yaml
from collections import defaultdict


TABLES = {
    "Cases": 386,
    "Findings": 389,
}

def load_schema(table_name):
    with open(f"schemas/{table_name}.yaml") as file:
        data = yaml.safe_load(file)
    return data


SCHEMAS = [
    load_schema(n) for n in TABLES
]

with open(".baserow_token") as tokenfile:
    TOKEN = tokenfile.readline().strip()


import requests

def load_personnel():
    resp = requests.get(
        "https://phenotips.charite.de/api/database/rows/table/390/?user_field_names=true",
        headers={
            "Authorization": f"Token {TOKEN}",
        }
    )
    return resp.json()['results']

PERSONNEL = load_personnel()


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


def get_writable_fields(table_id):
    writable_fields = [
        f for f in get_fields(table_id) if not f["read_only"]
    ]
    return writable_fields


WRITABLE_FIELDS = {
    table_id: get_writable_fields(table_id) for table_id in TABLES.values()
}

IDENTIFIER_FIELDS = ("Lastname", "Firstname", "Birthdate")
def create_key(entry):
    return "|".join(entry[k] for k in IDENTIFIER_FIELDS)


def get_matches(entries, table_data):
    matches = {}
    for entry_id, entry_data in entries:
        matches[entry_id] = 0
        for key ,value in table_data.items():
            entry_value = entry_data.get(key)
            if value == entry_value:
                matches[entry_id] += 1
    return dict(sorted(matches.items(), key=lambda item: item[1], reverse=True))


def merge_entries(dicta, dictb):
    merged_dict = {**dicta}
    for k, v in dictb.items():
        if k in merged_dict:
            if merged_dict[k] in (None, "", [], "None"):
                merged_dict[k] = v
            elif merged_dict[k] == v or v in (None, "", [], "None"):
                pass
            elif type(merged_dict[k]) is list and type(v) is list and len(merged_dict[k]) == len(v):
                pass
            else:
                print(f"Merge Conflict: {k} - {v} {merged_dict[k]}")
                merged_dict[k] = v
        else:
            merged_dict[k] = v
    return merged_dict

def match_data(data_by_id, table_datas):
    merged_entries = [(i, d) for i, d in data_by_id.items()]

    all_matches = {}
    for ti, table_data in enumerate(table_datas):
        matches = get_matches(merged_entries, table_data)
        all_matches[ti] = matches

    # reorder matches by entry_ids
    matches_by_entry_id = defaultdict(list)
    for tid, matches in all_matches.items():
        for eid, match_score in matches.items():
            matches_by_entry_id[eid].append((tid, match_score))
    for m in matches_by_entry_id:
        matches_by_entry_id[m] = sorted(matches_by_entry_id[m], key=lambda i: i[1], reverse=True)

    used_tids = []
    new_merged_entries = []
    for entry_id, matches in matches_by_entry_id.items():
        entry_tid = None
        for tid, _ in matches:
            if tid not in used_tids:
                entry_tid = tid
                break
        else:
            raise RuntimeError("Unmatched existing entry")
        merged_entry = merge_entries(data_by_id[entry_id], table_datas[entry_tid])
        used_tids.append(entry_tid)
        new_merged_entries.append((entry_id, merged_entry))

    for tid in set(all_matches.keys()) - set(used_tids):
        new_merged_entries.append((None, table_datas[tid]))
    return new_merged_entries


def upsert_entries(table_id, entry_ids, table_datas):
    data_by_id = {}
    for entry_id in entry_ids:
        resp = requests.get(
            f"https://phenotips.charite.de/api/database/rows/table/{table_id}/{entry_id}/?user_field_names=true",
            headers={
                "Authorization": f"Token {TOKEN}",
            }
        )
        resp.raise_for_status()
        existing_data = {k: v for k, v in resp.json().items() if k in [f['name'] for f in WRITABLE_FIELDS[table_id]]}
        for key, value in existing_data.items():
            if type(value) is list and len(value) > 0:
                existing_data[key] = [v['id'] for v in value]
            if type(value) is dict:
                existing_data[key] = value['id']
        data_by_id[entry_id] = existing_data
    new_entry_ids = []

    matched_entries = match_data(data_by_id, table_datas)
    if len(matched_entries) > 1:
        print(matched_entries)
        raise RuntimeError("Oh no")
    for entry_id, merged_entry in matched_entries:
        merged_entry = {
            k: v for k, v in merged_entry.items() if k in [f["name"] for f in WRITABLE_FIELDS[table_id]]
        }
        print("MERGED", merged_entry)
        if entry_id:
            resp = requests.patch(
                f"https://phenotips.charite.de/api/database/rows/table/{table_id}/{entry_id}/?user_field_names=true",
                headers={
                    "Authorization": f"Token {TOKEN}",
                    "Content-Type": "application/json",
                },
                json=merged_entry,
            )
            new_entry_ids.append(entry_id)
        else:
            resp = requests.post(
                f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true",
                headers={
                    "Authorization": f"Token {TOKEN}",
                    "Content-Type": "application/json",
                },
                json=merged_entry,
            )
            new_entry_ids.append(resp.json()["id"])
    return new_entry_ids


import shelve

def match_person(person_string):
    person_string_parts = [p.strip() for p in re.split("/| |,|;", person_string) if p]
    for pers in PERSONNEL:
        if person_string == pers["Shorthand"]:
            return [pers["id"]]
        elif person_string == pers["Lastname"] or pers["Lastname"] in person_string_parts:
            return [pers["id"]]
    return []


LINKED_FIELDS_ROW = ("Indexpatient", "Members", "Findings")
def extractLink(entry, field, entry_ids):
    link_table = field['link_table']
    if field["to"] in LINKED_FIELDS_ROW:
        table_id, = [v for k, v in TABLES.items() if k == link_table]
        link_id = entry_ids[table_id]
        return link_id
    elif link_table == "Personnel":
        if field["to"] != "Validator":
            p = match_person(entry[field["to"]])
            if p:
                return [p]
            else:
                return []
    return None

class FieldNotFoundError(Exception):
    pass

def extractText(entry, field, *_):
    if field["to"] == "Name":
        return f"Familie {entry['Lastname']}"
    try:
        val = entry[field["to"]]
    except KeyError as err:
        raise FieldNotFoundError(f"Field {field['to']} failed with: {err}")
    return val

SELECT_MAPPINGS = {
    "pathogen": "Pathogenic (V)",
    "homozygot": "Homozygous",
    "f": "Female",
    "m": "Male",
    "?": "Other",
}

def extractSelect(entry, field, *_):
    if field['to'] in ('Bearbeitungsstatus', 'ACMG Classification', 'Zygosity'):
        raise FieldNotFoundError
    raw_value = entry[field["to"]]

    options = field["select_options"]
    if raw_value is None or raw_value == "None" and field["to"] == "Falltyp":
        raise FieldNotFoundError
    if raw_value in SELECT_MAPPINGS:
        raw_value = SELECT_MAPPINGS[raw_value]
    for option in options:
        if option["value"] == raw_value:
            return option["id"]
    raise KeyError(f"Could not find {raw_value} for {field['to']}")

import datetime
import re

def extractDate(entry, field, *_):
    if field['to'] in ('Datum Labor', "HSA Termin"):
        raise FieldNotFoundError
    raw_value = entry[field['to']]
    if raw_value is None or raw_value == "None" or re.search('[a-zA-Z]', raw_value):
        raise FieldNotFoundError
    if raw_value.startswith("("):
        raw_value = raw_value.split(",")[0].strip("('")
    d = datetime.datetime.strptime(raw_value, "%d.%m.%Y")
    return d.date().isoformat()


def extractBoolean(entry, field, *_):
    raw_value = entry[field['to']]
    if raw_value is None or raw_value == "None":
        return False
    return bool(raw_value)


def extractMultiSelect(entry, field, *_):
    dest_field = field["to"]
    if dest_field == "Bisherige Diagnostik":
        select_values = []
        has_basis = entry["Basisdiagnostik"]
        if has_basis is None or has_basis == "None":
            has_basis = False
        panelnames = entry["Paneldiagnostik"]
        if panelnames is None or panelnames == "None":
            panelnames = False
        select_options = field["select_options"]
        if has_basis:
            select_values += ('Karyotyping', 'Array-CGH')
        if panelnames:
            select_values += ('Panel',)
        select_ids = [[v['id'] for v in select_options if v['value'] == s][0] for s in select_values]
        return select_ids
    else:
        raise RuntimeError(f"{dest_field} multiselect not supported")

HANDLERS = {
    "link_row": extractLink,
    "text": extractText,
    "long_text": extractText,
    "single_select": extractSelect,
    "date": extractDate,
    "boolean": extractBoolean,
    "multiple_select": extractMultiSelect,
}

def extract_data(entry, schema):
    mapped_entry = {}
    for field in schema["fields"]:
        field_type = field["type"]
        if field_type == "link_row" and field["link_table"] == "Personnel":
            mapped_entry[field["to"]] = match_person(entry.get(field["to"], ""))
        if field_type in ("formula", "created_on", "url", "link_row"):
            continue
        if field_type in HANDLERS:
            try:
                result_value = HANDLERS[field_type](entry, field)
            except FieldNotFoundError as err:
                print(err)
                continue
        else:
            raise RuntimeError(f"Unknown type {field_type}")
        mapped_entry[field["to"]] = result_value
    return mapped_entry

def split_findings(entry):
    var_fields = [{
        k.split("|")[-1]: v for k, v in entry.items()
        if k.startswith(f"V|{i}")
    } for i in range(1, 4)]
    var_fields[0]["ResultType"] = "Main"
    var_fields[1]["ResultType"] = "Incidental"
    var_fields[2]["ResultType"] = "Research"
    # prune empty values
    var_fields = [v for v in [
        {kk: vv for kk, vv in v.items() if vv} for v in var_fields
    ] if len(v) > 1]
    print("SplitFinding", len(var_fields))

    return var_fields


def extract_table_data(entry, schema):
    if schema["name"] == "Findings":
        finding_entries = split_findings(entry)
        datas = [
            extract_data(e, schema) for e in finding_entries
        ]
        return datas
    else:
        return [extract_data(entry, schema)]


def upsert_data(entry, entry_ids):
    # insertion order in order of tables in SCHEMA
    for schema in SCHEMAS:
        table_data = entry[schema["id"]]
        schema_links = [
            {"link_table_id": TABLES[field["link_table"]], **field} for field in schema["fields"] if field["type"] == "link_row" and field["link_table"] in TABLES
        ]
        for link_field in schema_links:
            link_ids = entry_ids.get(link_field["link_table_id"], [])
            entry[link_field["to"]] = link_ids

        ids = upsert_entries(schema["id"], entry_ids[schema["id"]], table_data)
        entry_ids[schema["id"]] = ids
    return entry_ids


def map_raw_entry(entry):
    return {
        schema["id"]: extract_table_data(entry, schema)
        for schema in SCHEMAS
    }


if __name__ == "__main__":

    with open("tnamse.json") as tn:
        entries = json.load(tn)


    ENTRY_CACHE = shelve.open("baserow_seen.shlv")


    # split into two-step process
    # split into separate dicts
    # match against existing entries in database
    # add ids
    for entry in entries:
        data = map_raw_entry(entry)
        entry_key = create_key(entry)
        entry_ids = ENTRY_CACHE.get(entry_key, {})
        new_ids = upsert_data(data, entry_ids)
        ENTRY_CACHE[entry_key] = new_ids

    ENTRY_CACHE.close()
