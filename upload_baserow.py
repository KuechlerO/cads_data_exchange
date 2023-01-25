import json
import yaml


TABLES = {
    "Findings": 389,
    "Patients": 387,
    "Families": 388,
    "Cases": 386,
}

def load_schema(table_name):
    with open(f"schemas/{table_name}.yaml") as file:
        data = yaml.safe_load(file)
    return data


table_schemas = {
    n: load_schema(n) for n in TABLES
}


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

personnel_data = load_personnel()


with open("tnamse.json") as tn:
    entries = json.load(tn)

IDENTIFIER_FIELDS = ("Lastname", "Firstname", "Birthdate")
def create_key(entry):
    return "|".join(entry[k] for k in IDENTIFIER_FIELDS)

def create_entry(table_id, table_data):
    resp = requests.post(
        f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true",
        headers={
            "Authorization": f"Token {TOKEN}",
            "Content-Type": "application/json",
        },
        json=table_data,
    )
    print(resp.text)
    return resp.json()["id"]

def update_entry(table_id, entry_id, table_data):
    resp = requests.patch(
        f"https://phenotips.charite.de/api/database/rows/table/{table_id}/{entry_id}/?user_field_names=true",
        headers={
            "Authorization": f"Token {TOKEN}",
            "Content-Type": "application/json",
        },
        json=table_data,
    )
    print(resp.text)
    return resp.json()["id"]


def create_or_update_entry(table_id, entry_id, table_data):
    if entry_id:
        return update_entry(table_id, entry_id, table_data)
    else:
        return create_entry(table_id, table_data)

import shelve

def match_person(person_string):
    for pers in personnel_data:
        if person_string == pers["Shorthand"]:
            return pers["id"]
        elif person_string == pers["Lastname"]:
            return pers["id"]
    return None


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
    if raw_value == "None" and field["to"] == "Falltyp":
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
    if raw_value.startswith("("):
        raw_value = raw_value.split(",")[0].strip("('")
    if raw_value is None or raw_value == "None" or re.search('[a-zA-Z]', raw_value):
        raise FieldNotFoundError
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

def extract_data(entry, schema, entry_ids):
    mapped_entry = {}
    for field in schema["fields"]:
        field_type = field["type"]
        if field_type in ("formula", "created_on"):
            continue
        if field_type in HANDLERS:
            try:
                result_value = HANDLERS[field_type](entry, field, entry_ids)
            except FieldNotFoundError as err:
                print(err)
                continue
        else:
            raise RuntimeError(f"Unknown type {field_type}")
        mapped_entry[field["to"]] = result_value
    return mapped_entry

ENTRY_CACHE = shelve.open("baserow_seen.shlv")

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
    print(var_fields)

    return var_fields


def process_entry(table_name, table_id, table_schema, entry, existing_id=None):
    if table_name == "Findings":
        finding_entries = split_findings(entry)
        finding_ids = []
        for finding_entry in finding_entries:
            tdata = extract_data(finding_entry, table_schema, entry_ids)
            tdata = {k: v for k, v in tdata.items() if v}
            finding_id = create_or_update_entry(table_id, existing_id, tdata)
            finding_ids.append(finding_id)
        return finding_ids
    else:
        tdata = extract_data(entry, table_schema, entry_ids)
        tdata = {k: v for k, v in tdata.items() if v}
        entry_id = create_or_update_entry(table_id, existing_id, tdata)
        return [entry_id]


for entry in entries:
    table_data = {}
    entry_key = create_key(entry)

    entry["Affected"] = True
    entry["Tested"] = True

    if entry_key not in ENTRY_CACHE:
        entry_ids = {}
        for table_name, table_schema in table_schemas.items():
            table_id = TABLES[table_name]
            entry_ids[table_id] = process_entry(table_name, table_id, table_schema, entry)
        # ENTRY_CACHE[entry_key] = entry_ids
    else:
        entry_ids = ENTRY_CACHE[entry_key]
        for table_name, table_schema in table_schemas.items():
            table_id = TABLES[table_name]
            if table_id in entry_ids:
                # already has entry for item
                existing_ids = entry_ids[table_id]
                print(existing_ids)
                if len(existing_ids) <= 1:
                    existing_id = None
                    if existing_ids:
                        existing_id = existing_ids[0]
                    entry_ids[table_id] = process_entry(table_name, table_id, table_schema, entry, existing_id)
                else:
                    raise RuntimeError("Multiple entries not supported yet")
            else:
                entry_ids[table_id] = process_entry(table_name, table_id, table_schema, entry)

        # TODO implement update entry
        ...

ENTRY_CACHE.close()
