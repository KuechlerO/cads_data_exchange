import re
import datetime
import json
import yaml
from collections import defaultdict
from typing import List

import requests
from argparse import ArgumentParser


def load_json(path):
    with open(path) as file:
        data = json.load(file)
    return data


with open(".baserow_token") as tokenfile:
    TOKEN = tokenfile.readline().strip()


DATA_PATHS = {
    "tnamse": "./data/tnamse.json",
    "clinicians": "./data/clinicians.json",
}


def load_schema(table_name):
    with open(f"schemas/{table_name}.yaml") as file:
        data = yaml.safe_load(file)
    return data


def _get_data(url):
    resp = requests.get(
        url,
        headers={
            "Authorization": f"Token {TOKEN}"
        }
    )
    data = resp.json()

    if "results" not in data:
        raise RuntimeError

    if data["next"]:
        return data["results"] + _get_data(data["next"])
    return data["results"]


def format_value(raw_value, field_info):
    if field_info["type"] == "single_select":
        if isinstance(raw_value, dict):
            return raw_value["id"]
        elif raw_value is None:
            return raw_value
        raise RuntimeError(f"malformed single_select {raw_value}")
    elif field_info["type"] == "multiple_select":
        if isinstance(raw_value, list):
            return [
                v["id"] for v in raw_value
            ]
        raise RuntimeError(f"malformed multiple_select {raw_value}")
    elif field_info["type"] == "link_row":
        if isinstance(raw_value, list):
            return [
                v["id"] for v in raw_value
            ]
        raise RuntimeError(f"malformed link_row {raw_value}")
    else:
        return raw_value


def get_data(table_id):
    """Check a given table for empty keys.
    """
    writable_fields = get_writable_fields(table_id)
    writable_names = {f['name']: f for f in writable_fields}
    data = _get_data(f"https://phenotips.charite.de/api/database/rows/table/{table_id}/?user_field_names=true")

    writable_data = {
        d['id']: {k: format_value(v, writable_names[k]) for k, v in d.items() if k in writable_names} for d in data
    }

    return writable_data


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
    "Gender": {
        "f": "Female",
        "m": "Male",
    },
    "Case Status": {
        "ja": "Solved",
        "Ja": "Solved",
        "UV3": "VUS",
        "nein": "Unsolved",
        "Nein": "Unsolved",
        "kein Ordner": "Invalid",
        "offen": "Active",
    },
    "Zygosity": {
        "homozygot": "Homozygous",
    },
    "GENERIC": {
        "pathogen": "Pathogenic (V)",
        "?": "Other",
    },
}

def extractSelect(entry, field, *_):
    if field['to'] in ('Bearbeitungsstatus', 'ACMG Classification', 'Zygosity'):
        raise FieldNotFoundError(f"non supported field {field['to']}")
    raw_value = entry[field["to"]]

    options = field["select_options"]
    if raw_value is None or raw_value == "None" and field["to"] == "Falltyp":
        raise FieldNotFoundError(f"Value is {raw_value} and thus ignored")
    raw_value = raw_value.strip()

    if field["to"] in SELECT_MAPPINGS:
        field_mappings = SELECT_MAPPINGS[field["to"]]
        if raw_value in field_mappings:
            raw_value = field_mappings[raw_value]
        elif raw_value in SELECT_MAPPINGS["GENERIC"]:
            raw_value = SELECT_MAPPINGS["GENERIC"][raw_value]
    else:
        if raw_value in SELECT_MAPPINGS["GENERIC"]:
            raw_value = SELECT_MAPPINGS["GENERIC"][raw_value]

    for option in options:
        if option["value"] == raw_value:
            return option["id"]
    raise KeyError(f"Could not find {raw_value} for {field['to']}")

def extractDate(entry, field, *_):
    if field['to'] in ('Datum Labor', "HSA Termin"):
        raise FieldNotFoundError(f"Ignored field: {field['to']}")
    raw_value = entry[field['to']]
    if raw_value is None or raw_value == "None" or re.search('[a-zA-Z]', raw_value):
        raise FieldNotFoundError(f"Ignored value {raw_value} for field {field['to']}")
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
        if has_basis is None or has_basis in ("None", "nein", "unklar"):
            has_basis = False
        panelnames = entry["Paneldiagnostik"]
        if panelnames is None or panelnames in ("None", "nein"):
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

LINKED_FIELDS_ROW = ("Indexpatient", "Members", "Findings")

def case_entries_equal(local, other):
    if other is None:
        return False

    mismatches = {}
    for key, value in local.items():
        other_value = other[key]
        if other_value != value:
            mismatches[key] = (value, other_value)
    if mismatches:
        print(mismatches)
        return False
    else:
        return True

class BaserowContext:
    def __init__(self, tables):
        self._tables = tables
        self._persons = load_json(DATA_PATHS["clinicians"])
        self._schemas = [load_schema(n) for n in tables]
        self._handlers = {
            "link_row": self.extractLink,
            "text": extractText,
            "long_text": extractText,
            "single_select": extractSelect,
            "date": extractDate,
            "boolean": extractBoolean,
            "multiple_select": extractMultiSelect,
        }
        self._writable = {
            table_id: get_writable_fields(table_id) for table_id in self._tables.values()
        }

    def match_person(self, person_string: str) -> List[int]:
        person_string_parts = [p.strip() for p in re.split("/| |,|;", person_string) if p]
        for pers in self._persons:
            if person_string == pers["Shorthand"]:
                return [pers["id"]]
            elif person_string == pers["Lastname"] or pers["Lastname"] in person_string_parts:
                return [pers["id"]]
        return []

    def extractLink(self, entry, field, entry_ids):
        link_table = field['link_table']
        if field["to"] in LINKED_FIELDS_ROW:
            table_id, = [v for k, v in self._tables.items() if k == link_table]
            link_id = entry_ids[table_id]
            return link_id
        elif link_table == "Personnel":
            if field["to"] != "Validator":
                p = self.match_person(entry[field["to"]])
                if p:
                    return [p]
                else:
                    return []
        return None

    def get_existing_data(self) -> dict:
        table_data = {}
        for name, tid in self._tables.items():
            table_data[name] = get_data(tid)
        return table_data

    def split_findings(self, entry):
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
        return var_fields

    def extract_data(self, entry, schema):
        mapped_entry = {}
        for field in schema["fields"]:
            field_type = field["type"]
            if field_type == "link_row" and field["link_table"] == "Personnel":
                mapped_entry[field["to"]] = self.match_person(entry.get(field["to"], ""))
            if field_type in ("formula", "created_on", "url", "link_row"):
                continue
            if field_type in self._handlers:
                try:
                    result_value = self._handlers[field_type](entry, field)
                except FieldNotFoundError as err:
                    print(f"FieldNotFound:", err)
                    continue
            else:
                raise RuntimeError(f"Unknown type {field_type}")
            mapped_entry[field["to"]] = result_value
        return mapped_entry


    def extract_table_data(self, entry, schema):
        if schema["name"] == "Findings":
            finding_entries = self.split_findings(entry)
            return [
                self.extract_data(e, schema) for e in finding_entries
            ]
        else:
            return [self.extract_data(entry, schema)]


    def map_entry(self, entry) -> dict:
        mapped_entry = {
            schema["id"]: self.extract_table_data(entry, schema)
            for schema in self._schemas
        }
        return mapped_entry


    def upsert_data(self, entry, case_id, existing_case_entry=None):
        # insertion order in order of tables in SCHEMA
        case_table_id = self._tables["Cases"]
        findings_table_id = self._tables["Findings"]

        case_data, = entry[case_table_id]
        if existing_case_entry:
            case_data["Findings"] = existing_case_entry["Findings"]
        else:
            case_data["Findings"] = []

        if not case_entries_equal(case_data, existing_case_entry):
            if not existing_case_entry:
                resp = requests.post(
                    f"https://phenotips.charite.de/api/database/rows/table/{case_table_id}/?user_field_names=true",
                    headers={
                        "Authorization": f"Token {TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=case_data,
                )
                actual_case_id = resp.json()["id"]
                assert case_id == actual_case_id, "Case ID should never mismatch"
            else:
                resp = requests.patch(
                    f"https://phenotips.charite.de/api/database/rows/table/{case_table_id}/{case_id}/?user_field_names=true",
                    headers={
                        "Authorization": f"Token {TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=case_data,
                )


        findings_data = entry[findings_table_id]
        schema, = [s for s in self._schemas if s["name"] == "Findings"]
        schema_links = [
            {"link_table_id": self._tables[field["link_table"]], **field} for field in schema["fields"] if field["type"] == "link_row" and field["link_table"] in self._tables
        ]

        for link_field in schema_links:
            if link_field["link_table"] == "Cases":
                for finding in findings_data:
                    finding[link_field["to"]] = [case_id]

        for i, finding in enumerate(findings_data):
            existing_id, = case_data["Findings"][i:i+1] or [None]
            # ids = upsert_entries(schema["id"], entry_ids[schema["id"]], table_data)
            if existing_id is None:
                resp = requests.post(
                    f"https://phenotips.charite.de/api/database/rows/table/{findings_table_id}/?user_field_names=true",
                    headers={
                        "Authorization": f"Token {TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=finding,
                )
                resp.raise_for_status()
            else:
                resp = requests.patch(
                    f"https://phenotips.charite.de/api/database/rows/table/{findings_table_id}/{existing_id}/?user_field_names=true",
                    headers={
                        "Authorization": f"Token {TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json=finding,
                )


def add_vertrag(tn_data):
    for entry in tn_data:
        sap_entry = entry["SAP ID"]
        if sap_entry is None:
            entry["SAP ID"] = None
            entry["Vertrag"] = None
            continue

        sap_entry = sap_entry.strip()
        if sap_entry.isnumeric():
            entry["SAP ID"] = sap_entry
            entry["Vertrag"] = "Selektivvertrag"
        elif sap_entry:
            entry["SAP ID"] = None
            entry["Vertrag"] = sap_entry
        else:
            raise RuntimeError(f"{sap_entry} is something wrong")
    return tn_data


def main(case_table_id, findings_table_id):
    tables = {
        "Cases": case_table_id,
        "Findings": findings_table_id,
    }

    ctx = BaserowContext(tables)

    tn_data = load_json(DATA_PATHS["tnamse"])

    tn_data = add_vertrag(tn_data)

    baserow_data = ctx.get_existing_data()
    baserow_case_data = baserow_data["Cases"]
    n = 0
    for entry in tn_data:
        print(entry)
        data = ctx.map_entry(entry)
        entry_key = entry["Medgen ID"]

        if m := re.match("SV-(\d+)", entry_key):
            case_id = int(m.group(1))
            existing_case_entry = baserow_case_data.get(case_id, None)
            ctx.upsert_data(data, case_id, existing_case_entry)
        else:
            raise RuntimeError(f"Invalid entry key {entry_key} format should be SV-<NUMBER>")
        # if n >= 20:
        #     break
        n += 1


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("--case_table_id", type=int, default=579)
    parser.add_argument("--findings_table_id", type=int, default=581)

    args = parser.parse_args()
    main(args.case_table_id, args.findings_table_id)
