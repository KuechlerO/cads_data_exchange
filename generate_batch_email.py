import re
import json
import pandas as pd

import datetime
from pathlib import Path

from python_baserow_simple import BaserowApi
from constants import CASE_TABLE_ID, PERSONNEL_TABLE_ID

from unidecode import unidecode


BR = BaserowApi(token_path=".baserow_token")


def load_json(path, cast=True):
    with open(str(path)) as jsfile:
        data = json.load(jsfile)
    if not cast:
        return data
    return pd.DataFrame.from_records(data)

def load_tsv(path):
    table_data = pd.read_csv(path, sep="\t")
    return table_data

def get_paths_by_date(path):
    path = Path(path)
    if not path.is_dir():
        raise RuntimeError("Needs to be directory with date formatted files")
    found_files = []
    for file in Path(path).iterdir():
        if not file.is_dir() and (m := re.search(r"\d{4}-\d{2}-\d{2}", file.name)):
            found_files.append((datetime.date.fromisoformat(m.group(0)), file))
    found_files = sorted(found_files, key=lambda t: t[0], reverse=True)
    return found_files

def get_second_latest_path(path):
    paths_by_date = get_paths_by_date(path)
    if len(paths_by_date) >= 2:
        return paths_by_date[1][1]
    return None

def get_latest_path(path):
    paths_by_date = get_paths_by_date(path)
    if len(paths_by_date) >= 1:
        return paths_by_date[0][1]
    return None

def load_latest(path):
    latest_path = get_latest_path(path)
    if latest_path:
        data = load_json(latest_path)
        return data
    return None

def latest_has_changed(path):
    latest = get_latest_path(path)
    second_latest = get_second_latest_path(path)

    # return true if second latest does not exist
    if not latest:
        raise RuntimeError(f"{path} does not contained date numbered files")

    latest_data = load_json(latest)

    if not second_latest:
        return set(latest_data["name"])

    second_latest_data = load_json(second_latest)

    new_names = set(latest_data["name"]) - set(second_latest_data["name"])
    return new_names


LB_ID_INNER = re.compile(r"(\d{2})[-_](\d{4})")
def matchLbId(lbid1, lbid2):
    m1 = LB_ID_INNER.search(str(lbid1))
    m2 = LB_ID_INNER.search(str(lbid2))
    if not (m1 and m2):
        return False
    return (m1.group(1), m1.group(2)) == (m2.group(1), m2.group(2))


def matchName(name1, name2, strict=True):
    """Match names for two persons. Ignores case and special characters.

    If strict is set names will be directly compared.
    """

    n1 = unidecode(name1).lower().replace("-", " ")
    n2 = unidecode(name2).lower().replace("-", " ")

    # provide direct comparison if strict
    if strict:
        return n1 == n2

    # compare first name if not strict
    nparts1 = n1.split(" ")
    nparts2 = n2.split(" ")
    return nparts1[0] == nparts2[0]

def format_date(datestr):
    if datestr is None:
        return datestr
    if "." in datestr:
        datestr = datetime.datetime.strptime(datestr, "%d.%m.%Y").date().isoformat()
    return datestr

def matchDate(date1, date2):
    """Match 2 dates.
    """
    d1 = format_date(date1)
    d2 = format_date(date2)
    return d1 == d2


def matchSample(s, index_entry):

    return (
        matchLbId(s["LB ID"], index_entry["LB-ID"])
    ) or (
        matchName(s["Lastname"], index_entry["Fam Name"], strict=True)
        and
        matchName(s["Firstname"], index_entry["Given Name"], strict=False)
        and
        matchDate(s["Birthdate"], index_entry["Birthdate"])
    )


def match_last_name(name, other):
    if name == other:
        return True

    name_parts = name.split("-")
    other_parts = other.split("-")
    if name_parts[0] == other_parts[0]:
        return True
    return False


def matchPerson(persons, first=None, last=None, short=None):
    for person in persons:
        matches = []
        if first:
            matches.append(first == person["Firstname"])
        if last:
            matches.append(match_last_name(person["Lastname"], last))
        if short:
            matches.append(short == person["Shorthand"])
        if matches and all(matches):
            return person
    return None


def matchAny(persons, name_string):
    names = [n.strip() for n in name_string.split("/")]
    for name in names:
        if p := matchPerson(persons, last=name):
            return p
    return None


def infer_analysis_type(num_entries, analysis_type):
    if num_entries == 1:
        return "Single-Genome"
    types = {
        1: "Single",
        2: "Duo",
        3: "Trio",
        4: "Quattro",
    }
    count_name = types.get(num_entries, num_entries)
    return f"{count_name}-{analysis_type}"


def gather_info(fam_id, cases_data, lb_data, varfish_data):
    varfish_fam = varfish_data.loc[varfish_data["name"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")

    lb_fam = lb_data.loc[lb_data["Index-ID"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")

    lb_index = [l for l in lb_fam if l["Index-ID"] == l["LB-ID"]]
    if len(lb_index) == 0:
        lb_index = [l for l in lb_fam if matchLbId(l["Index-ID"], l["LB-ID"])]

    if len(lb_index) != 1:
        print(f"{fam_id} index samples not exactly 1: {lb_index}")
        return None
    if len(lb_index) >= 1:
        lb_index = lb_index[0]
    else:
        lb_index = {
            "Fam Name": "",
            "Given Name": "",
            "Birthdate": "",
            "LB-ID": fam_id,
        }

    # found in baserow
    cases_index_found = [(eid, edata) for eid, edata in cases_data.items() if matchSample(edata, lb_index)]

    lbid_fmt = fam_id.replace("_", "-")
    firstname_fmt = ""
    lastname_fmt = ""
    birthdate_fmt = ""
    sender_fmt = ""
    firstlook_fmt = ""
    errors = []
    entry_id = None
    tnamse_index = None
    if lb_index:
        firstname_fmt = lb_index["Given Name"]
        lastname_fmt = lb_index["Fam Name"]
        birthdate_fmt = format_date(lb_index["Birthdate"])
        lbid_fmt = lb_index["LB-ID"]
    if cases_index_found:
        if len(cases_index_found) == 1:
            (entry_id, tnamse_index), = cases_index_found
        elif len(cases_index_found) > 1:
            index_candidates = [(i, d) for i, d in cases_index_found if d["LB ID"] == lb_index["LB-ID"]]
            if len(index_candidates) == 1:
                (entry_id, tnamse_index), = index_candidates
            else:
                print("Duplicate baserow entries {[i for i, _ in index_candidates]} with same LB ID: {lbid_fmt}")
        else:
            found_data = [(i, d["LB ID"]) for i, d in cases_index_found]
            print(f"Multiple index cases found in baserow: {found_data}")

        if lb_index:
            if lb_index["Given Name"] != tnamse_index["Firstname"]:
                firstname_fmt = f"TN({tnamse_index['Firstname']})|LB({lb_index['Given Name']})"
                errors.append(("firstname", firstname_fmt))
            if lb_index["Fam Name"] != tnamse_index["Lastname"]:
                lastname_fmt = f"TN({tnamse_index['Lastname']})|LB({lb_index['Fam Name']})"
                errors.append(("lastname", lastname_fmt))
            if (d1 := format_date(lb_index["Birthdate"])) != (d2 := format_date(tnamse_index["Birthdate"])):
                birthdate_fmt = f"TN({d1})|LB({d2})"
                errors.append(("birthdate", birthdate_fmt))
            if tnamse_index["LB ID"] not in (None, "") and not matchLbId(lb_index["LB-ID"], tnamse_index["LB ID"]):
                lbid_fmt = f"TN({tnamse_index['LB ID']})|LB({lb_index['LB-ID']})"
                errors.append(("lbid", lbid_fmt))
        sender_fmt = tnamse_index["Clinician"]
        firstlook_fmt = tnamse_index["First Look"]
    else:
        print(lb_index, " not found")

    return {
        "fam_id": fam_id,
        "lb_id": lbid_fmt,
        "firstname": firstname_fmt,
        "lastname": lastname_fmt,
        "birthdate": birthdate_fmt,
        "sender": sender_fmt,
        "firstlook": firstlook_fmt,
        "errors": errors,
        "varfish_found": bool(varfish_fam),
        "lb_found": bool(lb_index),
        "varfish_data": varfish_fam[0] if varfish_fam else None,
        "lb_data": lb_index,
        "baserow_data": tnamse_index,
        "baserow_id": entry_id,
    }


def format_info_line(info, clinicians_data):
    if not info["firstlook"]:
        firstlook_fmt = "MISSING"
    else:
        firstlook_datas = [clinicians_data[str(i)] for i in info["firstlook"]]
        firstlook_fmt = ",".join(fl["Lastname"] for fl in firstlook_datas)

    if not info["sender"]:
        sender_fmt = "MISSING"
    else:
        sender_datas = [clinicians_data[str(i)] for i in info["sender"]]
        sender_fmt = ",".join(d["Lastname"] for d in sender_datas)

    notifications = []
    if not info["varfish_found"]:
        notifications.append("not in varfish")
    if not info["lb_found"]:
        notifications.append("not in LB PEL")
    if info["baserow_id"] is None:
        notifications.append("not in Baserow")
    notification = ",".join(notifications)

    error = ",".join(f"{n}({err})" for n, err in info["errors"])
    info_line = f"{info['fam_id']}({info['lastname']} {info['birthdate']}) Einsender: {sender_fmt} First Look: {firstlook_fmt} {notification} {error}"
    return info_line


VARFISH_STATUS_TO_BASEROW = {
    "closed-solved": "Solved",
    "closed-uncertain": "VUS",
    "closed-unsolved": "Unsolved",
    "active": "Active",
    "initial": "Varfish Initial",
}

STATUS_ORDER = [
    "Invalid",
    "Storniert",
    "Varfish Initial",
    "Active",
    "Unsolved",
    "VUS",
    "Solved",
]

def status_newer(new_status, old_status):
    """Check if the new status is higher rank than old one."""
    if old_status is None:
        return True
    new_index = STATUS_ORDER.index(new_status)
    old_index = STATUS_ORDER.index(old_status)
    return new_index > old_index


def update_baserow(data):
    varfish_data = data["varfish_data"]
    lb_data = data["lb_data"]

    baserow_data = data["baserow_data"]
    changed_fields = []
    if lb_data["ProbenDate"] is not None and (m := re.search(r"\d{2}.\d{2}.\d{4}", lb_data["ProbenDate"])):
        new_proben_date = datetime.datetime.strptime(m.group(0), "%d.%m.%Y").date().isoformat()
        if baserow_data["Datum Labor"] != new_proben_date:
            baserow_data["Datum Labor"] = new_proben_date
            changed_fields.append("Datum Labor")

    if baserow_data["Varfish"] in (None, "") and varfish_data is not None:
        baserow_data["Varfish"] = f"https://varfish.bihealth.org/variants/f2acceb7-067d-41a4-8e39-236c022678f1/case/{varfish_data['sodar_uuid']}"
        changed_fields.append("Varfish")

    if baserow_data["LB ID"] in (None, "") and data["lb_id"]:
        baserow_data["LB ID"] = data["lb_id"]
        changed_fields.append("LB ID")

    if varfish_data is not None:
        cur_status = baserow_data["Case Status"]
        varfish_conv_status = VARFISH_STATUS_TO_BASEROW[varfish_data["status"]]
        if status_newer(varfish_conv_status, cur_status):
            baserow_data["Case Status"] = varfish_conv_status
            changed_fields.append("Case Status")

    if changed_fields:
        print(f"Updating baserow entry {data['baserow_id']} {baserow_data['LB ID']} Fields: {','.join(changed_fields)}")
        BR.add_data(CASE_TABLE_ID, baserow_data, row_id=data["baserow_id"])


def format_message(output_per_batch, padding=""):
    message_lines = []
    for name, lines in output_per_batch.items():
        message_lines.append(f"Batch {name}")
        message_lines += lines

    message_lines = [f"{padding}{l}" for l in message_lines]
    return "\n".join(message_lines)


import smtplib
from email.message import EmailMessage

def send_email(title, message_text, recipients):
    msg = EmailMessage()
    msg["Subject"] = title
    msg.set_content(message_text)

    msg['From'] = "max.zhao@charite.de"
    msg['To'] = ",".join(recipients)
    s = smtplib.SMTP('smtp-out.charite.de', port=25)
    s.send_message(msg)
    s.quit()


def get_varfish_new(varfish_data, cases_data):
    case_uuids = [c["Varfish"].split("/")[-1] for c in cases_data.values() if c["Varfish"]]
    case_comments = [c["Kommentar"] for c in cases_data.values()]
    missing_uuids = [f"{l} ({i})" for i, l in zip(varfish_data["sodar_uuid"], varfish_data["name"]) if i not in case_uuids and not any((i in c) for c in case_comments if c)]
    return missing_uuids


def main():
    paths = {
        "cases": "data/cases.json",
        "lb": "data/lb_pel.tsv",
        "sodar": "data/sodar/s_CADS_Exomes_Diagnostics.txt",
        "varfish": "data/varfish",
        "clinicians": "data/clinicians.json",
    }
    cases_data = load_json(paths["cases"], cast=False)
    clinicians_data = load_json(paths["clinicians"], cast=False)
    lb_data = load_tsv(paths["lb"])
    sodar_data = load_tsv(paths["sodar"])
    varfish_data = load_latest(paths["varfish"])

    new_varfish_ids = get_varfish_new(varfish_data, cases_data)

    print("New varfish entries: ", ", ".join(new_varfish_ids))

    output_per_batch = {}
    for batch_id, sodar_batch in sodar_data.groupby("Characteristics[Batch]"):
        output_per_batch[batch_id] = []
        for fam_id, sodar_fam in sodar_batch.groupby("Characteristics[Family]"):
            info = gather_info(fam_id, cases_data, lb_data, varfish_data)
            if info["baserow_id"] is not None:
                update_baserow(info)
            formatted = format_info_line(info, clinicians_data)
            output_per_batch[batch_id].append(formatted)

    output_per_batch = dict(sorted(output_per_batch.items(), reverse=True))

    message_text = format_message(output_per_batch, padding="\t")
    recipients = [
        "max.zhao@charite.de",
        "ronja.adam@charite.de",
    ]

    message_text = f"""
    Liebe CADS Befunder,

    anbei eine Liste der bisher prozessierten Batches:

{message_text}
"""
    if new_varfish_ids:
        send_email("CADS Diagnostics - New Data in Varfish", message_text, recipients)


if __name__ == "__main__":
    main()
