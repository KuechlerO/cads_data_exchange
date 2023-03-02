from os import wait
import re
import json
import pandas as pd

import datetime
from pathlib import Path

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


def matchSample(s, index_entry):
    return (
        matchLbId(s["LB ID"], index_entry["LB-ID"])
    ) or (
        s["Lastname"] == index_entry["Fam Name"]
        and
        s["Firstname"] == index_entry["Given Name"]
        and
        s["Birthdate"] == index_entry["Birthdate"]
    )

def get_reporting_clinicians(clinicians):
    return [
        c
        for c in clinicians
        if c["Active"] and any(r["value"] == "Investigator" for r in c["Role"])
    ]


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


def gather_info(fam_id, tnamse_data, lb_data, varfish_data):
    varfish_fam = varfish_data.loc[varfish_data["name"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")

    lb_fam = lb_data.loc[lb_data["Index-ID"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")

    lb_index = [l for l in lb_fam if l["Index-ID"] == l["LB-ID"]]
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

    tnamse_index_found = tnamse_data.loc[tnamse_data.apply(lambda s: matchSample(s, lb_index), axis=1)].to_dict("records")

    lbid_fmt = fam_id
    firstname_fmt = ""
    lastname_fmt = ""
    birthdate_fmt = ""
    sender_fmt = ""
    firstlook_fmt = ""
    errors = []
    if lb_index:
        firstname_fmt = lb_index["Given Name"]
        lastname_fmt = lb_index["Fam Name"]
        birthdate_fmt = lb_index["Birthdate"]
        lbid_fmt = lb_index["LB-ID"]
    if tnamse_index_found:
        tnamse_index, = tnamse_index_found
        if lb_index:
            if lb_index["Given Name"] != tnamse_index["Firstname"]:
                firstname_fmt = f"TN({tnamse_index['Firstname']})|LB({lb_index['Given Name']})"
                errors.append(("firstname", firstname_fmt))
            if lb_index["Fam Name"] != tnamse_index["Lastname"]:
                lastname_fmt = f"TN({tnamse_index['Lastname']})|LB({lb_index['Fam Name']})"
                errors.append(("lastname", lastname_fmt))
            if lb_index["Birthdate"] != tnamse_index["Birthdate"]:
                birthdate_fmt = f"TN({tnamse_index['Birthdate']})|LB({lb_index['Birthdate']})"
                errors.append(("birthdate", birthdate_fmt))
            if not matchLbId(lb_index["LB-ID"], tnamse_index["LB ID"]):
                lbid_fmt = f"TN({tnamse_index['LB ID']})|LB({lb_index['LB-ID']})"
                errors.append(("lbid", lbid_fmt))
        sender_fmt = tnamse_index["Clinician"]
        firstlook_fmt = tnamse_index["First Look"]

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
        "tnamse_found": bool(tnamse_index_found),
    }


def format_info_line(info):
    if not info["firstlook"]:
        firstlook_fmt = "MISSING"
    else:
        firstlook_fmt = info["firstlook"]

    notifications = []
    if not info["varfish_found"]:
        notifications.append("not in varfish")
    if not info["lb_found"]:
        notifications.append("not in LB PEL")
    if not info["tnamse_found"]:
        notifications.append("not in TNAMSE Table")
    notification = ",".join(notifications)

    error = ",".join(f"{n}({err})" for n, err in info["errors"])
    info_line = f"{info['fam_id']}({info['lastname']}) Einsender: {info['sender']} First Look: {firstlook_fmt} - {notification} - {error}"
    return info_line


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


def main():
    paths = {
        "tnamse": "data/tnamse.json",
        "lb": "data/lb_pel.tsv",
        "sodar": "data/sodar/s_CADS_Exomes_Diagnostics.txt",
        "varfish": "data/varfish",
        "clinicians": "data/clinicians.json",
    }
    tnamse_data = load_json(paths["tnamse"])
    lb_data = load_tsv(paths["lb"])
    sodar_data = load_tsv(paths["sodar"])

    varfish_data = load_latest(paths["varfish"])
    new_varfish_names = latest_has_changed(paths["varfish"])
    if not new_varfish_names:
        print("No new varfish entries. Do nothing")
        return 0
    print("New varfish entries: ", ", ".join(new_varfish_names))

    all_clinicians = load_json(paths["clinicians"], cast=False)
    reporting_clinicians = get_reporting_clinicians(all_clinicians)

    output_per_batch = {}
    for batch_id, sodar_batch in sodar_data.groupby("Characteristics[Batch]"):
        output_per_batch[batch_id] = []
        for fam_id, sodar_fam in sodar_batch.groupby("Characteristics[Family]"):
            info = gather_info(fam_id, tnamse_data, lb_data, varfish_data)
            formatted = format_info_line(info)
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
    send_email("CADS Diagnostics - New Data in Varfish", message_text, recipients)


if __name__ == "__main__":
    main()
