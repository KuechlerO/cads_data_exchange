import re
import json
from dataclasses import dataclass
import pandas as pd

import datetime
from pathlib import Path

from python_baserow_simple import BaserowApi
from constants import CASE_TABLE_ID

from unidecode import unidecode

from sams import SAMS, phenopacket_to_varfish_format


BR = BaserowApi(token_path=".baserow_token")
SM = SAMS.with_credentials(credentials_file=".sams_credentials")


class NameInfoException(Exception):
    pass


def load_json(path, cast=True):
    with open(str(path)) as jsfile:
        data = json.load(jsfile)
    if not cast:
        return data
    return pd.DataFrame.from_records(data)

def load_tsv(path):
    table_data = pd.read_csv(path, sep="\t")
    return table_data

def get_paths_by_date(path, prefix):
    path = Path(path)
    if not path.is_dir():
        raise RuntimeError("Needs to be directory with date formatted files")
    found_files = []
    for file in Path(path).iterdir():
        if not file.is_dir() and file.name.startswith(prefix) and (m := re.search(r"\d{4}-\d{2}-\d{2}", file.name)):
            found_files.append((datetime.date.fromisoformat(m.group(0)), file))
    found_files = sorted(found_files, key=lambda t: t[0], reverse=True)
    return found_files

def get_second_latest_path(path, prefix):
    paths_by_date = get_paths_by_date(path, prefix=prefix)
    if len(paths_by_date) >= 2:
        return paths_by_date[1][1]
    return None

def get_latest_path(path, prefix):
    paths_by_date = get_paths_by_date(path, prefix)
    if len(paths_by_date) >= 1:
        return paths_by_date[0][1]
    return None

def load_latest(path, prefix="cases"):
    latest_path = get_latest_path(path, prefix=prefix)
    if latest_path:
        data = load_json(latest_path)
        return data
    return None

def latest_has_changed(path, prefix="cases"):
    latest = get_latest_path(path, prefix=prefix)
    second_latest = get_second_latest_path(path, prefix=prefix)

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


def analysezahl_to_int(analysezahl):
    return {
            "Single": 1,
            "Duo": 2,
            "Trio": 3,
            "Quattro": 4,
    }.get(analysezahl, 0)


def format_info_line(info, clinicians_data, matched_cases):
    if not info["First Look"]:
        firstlook_fmt = "MISSING"
    else:
        firstlook_datas = [clinicians_data[str(i)] for i in info["First Look"]]
        firstlook_fmt = ",".join(fl["Lastname"] for fl in firstlook_datas)

    if not info["Clinician"]:
        sender_fmt = "MISSING"
    else:
        sender_datas = [clinicians_data[str(i)] for i in info["Clinician"]]
        sender_fmt = ",".join(d["Lastname"] for d in sender_datas)

    bsId = list(matched_cases.keys())[0]

    info_line = f"{info['LB ID']}(SV-{bsId} {info['Lastname']} {info['Birthdate']}) Einsender: {sender_fmt} First Look: {firstlook_fmt}"
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
    uuids_case = {c["Varfish"]: c["LB ID"] for c in cases_data.values() if c["Varfish"]}
    case_uuids = {c["LB ID"]: c["Varfish"] for c in cases_data.values() if c["Varfish"]}
    case_comments = [c["Kommentar"] for c in cases_data.values()]
    missing_uuids = []
    for uuid, vname in zip(varfish_data["sodar_uuid"], varfish_data["name"]):
        if uuid not in uuids_case and not any(matchLbId(vname, n) for n in case_uuids):
            missing_uuids.append(f"{vname} ({uuid})")
    return missing_uuids


@dataclass(frozen=True, eq=True)
class NameInfo:
    first: str
    last: str
    birthdate: datetime.date

    @classmethod
    def from_any(cls, first, last, birthdate):
        if first is None:
            first = ""
        if last is None:
            last = ""

        first = unidecode(first).strip().replace("-", " ").lower().split()
        last = unidecode(last).strip().replace("-", " ").lower().split()
        if type(birthdate) is str:
            if "-" in birthdate:
                birthdate = datetime.datetime.strptime(birthdate, "%Y-%m-%d").date()
            elif "." in birthdate:
                birthdate = datetime.datetime.strptime(birthdate, "%d.%m.%Y").date()
            else:
                raise NameInfoException(f"Invalid date string: {birthdate} from {last}, {first}")
        return cls(first=first, last=last, birthdate=birthdate)

    def match(self, other):
        birthdate_valid = (self.birthdate == other.birthdate) or not self.birthdate

        name_valid = False
        if not self.first or not self.last or not other.first or not other.last:
            name_valid = False
        elif self.first == other.first and self.last == other.last:
            name_valid = True
        elif self.first[0] == other.first[0] and self.last == other.last:
            name_valid = True

        if self.last == ["kratt"] and other.last == ['kratt']:
            print(self, other, name_valid, birthdate_valid)

        return birthdate_valid and name_valid


def update_baserow_from_lb(cases_data, lb_data):
    ID_FIELD = "LB ID"
    INDEX_FIELD = "Index ID"
    COUNT_FIELD = "Analysezahl"

    def match_sample_by_sampleid(lb_data, sample_id):
        found = []
        for entry in lb_data.values():
            if matchLbId(sample_id, entry.get(ID_FIELD)):
                found.append(entry)
        return found

    def match_sample_by_indexid(lb_data, index_id):
        found = []
        for entry in lb_data.values():
            if matchLbId(index_id, entry.get(INDEX_FIELD)):
                found.append(entry)
        return found

    def match_sample_by_name(lb_data, firstname, lastname, birthdate):
        try:
            name_info = NameInfo.from_any(firstname, lastname, birthdate)
        except NameInfoException as err:
            print("Invalid name in terminland", err)
            raise
        found = []
        for entry in lb_data.values():
            if not entry["Birthdate"] and not entry["LB ID"]:
                continue
            try:
                entry_info = NameInfo.from_any(
                    entry["Firstname"], entry["Lastname"], entry["Birthdate"]
                )
            except NameInfoException as err:
                print("Invalid name in LB PEL", err, entry)
                continue
            if name_info.match(entry_info):
                found.append(entry)
        return found

    all_updates = {}
    for bs_id, bs_entry in cases_data.items():
        updates = {
            "current": bs_entry,
            "updates": []
        }
        bs_lbid = bs_entry[ID_FIELD]
        bs_probendate = bs_entry["Datum Labor"]
        bs_count = bs_entry[COUNT_FIELD]
        bs_count_int = analysezahl_to_int(bs_count)
        if bs_lbid:
            lb_samples = match_sample_by_sampleid(lb_data, bs_lbid)
        else:
            lb_samples = match_sample_by_name(lb_data, bs_entry["Firstname"], bs_entry["Lastname"], bs_entry["Birthdate"])

        if not lb_samples:
            if not bs_lbid:
                print(f"No LB PEL Entry matched for {bs_id} {bs_entry['Firstname']} {bs_entry['Lastname']} {bs_entry['Birthdate']} LBID({bs_lbid})")
            continue
        if len(lb_samples) > 1:
            print(f"{len(lb_samples)} PEL entries matched for {bs_id} {bs_entry['Firstname']} {bs_entry['Lastname']} {bs_entry['Birthdate']} LBID({bs_lbid})")
        lb_entry = lb_samples[-1]
        lb_index = lb_entry[INDEX_FIELD]
        lb_fam_entries = match_sample_by_indexid(lb_data, lb_index)

        lb_lbid = lb_entry[ID_FIELD]
        lb_probendate = lb_entry["Datum Labor"]

        if not bs_lbid and lb_lbid:
            updates["updates"].append((ID_FIELD, lb_lbid))
        if not bs_probendate and lb_probendate:
            if m := re.search(r"\d{2}.\d{2}.\d{4}", lb_entry["Datum Labor"]):
                lb_probendate_fmt = datetime.datetime.strptime(m.group(0), "%d.%m.%Y").date().isoformat()
                updates["updates"].append(("Datum Labor", lb_probendate_fmt))

        # only autoupdate trio for now
        if not bs_count and lb_fam_entries == 3:
            updates["updates"].append(("Analysezahl", "Trio"))

        if updates["updates"]:
            all_updates[bs_id] = updates

    return all_updates


def match_cases(cases_data, fam_id, sample_ids):
    found = {}
    for bs_id, bs_entry in cases_data.items():
        bs_lbid = bs_entry["LB ID"]
        if matchLbId(bs_lbid, fam_id):
            found[bs_id] = bs_entry
        elif any(matchLbId(bs_lbid, sample_id) for sample_id in sample_ids):
            found[bs_id] = bs_entry
    return found


def update_baserow_from_sams(cases_data):
    all_updates = {}
    sams_data = SM.get_phenopackets()

    def get_sams_case(sv_id):
        for entry in sams_data:
            if sv_id == entry["subject"]["id"]:
                return entry
        return {}

    for bs_id, bs_entry in cases_data.items():
        update = {"current": bs_entry, "updates": []}

        # ignore if HPO Terms had already been submitted
        if bs_entry["HPO Terms"]:
            continue

        sv_id = f"SV-{bs_id}"
        if sams_sample := get_sams_case(sv_id):
            hpo_string = phenopacket_to_varfish_format(sams_sample)
            if hpo_string:
                update["updates"].append(("HPO Terms", hpo_string))

        if update["updates"]:
            if bs_id not in all_updates:
                all_updates[bs_id] = update
            else:
                all_updates[bs_id]["updates"] += update["updates"]
    return all_updates


def update_baserow_from_varfish(cases_data, varfish_data):
    all_updates = {}
    for bs_id, bs_entry in cases_data.items():
        update = {"current": bs_entry, "updates": []}
        varfish_sodar_uuid = None

        fam_id = bs_entry["LB ID"]
        if not fam_id:
            continue

        varfish_fam = varfish_data.loc[varfish_data["name"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")
        if not varfish_fam:
            continue

        varfish_sodar_uuid = varfish_fam[0]["sodar_uuid"]
        varfish_status = VARFISH_STATUS_TO_BASEROW[varfish_fam[0]["status"]]

        if (not bs_entry["Varfish"] or "http" in bs_entry["Varfish"]) and varfish_sodar_uuid:
            update["updates"].append(("Varfish", varfish_sodar_uuid))

        # bs_status = bs_entry["Case Status"]
        # if varfish_status is not None and status_newer(varfish_status, bs_status):
        #     update["updates"].append(("Case Status", varfish_status))

        if update["updates"]:
            if bs_id not in all_updates:
                all_updates[bs_id] = update
            else:
                all_updates[bs_id]["updates"] += update["updates"]
    return all_updates



def update_baserow_from_sodar(cases_data, sodar_data, varfish_data):


    all_updates = {}

    for fam_id, sodar_fam in sodar_data.groupby("Characteristics[Family]"):
        varfish_fam = varfish_data.loc[varfish_data["name"].apply(lambda n: matchLbId(fam_id, n))].to_dict("records")
        batch_ids = sodar_fam["Characteristics[Batch]"]
        batch_id = int(batch_ids.values[0])
        sample_ids = sodar_fam["Sample Name"]

        if varfish_fam:
            varfish_sodar_uuid = varfish_fam[0]["sodar_uuid"]
            varfish_status = VARFISH_STATUS_TO_BASEROW[varfish_fam[0]["status"]]
        else:
            varfish_sodar_uuid = None
            varfish_status = None

        cases = match_cases(cases_data, fam_id, sample_ids)
        if cases:
            for bs_id, bs_entry in cases.items():
                update = {"current": bs_entry, "updates": []}

                if not bs_entry["Batch"] and batch_id:
                    update["updates"].append(("Batch", batch_id))

                if (not bs_entry["Varfish"] or "http" in bs_entry["Varfish"]) and varfish_sodar_uuid:
                    update["updates"].append(("Varfish", varfish_sodar_uuid))

                bs_status = bs_entry["Case Status"]
                if varfish_status is not None and status_newer(varfish_status, bs_status):
                    update["updates"].append(("Case Status", varfish_status))

                if update["updates"]:
                    if bs_id not in all_updates:
                        all_updates[bs_id] = update
                    else:
                        all_updates[bs_id]["updates"] += update["updates"]

    return all_updates


def apply_updates_to_baserow(lb_updates):
    for bs_id, bs_update in lb_updates.items():
        entry = bs_update["current"].copy()
        updates = bs_update["updates"]

        print(f"Updating baserow entry {bs_id} {entry['Firstname']} {entry['Lastname']} *{entry['Birthdate']} Fields: {', '.join(f for f, _ in updates)}")
        for field, value in updates:
            entry[field] = value
        BR.add_data(CASE_TABLE_ID, entry, row_id=bs_id)


def main():
    paths = {
        "cases": "data/cases.json",
        "lb": "data/lb_pel.json",
        "clinicians": "data/clinicians.json",
        "sodar": "data/sodar/s_CADS_Exomes_Diagnostics.txt",
        "varfish": "data/varfish",
    }
    cases_data = load_json(paths["cases"], cast=False)
    lb_data = load_json(paths["lb"], cast=False)
    clinicians_data = load_json(paths["clinicians"], cast=False)
    sodar_data = load_tsv(paths["sodar"])
    varfish_data = load_latest(paths["varfish"])
    varfish_tn_data = load_latest(paths["varfish"], prefix="tn_cases")
    varfish_exomes_data = load_latest(paths["varfish"], prefix="exomes_cases")

    lb_updates = update_baserow_from_lb(cases_data, lb_data)
    if lb_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(lb_updates)

    sodar_updates = update_baserow_from_sodar(cases_data, sodar_data, varfish_data)
    if sodar_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(sodar_updates)

    tn_updates = update_baserow_from_varfish(cases_data, varfish_tn_data)
    if tn_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(tn_updates)

    exomes_updates = update_baserow_from_varfish(cases_data, varfish_exomes_data)
    if exomes_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(exomes_updates)

    sams_updates = update_baserow_from_sams(cases_data)
    if sams_updates:
        print("There are sams updates to be submitted")
        apply_updates_to_baserow(sams_updates)

    new_varfish_ids = get_varfish_new(varfish_data, cases_data)
    print("New varfish entries: ", ", ".join(new_varfish_ids))

    output_per_batch = {}
    for batch_id, sodar_batch in sodar_data.groupby("Characteristics[Batch]"):
        output_per_batch[batch_id] = []
        for fam_id, sodar_fam in sodar_batch.groupby("Characteristics[Family]"):
            matched_cases = match_cases(cases_data, fam_id, [])
            info = list(matched_cases.values())[0]
            formatted = format_info_line(info, clinicians_data, matched_cases)
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
