import re
import json
import subprocess
import datetime
from typing import Optional
from dataclasses import dataclass

from enum import Enum

from python_baserow_simple import BaserowApi
from data_exchange.config import settings
from data_exchange.planner_match import is_cads

DB_PATH = "/media/WMG/WMG-LZG/klinische Genetik/Terminplaner v.240/Arztpra2.MDB"


def queryMdb(mdbPath, table):
    result = subprocess.run(["mdb-json", mdbPath, table], capture_output=True)
    data = [json.loads(r) for r in result.stdout.split(b"\n") if r]
    return data


def parseDate(datestring):
    if datestring:
        date = datetime.datetime.strptime(datestring, "%m/%d/%y %H:%M:%S")
        return date
    return None


class PresentEnum(Enum):
    NONE = 0
    PRESENT = 1
    CANCELED = 12


@dataclass
class Appointment:
    status: int
    info: str
    name: str
    color_id: int
    date_begin: Optional[datetime.datetime]
    date_end: Optional[datetime.datetime]
    resources: list
    raw: dict
    present: int
    patient_id: Optional[int]

    @property
    def identifier(self):
        if self.date_begin is None or self.date_begin is None or self.patient_id is None:
            return None
        return f"{self.patient_id}:{self.date_begin.isoformat()}{self.date_end.isoformat()}"

    @classmethod
    def from_raw_json(cls, data):
        try:
            date_begin = parseDate(data["Datum_Beginn"])
            date_end = parseDate(data["Datum_Ende"])
            color_id = data["Farb_Id"]
            resources = [r for r in data["Resources"].split(";") if r]
            info = data.get("Info", None)
            name = data.get("Name", None)
            patient_id = data.get('Patient_Id')
            status = data["Status_Id"]
            present = data.get("Anwesend", 0)
            raw = data
            return cls(status=status, info=info, color_id=color_id, date_begin=date_begin, date_end=date_end, name=name, raw=raw, resources=resources, present=present, patient_id=patient_id)
        except KeyError:
            return None


@dataclass
class NameInfo:
    firstname: Optional[str]
    lastname: Optional[str]
    dob: Optional[datetime.date]

colors = queryMdb(DB_PATH, "Farben")
doctors = queryMdb(DB_PATH, "DocRooms")

active_doctors = [d for d in doctors if not d['hidden'] and d['Type'] == 0]

if not active_doctors:
    raise RuntimeError("Database empty")

appointments = [
    a for a in
    map(Appointment.from_raw_json, queryMdb(DB_PATH, "Termine"))
    if a and a.date_begin and a.date_begin.year >= 2023
]
import calendar

from io import StringIO
msg_build = StringIO()
print("Sprechstundentermine der nächsten 14 Tage\n", file=msg_build)
start = datetime.datetime.now()
next_appointments = []


def is_cads_appointment(app):
    return is_cads(app.info) and app.present == PresentEnum.PRESENT.value


def is_recent_appointment(app):
    today = datetime.date.today()
    recent_timespan = datetime.timedelta(days=30)
    return app.date_begin.date() <= today and today - app.date_begin.date() <= recent_timespan


hsa_appointments = []

for app in appointments:
    if is_cads_appointment(app):
        if is_recent_appointment(app):
            hsa_appointments.append(app)


br = BaserowApi(database_url=settings.baserow.url, token=settings.baserow_token)
CASE_TABLE_ID = 579
PERSONNEL_TABLE_ID = 582

existing_case_data = br.get_data(CASE_TABLE_ID)
personnel_data = br.get_data(PERSONNEL_TABLE_ID)

def extract_patient_name(name_text):
    if name_text is None:
        return None, None, None
    name = name_text
    dob = None
    if m := re.search(r"\d{2}.\d{2}.\d{4}", name_text):
        name = name_text[:m.start()]
        dob = datetime.datetime.strptime(m.group(0), "%d.%m.%Y").date()
    elif m := re.search(r"\d{2}.\d{2}.\d{2}", name_text):
        name = name_text[:m.start()]
        dob = datetime.datetime.strptime(m.group(0), "%d.%m.%y").date()

    name = name.replace("geb. am", "")
    name = name.strip(", *")
    if "," in name:
        lastname, firstname, *_ = name.split(",")
    else:
        lastname = name
        firstname = None

    if firstname:
        firstname = firstname.strip()
    if lastname:
        lastname = lastname.strip()

    return NameInfo(firstname=firstname, lastname=lastname, dob=dob)


def create_name_info_baserow_data(data):
    firstname = data["Firstname"]
    lastname = data["Lastname"]
    if data["Birthdate"]:
        dob = datetime.date.fromisoformat(data["Birthdate"])
    else:
        dob = None
    return NameInfo(firstname=firstname, lastname=lastname, dob=dob)


def match_patient_name(n1, n2):
    lastname_exists = n1.lastname not in (None, "") and n2.lastname not in (None, "")
    if lastname_exists:
        lastname_matches = n1.lastname == n2.lastname
    else:
        lastname_matches = False
    firstname_exists = n1.firstname not in (None, "") and n2.firstname not in (None, "")
    if firstname_exists:
        fn = n1.firstname.split()[0]
        fn2 = n2.firstname.split()[0]
        firstname_matches = fn == fn2
    else:
        firstname_matches = False

    dob_exists = n1.dob is not None and n2.dob is not None
    if dob_exists:
        dob_matches = n1.dob == n2.dob
    else:
        dob_matches = False

    reversed_matches = False
    if firstname_exists and lastname_exists:
        reversed_matches = n1.firstname == n2.lastname and n1.lastname == n2.firstname

    if firstname_matches and lastname_matches and dob_matches:
        return True
    elif dob_matches and lastname_matches:
        return True
    elif firstname_matches and lastname_matches:
        return True
    elif dob_matches and firstname_matches:
        return True
    elif dob_matches and reversed_matches:
        return True
    elif lastname_matches and not (firstname_exists and dob_exists):
        return True
    elif dob_matches:
        ...
    return False


def get_responsible_physician(active_doctors, app):
    for doctor in active_doctors:
        doctor_id = f"D{doctor['Kennummer']}"
        if doctor_id in app.resources:
            return doctor

    # match by color if we can't find the person id, this might happen if users
    # manually set the color to match the person
    app_color = app.raw["Farb_Id"]
    for doctor in active_doctors:
        if doctor["Farb_Id"] == app_color:
            return doctor

    return None


def get_baserow_physician_id(doctor, baserow_physicians):
    for person_id, person_data in baserow_physicians.items():
        if doctor["Name"].endswith(person_data["Lastname"]):
            return person_id
    return None


def get_empty_rows(existing_case_data):
    empty_row_ids = []
    empty_criteria = ["Firstname", "Lastname", "Birthdate"]

    for row_id, row_data in existing_case_data.items():
        if all(row_data[c] is None or row_data[c] == "" for c in empty_criteria):
            empty_row_ids.append(row_id)
    return empty_row_ids


def update_entry(br, app, matched_entry_id, existing_data):
    doctor = get_responsible_physician(active_doctors, app)
    if doctor is not None:
        matched_doctor_id = get_baserow_physician_id(doctor, personnel_data)
    else:
        matched_doctor_id = None

    name_info = extract_patient_name(app.name)
    fmt_date = app.date_begin.date().isoformat()

    updated_fields = []
    if existing_data["Firstname"] in (None, ""):
        if name_info.firstname is not None:
            existing_data["Firstname"] = name_info.firstname
            updated_fields.append("Firstname")
    if existing_data["Lastname"] in (None, ""):
        if name_info.lastname is not None:
            existing_data["Lastname"] = name_info.lastname
            updated_fields.append("Lastname")
    if existing_data["Birthdate"] in (None, ""):
        if name_info.dob is not None:
            existing_data["Birthdate"] = name_info.dob.isoformat()
            updated_fields.append("Birthdate")
    if not existing_data["Clinician"]:
        if matched_doctor_id is not None:
            existing_data["Clinician"] = [matched_doctor_id]
            updated_fields.append("Clinician")
    if existing_data["Datum Einschluss"] in (None, ""):
        if app.date_begin is not None:
            existing_data["Datum Einschluss"] = fmt_date
            updated_fields.append("Datum Einschluss")

    if not updated_fields:
        return None

    # print(existing_data)
    br.add_data(CASE_TABLE_ID, existing_data, row_id=matched_entry_id)

    return f"{matched_entry_id}: {existing_data['Firstname']} {existing_data['Lastname']} {existing_data['Birthdate']} - Termin {fmt_date}: {updated_fields=}"


def create_entry(br, app):
    doctor = get_responsible_physician(active_doctors, app)
    if doctor is not None:
        matched_doctor_id = get_baserow_physician_id(doctor, personnel_data)
    else:
        matched_doctor_id = None
    name_info = extract_patient_name(app.name)

    if name_info.dob:
        dob = name_info.dob.isoformat()
    else:
        dob = None

    fmt_date = app.date_begin.date().isoformat()

    new_data = {
        "Firstname": name_info.firstname,
        "Lastname": name_info.lastname,
        "Birthdate": dob,
        "Clinician": [matched_doctor_id],
        "Datum Einschluss": fmt_date,
    }

    entry_id = br.add_data(CASE_TABLE_ID, new_data)

    return f"{entry_id}: {new_data['Firstname']} {new_data['Lastname']} {new_data['Birthdate']} - Termin {fmt_date}"



empty_row_ids = get_empty_rows(existing_case_data)

new_entry_lines = []
updated_entry_lines = []
for app in hsa_appointments:
    name_info = extract_patient_name(app.name)
    matched_entry_id = None
    for entry_id, existing_data in existing_case_data.items():
        existing_info = create_name_info_baserow_data(existing_data)
        if match_patient_name(name_info, existing_info):
            matched_entry_id = entry_id
    if matched_entry_id:
        l = update_entry(br, app, matched_entry_id, existing_case_data[matched_entry_id])
        if l is not None:
            updated_entry_lines.append(l)
    elif empty_row_ids:
        matched_entry_id = empty_row_ids.pop(0)
        l = update_entry(br, app, matched_entry_id, existing_case_data[matched_entry_id])
        if l is not None:
            new_entry_lines.append(l)
    else:
        l = create_entry(br, app)
        new_entry_lines.append(l)

new_entries = len(new_entry_lines)
updated_entries = len(updated_entry_lines)
if new_entry_lines or updated_entries:
    print(f"Terminland Baserow Sync: {new_entries=} {updated_entries=}")
if len(new_entry_lines):
    print("Created new entries:")
    print("\n".join(new_entry_lines))

if len(updated_entry_lines):
    print("Updated entries:")
    print("\n".join(updated_entry_lines))
