import json
import subprocess
import datetime
from dataclasses import dataclass

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


@dataclass
class Appointment:
    status: int
    info: str
    name: str
    color_id: int
    date_begin: datetime.datetime
    date_end: datetime.datetime
    resources: list
    raw: dict

    @classmethod
    def from_raw_json(cls, data):
        try:
            date_begin = parseDate(data["Datum_Beginn"])
            date_end = parseDate(data["Datum_Ende"])
            color_id = data["Farb_Id"]
            resources = [r for r in data["Resources"].split(";") if r]
            info = data.get("Info", None)
            name = data.get("Name", None)
            status = data["Status_Id"]
            raw = data
        except KeyError:
            print(data)
            return None
        return cls(status=status, info=info, color_id=color_id, date_begin=date_begin, date_end=date_end, name=name, raw=raw, resources=resources)

colors = queryMdb(DB_PATH, "Farben")
doctors = queryMdb(DB_PATH, "DocRooms")

active_doctors = [d for d in doctors if not d['hidden'] and d['Type'] == 0]

max_zhao, = [d for d in active_doctors if 'Zhao' in d['Name']]

appointments = [
    a for a in
    map(Appointment.from_raw_json, queryMdb(DB_PATH, "Termine"))
    if a and a.date_begin.year >= 2023
]
import calendar

from io import StringIO
msg_build = StringIO()
print("Sprechstundentermine der nächsten 14 Tage\n", file=msg_build)
start = datetime.datetime.now()
end = start + datetime.timedelta(days=14)
for app in appointments:
    if any(r.endswith(str(max_zhao["Kennummer"])) for r in app.resources):
        if app.date_begin >= start and app.date_end <= end:
            print(app.date_begin.date(), calendar.day_name[app.date_begin.weekday()], f'{app.date_begin.strftime("%H:%M")}-{app.date_end.strftime("%H:%M")}', file=msg_build)
            print("  ", app.name, app.info, file=msg_build)

mail_string = msg_build.getvalue()
if mail_string:
    print("Found new data. Sending mail...")

    import smtplib

    # Import the email modules we'll need
    from email.message import EmailMessage

    # Open the plain text file whose name is in textfile for reading.
    msg = EmailMessage()
    msg.set_content(mail_string)

    # me == the sender's email address
    # you == the recipient's email address
    msg['Subject'] = f'Terminland: Sprechstundentermine der nächsten 14 Tage'

    msg['From'] = "max.zhao@charite.de"
    msg['To'] = "max.zhao@charite.de"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('smtp-out.charite.de', port=25)
    s.send_message(msg)
    s.quit()
