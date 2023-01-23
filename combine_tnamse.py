import json

import pandas as pd

import requests
from requests.auth import HTTPBasicAuth
from argparse import ArgumentParser

auth = HTTPBasicAuth("tn", "20LB22_tn")

r = requests.get("http://s-labb-ngs01.laborberlin.intern/hum/tn/1_Laufzettel_zur_Befundung/0_TABELLE/metadaten.html", auth=auth)
table, = pd.read_html(r.content, encoding="utf-8")
table.columns = [t for c in table.columns for t in c if "Unnamed" not in t]

with open("./tnamse.json") as jsfile:
    tnamse_records = json.load(jsfile)

tnamse_df = pd.DataFrame.from_records(tnamse_records)

missing_lb = tnamse_df.loc[tnamse_df['LB ID'] == 'None']


def match_sample(s, index_entry):
    return (
        s["Lastname"] == index_entry["Fam Name"]
        and
        s["Firstname"] == index_entry["Given Name"]
        and
        s["Birthdate"] == index_entry["Birthdate"]
    )


parser = ArgumentParser()
parser.add_argument("--batch", type=int, required=False)
args = parser.parse_args()

STARTING_BATCH = args.batch

persons = [
    {
        "short": "AA",
        "first": "Angela",
        "last": "Abad",
        "reports": True,
        "quota": 1,
    },
    {
        "short": "RA",
        "first": "Ronja",
        "last": "Adam",
        "reports": True,
        "quota": 2,
    },
    {
        "short": "LS",
        "first": "Lara",
        "last": "Segebrecht",
        "reports": True,
        "quota": 2,
    },
    {
        "short": "HS",
        "first": "Henrike",
        "last": "Sczakiel",
        "reports": True,
        "quota": 2,
    },
    {
        "short": "DH",
        "first": "Denise",
        "last": "Horn",
        "reports": False,
        "quota": 0,
    },
    {
        "short": "FB",
        "first": "Felix",
        "last": "Boschann",
        "reports": True,
        "quota": 0,
    },
    {
        "short": "CE",
        "first": "Rici",
        "last": "Ott",
        "reports": False,
        "quota": 0,
    },
    {
        "short": "NE",
        "first": "Nadja",
        "last": "Ehmke",
        "reports": False,
        "quota": 0,
    },
    {
        "short": "SW",
        "first": "Sarina",
        "last": "Schwartzmann",
        "reports": True,
        "quota": 1,
    },
    {
        "short": "MD",
        "first": "Magdalena",
        "last": "Danyel",
        "reports": True,
        "quota": 0,
    },
]


def matchPerson(first=None, last=None, short=None):
    for person in persons:
        matches = []
        if first:
            matches.append(first == person["first"])
        if last:
            matches.append(last == person["last"])
        if short:
            matches.append(short == person["short"])
        if matches and all(matches):
            return person
    return None


from random import choice

def select_random_first_look(persons):
    return choice([p for p in persons if p["reports"] and p["quota"] > 0])


def select_first_look(counts, persons, sender):
    if sender:
        sender_count = counts[sender["short"]]
        if sender_count < sender["quota"]:
            return sender
    reporting_persons_with_open_quota = [
        p for p in persons if p["reports"] and p["quota"] - counts[p["short"]] > 0
    ]
    if reporting_persons_with_open_quota:
        return choice(reporting_persons_with_open_quota)
    return select_random_first_look(persons)


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


from collections import Counter
first_look_counts = Counter()


from io import StringIO
from pathlib import Path


CACHE_PATH = Path("cache_batch.json")


mail_message = StringIO()


if not STARTING_BATCH and CACHE_PATH.exists():
    with CACHE_PATH.open() as f:
        f = json.load(f)
    STARTING_BATCH = f["last_seen"]
    print(f"Starting again from previous state, only including batches {STARTING_BATCH} and up")


last_seen_batch = STARTING_BATCH
sodar_df = pd.read_csv("./sodar/s_CADS_Exomes_Diagnostics.txt", sep="\t")
had_error = False
for group, data in sodar_df.groupby("Characteristics[Batch]"):
    if group > STARTING_BATCH:
        last_seen_batch = group
        print(f"Batch", group, file=mail_message)
        for family, famentries in data.groupby("Characteristics[Family]"):
            # check that families are contained in PEL
            index_id = family.split("_", 1)[1].replace("_", "-")
            pel_entries = table.loc[table["Index-ID"] == index_id]
            pel_entry_index = pel_entries.loc[pel_entries["LB-ID"] == index_id]
            if pel_entry_index.shape[0] == 0:
                print(f"> Could not find {index_id} in LB PEL", file=mail_message)
                had_error = True
            else:
                pel_entry_index = pel_entry_index.squeeze()
                tnamse_entry = tnamse_df.loc[tnamse_df.apply(lambda s: match_sample(s, pel_entry_index), axis=1)]
                if tnamse_entry.shape[0] == 0:
                    print(
                        "> Could not find entry in NAMSE Table",
                        pel_entry_index["Fam Name"], "|",
                        pel_entry_index["Given Name"],
                        pel_entry_index["Birthdate"],
                        file=mail_message,
                    )
                    had_error = True
                elif tnamse_entry.shape[0] > 1:
                    print(">", index_id, "Found too many entries ", tnamse_entry[["Lastname", "Firstname", "Birthdate"]], file=mail_message)
                    had_error = True
                else:
                    tnamse_entry = tnamse_entry.squeeze()
                    first_look = tnamse_entry["First Look"]
                    sender = tnamse_entry["Clinician"]

                    matched_first = matchPerson(short=first_look)
                    matched_sender = matchPerson(last=sender)

                    if matched_first:
                        first_look_counts[matched_first["short"]] += 1
                    else:
                        matched_first = select_first_look(first_look_counts, persons, matched_sender)
                        first_look_counts[matched_first["short"]] += 1

                    print(matched_sender, sender)
                    print(matched_first, first_look)

                    analysis_type = infer_analysis_type(famentries.shape[0], pel_entry_index["Analysetyp"])
                    print(family, f"({tnamse_entry['Lastname']})", analysis_type, f"({matched_sender['first']})", "->", matched_first["first"], file=mail_message)

if not had_error:
    with CACHE_PATH.open("w") as f:
        json.dump({"last_seen": last_seen_batch}, f)

mail_string = mail_message.getvalue()

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
    if STARTING_BATCH + 1 != last_seen_batch:
        msg['Subject'] = f'Processing of Batches {STARTING_BATCH+1} to {last_seen_batch}'
    else:
        msg['Subject'] = f'Processing of Batch {last_seen_batch}'

    msg['From'] = "max.zhao@charite.de"
    msg['To'] = "max.zhao@charite.de"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('smtp-out.charite.de', port=25)
    s.send_message(msg)
    s.quit()
