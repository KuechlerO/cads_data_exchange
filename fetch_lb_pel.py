import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("output_file")
parser.add_argument("--username", required=True)
parser.add_argument("--password", required=True)

args = parser.parse_args()

auth = HTTPBasicAuth(args.username, args.password)

r = requests.get("http://s-labb-ngs01.laborberlin.intern/hum/tn/1_Laufzettel_zur_Befundung/0_TABELLE/metadaten.html", auth=auth)
table, = pd.read_html(r.content, encoding="utf-8")
table.columns = [t for c in table.columns for t in c if "Unnamed" not in t]

print(f"Saving LB PEL to {args.output_file}")

table.to_csv(args.output_file, index=False, sep="\t")
