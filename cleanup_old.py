#!/usr/bin/env python3

import sys
from argparse import ArgumentParser
from pathlib import Path

import re
import datetime

parser = ArgumentParser()

parser.add_argument("target_dir", type=Path)

args = parser.parse_args()

if not args.target_dir.is_dir():
    print(f"{args.target_dir} is not a directory")
    sys.exit(1)


DATE_REGEX = re.compile(r"\d{4}-\d{2}-\d{2}")

TODAY = datetime.date.today()

MAX_AGE = datetime.timedelta(days=30)

for file in args.target_dir.iterdir():
    if not file.is_dir() and (m := DATE_REGEX.search(file.name)):
        filedate = datetime.date.fromisoformat(m.group(0))
        filedelta = TODAY - filedate

        if filedelta >= MAX_AGE:
            print(f"Removing old file {file}")
            file.unlink()
