"""
Re-Upload the batch export for batch from clinvar-this to baserow.
"""

import csv
from pathlib import Path
import re
from typing import List

import typer

from data_exchange.baserow import apply_updates, BaserowUpdate, get_table
from data_exchange.updates import to_position_key


def read_clinvar_this_export(path: Path):
    entries = []
    with path.open("r") as f:
        r = csv.DictReader(f, delimiter="\t")
        for entry in r:
            entries.append(entry)
    return entries


def match_finding(entry, base_data) -> int:
    """Returns the matching entry ID."""
    vcf_key = to_position_key({
        "release": entry["ASSEMBLY"],
        "chromosome": entry["CHROM"],
        "start": entry["POS"],
        "reference": entry["REF"],
        "alternative": entry["ALT"],
    })

    for fid, base_entry in base_data.items():
        if vcf_key == base_entry["Position (VCF)"]:
            return fid


def parse_existing_error_msg(error_msg):
    if m := re.search(r"because your organization previously submitted (SCV\d+) for the same variant", error_msg):
        clinvar_id_existing = m.group(1)
        return clinvar_id_existing


def create_findings_updates(ct_entries: List[dict]):
    findings_data = get_table(FINDINGS)
    updates = []
    for entry in ct_entries:
        if fid := match_finding(entry, findings_data):
            update = BaserowUpdate(fid, findings_data[fid])
            if entry["KEY"]:
                update.add_update("Clinvar-Upload-Key", entry["KEY"])
            if entry["ACCESSION"]:
                if entry["ACCESSION"] == "None":
                    raise RuntimeError
                update.add_update("Clinvar-ID", entry["ACCESSION"])
            if entry["error_msg"]:
                if existing_clinvar := parse_existing_error_msg(entry["error_msg"]):
                    update.add_update("Clinvar-ID", existing_clinvar)
            update.add_update("Clinvar-Errors", entry["error_msg"])
            if update.has_updates:
                updates.append(update)
    return updates


FINDINGS = "Findings"


def main(clinvar_export_file: Path, dry_run: bool = False):
    exported_entries = read_clinvar_this_export(clinvar_export_file)
    updates = create_findings_updates(exported_entries)

    apply_updates(FINDINGS, updates, dry_run=dry_run)


if __name__ == "__main__":
    typer.run(main)
