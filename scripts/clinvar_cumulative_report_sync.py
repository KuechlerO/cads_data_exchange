"""
Synchronize clinvar cumulative report with findings in Baserow.
"""
import csv
from pathlib import Path
from typing import List
from xml.etree import ElementTree as ET

from loguru import logger
import typer

from data_exchange.baserow import apply_updates, get_table, BaserowUpdate


def read_clinvar_tsv(clinvar_tsv_path: Path) -> List[dict]:
    """Read clinvar tsv into list of dicts format."""
    entries = []
    with clinvar_tsv_path.open("r") as f:
        prev_pos = None
        for row in f:
            if row.startswith("#Your_variant_id\tVariationID"):
                headers = row.split("\t")
                break
        else:
            raise RuntimeError(f"Failed to find header")
        dr = csv.DictReader(f, fieldnames=headers, delimiter="\t")
        for entry in dr:
            entries.append(entry)
    return entries


def coords_to_position(clinvar_entry):
    coords_raw = clinvar_entry["Your_variant_description_chromosome_coordinates"]
    if not coords_raw:
        return None

    tree = ET.fromstring(coords_raw)

    chrom_str = None
    try:
        assm = tree.attrib["Assembly"]
        chrom = tree.attrib["Chr"]
        begin = tree.attrib["start"]
        ref = tree.attrib["referenceAllele"]
        alt = tree.attrib["alternateAllele"]
        chrom_str = f"{assm}_{chrom}-{begin}-{ref}-{alt}"
    except KeyError:
        logger.warning(f"Malformed entry {coords_raw}")

    return chrom_str


BASEROW_FINDING_KEY_FIELD = "Clinvar-Upload-Key"
BASEROW_CLINVAR_ID = "Clinvar-ID"


def main(clinvar_tsv: Path, dry_run: bool = False):
    logger.info("Reading", clinvar_tsv)

    clinvar_data = read_clinvar_tsv(clinvar_tsv)
    from pprint import pprint
    pprint(clinvar_data[0])

    finding_data = get_table("Findings")

    matched = 0
    unmatched = 0
    updates = []
    for clinvar_entry in clinvar_data:
        clinvar_key = clinvar_entry["Your_record_id"]
        clinvar_id = clinvar_entry["SCV"]
        if position := coords_to_position(clinvar_entry):
            for fid, finding in finding_data.items():
                vcf_position_key = finding["Position (VCF)"]
                if position == vcf_position_key:
                    update = BaserowUpdate(fid, finding)
                    if not finding[BASEROW_FINDING_KEY_FIELD]:
                        update.add_update(BASEROW_FINDING_KEY_FIELD, clinvar_key)
                    if clinvar_id and (not finding[BASEROW_CLINVAR_ID] or "SCV" not in finding[BASEROW_CLINVAR_ID]):
                        update.add_update(BASEROW_CLINVAR_ID, clinvar_id)
                    if update.has_updates:
                        updates.append(update)
                    matched += 1
            else:
                unmatched += 1

    logger.info(f"Matched {matched} clinvar entries. {unmatched} unmatched entries remaining")

    if updates:
        apply_updates("Findings", updates, dry_run=dry_run)


if __name__ == "__main__":
    typer.run(main)
