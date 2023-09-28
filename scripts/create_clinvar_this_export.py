from pathlib import Path
from loguru import logger
import typer
from data_exchange.baserow import get_table

from csv import DictWriter


FINDINGS = "Findings"
CASES = "Cases"


def finding_is_exportable(finding):
    correct_result_type = finding["ResultType"] in ("Main", "Incidental")
    has_position = bool(finding["Position (VCF)"])
    return correct_result_type and has_position


def is_exportable(entry):
    no_clinvar_errors = not entry["AutoValidation"] or "ClinVar" not in entry["AutoValidation"]
    has_not_uploaded_findings = any(not f["Clinvar-Upload-Key"] for f in entry["Findings"] if finding_is_exportable(f))
    is_completed = bool(entry["Case Status"] in ("Solved", "VUS"))

    return no_clinvar_errors and has_not_uploaded_findings and is_completed


def split_pos(position):
    assm, rem = position.split("_")
    chrom, pos, ref, alt = rem.split("-")
    return assm, chrom, pos, ref, alt


ACMG_MAPPING = {
    "Benign (I)": "Benign",
    "Likely Benign (II)": "Likely benign",
    "Uncertain Significance (III)": "Uncertain significance",
    "Likely Pathogenic (IV)": "Likely pathogenic",
    "Pathogenic (V)": "Pathogenic",
}


def format_clinvar_this_smallvar(finding, case_entry):
    assembly, chrom, pos, ref, alt = split_pos(finding["Position (VCF)"])
    omim = finding["OMIM"]
    inheritance = finding["Inheritance"]
    clin_sig = ACMG_MAPPING[finding["ACMG Classification"]]
    clin_eval = finding["EvaluationDate"]

    hpo = finding["HPO Terms"]

    consent = case_entry["Datenverarbeitung"]

    return {
        "ASSEMBLY": assembly,
        "CHROM": chrom,
        "POS": pos,
        "REF": ref,
        "ALT": alt,
        "OMIM": omim,
        "MOI": inheritance,
        "CLIN_SIG": clin_sig,
        "CLIN_EVAL": clin_eval,
        "CLIN_COMMENT": finding["Interpretation (ClinVar)"],
        "KEY": finding["Clinvar-Upload-Key"],
        "HPO": hpo if consent else "",
    }


def write_to_tsv(data, path):
    fieldnames = data[0].keys()
    with path.open("w") as f:
        writer = DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for entry in data:
            writer.writerow(entry)


def main(output_tsv_file: Path):
    findings_data = get_table(FINDINGS)
    cases_data = get_table(CASES)

    for case in cases_data.values():
        case["Findings"] = [
            {"id": fid, **findings_data[fid]} for fid in case["Findings"]
        ]

    exportable_cases = [c for c in cases_data.values() if is_exportable(c)]

    exportable_findings = [(f, c) for c in exportable_cases for f in c["Findings"] if finding_is_exportable(f)]

    logger.info(f"{len(exportable_cases)} cases with total {len(exportable_findings)} exportable findings")

    clinvar_mapped_results = [format_clinvar_this_smallvar(f, c) for f, c in exportable_findings]

    write_to_tsv(clinvar_mapped_results, output_tsv_file)


if __name__ == "__main__":
    typer.run(main)
