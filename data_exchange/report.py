"""Create reports with validation errors etc.
"""


from collections import defaultdict
from typing import List

from data_exchange.validation import ValidationError, combine_validation_errors_by_entry_id, combine_validation_errors_by_responsible

def to_text_report(errors) -> str:
    by_entry_id = combine_validation_errors_by_entry_id(errors)
    all_lines = []
    for entry_id, entry_errors in by_entry_id.items():
        lines = []
        header = f"== SV-{entry_id} =="
        lines = [header]
        for err in entry_errors:
            responsible = err.responsible
            responsible_str = ",".join(f"{r['Title']} {r['Firstname']} {r['Lastname']} ({r['Email']})" for r in responsible)
            line = f"{err.source.name}: {err.field} {err.comment} Verantwortlich: {responsible_str}"
            lines.append(line)
        footer = f"==="
        lines.append(footer)
        entry_text = "\n".join(lines)
        all_lines.append(entry_text)
    return "\n".join(all_lines)


def to_text_report_by_responsible(errors):
    by_responsibility = combine_validation_errors_by_responsible(errors)

    sensible_states = ("HPO+Omim+Inheritance+ACMG+Flag in VarFish", "Abgeglichen")
    for person, p_errs in by_responsibility.items():

        clinvar_errors = [e for e in p_errs if e.source.name == "ClinVar Upload"]
        clinvar_errors_by_case = defaultdict(list)
        for err in clinvar_errors:
            clinvar_errors_by_case[err.entry_id].append(err)
        print(person)
        for cid, errs in clinvar_errors_by_case.items():
            if errs[0].entry["Varfish=Befund"] in sensible_states:
                print(f"  === SV-{cid} ===")
                print("  Cur Baserow Findings (Main/Incidental)")
                for f in errs[0].entry["Findings"]:
                    if f["id"] and f["ResultType"] in ("Main", "Incidental"):
                        print("    ", f["Genename"], f["NM Transcript"], f["Mutation"])
                print("  Unmatched Varfish Findings")
                for f in errs[0].entry["Findings"]:
                    if not f["id"]:
                        print("    ", f.get("Genename"), f.get("NM Transcript"), f.get("Mutation"), f["Position (VCF)"])
                print("  ><><><><><><")
                for err in errs:
                    print("    ", err.comment)
