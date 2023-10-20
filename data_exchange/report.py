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
    text_lines = []
    for person, p_errs in by_responsibility.items():

        errors_by_case = combine_validation_errors_by_entry_id(p_errs)

        text_lines.append(person)

        for cid, errs in errors_by_case.items():
            if errs[0].entry["Varfish=Befund"] in sensible_states:
                text_lines.append(f"  === SV-{cid} ===")
                text_lines.append("  Cur Baserow Findings (Main/Incidental)")
                for f in errs[0].entry["Findings"]:
                    if f["id"] and f["ResultType"] in ("Main", "Incidental"):
                        text_lines.append(f"    {f['Genename']} {f['NM Transcript']} {f['Mutation']}")
                text_lines.append("  Unmatched Varfish Findings")
                for f in errs[0].entry["Findings"]:
                    if not f["id"]:
                        text_lines.append(f"    {f.get('Genename')} {f.get('NM Transcript')} {f.get('Mutation')} {f['Position (VCF)']}")
                text_lines.append("  ><><><><><><")
                for err in errs:
                    text_lines.append("    " + err.comment)

    report_text = "\n".join(text_lines)
    return report_text
