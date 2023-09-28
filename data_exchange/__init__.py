from . import sams, varfish, baserow, sodar


def format_info_line(info, clinicians_data, matched_cases):
    if not info["First Look"]:
        firstlook_fmt = "MISSING"
    else:
        firstlook_datas = [clinicians_data[str(i)] for i in info["First Look"]]
        firstlook_fmt = ",".join(fl["Lastname"] for fl in firstlook_datas)

    if not info["Clinician"]:
        sender_fmt = "MISSING"
    else:
        sender_datas = [clinicians_data[str(i)] for i in info["Clinician"]]
        sender_fmt = ",".join(d["Lastname"] for d in sender_datas)

    bsId = list(matched_cases.keys())[0]

    info_line = f"{info['LB ID']}(SV-{bsId} {info['Lastname']} {info['Birthdate']}) Einsender: {sender_fmt} First Look: {firstlook_fmt}"
    return info_line


def format_message(output_per_batch, padding=""):
    message_lines = []
    for name, lines in output_per_batch.items():
        message_lines.append(f"Batch {name}")
        message_lines += lines

    message_lines = [f"{padding}{l}" for l in message_lines]
    return "\n".join(message_lines)


import smtplib
from email.message import EmailMessage

def send_email(title, message_text, recipients):
    msg = EmailMessage()
    msg["Subject"] = title
    msg.set_content(message_text)

    msg['From'] = "max.zhao@charite.de"
    msg['To'] = ",".join(recipients)
    s = smtplib.SMTP('smtp-out.charite.de', port=25)
    s.send_message(msg)
    s.quit()


def get_varfish_new(varfish_data, cases_data):
    uuids_case = {c["Varfish"]: c["LB ID"] for c in cases_data.values() if c["Varfish"]}
    case_uuids = {c["LB ID"]: c["Varfish"] for c in cases_data.values() if c["Varfish"]}
    case_comments = [c["Kommentar"] for c in cases_data.values()]
    missing_uuids = []
    for uuid, vname in zip(varfish_data["sodar_uuid"], varfish_data["name"]):
        if uuid not in uuids_case and not any(matchLbId(vname, n) for n in case_uuids):
            missing_uuids.append(f"{vname} ({uuid})")
    return missing_uuids




def main():
    paths = {
        "cases": "data/cases.json",
        "lb": "data/lb_pel.json",
        "clinicians": "data/clinicians.json",
        "sodar": "data/sodar/s_CADS_Exomes_Diagnostics.txt",
        "varfish": "data/varfish",
    }
    cases_data = load_json(paths["cases"], cast=False)
    lb_data = load_json(paths["lb"], cast=False)
    clinicians_data = load_json(paths["clinicians"], cast=False)
    sodar_data = load_tsv(paths["sodar"])
    varfish_data = load_latest(paths["varfish"])
    varfish_tn_data = load_latest(paths["varfish"], prefix="tn_cases")
    varfish_exomes_data = load_latest(paths["varfish"], prefix="exomes_cases")

    lb_updates = update_baserow_from_lb(cases_data, lb_data)
    if lb_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(lb_updates)

    sodar_updates = update_baserow_from_sodar(cases_data, sodar_data, varfish_data)
    if sodar_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(sodar_updates)

    tn_updates = update_baserow_from_varfish(cases_data, varfish_tn_data)
    if tn_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(tn_updates)

    exomes_updates = update_baserow_from_varfish(cases_data, varfish_exomes_data)
    if exomes_updates:
        print("There are baserow updates to be submitted.")
        apply_updates_to_baserow(exomes_updates)

    sams_updates = update_baserow_from_sams(cases_data)
    if sams_updates:
        print("There are sams updates to be submitted")
        apply_updates_to_baserow(sams_updates)

    new_varfish_ids = get_varfish_new(varfish_data, cases_data)
    print("New varfish entries: ", ", ".join(new_varfish_ids))

    output_per_batch = {}
    for batch_id, sodar_batch in sodar_data.groupby("Characteristics[Batch]"):
        output_per_batch[batch_id] = []
        for fam_id, sodar_fam in sodar_batch.groupby("Characteristics[Family]"):
            matched_cases = match_cases(cases_data, fam_id, [])
            info = list(matched_cases.values())[0]
            formatted = format_info_line(info, clinicians_data, matched_cases)
            output_per_batch[batch_id].append(formatted)

    output_per_batch = dict(sorted(output_per_batch.items(), reverse=True))

    message_text = format_message(output_per_batch, padding="\t")
    recipients = [
        "max.zhao@charite.de",
        "ronja.adam@charite.de",
    ]

    message_text = f"""
    Liebe CADS Befunder,

    anbei eine Liste der bisher prozessierten Batches:

{message_text}
"""
    send_email("CADS Diagnostics - New Data in Varfish", message_text, recipients)


if __name__ == "__main__":
    main()
