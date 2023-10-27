from collections import defaultdict
import re
import datetime
from typing import List, Callable, Optional
from enum import Enum

from loguru import logger
from attrs import asdict, define

from .baserow import VALI_FAIL, VALI_FIN, VALI_OK, BaserowUpdate, matchLbId, status_newer
from .nameinfo import NameInfo, NameInfoException

from .varfish import VARFISH_STATUS_TO_BASEROW, get_findings

from .sams import phenopacket_to_varfish_format


class Condition(str, Enum):
    NONE = "none"
    DIFFERENT = "different"
    CUSTOM = "custom"


@define
class Mapping:
    source: str
    dest: str
    when: Condition = Condition.NONE
    transform: Callable = lambda d: d
    cmp: Callable = lambda a, b: a > b


    def applies(self, source_value, dest_value) -> bool:
        """Check if an update can be applied."""
        if self.when is Condition.NONE:
            return not bool(dest_value) and bool(source_value)
        elif self.when is Condition.DIFFERENT:
            return source_value != dest_value
        elif self.when is Condition.CUSTOM:
            return self.cmp(source_value, dest_value)
        else:
            raise RuntimeError(f"Condition {self.when} not handled yet")


    def apply(self, source_data, dest_data) -> Optional[tuple]:
        if not hasattr(source_data, "get"):
            source_data = asdict(source_data)
        if not hasattr(dest_data, "get"):
            dest_data = asdict(dest_data)

        if self.source:
            source_entry = source_data.get(self.source)
        else:
            source_entry = source_data

        dest_entry = dest_data.get(self.dest)

        transformed_entry = self.transform(source_entry)
        if self.applies(transformed_entry, dest_entry):
            return self.dest, transformed_entry



class CaseNotFoundError(Exception):
    pass


ID_FIELD = "LB ID"
INDEX_FIELD = "Index ID"
COUNT_FIELD = "Analysezahl"


def match_sample_by_sampleid(lb_data, sample_id):
    found = []
    for entry in lb_data.values():
        if matchLbId(sample_id, entry.get(ID_FIELD)):
            found.append(entry)
    return found

def match_sample_by_indexid(lb_data, index_id):
    found = []
    for entry in lb_data.values():
        if matchLbId(index_id, entry.get(INDEX_FIELD)):
            found.append(entry)
    return found

def match_sample_by_name(lb_data, firstname, lastname, birthdate):
    name_info = NameInfo.from_any(firstname, lastname, birthdate)
    found = []
    for entry in lb_data.values():
        if not entry["Birthdate"] and not entry["LB ID"]:
            continue
        try:
            entry_info = NameInfo.from_any(
                entry["Firstname"], entry["Lastname"], entry["Birthdate"]
            )
        except NameInfoException as err:
            logger.error("Invalid name in LB PEL", err, entry)
            continue
        if name_info.match(entry_info):
            found.append(entry)
    return found


def update_baserow_from_lb(cases_data, lb_data) -> List[BaserowUpdate]:
    all_updates = []
    for bs_id, bs_entry in cases_data.items():
        updates = BaserowUpdate(bs_id, bs_entry)

        bs_lbid = bs_entry[ID_FIELD]
        bs_probendate = bs_entry["Datum Labor"]
        bs_count = bs_entry[COUNT_FIELD]
        # bs_count_int = analysezahl_to_int(bs_count)

        if bs_lbid:
            lb_samples = match_sample_by_sampleid(lb_data, bs_lbid)
        else:
            lb_samples = match_sample_by_name(lb_data, bs_entry["Firstname"], bs_entry["Lastname"], bs_entry["Birthdate"])

        if not lb_samples:
            # if not bs_lbid:
            #     logger.info(f"No LB PEL Entry matched for {bs_id} {bs_entry['Firstname']} {bs_entry['Lastname']} {bs_entry['Birthdate']} LBID({bs_lbid})")
            continue
        if len(lb_samples) > 1:
            logger.warning(f"{len(lb_samples)} PEL entries matched for {bs_id} {bs_entry['Firstname']} {bs_entry['Lastname']} {bs_entry['Birthdate']} LBID({bs_lbid})")
        lb_entry = lb_samples[-1]
        lb_index = lb_entry[INDEX_FIELD]
        lb_fam_entries = match_sample_by_indexid(lb_data, lb_index)

        lb_lbid = lb_entry[ID_FIELD]
        lb_probendate = lb_entry["Datum Labor"]

        if not bs_lbid and lb_lbid:
            updates.add_update(ID_FIELD, lb_lbid)
        if not bs_probendate and lb_probendate:
            if m := re.search(r"\d{2}.\d{2}.\d{4}", lb_entry["Datum Labor"]):
                lb_probendate_fmt = datetime.datetime.strptime(m.group(0), "%d.%m.%Y").date().isoformat()
                updates.add_update("Datum Labor", lb_probendate_fmt)

        # only autoupdate trio for now
        if not bs_count and lb_fam_entries == 3:
            updates.add_update("Analysezahl", "Trio")

        if updates.has_updates:
            all_updates.append(updates)

    return all_updates


SODAR_FIELD_FAM = "Characteristics[Family]"
SODAR_FIELD_ID = "Sample Name"
SODAR_FIELD_BATCH = "Characteristics[Batch]"

VARFISH_FIELD_STATUS = "status"
VARFISH_FIELD_UUID = "sodar_uuid"

BASEROW_FIELD_BATCH = "Batch"
BASEROW_FIELD_STATUS = "Case Status"
BASEROW_FIELD_VARFISH = "Varfish"
BASEROW_FIELD_HPO = "HPO Terms"

SODAR_UPDATE_MAPPINGS = [
    Mapping(
        source=SODAR_FIELD_BATCH,
        dest=BASEROW_FIELD_BATCH,
    ),
]

def apply_mapping(mappings: List[Mapping], update: BaserowUpdate, other_entry: dict) -> BaserowUpdate:
    for mapping in mappings:
        if kv := mapping.apply(other_entry, update.entry):
            update.add_update(*kv)
    return update

def create_update(mappings, entry_id, entry, sodar_entry):
    update = BaserowUpdate(entry_id, entry)

    for mapping in mappings:
        if kv := mapping.apply(sodar_entry, entry):
            update.add_update(*kv)
    if update.has_updates:
        return update


def update_baserow_from_data(baserow_data, other_data, getter, mappings):
    all_updates = []
    for bs_id, bs_entry in baserow_data.items():
        if matches := getter(bs_id, bs_entry, other_data):
            if len(matches) > 1:
                logger.warning(f"SV-{bs_id} matched in multiple entries, using first")
            match = matches[0]
            if update := create_update(mappings, bs_id, bs_entry, match):
                all_updates.append(update)

    return all_updates


def update_baserow_from_sodar(baserow_data, sodar_data):
    def match_entry_sodar(_, entry, sodar_data):
        lbid = entry["LB ID"]
        matches = []
        for project in sodar_data:
            # proj_name = project["name"]
            cases = project["data"]
            for case in cases:
                # sodar_fam = case[SODAR_FIELD_FAM]
                sodar_id = case[SODAR_FIELD_ID]
                if matchLbId(lbid, sodar_id):
                    matches.append(case)
        return matches

    return update_baserow_from_data(baserow_data, sodar_data, match_entry_sodar, SODAR_UPDATE_MAPPINGS)


VARFISH_UPDATE_MAPPINGS = [
    Mapping(
        VARFISH_FIELD_STATUS,
        BASEROW_FIELD_STATUS,
        when=Condition.CUSTOM,
        transform=lambda d: VARFISH_STATUS_TO_BASEROW[d],
        cmp=status_newer,
    ),
    Mapping(
        VARFISH_FIELD_UUID,
        BASEROW_FIELD_VARFISH,
        transform=lambda d: str(d),
    )
]


def update_baserow_from_varfish(baserow_data, varfish_data):
    def match_entry_varfish(_, entry, varfish_data):
        lbid = entry["LB ID"]
        matches = []
        for project in varfish_data:
            cases = project["cases"]
            for case in cases:
                for member in case.pedigree:
                    if matchLbId(lbid, member.name):
                        matches.append(case)
                        break
        return matches

    return update_baserow_from_data(baserow_data, varfish_data, match_entry_varfish, VARFISH_UPDATE_MAPPINGS)


def update_baserow_from_sams(baserow_data, sams_data):
    def get_sams_case(entry_id, entry, sams_data):
        sv_id = f"SV-{entry_id}"
        matches = []
        for entry in sams_data:
            if sv_id == entry["subject"]["id"]:
                matches.append(entry)
        return matches

    mappings = [
        Mapping(
            "",
            BASEROW_FIELD_HPO,
            transform=phenopacket_to_varfish_format,
        )
    ]

    return update_baserow_from_data(baserow_data, sams_data, get_sams_case, mappings)


BASEROW_ACMG_CLASSIFICATION = {
    1: "Benign (I)",
    2: "Likely Benign (II)",
    3: "Uncertain Significance (III)",
    4: "Likely Pathogenic (IV)",
    5: "Pathogenic (V)",
}


def varfish_to_acmg(varfish_entry):
    acmg_auto = varfish_entry["acmg_class_auto"]
    acmg_class = varfish_entry["acmg_class_override"]
    if acmg_class is None and acmg_auto != 3:
        acmg_class = acmg_auto
    return BASEROW_ACMG_CLASSIFICATION.get(acmg_class, "")


def varfish_to_zygosity(varfish_entry):
    genotype_entry = varfish_entry["genotype"]
    max_geno = max([sum(int(d) for d in re.findall(r"\d+", g['gt']) if d and d != ".") for g in genotype_entry.values()])
    max_count = max([len(re.findall(r"\d+", g['gt'])) for g in genotype_entry.values()])
    max_ab = max([float(g['ad']) / float(g['dp']) for g in genotype_entry.values() if g['dp']])
    if max_count == 1 and max_geno == 1:
        return "Hemizygous"
    if varfish_entry["chromosome"] == "MT":
        if max_ab > 0.9:
            return "Homoplasmic"
        else:
            return "Heteroplasmic"
    else:
        if max_ab < 0.2:
            return "Mosaik"

    assert max_count == 2
    return {
        2: "Homozygous",
        1: "Heterozygous",
    }[max_geno]


def varfish_to_resulttype(varfish_entry):
    if "zufallsbefund" in varfish_entry["comment_text"]:
        return "Incidental"
    if varfish_entry["flag_candidate"] and varfish_entry["acmg_class_override"] == 3:
        return "Candidate"
    if varfish_entry["flag_candidate"] and not varfish_entry["flag_final_causative"]:
        return "Research"
    return "Main"


VARFISH_SV_UPDATE_MAPPINGS = [
]

def to_position_key(e):
    return f"{e['release']}_{e['chromosome']}-{e['start']}-{e['reference']}-{e['alternative']}"


DISEASE_PREFIXES = ("OMIM", "ORPHA", "MONDO")
PHENO_PREFIXES = ("HP",)

def get_terms_prefix(terms, prefix):
    return [t for t in terms if any(t.startswith(p) for p in prefix)]


def select_terms(prefixes, mask_incidental=False):
    def inner(e):
        all_variant_terms = e["variant_terms"]
        all_case_terms = e["case_terms"]
        variant_terms = get_terms_prefix(all_variant_terms, prefixes)
        case_terms = get_terms_prefix(all_case_terms, prefixes)
        if mask_incidental and varfish_to_resulttype(e) == "Incidental":
            case_terms = []
        selected_terms = variant_terms or case_terms
        return ";".join(selected_terms)
    return inner


VARFISH_SMALL_VARIANT_UPDATE_MAPPINGS = [
    Mapping(
        "gene_symbol",
        "Genename",
    ),
    Mapping(
        "transcript_id",
        "NM Transcript"
    ),
    Mapping(
        "",
        "Mutation",
        transform=lambda d: f"{d['hgvs_c']} {d['hgvs_p']}",
    ),
    Mapping(
        "",
        "ACMG Classification",
        transform=varfish_to_acmg
    ),
    Mapping(
        "",
        "Zygosity",
        transform=varfish_to_zygosity,
    ),
    Mapping(
        "",
        "ResultType",
        transform=varfish_to_resulttype,
    ),
    Mapping(
        "inheritance",
        "Inheritance",
    ),
    Mapping(
        "inheritance_status",
        "de novo/vererbt",
    ),
    Mapping(
        "",
        "Position (VCF)",
        transform=to_position_key,
    ),
    Mapping(
        "",
        "OMIM",
        transform=select_terms(DISEASE_PREFIXES),
        when=Condition.DIFFERENT,
    ),
    Mapping(
        "",
        "HPO Terms",
        transform=select_terms(PHENO_PREFIXES, mask_incidental=True),
    ),
    Mapping(
        "acmg_eval_date",
        "EvaluationDate",
    )
]


def fuzzy_match_hgvs(left_h, right_h):
    right_h = right_h.replace(" ", "")
    for d in ("del", "dup", "ins", "inv", "fs", "ext"):
        if d in left_h:
            return left_h.split(d)[0] in right_h
    return left_h in right_h


def update_baserow_from_varfish_variants(all_cases, findings) -> List[BaserowUpdate]:
    def case_applicable(case) -> bool:
        included_status = ["Solved", "VUS"]
        return case["Varfish"] and case["Varfish=Befund"] != "ClinVar Uploaded" and case["Case Status"] in included_status

    def match_finding_id(finding_rows, varfish_variant):
        """Match varfish variant to finding rows.
        """
        if "sv_type" in varfish_variant:
            logger.warning("SV not supported yet.")
            return None
        else:
            gene_symbol = varfish_variant["gene_symbol"]
            hgvs_c = varfish_variant["hgvs_c"]
            vcf_position_key = to_position_key(varfish_variant)

            for fid, finding in finding_rows.items():
                # position key is transferred and thus most correct
                if finding.get("Position (VCF)") == vcf_position_key:
                    return fid

                if gene_symbol and hgvs_c:
                    if gene_symbol == finding["Genename"] and hgvs_c in finding["Mutation"]:
                        return fid

                for mehari_tx in varfish_variant["mehari"]["result"]:
                    gene_symbol = mehari_tx["gene-symbol"]
                    hgvs_c = mehari_tx["hgvs-t"]
                    if gene_symbol and hgvs_c:
                        if gene_symbol == finding["Genename"] and fuzzy_match_hgvs(hgvs_c, finding["Mutation"]):
                            varfish_variant["gene_symbol"] = gene_symbol
                            varfish_variant["hgvs_c"] = hgvs_c
                            varfish_variant["hgvs_p"] = mehari_tx["hgvs-p"]
                            varfish_variant["transcript_id"] = mehari_tx["feature-id"]
                            return fid
                if myvariant_results := varfish_variant.get("myvariant"):
                    if snpeff_results := myvariant_results.get("snpeff"):
                        if type(snpeff_results["ann"]) is dict:
                            snpeff_results["ann"] = [snpeff_results["ann"]]
                        for ann in snpeff_results["ann"]:
                            gene_symbol = ann.get("genename")
                            hgvs_c = ann.get("hgvs_c")
                            hgvs_p = ann.get("hgvs_p")
                            transcript_id = ann.get("feature_id")
                            if hgvs_c and transcript_id and gene_symbol == finding["Genename"] and fuzzy_match_hgvs(hgvs_c, finding["Mutation"]):
                                varfish_variant["gene_symbol"] = gene_symbol
                                varfish_variant["hgvs_c"] = hgvs_c
                                varfish_variant["hgvs_p"] = hgvs_p
                                varfish_variant["transcript_id"] = transcript_id
                                return fid

    def create_update_variant(cid, fid, case_finding, varfish_variant):
        if "sv_type" in varfish_variant:
            mapping = VARFISH_SV_UPDATE_MAPPINGS
        else:
            mapping = VARFISH_SMALL_VARIANT_UPDATE_MAPPINGS
        update = create_update(mapping, fid, case_finding, varfish_variant)
        if update:
            case_links = case_finding.get("Cases", [])
            update.add_update("Cases", list(set(case_links + [cid])))
        return update

    def match_all_variants(current_findings, varfish_variants):
        # all flagged varfish variants should exist in current findings, but
        # not the other way around
        available_findings = [fid for fid in current_findings]
        matched_variants = {}
        unmatched_variants = []
        for varfish_variant in varfish_variants:
            matched_id = match_finding_id({fid: current_findings[fid] for fid in available_findings}, varfish_variant)
            if matched_id:
                available_findings.remove(matched_id)
                matched_variants[matched_id] = varfish_variant
            else:
                unmatched_variants.append(varfish_variant)
        return matched_variants, unmatched_variants

    included_cases = {cid: case for cid, case in all_cases.items() if case_applicable(case)}

    varfish_ids = [case["Varfish"] for case in included_cases.values() if case["Varfish"]]
    all_varfish_variants = get_findings(varfish_ids)

    all_updates = []
    for cid, case in included_cases.items():
        case_findings = {fid: findings[fid] for fid in case["Findings"]}

        mapped_case_variants, unmapped = match_all_variants(case_findings, all_varfish_variants[case["Varfish"]])

        for fid, varfish_variant in mapped_case_variants.items():
            case_finding = case_findings[fid]
            if update := create_update_variant(cid, fid, case_finding, varfish_variant):
                all_updates.append(update)

        for varfish_variant in unmapped:
            if update := create_update_variant(cid, None, {}, varfish_variant):
                if cid == 149:
                    from pprint import pprint
                    pprint(varfish_variant)
                    print(update)
                all_updates.append(update)

    return all_updates


def get_clinvar_upload_state(entry):
    current_state = entry["Varfish=Befund"]
    findings = entry["Findings"]

    BLOCKING_STATES = [
        "Forschungsgen. Bitte noch kein ClinVar Upload",
        "komplexe SV",
    ]

    if current_state in BLOCKING_STATES:
        return current_state

    def main_findings_uploadable(findings):
        has_main = any(f["ResultType"] == "Main" for f in findings)
        has_incidental = any(f["ResultType"] == "Incidental" for f in findings)
        all_main_filled = True
        all_incidental_filled = True
        for finding in findings:
            if finding["ResultType"] == "Main" and not all(finding[f] for f in ("Inheritance", "ACMG Classification", "HPO Terms", "OMIM")):
                all_main_filled = False
            elif finding["ResultType"] == "Incidental" and not all(finding[f] for f in ("Inheritance", "ACMG Classification", "OMIM")):
                all_incidental_filled = False
        return has_main and all_main_filled and (not has_incidental or (has_incidental and all_incidental_filled))


    new_state = current_state
    if any("SCV" in f["Clinvar-ID"] for f in findings if f["Clinvar-ID"]):
        new_state = VALI_FIN
    elif "ClinVar" in entry["AutoValidation"]:
        new_state = VALI_FAIL
    elif main_findings_uploadable(findings):
        new_state = VALI_OK
    return new_state


def get_contract_control_state(entry):
    """This state is used by clerical personnel for controlling the completeness of our documentation.
    """
    MISSING_PARTICIPATION = "fehlt"
    NO_BILLING = "keine Abrechnung"

    NON_BILLED_CONTRACT_TYPES = [
        "Keiner",
        "Station",
        "Privat",
        "Forschung",
        "Labor Berlin Befund nach KÜ",
    ]

    BILLED_CONTRACT_TYPES = [
        "Selektivvertrag",
        "Beratung"
    ]

    new_state = entry["Kati: Teilnahmeerklärung"]
    if entry["Vertrag"] in NON_BILLED_CONTRACT_TYPES:
        new_state = NO_BILLING
    elif entry["Vertrag"] in BILLED_CONTRACT_TYPES:
        match (entry["Vertrag"], entry["LB ID"], entry["Falltyp"]):
            case "Selektivvertrag", lb_id, _ if not lb_id:
                new_state = MISSING_PARTICIPATION
            case "Beratung", _, _:
                new_state = MISSING_PARTICIPATION
            case _, _, "Re-Analyse":
                new_state = MISSING_PARTICIPATION

    return new_state


def get_billing_clearance(entry):
    """Once billing is allowed, the state will be set to yes.
    """
    CLEARED = "Ja"

    if entry["Batch"] and entry["Vertrag"] == "Selektivvertrag":
        return CLEARED

    if entry["Falltyp"] == "Beratung" and entry["FK1"]:
        return CLEARED

    if entry["Falltyp"] == "Re-Analyse Exom" and entry["FK2"]:
        return CLEARED

    if entry["Falltyp"] == "Re-Analyse Genom" and entry["FK2"]:
        return CLEARED

    return ""


STATUS_UPDATE_MAPPINGS = [
    Mapping(
        "",
        "Varfish=Befund",
        transform=get_clinvar_upload_state,
        when=Condition.DIFFERENT,
    ),
    Mapping(
        "",
        "Kati: Teilnahmeerklärung",
        transform=get_contract_control_state,
        when=Condition.NONE,
    ),
    Mapping(
        "",
        "Abrechnung freigegeben",
        transform=get_billing_clearance,
        when=Condition.NONE,
    ),
]


def update_entry_status(case_entries: dict, full_data: dict) -> List[BaserowUpdate]:
    status_updates = []
    for entry_id, entry_data in case_entries.items():
        update = BaserowUpdate(entry_id, entry_data)
        apply_mapping(STATUS_UPDATE_MAPPINGS, update, full_data[update.id])
        if update.has_updates:
            status_updates.append(update)
    return status_updates
