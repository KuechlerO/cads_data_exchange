import re
import itertools
from collections import defaultdict
from typing import List, Dict
from loguru import logger
import requests
from lxml import html

from attrs import define, field

from varfish_cli import api

from data_exchange.myvariant import MyVariantAPI

from .config import settings
# from requests_cache import CachedSession

VARFISH_DEFAULT_LOCATION = "GRCh37"

HPO_AD = "HP:0000006"
HPO_AR = "HP:0000007"
HPO_SEMIDOMINANT = "HP:0032113"
HPO_XL = "HP:0001417"
HPO_XLD = "HP:0001423"
HPO_XLR = "HP:0001419"
HPO_YL = "HP:0001450"
HPO_PSEUDOAUTOSOMAL = "HP:0034339"
HPO_PSEUDOAUTOSOMAL_DOM = "HP:0034340"
HPO_PSEUDOAUTOSOMAL_REZ = "HP:0034341"
HPO_MT = "HP:0001427"

HPO_DE_NOVO = "HP:0025352"

HPO_INHERITANCE_MAPPING = {
    HPO_AD: "Autosomal dominant inheritance",
    HPO_AR: "Autosomal recessive inheritance",
    HPO_SEMIDOMINANT: "Semidominant inheritance",
    HPO_XL: "X-linked inheritance",
    HPO_XLD: "X-linked dominant inheritance",
    HPO_XLR: "X-linked recessive inheritance",
    HPO_YL: "Y-linked inheritance",
    HPO_PSEUDOAUTOSOMAL: "Other",
    HPO_PSEUDOAUTOSOMAL_DOM: "Other",
    HPO_PSEUDOAUTOSOMAL_REZ: "Other",
    HPO_MT: "Mitochondrial inheritance",
}

VARFISH_STATUS_TO_BASEROW = {
    "closed-solved": "Solved",
    "closed-uncertain": "VUS",
    "closed-unsolved": "Unsolved",
    "active": "Active",
    "initial": "Varfish Initial",
}


def create_key(request, **kwargs) -> str:
    return request.method + request.url


def create_session():
    return requests.Session()
    # return CachedSession(allowable_methods=["GET", "HEAD", "POST"], filter_fn=lambda r: True, key_fn=create_key)


def get_inheritance_status(genotypes, pedigree):
    def parse_gt(gt):
        geno_nums = re.findall(r"\d+", gt)
        geno_alt_count = sum(map(int, geno_nums))
        geno_alt_num = len(geno_nums)
        return geno_alt_count, geno_alt_num

    index_name = None
    index_sum = 0
    index_parent_count = 0
    for name, gt_info in genotypes.items():
        cur_sum, _ = parse_gt(gt_info["gt"])
        ped_info = [p for p in pedigree if p["name"] == name]
        ped_parent_count = ped_info[0]["mother"] != "0" + ped_info[0]["father"] != "0"

        if index_name is None or cur_sum > index_sum or (cur_sum == index_sum and ped_parent_count > index_parent_count):
            index_name = name
            index_sum = cur_sum
            index_parent_count = ped_parent_count

    assert index_name is not None, "Could not find index sample"

    gt_info = genotypes[index_name]
    index_sum, index_num = parse_gt(gt_info["gt"])

    ped_info = [p for p in pedigree if p["name"] == index_name]
    maternal_inherited = None
    if ped_info and (mother_name := ped_info[0]["mother"]) != "0":
        mother_count, mother_num = parse_gt(genotypes[mother_name]["gt"])
        if mother_num > 0 and mother_count == 0:
            maternal_inherited = False
        elif mother_count > 0:
            maternal_inherited = True

    paternal_inherited = None
    if ped_info and (father_name := ped_info[0]["father"]) != "0":
        father_count, father_num = parse_gt(genotypes[father_name]["gt"])
        if father_num > 0 and father_count == 0:
            paternal_inherited = False
        elif father_count > 0:
            paternal_inherited = True

    if paternal_inherited and maternal_inherited and index_sum == 2:
        return "beide Kopien vererbt"
    if paternal_inherited:
        return "paternal vererbt"
    if maternal_inherited:
        return "maternal vererbt"
    if paternal_inherited is False and maternal_inherited is False:
        return "de novo"

    return ""

def get_terms_from_text(text):
    terms = []
    for match in re.finditer(r"((HPO|HP|OMIM|ORPHA|MONDO):?\d+)", text):
        terms.append(match.group(1))
    return terms


def get_case_custom_text(case_data, case_comment_data):
    case_notes = case_data["notes"]
    case_comments = "\n".join([c['comment'] for c in case_comment_data])
    return case_notes + "\n" + case_comments


def get_case_phenotype_info(case_data, case_comment_data):
    case_comments = get_case_custom_text(case_data, case_comment_data)
    phenotype_terms = case_data["phenotype_terms"]
    individual_to_terms = {
        p["individual"]: p["terms"] for p in phenotype_terms
    }
    individual_to_terms[""] = get_terms_from_text(case_comments)
    all_person_terms = list(itertools.chain.from_iterable(v for k, v in individual_to_terms.items() if k))

    return individual_to_terms, all_person_terms


def hpos_to_inheritance(terms):
    terms = list(set(terms))
    inheritances = []
    for term in terms:
        if mapped := HPO_INHERITANCE_MAPPING.get(term):
            inheritances.append(mapped)
    if len(inheritances) > 1:
        logger.warning(f"Multiple inheritances found: {inheritances}")
    if inheritances:
        return inheritances[0]


@define
class Varfish:
    hostname: str
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        'authority': 'varfish.bihealth.org',
        'referer': 'https://varfish.bihealth.org/login/',
        'origin': 'https://varfish.bihealth.org',
    }
    login_url = "/login/"
    session: requests.Session = field(factory=create_session)

    case_info_url = "/variants/api/case/retrieve/{varfish_uuid}/"
    case_comment_url = "/cases/api/case-comment/list-create/{varfish_uuid}"
    smallvar_annos_url = "/variants/ajax/smallvariant/user-annotated-case/{varfish_uuid}"

    smallvar_flags_url = "/variants/api/small-variant-flags/list-create/{varfish_uuid}"
    smallvar_comment_url = "/variants/api/small-variant-comment/list-create/{varfish_uuid}/"
    smallvar_acmg_url = "/variants/api/acmg-criteria-rating/list-create/{varfish_uuid}/"
    smallvar_row_url = "/variants/api/query-result-row/retrieve/{smallvar_uuid}"

    mehari_url = "/proxy/varfish/mehari/seqvars/csq"

    svs_flags_url = "/svs/ajax/structural-variant-flags/list-create/{varfish_uuid}"
    svs_comment_url = "/svs/ajax/structural-variant-comment/list-create/{varfish_uuid}"
    svs_details_url = "/svs/sv-query-result-row/retrieve/{sv_uuid}"

    def url(self, urlstr: str) -> str:
        return self.hostname + urlstr

    def login(self, username, password):
        self.session.headers = self.headers  # type: ignore
        resp = self.session.get(self.url(self.login_url))
        root = html.fromstring(resp.text)
        middleware_element = root.xpath("//input[@name=\"csrfmiddlewaretoken\"]")
        csrfmiddlewaretoken = ""
        if middleware_element:
            csrfmiddlewaretoken = middleware_element[0].value
        data = {
            'csrfmiddlewaretoken': csrfmiddlewaretoken,
            'username': username,
            'password': password,
        }
        resp = self.session.post(self.url(self.login_url), data=data)
        resp.raise_for_status()
        # assert "sessionid" in self.session.cookies, "Not logged in properly"

    def _get(self, url, varfish_uuid=None, smallvar_uuid=None, sv_uuid=None):
        fmt_dict = {
            "varfish_uuid": varfish_uuid,
            "smallvar_uuid": smallvar_uuid,
            "sv_uuid": sv_uuid,
        }
        result_url = self.url(url).format_map(fmt_dict)
        resp = self.session.get(result_url)
        resp.raise_for_status()
        data = resp.json()
        return data


    def get_variant_mehari(self, chrom, pos, ref, alt, hgnc) -> dict:
        """Get Mehari results for variant."""
        if not hgnc:
            hgnc = None
        params = {
            "genome_release": VARFISH_DEFAULT_LOCATION.lower(),
            "chromosome": chrom,
            "position": pos,
            "reference": ref,
            "alternative": alt,
            "hgnc-id": hgnc
        }
        resp = self.session.get(self.url(self.mehari_url), params=params)
        resp.raise_for_status()
        return resp.json()

    def get_final_variants(self, varfish_uuid: str):
        case_data = self._get(self.case_info_url, varfish_uuid=varfish_uuid)
        case_comment_data = self._get(self.case_comment_url, varfish_uuid=varfish_uuid)
        individual_to_terms, all_person_terms = get_case_phenotype_info(case_data, case_comment_data)

        variant_data_resp = self._get(self.smallvar_annos_url, varfish_uuid=varfish_uuid)
        def to_pos(entry):
            return (entry["chromosome"], entry["start"], entry["reference"], entry["alternative"])

        final_variants_by_pos = {
            to_pos(entry): entry for entry in variant_data_resp["rows"] if entry["flag_final_causative"]
        }

        variant_comments_by_pos = defaultdict(list)
        for entry in self._get(self.smallvar_comment_url, varfish_uuid=varfish_uuid):
            variant_comments_by_pos[to_pos(entry)].append(entry["text"])

        variant_acmg_by_pos = defaultdict(list)
        for entry in self._get(self.smallvar_acmg_url, varfish_uuid=varfish_uuid).get("results", []):
            variant_acmg_by_pos[to_pos(entry)].append(entry)

        final_variants = []
        for index, variant in final_variants_by_pos.items():
            variant_terms = []
            variant["comment_text"] = ""
            if comment_lines := variant_comments_by_pos.get(index):
                variant["comment_text"] = "\n".join(comment_lines)
                variant_terms = get_terms_from_text(variant["comment_text"])

            variant["acmg_eval_date"] = ""
            if acmg_info := variant_acmg_by_pos.get(index):
                variant["acmg_eval_date"] = acmg_info[0]["date_modified"].split("T")[0]

            if variant_terms:
                valid_terms = variant_terms
            else:
                valid_terms = all_person_terms

            # remove duplicates
            valid_terms = list(set(valid_terms))

            variant["variant_terms"] = variant_terms
            variant["case_terms"] = all_person_terms
            variant["inheritance"] = hpos_to_inheritance(valid_terms)
            variant["inheritance_status"] = get_inheritance_status(variant["genotype"], case_data["pedigree"])
            variant["mehari"] = self.get_variant_mehari(variant["chromosome"], variant["start"], variant["reference"], variant["alternative"], variant["hgnc_id"])

            if not variant["mehari"]["result"]:
                # variant["myvariant"] = MyVariantAPI().get_annotations(variant["chromosome"], variant["start"], variant["reference"], variant["alternative"])
                raise RuntimeError("Mehari has no annotation for variant")

            if not variant["gene_symbol"]:
                logger.warning("Variant with empty gene_symbol, try to fill from additional annotation sources")

                gene_symbol = None
                hgvs_c = None
                hgvs_p = None
                transcript_id = None
                found_tx = False
                if mehari_results := variant["mehari"]["result"]:
                    for mehari_tx in mehari_results:
                        gene_symbol = mehari_tx["gene_symbol"]
                        hgvs_c = mehari_tx["hgvs_t"]
                        hgvs_p = mehari_tx["hgvs_p"]
                        transcript_id = mehari_tx["feature_id"]
                        if hgvs_c and transcript_id and hgvs_p and gene_symbol:
                            found_tx = True
                            break
                # if not found_tx and (myvariant_results := variant.get("myvariant")):
                #     if snpeff_results := myvariant_results.get("snpeff"):
                #         if type(snpeff_results["ann"]) is dict:
                #             snpeff_results["ann"] = [snpeff_results["ann"]]
                #         for ann in snpeff_results["ann"]:
                #             gene_symbol = ann.get("genename")
                #             hgvs_c = ann.get("hgvs_c")
                #             hgvs_p = ann.get("hgvs_p")
                #             transcript_id = ann.get("feature_id")
                #             if gene_symbol and hgvs_c and hgvs_p and transcript_id:
                #                 found_tx = True
                #                 break

                if found_tx and (gene_symbol or hgvs_c or hgvs_p or transcript_id):
                    variant["gene_symbol"] = gene_symbol
                    variant["hgvs_c"] = hgvs_c
                    variant["hgvs_p"] = hgvs_p
                    variant["transcript_id"] = transcript_id

            final_variants.append(variant)

        def to_pos_sv(entry):
            return (entry["chromosome"], entry["start"], entry["end"], entry["sv_sub_type"])

        sv_pos_to_comment = {
            to_pos_sv(entry): "\n".join([entry["text"]])
            for entry in
            self._get(self.svs_comment_url, varfish_uuid=varfish_uuid)
        }

        final_sv_variants = [
            {"comment": sv_pos_to_comment.get(to_pos_sv(entry), ""), **entry} for entry in
            self._get(self.svs_flags_url, varfish_uuid=varfish_uuid)
            if entry["flag_final_causative"]
        ]

        resp = self.session.get(self.url(self.svs_flags_url).format(varfish_uuid=varfish_uuid))
        resp.raise_for_status()
        final_sv_variants = [
            entry for entry in resp.json() if entry["flag_final_causative"]
        ]

        return final_variants + final_sv_variants


def format_sv(sv_data):
    fmt_str = f"{sv_data['chromosome']}:{sv_data['start']}-{sv_data['end']}{sv_data['sv_type']}"
    return fmt_str


def format_smallvar(data):
    acmg_class = max(data['acmg_class_auto'] or 0, data['acmg_class_override'] or 0)
    fmt_str = f"{data['symbol']}({data['transcript_id']}):{data['hgvs_c']} {data['hgvs_p']} ({'/'.join(data['effect'])} ACMG {acmg_class})"
    return fmt_str


def format_var(data):
    if "sv_type" in data:
        return format_sv(data)
    return format_smallvar(data)


def get_data():
    all_data = []

    for project in settings.sodar.project:
        cases = api.case_list(settings.varfish.url, settings.varfish_token, project.id)

        all_data.append({
            "uuid": project.id,
            "cases": cases,
        })
    return all_data


def get_findings(varfish_uuids: List[str]) -> Dict[str, List[dict]]:
    v = Varfish(settings.varfish.url)
    v.login(settings.varfish_user, settings.varfish_password)
    results = {}
    for uuid in varfish_uuids:
        results[uuid] = v.get_final_variants(uuid)
    return results
