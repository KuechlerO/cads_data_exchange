from collections import defaultdict
from enum import Enum
from typing import Any, Callable, List
from attrs import define, field

from loguru import logger

from data_exchange.baserow import COLUMN_CLINVAR_REASON, REASON_AUTOVALIDATION, VALI_BLOCKED, BaserowUpdate, expand_updates, VALI_OK, COLUMN_CLINVAR_STATUS


class PersonRole(str, Enum):
    FIRST_LOOK = "First Look"
    SECOND_LOOK = "Second Look"
    CLINICIAN = "Clinician"
    VALIDATOR = "Validator"


@define
class ValidationRule:
    name: str
    message: str
    responsible: PersonRole
    applies: Callable
    checks: Callable
    table_view_id: int


@define
class ValidationError:
    source: ValidationRule
    entry: dict
    field: str
    found: Any
    expected: Any
    comment: str

    @property
    def entry_id(self):
        return self.entry['id']

    @property
    def responsible(self):
        linked_persons = self.entry[self.source.responsible.value]
        return linked_persons

    def __repr__(self):
        return f"SV-{self.entry_id} {self.entry['LB ID']} {self.entry['Lastname']} *{self.entry['Birthdate']}|{self.source.name} {self.field} {self.comment} (found: {self.found} expected: {self.expected})"



def applies_to_external(entry):
    return entry["Vertrag"] in ("Labor Berlin Befund",)


def applies_to_sequenced_non_transferred(entry):
    if applies_to_external(entry):
        return False
    return entry["Datum Labor"] and (not entry["Batch"] or not entry["Varfish"])


def applies_to_varfish_transferred_not_distributed(entry):
    if applies_to_external(entry):
        return False
    return entry["Varfish"] and entry["Case Status"] not in ("Solved", "Unsolved", "VUS") and not entry["First Look"]


def applies_to_varfish_transferred_not_finalized_fl(entry):
    if applies_to_external(entry):
        return False
    return entry["Varfish"] and entry["Case Status"] not in ("Solved", "Unsolved", "VUS") and entry["First Look"]


def applies_to_finalized_cases(entry):
    if applies_to_external(entry):
        return False
    return entry["Case Status"] in ("Solved", "Unsolved", "VUS")


def applies_to_clinvar_cases(entry):
    if applies_to_external(entry):
        return False
    has_result = entry["Case Status"] in ("Solved", "VUS")
    has_varfish = bool(entry["Varfish"])
    clinvar_not_blocked = entry[COLUMN_CLINVAR_STATUS] != VALI_BLOCKED
    return has_result and has_varfish and clinvar_not_blocked


def check_required(rule, entry, required_fields) -> List[ValidationError]:
    errors = []
    for field_name in required_fields:
        if not entry[field_name]:
            errors.append(
                ValidationError(rule, entry, field_name, entry[field_name], "", "muss ausgefüllt werden")
            )
    return errors


def check_field_not_in(rule, entry, field_name, field_values) -> List[ValidationError]:
    errors = []
    if entry[field_name] not in field_values:
        errors.append(
            ValidationError(rule, entry, field_name, entry[field_name], "", f"entspricht nicht den Werten {', '.join(field_values)}")
        )
    return errors


def check_sending(rule, entry) -> List[ValidationError]:
    required_fields = [
        "Clinician",
        "Lastname",
        "Firstname",
        "Birthdate",
        "Gender",
        "Vertrag",
        "FK1",
        "Analysezahl",
        "Falltyp",
    ]
    errors = []
    errors += check_required(rule, entry, required_fields)
    return errors


def check_fill_before_sign(rule, entry) -> List[ValidationError]:
    required_fields = [
        "FK2",
        "Datum Befund",
        COLUMN_CLINVAR_STATUS
    ]
    errors = []
    errors += check_required(rule, entry, required_fields)
    if entry["Case Status"] in ("Solved", "VUS") and not entry["Findings"]:
        errors.append(
            ValidationError(rule, entry, "Findings", "", "in Varfish ausfüllen", " müssen in Varfish ausgefüllt werden")
        )
    for finding in entry["Findings"]:
        if finding["ResultType"] == "Incidental" and not entry["Zufallsbefunde"] and finding["Berichtet"] != "Nein":
            errors.append(
                ValidationError(rule, entry, f"Findings/{finding['id']}", "", "", " ist Zufallsbefund, jedoch keine Einwilligung vorliegend. Entweder berichtet nein oder Einwilligung einholen.")
            )

    if (not entry["EV kontrolliert"]) and entry["Falltyp"] == "Genom":
        errors.append(
            ValidationError(rule, entry, f"EV kontrolliert", "", "", "EV liegt nicht vor"),
        )

    if entry["Kati: Teilnahmeerklärung versendet"] == "fehlt":
        errors.append(
            ValidationError(rule, entry, f"Kati: Teilnahmeerklärung versendet", "", "", "Teilnahmeerklärung Selektivvertrag fehlt. Diese muss in die Ablage."),
        )
    return errors


def check_clinvar(rule, entry) -> List[ValidationError]:
    case_status = entry[COLUMN_CLINVAR_STATUS]
    findings = entry["Findings"]

    entry_confirmed = case_status == VALI_OK
    errors = []

    for finding in findings:
        if finding.get("ResultType") not in ("Main", "Incidental"):
            continue
        if not (finding.get("OMIM") or finding.get("PMIDs")) and entry_confirmed:
            errors.append(ValidationError(
                rule, entry,
                f"Findings/{finding['id']}", "", "", " OMIM fehlt"
            ))
        if not finding.get("HPO Terms") and finding.get("ResultType") == "Main":
            errors.append(ValidationError(
                rule, entry,
                f"Findings/{finding['id']}", "", "", " HPO fehlt"
            ))
        if not finding.get("ACMG Classification"):
            errors.append(ValidationError(
                rule, entry,
                f"Findings/{finding['id']}", "", "", " ACMG fehlt. Expliziter Override ist am besten."
            ))
        if not finding.get("Inheritance"):
            errors.append(ValidationError(
                rule, entry,
                f"Findings/{finding['id']}", "", "", " Erbgang der Krankheit fehlt. Explizit Erbgang als HPO Term angeben."
            ))

    return errors


RULES = [
    ValidationRule(
        name="Ausfüllen vor Probenversand",
        responsible=PersonRole.CLINICIAN,
        message="Probe wurde bei LaborBerlin im Probeneingang registiert, jedoch noch nicht fertig ausgewertet.",
        applies=applies_to_sequenced_non_transferred,
        checks=check_sending,
        table_view_id=2763,
    ),
    ValidationRule(
        name="Ausfüllen vor Unterschrift",
        responsible=PersonRole.FIRST_LOOK,
        message="Befund wurde erstellt, jedoch fehlen noch Informationen",
        applies=applies_to_finalized_cases,
        checks=check_fill_before_sign,
        table_view_id=2764,
    ),
    ValidationRule(
        name="First Look bestimmen",
        responsible=PersonRole.CLINICIAN,
        message="Fall in Varfish, jedoch kein First Look eingetragen",
        applies=applies_to_varfish_transferred_not_distributed,
        checks=lambda r, e: check_required(r, e, ["First Look"]),
        table_view_id=2763,
    ),
    ValidationRule(
        name="Fall bearbeiten",
        responsible=PersonRole.FIRST_LOOK,
        message="Fall in Varfish, jedoch bearbeitung noch offen",
        applies=applies_to_varfish_transferred_not_finalized_fl,
        checks=lambda r, e: check_field_not_in(r, e, "Case Status", ["Solved", "Unsolved", "VUS"]),
        table_view_id=2763,
    ),
    ValidationRule(
        name="ClinVar Upload",
        responsible=PersonRole.FIRST_LOOK,
        message="Vollständige Doku für Upload zu ClinVar",
        applies=applies_to_clinvar_cases,
        checks=check_clinvar,
        table_view_id=-1
    ),
]


def use_rule(rule, entry):
    if rule.applies(entry):
        return rule.checks(rule, entry)


def apply_validations(personnel_data, cases_data, cases_updates, findings_data, findings_updates):
    all_case_updates = expand_updates(cases_data, cases_updates)
    all_finding_updates = expand_updates(findings_data, findings_updates)
    all_errors = []

    findings_by_case_id = defaultdict(list)
    for finding_update in all_finding_updates:
        f = finding_update.result_entry()
        for cid in f["Cases"]:
            findings_by_case_id[cid].append(f)

    rule_totals = {
        rule.name: {"total": 0, "failed": 0} for rule in RULES
    }

    for update in all_case_updates:
        result = update.result_entry()
        for personnel_type in PersonRole:
            result[personnel_type] = [
                {"id": person_id, **personnel_data[person_id]} for person_id in result[personnel_type]
            ]
        result["Findings"] = findings_by_case_id[result["id"]]
        for rule in RULES:
            if rule.applies(result):
                rule_totals[rule.name]["total"] += 1
                if errors := rule.checks(rule, result):
                    rule_totals[rule.name]["failed"] += 1
                    all_errors += errors
    logger.info(rule_totals)
    return all_errors


def group_by_errors(errors: List[ValidationError], key_fun):
    group_by_results = defaultdict(lambda: defaultdict(list))
    for err in errors:
        for k in key_fun(err):
            group_by_results[k][err.entry_id].append(err)
    return group_by_results


def combine_validation_errors_by_entry_id(errors: List[ValidationError]):
    by_entry_id = defaultdict(list)
    for err in errors:
        by_entry_id[err.entry_id].append(err)
    return by_entry_id

def combine_validation_errors_by_responsible(errors: List[ValidationError]):
    by_responsible = defaultdict(list)
    for err in errors:
        for resp in err.responsible:
            by_responsible[resp["Shorthand"]].append(err)
    return by_responsible


def create_validation_updates(base_samples, errors) -> List[BaserowUpdate]:
    errors_by_id = combine_validation_errors_by_entry_id(errors)
    updates = []
    def format_line(err):
        responsible = "/".join([r["Shorthand"] for r in err.responsible])
        return f"{err.source.name} (verantw. {responsible}): {err.field} {err.comment}"

    for entry_id, base_entry in base_samples.items():
        update = BaserowUpdate(entry_id, base_entry)
        entry_errors = errors_by_id.get(entry_id)
        clinvar_reasons = base_entry[COLUMN_CLINVAR_REASON]
        if entry_errors:
            auto_validation_lines = []
            for err in entry_errors:
                auto_validation_lines.append(format_line(err))
            auto_validation_text = "\n".join(auto_validation_lines)
            update.add_update("AutoValidation", "\n".join(auto_validation_lines))
            if "ClinVar" in auto_validation_text:
                if REASON_AUTOVALIDATION not in clinvar_reasons:
                    update.add_update(COLUMN_CLINVAR_REASON, [*clinvar_reasons, REASON_AUTOVALIDATION])
            else:
                if REASON_AUTOVALIDATION in clinvar_reasons:
                    clinvar_reasons_updated = clinvar_reasons.copy()
                    clinvar_reasons_updated.remove(REASON_AUTOVALIDATION)
                    update.add_update(COLUMN_CLINVAR_REASON, clinvar_reasons_updated)
        else:
            update.add_update("AutoValidation", "")
            if REASON_AUTOVALIDATION in clinvar_reasons:
                clinvar_reasons_updated = clinvar_reasons.copy()
                clinvar_reasons_updated.remove(REASON_AUTOVALIDATION)
                update.add_update(COLUMN_CLINVAR_REASON, clinvar_reasons_updated)
        updates.append(update)

    return updates
