from data_exchange.report import to_text_report, to_text_report_by_responsible

import requests_cache
# requests_cache.install_cache("dev_cache")

from . import sams, varfish, baserow, sodar
from .updates import update_baserow_from_lb, update_baserow_from_sodar, update_baserow_from_varfish, update_baserow_from_sams, update_baserow_from_varfish_variants
from .baserow import apply_updates
from .validation import apply_validations, create_validation_updates
import typer

BASEROW = "baserow"
VARFISH = "varfish"
SODAR = "sodar"
SAMS = "sams"
ALL_DATA = {
    SAMS: sams.get_data,
    BASEROW: baserow.get_data,
    VARFISH: varfish.get_data,
    SODAR: sodar.get_data,
}

def get_baserow_table(tables, name):
    for table in tables:
        if table["name"] == name:
            return table["data"]


CASE_TABLE_NAME = "Cases"
FINDINGS_TABLE_NAME = "Findings"


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
def main(dry_run: bool = False):
    all_baserow_data = ALL_DATA[BASEROW]()

    phenotips_data = get_baserow_table(all_baserow_data, "Cases")
    pel_data = get_baserow_table(all_baserow_data, "LB-Metadata")
    personnel_data = get_baserow_table(all_baserow_data, "Personnel")
    findings_data = get_baserow_table(all_baserow_data, "Findings")

    all_updates = []
    all_updates += update_baserow_from_lb(phenotips_data, pel_data)
    # all_updates += update_baserow_from_sodar(phenotips_data, ALL_DATA[SODAR]())
    with requests_cache.enabled("dev_cache"):
        all_updates += update_baserow_from_varfish(phenotips_data, ALL_DATA[VARFISH]())
    # all_updates += update_baserow_from_sams(phenotips_data, ALL_DATA[SAMS]())

    with requests_cache.enabled("dev_cache"):
        findings_updates = update_baserow_from_varfish_variants(phenotips_data, findings_data)

    # perform validation steps
    validation_errors = apply_validations(personnel_data, phenotips_data, all_updates, findings_data, findings_updates)
    all_updates += create_validation_updates(phenotips_data, validation_errors)

    apply_updates(CASE_TABLE_NAME, all_updates, dry_run=dry_run)
    apply_updates(FINDINGS_TABLE_NAME, findings_updates, dry_run=dry_run)

    # text_vali_report = to_text_report(validation_errors)
    to_text_report_by_responsible(validation_errors)


app()
