from . import sams, varfish, baserow, sodar
from .baserow import get_baserow_table, apply_updates, merge_entries
from .updates import update_baserow_from_lb, update_baserow_from_sodar, update_baserow_from_varfish, update_baserow_from_sams, update_baserow_from_varfish_variants, update_entry_status, update_baserow_relatives
from .validation import apply_validations, create_validation_updates

from loguru import logger

BASEROW = "baserow"
VARFISH = "varfish"
SODAR = "sodar"
SAMS = "sams"

CASE_TABLE_NAME = "Cases"
FINDINGS_TABLE_NAME = "Findings"

DATA_EXCHANGE_VERSION = "1"

ALL_DATA = {
    SAMS: sams.get_data,
    BASEROW: baserow.get_data,
    VARFISH: varfish.get_data,
    SODAR: sodar.get_data,
}

def run(dry_run: bool, get_sodar: bool, get_varfish: bool, get_sams: bool):
    """Fetch data from all data sources and create updates and validations.
    """
    all_baserow_data = ALL_DATA[BASEROW]()

    phenotips_data = get_baserow_table(all_baserow_data, "Cases")
    relatives_data = get_baserow_table(all_baserow_data, "Patients")
    pel_data = get_baserow_table(all_baserow_data, "LB-Metadata")
    personnel_data = get_baserow_table(all_baserow_data, "Personnel")
    findings_data = get_baserow_table(all_baserow_data, "Findings")

    all_updates = []
    all_updates += update_baserow_from_lb(phenotips_data, pel_data)

    sodar_data = None
    varfish_data = None
    findings_updates = []

    if get_sodar:
        logger.info("Loading SODAR data")
        sodar_data = ALL_DATA[SODAR]()
        all_updates += update_baserow_from_sodar(phenotips_data, sodar_data)

    if get_varfish:
        logger.info("Loading VarFish data")
        varfish_data = ALL_DATA[VARFISH]()
        all_updates += update_baserow_from_varfish(phenotips_data, varfish_data)
        findings_updates += update_baserow_from_varfish_variants(phenotips_data, findings_data)

    if get_sams:
        logger.info("Loading SAMS data")
        all_updates += update_baserow_from_sams(phenotips_data, ALL_DATA[SAMS]())

    relatives_updates = update_baserow_relatives(phenotips_data, relatives_data, pel_data, sodar_data, varfish_data)

    # perform validation steps
    validation_errors = apply_validations(personnel_data, phenotips_data, all_updates, findings_data, findings_updates)
    all_updates += create_validation_updates(phenotips_data, validation_errors)

    full_data = merge_entries(phenotips_data, all_updates, "Findings", findings_data, findings_updates, "Cases")
    all_updates += update_entry_status(phenotips_data, full_data)

    apply_updates(CASE_TABLE_NAME, all_updates, dry_run=dry_run)
    apply_updates(FINDINGS_TABLE_NAME, findings_updates, dry_run=dry_run)
    apply_updates("Patients", relatives_updates, dry_run=dry_run)

    return full_data, all_updates, findings_updates, validation_errors
