import re
from collections import defaultdict
from functools import reduce
from typing import Dict, List
from .config import settings
from python_baserow_simple import BaserowApi
from attrs import define, field
from loguru import logger


COLUMN_CLINVAR_STATUS = "ClinVar-Upload Status"
COLUMN_CLINVAR_REASON = "ClinVar-Upload Begründung"
REASON_AUTOVALIDATION = "AutoValidation Fehler"

VALI_UPLOADED = "ClinVar Uploaded"
VALI_OK = "ClinVar freigegeben"
VALI_FAIL = "Behebbare Fehler. Siehe Begründung"
VALI_BLOCKED = "Kein Upload. Siehe Begründung"
VALI_FIN = "ClinVar Uploaded"

STATUS_ORDER = [
    "Invalid",
    "Storniert",
    "Varfish Initial",
    "Active",
    "Unsolved",
    "VUS",
    "Solved",
]


def status_newer(new_status: str, old_status: str) -> bool:
    """Check if the new status is higher rank than old one."""
    if old_status is None:
        return True
    new_index = STATUS_ORDER.index(new_status)
    old_index = STATUS_ORDER.index(old_status)
    return new_index > old_index


def analysezahl_to_int(analysezahl: str) -> int:
    return {
            "Single": 1,
            "Duo": 2,
            "Trio": 3,
            "Quattro": 4,
    }.get(analysezahl, 0)


LB_ID_INNER = re.compile(r"(\d{2})[-_](\d{4})")
def matchLbId(lbid1, lbid2):
    m1 = LB_ID_INNER.search(str(lbid1))
    m2 = LB_ID_INNER.search(str(lbid2))
    if not (m1 and m2):
        return False
    return (m1.group(1), m1.group(2)) == (m2.group(1), m2.group(2))


BR = BaserowApi(database_url=settings.baserow.url, token=settings.baserow_token)


class TableNotFoundError(Exception):
    pass


class UpdateConflictError(Exception):
    pass


@define
class BaserowUpdate:
    id: int
    entry: dict
    updates: Dict[str, str] = field(factory=dict)

    @property
    def has_updates(self):
        return len(self.updates) > 0

    def add_updates(self, updates: Dict[str, str]) -> "BaserowUpdate":
        for key, value in updates.items():
            self.add_update(key, value)
        return self

    def add_update(self, key, value):
        # do not update if value idem
        if self.entry.get(key) == value:
            return

        if key in self.updates and self.updates[key] != value:
            raise UpdateConflictError(f"{key} current: {self.updates[key]} new: {value} entry: {self.entry}")
        self.updates[key] = value

    def result_entry(self):
        result = {}
        for key, value in self.entry.items():
            update_value = self.updates.get(key, value)
            result[key] = update_value
        for key, value in self.updates.items():
            if key not in result:
                result[key] = value
        if "id" not in result:
            result["id"] = self.id
        return result

    def __repr__(self):
        update_str = ";".join(f"{k}={v}" for k, v in self.updates.items())
        return f"SV-{self.id}|{update_str}"


def merge_updates(baserow_updates: List[BaserowUpdate]) -> List[BaserowUpdate]:
    """Merge multiple lists of baserow updates.
    """
    baserow_updates_by_id = defaultdict(list)
    new_updates = []
    for update in baserow_updates:
        if update.id is None:
            new_updates.append(update)
        else:
            baserow_updates_by_id[update.id].append(update)

    all_updates = []
    for updates in baserow_updates_by_id.values():
        merged_update = reduce(lambda a, b: a.add_updates(b.updates), updates)
        all_updates.append(merged_update)
    all_updates += new_updates
    return all_updates


def expand_updates(cases, updates: list) -> list:
    """Create BaserowUpdates from all cases."""
    merged_updates = merge_updates(updates)

    update_ids = {u.id: u for u in merged_updates if u.id}
    update_news = [u for u in merged_updates if u.id is None]

    all_updates = []
    for cid, case in cases.items():
        if update := update_ids.get(cid):
            all_updates.append(update)
        else:
            all_updates.append(BaserowUpdate(cid, case))

    all_updates += update_news
    return all_updates


def merge_entries(base_data, base_updates, base_field, linked_data, linked_updates, linked_field):
    all_updates = expand_updates(base_data, base_updates)
    all_linked_updates = expand_updates(linked_data, linked_updates)

    linked_by_id = defaultdict(list)
    for linked_update in all_linked_updates:
        result = linked_update.result_entry()
        for entry_id in result[linked_field]:
            linked_by_id[entry_id].append(result)

    result_entries = {}
    for update in all_updates:
        result = update.result_entry()
        result[base_field] = linked_by_id[update.id]
        result_entries[update.id] = result
    return result_entries


def apply_updates(target_table_name: str, baserow_updates: List[BaserowUpdate], dry_run: bool = True):
    merged_updates = merge_updates(baserow_updates)
    for table_config in settings.baserow.tables:
        if table_config.name == target_table_name:
            table_id = table_config.id
            break
    else:
        raise TableNotFoundError

    update_entries = []
    for update in merged_updates:
        if update.has_updates:
            logger.debug(f"Updating table {target_table_name} entry {update.id} Fields: {', '.join(f'{k}={v}' for k, v in update.updates.items())}")
            result_entry = update.result_entry()
            update_entries.append(result_entry)

    if not dry_run:
        BR.add_data_batch(table_id, update_entries)
    else:
        logger.warning(f"DRY RUN. DATA NOT UPLOADED.")


def get_baserow_table(tables, name):
    for table in tables:
        if table["name"] == name:
            return table["data"]


def get_table(table_name):
    table_configs = settings.baserow.tables
    for table_config in table_configs:
        if table_name == table_config.name:
            return BR.get_data(table_config.id)
    else:
        raise RuntimeError(f"Could not find a table with name {table_name} in configuration")


def get_data():
    table_configs = settings.baserow.tables
    all_data = []
    for table_config in table_configs:
        data = BR.get_data(table_config.id)
        all_data.append({
            "id": table_config.id,
            "name": table_config.name,
            "data": data
        })
    return all_data
