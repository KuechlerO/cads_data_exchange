#!/bin/bash

BACKUP_PATH="/media/BaserowExport"
TMP_PATH="/tmp/bsexport"

mkdir -p "$TMP_PATH"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 579 "${TMP_PATH}/Cases.xlsx"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 583 "${TMP_PATH}/AdditionalDiagnostics.xlsx"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 581 "${TMP_PATH}/Findings.xlsx"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 582 "${TMP_PATH}/Personnel.xlsx"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 660 "${TMP_PATH}/LB-Metadata.xlsx"

PASSWORD="/opt/.backup_password"
zip -r --password $(< ./.backup_password) $BACKUP_PATH/baserow_export_$(date +"%Y-%m-%d_%H-%M").zip $TMP_PATH

rm -r "$TMP_PATH"
