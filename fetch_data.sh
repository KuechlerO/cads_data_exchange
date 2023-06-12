#!/bin/bash
source ./.env
SODAR_PROJECT=f2acceb7-067d-41a4-8e39-236c022678f1

sodar_dir="data/sodar"
varfish_dir="data/varfish"
varfish_file="$varfish_dir/cases_$(date +"%Y-%m-%d").json"
pel_file="data/lb_pel.tsv"
clinicians_file="data/clinicians.json"
case_file="data/cases.json"

mkdir -p "$sodar_dir"
mkdir -p "$varfish_dir"

NO_PROXY="bihealth.org" sodar-cli samplesheet export --overwrite --write-output $sodar_dir $SODAR_PROJECT
NO_PROXY="bihealth.org" varfish-cli case --output-format json --output-file $varfish_file list $SODAR_PROJECT
NO_PROXY="laborberlin.intern" python ./fetch_lb_pel.py --username $PEL_USER --password $PEL_PASSWORD "$pel_file"
NO_PROXY="charite.de" python ./fetch_baserow_table.py "$clinicians_file"
NO_PROXY="charite.de" python ./fetch_baserow_table.py --table_id 579 "$case_file"
 
./cleanup_old.py $varfish_dir
