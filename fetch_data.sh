#!/bin/bash
SODAR_PROJECT=f2acceb7-067d-41a4-8e39-236c022678f1

sodar_dir="data/sodar"
varfish_dir="data/varfish"
varfish_file="$varfish_dir/cases_$(date +"%Y-%m-%d").json"

mkdir -p "$sodar_dir"
mkdir -p "$varfish_dir"

python -m pel_extract ./config_charite_namse.yaml
sodar-cli samplesheet export --overwrite --write-output $sodar_dir $SODAR_PROJECT
varfish-cli case --output-format json --output-file $varfish_file list $SODAR_PROJECT
# python combine_tnamse.py

cleanup_old.py $varfish_dir
