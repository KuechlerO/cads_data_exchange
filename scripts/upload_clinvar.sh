#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$(git -C ${SCRIPT_DIR} rev-parse --show-toplevel)"

batch_name="$1"
OUTPUT_DIR="$PROJECT_ROOT/data/clinvar_upload/$batch_name"
mkdir -p $OUTPUT_DIR

if [ -f "$OUTPUT_DIR/.retrieved" ]; then
	echo "Already submitted this batch and have not yet gotten response"
	exit 1
fi


tsvfile="$OUTPUT_DIR/findings_export.tsv"
python "$PROJECT_ROOT/scripts/create_clinvar_this_export.py" "$tsvfile"

clinvar-this batch import --name "$batch_name" "$tsvfile"
clinvar-this batch submit "$batch_name"

touch "$OUTPUT_DIR/.submitted"
