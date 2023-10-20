#!/bin/bash
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$(git -C ${SCRIPT_DIR} rev-parse --show-toplevel)"

batch_name="$1"
OUTPUT_DIR="$PROJECT_ROOT/data/clinvar_upload/$batch_name"

if [ ! -d "$OUTPUT_DIR" ]; then
	echo "Unknown batch"
	exit 1
fi

if [ ! -f "$OUTPUT_DIR/.submitted" ]; then
	echo "Not yet submitted"
	exit 1
fi

# if [ -f "$OUTPUT_DIR/.retrieved" ]; then
# 	echo "Already successfully retrived"
# 	exit 1
# fi

CLINVAR_THIS_BATCH_DIR="$HOME/.local/share/clinvar-this/default/$batch_name"

retrieve_latest_status() {
	latest_status=""
	for resp in $CLINVAR_THIS_BATCH_DIR/retrieve-response.*.json; do
		response_status=$(jq -r '.status.actions | map(.status) | add' "$resp")
		latest_status="$response_status"
	done
	echo $latest_status
}


SLEEP_SECONDS=$((60 * 10))


wait_for_batch() {
	while true ; do
		logfile="$OUTPUT_DIR/batch_retrieve.$(date -Iminutes).log"
		clinvar-this batch retrieve "$batch_name" &>> $logfile
		latest_status="$(retrieve_latest_status)"
		echo "Latest status for batch $batch_name is $latest_status"
		if [[ ! ( ( $latest_status == "submitted" ) ||  ( $latest_status == "processing" ) ) ]]; then
			break
		fi
		echo "Will wait for another $SLEEP_SECONDS seconds and retry."
		sleep $SLEEP_SECONDS
	done
}


wait_for_batch

clinvar-this batch export --force $batch_name "$OUTPUT_DIR/clinvar_this_response.tsv"
python "$PROJECT_ROOT/scripts/sync_clinvar_results_baserow.py" "$OUTPUT_DIR/clinvar_this_response.tsv"

touch "$OUTPUT_DIR/.retrieved"
