#!/bin/bash

cd "$(dirname "$0")"
. /home/zhaom/micromamba/etc/profile.d/micromamba.sh
micromamba activate cads_data_exchange
bash fetch_data.sh
python ./mdb_to_mail.py
