#!/bin/bash

. $HOME/micromamba/etc/profile.d/micromamba.sh
micromamba activate cads_data_exchange
bash fetch_data.sh
