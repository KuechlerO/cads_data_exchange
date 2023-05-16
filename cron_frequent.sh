#!/bin/bash

MAMBA_EXE="/home/zhaom/.local/bin/micromamba";
MAMBA_ROOT_PREFIX="/home/zhaom/micromamba";
PATH="/home/zhaom/micromamba/condabin:/home/zhaom/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/zhaom/.zgenom/sources/unixorn/fzf-zsh-plugin/___/bin:/home/zhaom/.fzf/bin:/home/zhaom/.zgenom/sources/unixorn/fzf-zsh-plugin/___/bin"

cd "$(dirname "$0")"
. /home/zhaom/micromamba/etc/profile.d/micromamba.sh
micromamba activate cads_data_exchange
python ./mdb_to_mail.py