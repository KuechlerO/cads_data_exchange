#!/bin/bash
python -m pel_extract ./config_charite_namse.yaml
sodar-cli samplesheet export --overwrite --write-output sodar f2acceb7-067d-41a4-8e39-236c022678f1
python combine_tnamse.py
