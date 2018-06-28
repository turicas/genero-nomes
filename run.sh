#!/bin/bash

set -e
rm -rf data/output && mkdir data/output

NOMES="data/output/nomes.csv.xz"
GRUPOS="data/output/grupos.csv.xz"
DBNAME="data/output/genero-nomes.sqlite"

time python genero_nomes.py create-database
time python genero_nomes.py classify
time python genero_nomes.py define-groups
time python genero_nomes.py export-csv
time rows csv2sqlite "$NOMES" "$GRUPOS" "$DBNAME"
