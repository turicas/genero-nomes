#!/bin/bash

set -e

NOMES="data/output/nomes.csv.xz"
GRUPOS="data/output/grupos.csv.xz"
DBNAME="data/output/genero-nomes.sqlite"

rm -rf data/output && mkdir data/output
time python genero_nomes.py create-database
time python genero_nomes.py classify
time python genero_nomes.py define-groups
time python genero_nomes.py export-csv
time rows csv2sqlite "$NOMES" "$GRUPOS" "$DBNAME"
