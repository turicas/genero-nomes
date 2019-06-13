#!/bin/bash

set -e

rm -rf data/output
mkdir -p data/output

time python genero_nomes.py create-database
time python genero_nomes.py classify
time python genero_nomes.py define-groups
time python genero_nomes.py export-csv
