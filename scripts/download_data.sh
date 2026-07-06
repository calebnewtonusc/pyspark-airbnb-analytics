#!/usr/bin/env bash
# Download the real Inside Airbnb London open dataset into data/raw/.
#
# Uses the 2024-09-06 London snapshot (the one the ZTM Spark course uses). The
# files are large gzipped CSVs and are gitignored, so anyone cloning the repo
# runs this once to reproduce the pipeline against the real data. Browse other
# snapshots at https://insideairbnb.com/get-the-data/.
#
# macOS ships curl (not wget), so this script uses curl.
set -euo pipefail

DATE="2024-09-06"
BASE="https://data.insideairbnb.com/united-kingdom/england/london/${DATE}/data"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW="${ROOT}/data/raw"

mkdir -p "${RAW}"

for file in listings reviews calendar; do
  echo "Downloading ${file}.csv.gz ..."
  curl -L -o "${RAW}/${file}.csv.gz" "${BASE}/${file}.csv.gz"
done

echo "Done. Real Inside Airbnb London ${DATE} data is in ${RAW}"
