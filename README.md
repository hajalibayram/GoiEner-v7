# Dataset Access & Usage Instructions

This repository contains helpers and one-off scripts used to prepare and
process the Goiener imputed household electricity dataset.

## 1. Access the original dataset

The original datasets are hosted on Zenodo:

* [https://zenodo.org/records/14949245](https://zenodo.org/records/14949245)

## 2. Download the required files

You have two options to obtain the data:

### Option A: Use the download script

Run the provided script which wraps `curl` to fetch the required files into
`data/`:

```bash
python download.py
```

Note: `download.py` invokes the download function for the two Zenodo files
used by the preprocessing steps. You can also import and call
`download_with_curl(url, dest_folder)` from other tools.

### Option B: Manual download

Manually download the following files from Zenodo and place them under
`data/`:

* `imputed_goiener_v7.tar.zst`
* `metadata.csv`

Example layout:

```
data/
├── imputed_goiener_v7.tar.zst
└── metadata.csv
```

## 3. What the main scripts do

This project contains several small scripts and utilities. Short descriptions
and usage examples are below.

- `preprocessing.py`
  - Purpose: Normalize raw metadata, select post-COVID residential households
    with at least one year of coverage after a reference date, merge
    per-household CSVs into a single `data/household_kwh.csv`, and write
    `data/post_covid_household_kwh.csv` with per-hour readings limited to the
    post-COVID window.
  - Usage: run it after placing the downloaded files in `data/` (it will
    decompress the archive if needed):

```bash
python preprocessing.py
```

- `processing.py`
  - Purpose: Load `data/post_covid_household_kwh.csv` and the selected
    `metadata_post_covid_households_year.csv`, compute daily totals,
    identify households with consecutive zero-consumption-day runs, remove
    those households, attach metadata, and write
    `data/households_post_covid_features.csv` for analysis.
  - Usage:

```bash
python processing.py
```

- `load_goiener_data.py`
  - Purpose: A convenience script that normalizes metadata and merges
    per-household CSVs. It implements the `clean(data_dir, file_name)`
    function which extracts the archive (if not already extracted),
    normalizes metadata, and merges household CSVs into
    `data/household_kwh.csv`.
  - Usage:

```bash
python load_goiener_data.py
```

- `extractors.py`
  - Purpose: Provides an `Extractor` helper to decompress `.tar.zst` files
    and safely extract the contained tar file into a destination directory.
  - Notes: Extraction includes a safety filter to prevent path traversal.

- `csvmerger.py`
  - Purpose: `CSVMerger` utility to merge a list of per-household CSV files
    into a single CSV and add an `id` column derived from each filename.

- `download.py`
  - Purpose: Small wrapper around `curl` to download the two Zenodo files.
    It currently runs two example downloads (metadata and archive) when
    executed; you can edit or import the function if you need different
    targets.

- `simel/` scripts
  - Purpose: A small pipeline for converting SIMEL files into the internal
    CSV formats used later in the repository. The scripts are in the
    `simel/` directory and are named `1_simel2user.py`, `2_user2raw.py`,
    `3_raw2goi.py`, `4_goi2imp.py`.

## 4. Notebooks

The repository contains interactive notebooks to explore the data and the
processing steps:

* `preprocessing.ipynb` — walkthrough of metadata normalization and
  CSV merging
* `processing.ipynb` — demonstration of daily aggregation and household
  filtering logic

## 5. Python environment

This project uses **PEP 621 / pyproject.toml** for dependency metadata. To
create an isolated environment and install the package (recommended):

```bash
python -m venv .venv
source .venv/bin/activate  # macOS / Linux (zsh / bash)
pip install .
```

Requirements: Python **>= 3.10** (project uses recent Polars features).

## 6. Notes & safety

- The extraction step writes files under `data/` and may create many
  per-household CSVs; ensure you have disk space.
- `download.py` invokes `curl` via subprocess; ensure `curl` is installed.
- Several scripts perform file I/O and will overwrite files in `data/` with
  the same names. Back up any important files before re-running.

If you'd like, I can also:
- Add a small CLI wrapper to each script for more flexible execution flags.
- Add unit tests around the metadata normalization and the consecutive-zero
  run detection logic.

---

