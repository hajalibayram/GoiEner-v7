# Dataset Access & Usage Instructions

## 1. Access the original dataset

The original datasets are hosted on Zenodo:

* [https://zenodo.org/records/14949245](https://zenodo.org/records/14949245)

## 2. Download the required files

You have two options to obtain the data:

### Option A: Use the download script

Run the provided script:

* `download.py`

### Option B: Manual download

Manually download the following files from Zenodo:

* `imputed_goiener_v7.tar.zst`
* `metadata.csv`

Place both files directly into a folder of your choice, for example:

```
data/
├── imputed_goiener_v7.tar.zst
└── metadata.csv
```

## 3. Follow the tutorial

To understand how to load and use the dataset step by step, open and run:

* `tutorial.ipynb`

## 4. Load cleaned data directly (optional)

If you want to skip manual downloading and preprocessing, you can use:

* `load_goiener_data.py`

This script automatically downloads and loads the cleaned dataset for immediate use.

---

## Python Environment Configuration (pyproject.toml)

This project uses **PEP 621 / pyproject.toml** for Python dependency and environment management.

### Requirements

* Python **>= 3.10** (recommended)
* `pip` **>= 23.0`

### Using a virtual environment

Create and activate a virtual environment before installing dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux / macOS
.venv\Scripts\activate     # Windows
```

Then install the project:

```bash
pip install .
```

---