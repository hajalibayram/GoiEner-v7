"""preprocessing.py

One-off preprocessing script for Goiener dataset used to generate:
- data/metadata_post_covid_households_year.csv
- data/household_kwh.csv
- data/post_covid_household_kwh.csv

The script normalizes the original metadata, filters post-COVID households with
at least one year of data after a reference date, merges per-household CSVs
into a single CSV, and writes filtered hourly readings for the post-COVID
period.
"""

from extractors import Extractor
from pathlib import Path
import polars as pl

from csvmerger import CSVMerger

# Extract tar file
DATA_DIR = 'data'
FILE_NAME = 'imputed_goiener_v7.tar.zst'
EXTRACTED_DIR = Path(DATA_DIR, FILE_NAME.split('.')[0]) # imputed_goiener_v7

METADATA = Path(DATA_DIR, 'metadata.csv')

# If the extracted folder with per-household CSVs doesn't exist, decompress
# the provided archive into `data/imputed_goiener_v7/`.
if not EXTRACTED_DIR.exists():
    # Pass string paths to match the Extractor constructor annotation
    extractor = Extractor(str(Path(DATA_DIR, FILE_NAME)), DATA_DIR)

    extractor.decompress_tzst()
# Metadata standardization
# Read raw metadata and coerce types where possible. The schema overrides
# prevent some columns from being auto-inferred incorrectly (e.g. postal code
# and cnae as strings rather than integers).
metadata = pl.read_csv(METADATA, try_parse_dates=True, schema_overrides={'cnae':pl.String, 'codigo_postal':pl.String, 'fecha_alta':pl.String, 'fecha_baja':pl.String, 'p1_kw':pl.String, 'p2_kw':pl.String, 'p3_kw':pl.String, 'p4_kw':pl.String, 'p5_kw':pl.String, 'p6_kw':pl.String})

# Normalize and clean several metadata fields. This block attempts to parse
# dates in multiple formats and to coerce tariff tiers (p1_kw, etc.) to numeric
# where possible. 'NA' strings are converted to None.
metadata_standardized = metadata.with_columns(
    pl.col("cups").alias("id"),
    pl.when(pl.col("fecha_alta").str.contains("/"))
      .then(
          pl.col("fecha_alta")
            .str.strptime(pl.Date, "%d/%m/%Y", strict=False)
            .dt.strftime("%Y-%m-%d")
      )
      .otherwise(
          pl.when(pl.col("fecha_alta").str.contains("-"))
            .then(pl.col("fecha_alta"))
            .otherwise(None)
      ).alias("start_date")
    .cast(pl.Date),
    pl.when(pl.col("fecha_baja").str.contains("/"))
      .then(
          pl.col("fecha_baja")
            .str.strptime(pl.Date, "%d/%m/%Y", strict=False)
            .dt.strftime("%Y-%m-%d")
      )
      .otherwise(
          pl.when(pl.col("fecha_baja").str.contains("-"))
            .then(pl.col("fecha_baja"))
            .otherwise(None)
      ).alias("end_date")
    .cast(pl.Date),
    pl.when(pl.col("p1_kw") == "NA")
      .then(pl.lit(None).cast(pl.Float64))
      .otherwise(pl.col("p1_kw").cast(pl.Float64, strict=False)).alias("p1_kw"),
    # other p*_kw columns were intentionally left commented in the original
    # script; they can be restored if needed.

    pl.when(pl.col("codigo_postal") == "NA")
      .then(None)
      .otherwise(pl.col("codigo_postal").cast(pl.String, strict=False)).alias("postal_code"),
    pl.when(pl.col("cnae") == "NA")
      .then(None)
      .otherwise(pl.col("cnae").cast(pl.String, strict=False)).alias("cnae"),
    pl.when(pl.col("tarifa_atr") == "NA")
      .then(None)
      .otherwise(pl.col("tarifa_atr").cast(pl.String, strict=False)).alias("tarriff"),
).drop(["cups", "fecha_alta", "fecha_baja", "codigo_postal", "tarifa_atr"])

# Keep only the columns used downstream. This reduces memory and makes outputs
# predictable for subsequent steps.
metadata_standardized = metadata_standardized.select([
    pl.col("id"),
    pl.col("start_date"),
    pl.col("end_date"),
    pl.col("cnae"),
    pl.col("postal_code"),
    pl.col("p1_kw"),
    # pl.col("p2_kw"),
    # pl.col("p3_kw"),
    # pl.col("p4_kw"),
    # pl.col("p5_kw"),
    # pl.col("p6_kw"),
    pl.col("tarriff"),
])

# Remove duplicate metadata rows while preserving input order where possible.
metadata_standardized = metadata_standardized.unique(maintain_order=True)

# Select households that were active after the initial pandemic period (loosely
# defined here). This creates an intermediate 'post_covid' dataset used to
# filter the list of household CSVs to merge.
metadata_post_covid = metadata_standardized.filter([
    pl.col("end_date").is_between(pl.datetime(2021, 6, 1, time_zone="UTC"), pl.datetime(3020, 3, 1, time_zone="UTC"))
])

# Narrow to residential households by cnae code range.
metadata_post_covid_households = metadata_post_covid.filter(
    (pl.col('cnae') > '9699'),
    (pl.col('cnae') < '9900')
)

# Define a cutoff reference date (start of the post-COVID analysis window).
cutoff = pl.datetime(2021, 6, 1)  # use pl.date(...) if your cols are Date

# Compute number of days available after the reference for each metadata row.
# We choose the row per household that maximizes the days available after the
# reference date (after some grouping/aggregation below) so we can ensure at
# least a year of post-reference coverage.
metadata_post_covid_households = (
    metadata_post_covid_households
    .with_columns(
        pl.max_horizontal(pl.col("start_date"), cutoff).alias("ref")
    )
    .with_columns(
        (pl.col("end_date") - pl.col("ref")).dt.total_days().alias("days_from_ref_to_end")
    )
)

# Keep only households with at least 365 days of data after the reference date.
metadata_post_covid_households_year = metadata_post_covid_households.filter(pl.col('days_from_ref_to_end') >= 365)

# Determine group-by columns (all metadata columns except the date helpers).
group_cols = [c for c in metadata_post_covid_households_year.columns
              if c not in ["start_date", "end_date", "ref", "days_from_ref_to_end"]]

# For each household (and the other identifying metadata), keep the row that
# provides the maximum days_from_ref_to_end. This helps disambiguate multiple
# metadata entries per household and choose the most complete one.
metadata_post_covid_households_year = (
    metadata_post_covid_households_year
    .with_columns(
        pl.when(pl.col("start_date") < cutoff)
          .then(cutoff)
          .otherwise(pl.col("start_date"))
          .cast(pl.Date)
          .alias("start_date")
    )
    .with_columns(
        pl.max_horizontal(pl.col("start_date"), cutoff).alias("ref")
    )
    .with_columns(
        (pl.col("end_date") - pl.col("ref")).dt.total_days().alias("days_from_ref_to_end")
    )
    # choose row with max days_from_ref_to_end per group
    .sort(group_cols + ["days_from_ref_to_end"], descending=[False]*len(group_cols) + [True])
    .group_by(group_cols, maintain_order=True)
    .head(1)
)

# Keep a subset of useful output fields and persist the resulting metadata CSV
# for later stages.
metadata_post_covid_households_year = metadata_post_covid_households_year.select(['id', 'start_date', 'end_date', 'cnae','postal_code', 'p1_kw', 'tarriff', 'days_from_ref_to_end'])

# When multiple rows exist for the same household id, keep the last one after
# sorting by days_from_ref_to_end (i.e., the row with most coverage).
metadata_post_covid_households_year_clean = (
    metadata_post_covid_households_year
    .sort(["id", "days_from_ref_to_end"], descending=True)
    .unique(subset=["id"], keep="last", maintain_order=True)
)

# Persist the per-household metadata used later to choose which CSVs to merge.
metadata_post_covid_households_year.write_csv('data/metadata_post_covid_households_year.csv')
households = metadata_post_covid_households_year['id'].unique().to_list()
print(f"Post-COVID households={len(households)}")

# Build a list of per-household CSV paths to merge. These files are expected to
# be present under the folder produced by extracting the original archive.
households_csvs = [f'{EXTRACTED_DIR}/{f}.csv' for f in households]


# Use the CSVMerger utility to combine all per-household CSVs into a single
# file `data/household_kwh.csv`. The combined CSV will include an `id` column.
csvMerger = CSVMerger(households_csvs, None, 'data/household_kwh.csv')
csvMerger.combine_csv_files()


# Read the merged table, coerce types and rename the auto-generated index
# column to `timestamp` so downstream code expects the same column names.
households_df = pl.read_csv('data/household_kwh.csv', try_parse_dates=True, schema_overrides={'kWh':pl.Float64}).with_columns(pl.col('index').alias('timestamp')).select('id', 'timestamp', 'kWh', 'imp')

# Filter hourly readings to the post-COVID analysis window and write the
# per-reading CSV used by the `processing` script. Replace timezone to UTC
# to make subsequent date/duration computations consistent.
post_covid_households_df = households_df.filter([pl.col('timestamp')
                                          .dt.replace_time_zone("UTC")
                                          .is_between(
                                            pl.datetime(2021, 6, 1, time_zone="UTC"),
                                            pl.datetime(3023, 1, 1, time_zone="UTC"))]).drop("imp")

post_covid_households_df.write_csv('data/post_covid_household_kwh.csv')
