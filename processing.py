"""processing.py

Data processing pipeline for post-COVID household kWh data.

This module loads post-COVID household kWh and metadata CSV files, computes
daily totals per household, identifies households with runs of consecutive
zero-consumption days, filters them out, joins metadata and writes the final
features CSV used by downstream analysis.

Notes
-----
- This file is a small script-style pipeline intended to be run interactively
  or from the command line. It performs I/O on the `data/` directory and
  writes `data/households_post_covid_features.csv`.
"""

import polars as pl

# Load pre-selected household metadata (post-COVID, one-year coverage)
metadata = pl.read_csv('data/metadata_post_covid_households_year.csv', try_parse_dates=True, schema_overrides= {'cnae':pl.String, 'postal_code':pl.String, 'start_date':pl.Date, 'end_date':pl.Date, 'p1_kw':pl.String, 'count_at_least_this_many_days':pl.Int16})

# Load per-reading (hourly) household measurements filtered for the post-COVID window
households_df = pl.read_csv('data/post_covid_household_kwh.csv', try_parse_dates=True, schema_overrides={'kWh':pl.Float64})

# Keep only readings that occur on or after the chosen reference date and
# restrict to the household ids present in the metadata we prepared earlier.
households_pc = households_df.filter(
    (pl.col("timestamp")>=pl.datetime(2021, 6, 1)),
    (pl.col("id").is_in(metadata.select("id").unique().to_series())))

# Quick aggregate example: count very small readings per household (not persisted here)
households_pc.group_by("id").agg(
    (pl.col("kWh") < 0.001).sum().alias("n_lt_0_01")
)

# Work on the filtered dataframe from here on
df = households_pc

# 1) Compute daily totals per household
#    - Convert timestamp -> date
#    - Sum kWh per day
#    - Mark days whose total consumption is effectively zero
#    - Sort by id and date for deterministic processing
daily = (
    df.with_columns(pl.col("timestamp").dt.date().alias("date"))
      .group_by(["id", "date"])
      .agg(pl.col("kWh").sum().alias("kwh_day"))
      .with_columns((pl.col("kwh_day") < 0.01).alias("zero_day"))
      .sort(["id", "date"])
)

# 2) Identify households that have runs of consecutive zero days (length >= 2)
#    The approach below:
#      - Filter to days marked as zero_day
#      - For each household, detect where a new run begins by checking if the
#        gap between consecutive dates is not 1 day
#      - Cumulative-sum the run starts to assign a run id
#      - Group by (id, run_id) and compute the run length
#      - Keep households that have any run of length >= 2
ids_with_2_consecutive_zero_days = (
    daily.filter(pl.col("zero_day"))
         .with_columns(
             # start a new run when date is not previous date + 1 day
             (pl.col("date").diff().over("id") != pl.duration(days=1))
             .fill_null(True)
             .alias("new_run")
         )
         .with_columns(
             pl.col("new_run").cum_sum().over("id").alias("run_id")
         )
         .group_by(["id", "run_id"])
         .agg(pl.len().alias("run_len"))
         .filter(pl.col("run_len") >= 2)
         .select("id")
         .unique()
)

# 3) Remove (anti-join) households that exhibit the undesirable consecutive-zero runs
#    The anti join keeps only households that are NOT in `ids_with_2_consecutive_zero_days`.
clean_df = households_pc.join(
    ids_with_2_consecutive_zero_days, on="id", how="anti"
)

# 4) Attach metadata back to the cleaned readings so downstream tasks have
#    household-level attributes (start/end date, cnae, postal_code, tariff, etc.).
clean_with_metadata_df = clean_df.join(metadata, on="id", how="left").select(['id',
 'timestamp',
 'kWh',
 'start_date',
 'end_date',
 'cnae',
 'postal_code',
 'p1_kw',
 'tarriff'])

# 5) Persist the final per-reading features CSV used by modeling/analysis steps
clean_with_metadata_df.write_csv('data/households_post_covid_features.csv')
