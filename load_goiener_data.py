from extractors import Extractor
from csvmerger import CSVMerger
from pathlib import Path
import polars as pl
from random import random
import argparse

def clean(data_dir: str, file_name: str):
    # Extract tar file
    EXTRACTED_DIR = Path(data_dir, file_name.split('.')[0]) # imputed_goiener_v7
    METADATA = Path(data_dir, 'metadata.csv')
    if not EXTRACTED_DIR.exists():
        extractor = Extractor(Path(data_dir, file_name), data_dir)

        extractor.decompress_tzst()
    # Metadata normalization
    metadata = pl.read_csv(METADATA, try_parse_dates=True, schema_overrides={'cnae':pl.String, 'codigo_postal':pl.String, 'fecha_alta':pl.String, 'fecha_baja':pl.String, 'p1_kw':pl.String, 'p2_kw':pl.String, 'p3_kw':pl.String, 'p4_kw':pl.String, 'p5_kw':pl.String, 'p6_kw':pl.String})
    metadata_normalized = metadata.with_columns(
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
        pl.when(pl.col("p2_kw") == "NA")
          .then(pl.lit(None).cast(pl.Float64))
          .otherwise(pl.col("p2_kw").cast(pl.Float64, strict=False)).alias("p2_kw"),
        pl.when(pl.col("p3_kw") == "NA")
          .then(pl.lit(None).cast(pl.Float64))
          .otherwise(pl.col("p3_kw").cast(pl.Float64, strict=False)).alias("p3_kw"),
        pl.when(pl.col("p4_kw") == "NA")
          .then(pl.lit(None).cast(pl.Float64))
          .otherwise(pl.col("p4_kw").cast(pl.Float64, strict=False)).alias("p4_kw"),
        pl.when(pl.col("p5_kw") == "NA")
          .then(pl.lit(None).cast(pl.Float64))
          .otherwise(pl.col("p5_kw").cast(pl.Float64, strict=False)).alias("p5_kw"),
        pl.when(pl.col("p6_kw") == "NA")
          .then(pl.lit(None).cast(pl.Float64))
          .otherwise(pl.col("p6_kw").cast(pl.Float64, strict=False)).alias("p6_kw"),

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

    metadata_normalized = metadata_normalized.select([
        pl.col("id"),
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("cnae"),
        pl.col("postal_code"),
        pl.col("p1_kw"),
        pl.col("p2_kw"),
        pl.col("p3_kw"),
        pl.col("p4_kw"),
        pl.col("p5_kw"),
        pl.col("p6_kw"),
        pl.col("tarriff"),
    ])

    metadata_normalized.write_csv('data/metadata_normalized.csv')
    # metadata_normalized = pl.read_csv('data/metadata_normalized.csv', try_parse_dates=True, schema_overrides={'cnae':pl.String, 'postal_code':pl.String, 'start_date':pl.Date, 'end_date':pl.Date, 'p1_kw':pl.Float64, 'p2_kw':pl.Float64, 'p3_kw':pl.Float64, 'p4_kw':pl.Float64, 'p5_kw':pl.Float64, 'p6_kw':pl.Float64})
    # metadata_pre_covid = metadata_normalized.filter([
    #     pl.col("start_date").is_between(pl.datetime(2000, 5, 31, time_zone="UTC"), pl.datetime(2020, 3, 1, time_zone="UTC")),
    #     pl.col("end_date").is_between(pl.datetime(2000, 5, 31, time_zone="UTC"), pl.datetime(2020, 3, 1, time_zone="UTC"))
    # ])

    metadata_post_covid = metadata_normalized.filter([
        pl.col("start_date").is_between(pl.datetime(2020, 5, 31, time_zone="UTC"), pl.datetime(3020, 3, 1, time_zone="UTC")),
        pl.col("end_date").is_between(pl.datetime(2020, 5, 31, time_zone="UTC"), pl.datetime(3020, 3, 1, time_zone="UTC"))
    ])

    metadata_post_covid_households = metadata_post_covid.filter(
        (pl.col('cnae') > '9699'),
        (pl.col('cnae') < '9900')
    )
    metadata_post_covid_households.write_csv('data/metadata_post_covid_households.csv')
    households = metadata_post_covid_households['id'].unique().to_list()
    print(f"Post-COVID households={len(households)}")

    random_house = households[int(random() * len(households))]
    print(f"Random household id: {random_house}")
    random_house_df = pl.read_csv(EXTRACTED_DIR / f"{random_house}.csv", try_parse_dates=True, schema_overrides={'kWh':pl.Float64})
    # Combine CSV files
    households_csvs = [f'{EXTRACTED_DIR}/{f}.csv' for f in households]

    csvMerger = CSVMerger(households_csvs, None, 'data/household_kwh.csv')
    csvMerger.combine_csv_files()

    # households_df = pl.read_csv('data/household_kwh.csv', try_parse_dates=True, schema_overrides={'kWh':pl.Float64}).with_columns(pl.col('index').alias('timestamp')).select('id', 'timestamp', 'kWh', 'imp')

    # for col in households_df.columns:
    #     print(f"Column: {col}")
    #     print(households_df[col].value_counts().sort(by='count', descending=True).head(5))
    #     print("\n")


if __name__ == "__main__":
    DATA_DIR = 'data'
    FILE_NAME = 'imputed_goiener_v7.tar.zst'

    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', type=str, default=DATA_DIR, help='Directory where data is stored')
    parser.add_argument('--file_name', type=str, default=FILE_NAME, help='Name of the .tar.zst file')
    args = parser.parse_args()

    clean(data_dir=args.data_dir, file_name=args.file_name)