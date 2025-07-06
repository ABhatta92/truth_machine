import os
import sys
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from delta import configure_spark_with_delta_pip
from prefect import task

from utils.common_utils import *

def unpivot_countries(df):
    country_columns = [col for col in df.columns if col != "Year"]
    n = len(country_columns)

    def escape(col_name):
        return col_name.replace("'", "\\'")

    expr = f"stack({n}, " + ", ".join(
        [f"'{escape(col)}', `{col}`" for col in country_columns]
    ) + ") as (Country, Value)"

    df_unpivoted = df.selectExpr("Year", expr)

    return df_unpivoted


def unpivot_years_edstats(df):
    year_cols = [c for c in df.columns if re.match(r"yr\d{4}", c.lower())]
    meta_cols = [c for c in df.columns if c not in year_cols]

    n = len(year_cols)

    expr = f"stack({n}, " + ", ".join(
        [f"'{col[2:]}', `{col}`" for col in year_cols] 
    ) + ") as (Year, Value)"

    return df.selectExpr(*meta_cols, expr)

def process_gem_tables(spark, freq):
    counter = 0
    table_list = [x for x in config['bronze_table_paths'].keys() if f'{freq}' in x]
    for x in table_list:
        df = spark.read.parquet(config['bronze_table_paths'][x])
        df.withColumn('Year', col('Year').cast('int'))
        df = df.drop(col('insert_timestamp'))
        df = unpivot_countries(df)
        df = df.withColumn('Indicator', lit(x.replace('_annual','')))
        df = df.withColumn('Frequency', lit('annual'))
        if counter == 0:
            write_to_parquet(df, mode='overwrite', table_path= config['silver_table_paths'][f'fact_gem_{freq}'], sanitize=True)
            print(f'Overwritten {x}')
        else:
            write_to_parquet(df, mode='append', table_path= config['silver_table_paths'][f'fact_gem_{freq}'], sanitize=True)
            print(f'Appended {x}')
        counter+=1
        
    print(f'{freq} tables done!')

@task
def bronze_to_silver_world_bank():
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
    config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
    config = load_config(config_path)
    print(config)
    spark = create_spark_session('bronze_to_silver_world_bank', config)
    init_dbs(spark, config)

    project_root = config['paths']['project_root']
    raw_root = config['paths']['raw_folder']
    print(project_root)
    print(raw_root)

    print(20*'-',f'Starting bronze_to_silver_world_bank', 20*'-')

    # process_gem_tables(spark, 'annual')
    process_gem_tables(spark, 'monthly')
    # process_gem_tables(spark, 'quarterly')

    df = spark.read.parquet(config['bronze_table_paths']['wb_edstats'])
    df = unpivot_years_edstats(df)
    write_to_parquet(df, mode='overwrite', table_path= config['silver_table_paths']['fact_wb_edstats'], sanitize=True)


    all_bronze_tables = [x for x in config['bronze_table_paths'].keys()]
    annual = [x for x in all_bronze_tables if 'annual' in x]
    monthly = [x for x in all_bronze_tables if 'monthly' in x]
    quarterly = [x for x in all_bronze_tables if 'quarterly' in x]

    remaining_tables = list(set(all_bronze_tables)-set(annual+monthly+quarterly))
    remaining_tables


    stats_tables = [x for x in remaining_tables if '_stats' in x]
    counter = 0
    for x in stats_tables:
        df = spark.read.parquet(config['bronze_table_paths'][x])
        if x == 'wb_ids_stats':
            df = df.withColumnRenamed('series_name', 'indicator_name').withColumnRenamed('series_code', 'indicator_code')
        id_columns = ["country_name", "country_code", "indicator_name", "indicator_code"]
        year_pattern = re.compile(r"^(19[6-9]\d|20[0-2]\d|2023)$")
        year_columns = [col for col in df.columns if year_pattern.match(col)]

        n_years = len(year_columns)
        stack_expr = ", ".join([f"'{y}', `{y}`" for y in year_columns])

        # Apply the transformation
        melted_df = df.selectExpr(
            *id_columns,
            f"stack({n_years}, {stack_expr}) as (year, value)"
        )
        print(x)

        df_country_temp = melted_df.select(col('country_code'), col('country_name')).drop_duplicates()
        df_indicator_temp = melted_df.select(col('indicator_code'), col('indicator_name')).drop_duplicates()
        df_non_gem_stats = melted_df.select(col('country_code'), col('indicator_code'), col('year'), col('value'))
        df_non_gem_stats = df_non_gem_stats.withColumn('year', col('year').cast('int')).withColumn('value', col('value').cast('float'))

        if counter == 0:
            write_to_parquet(df_country_temp, mode='overwrite', table_path= config['silver_table_paths']['dim_country_wb'], sanitize=True)
            print(f'Overwritten dim_country_wb for {x}')
            write_to_parquet(df_indicator_temp, mode='overwrite', table_path= config['silver_table_paths']['dim_indicator_wb'], sanitize=True)
            print(f'Overwritten dim_indicator_wb for {x}')
            write_to_parquet(df_non_gem_stats, mode='overwrite', table_path= config['silver_table_paths']['fact_wb_stats_non_gem'], sanitize=True, partition_by='country_code')
            print(f'Overwritten fact_wb_stats_non_gem for {x}')
        else:
            write_to_parquet(df_country_temp, mode='append', table_path= config['silver_table_paths']['dim_country_wb'], sanitize=True)
            print(f'Appended dim_country_wb for {x}')
            write_to_parquet(df_indicator_temp, mode='append', table_path= config['silver_table_paths']['dim_indicator_wb'], sanitize=True)
            print(f'Appended dim_indicator_wb for {x}')
            write_to_parquet(df_non_gem_stats, mode='append', table_path= config['silver_table_paths']['fact_wb_stats_non_gem'], sanitize=True, partition_by='country_code')
            print(f'Appended fact_wb_stats_non_gem for {x}')
        counter+=1

    print(20*'-',f'Ended bronze_to_silver_world_bank', 20*'-')
    spark.stop()