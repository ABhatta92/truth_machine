import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from prefect import task

from utils.common_utils import *

@task
def raw_to_bronze_world_bank():
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
    config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
    config = load_config(config_path)
    print(config)
    spark = create_spark_session('raw_to_bronze_world_bank', config)
    init_dbs(spark, config)

    project_root = config['paths']['project_root']
    raw_root = config['paths']['raw_folder']

    print(20*'-',f'Getting non-GEM World Bank Country Data', 20*'-')
    df1 = spark.read.csv(config['raw_paths']['wb_gender_stats_country'], header=True)
    df2 = spark.read.csv(config['raw_paths']['wb_hnp_stats_country'], header=True)
    df3 = spark.read.csv(config['raw_paths']['wb_jobs_country'], header=True)
    df4 = spark.read.csv(config['raw_paths']['wb_ids_country'], header=True)

    df4 = df4.withColumnsRenamed({'Code': 'Country Code', 'PPP survey years': 'PPP survey year'})

    df1 = df1.drop(col('Alternative conversion factor'))
    df2 = df2.drop(col('Alternative conversion factor'))
    df3 = df3.drop(col('Alternative conversion factor'))

    df1 = sanitize_columns(df1)
    df2 = sanitize_columns(df2)
    df3 = sanitize_columns(df3)
    df4 = sanitize_columns(df4)
    df_countries_wb_non_gem = df1.union(df2).union(df3).union(df4)

    df_wb_non_gem_countries = df_countries_wb_non_gem.drop_duplicates()
    write_to_parquet(df_wb_non_gem_countries, mode='overwrite', table_path=config['bronze_table_paths']['wb_non_gem_countries'], sanitize=False)

    print(20*'-',f'Getting non-GEM World Bank Indicators Data', 20*'-')
    df1 = spark.read.csv(config['raw_paths']['wb_gender_stats_series'], header=True)
    df2 = spark.read.csv(config['raw_paths']['wb_hnp_stats_series'], header=True)
    df3 = spark.read.csv(config['raw_paths']['wb_jobs_series'], header=True)
    df4 = spark.read.csv(config['raw_paths']['wb_ids_series'], header=True)

    df1 = sanitize_columns(df1)
    df2 = sanitize_columns(df2)
    df3 = sanitize_columns(df3)
    df4 = sanitize_columns(df4)

    df4 = df4.withColumnRenamed('code', 'series_code')

    all_cols = set(df1.columns+df2.columns+df3.columns)

    cols_list = list(all_cols.intersection(set(df4.columns)))
    cols_list

    df1 = df1.select(*cols_list)
    df2 = df2.select(*cols_list)
    df3 = df3.select(*cols_list)
    df4 = df4.select(*cols_list)

    df_wb_non_gem_indicators = df1.union(df2).union(df3).union(df4)
    df_wb_non_gem_indicators = df_wb_non_gem_indicators.drop_duplicates()

    write_to_parquet(df_wb_non_gem_indicators, mode='overwrite', table_path= config['bronze_table_paths']['wb_non_gem_indicators'], sanitize=False)

    print(20*'-',f'Getting Non=GEM Stats', 20*'-')
    df1 = spark.read.csv(config['raw_paths']['wb_gender_stats'], header=True)
    df2 = spark.read.csv(config['raw_paths']['wb_hnp_stats'], header=True)
    df3 = spark.read.csv(config['raw_paths']['wb_ids_stats'], header=True)
    df4 = spark.read.csv(config['raw_paths']['wb_jobs_stats'], header=True)

    write_to_parquet(df1, mode='overwrite', table_path= config['bronze_table_paths']['wb_gender_stats'], sanitize=True)
    write_to_parquet(df2, mode='overwrite', table_path= config['bronze_table_paths']['wb_hnp_stats'], sanitize=True)
    write_to_parquet(df3, mode='overwrite', table_path= config['bronze_table_paths']['wb_ids_stats'], sanitize=True)
    write_to_parquet(df4, mode='overwrite', table_path= config['bronze_table_paths']['wb_jobs_stats'], sanitize=True)

    print(20*'-',f'Getting EdStats Data', 20*'-')
    df = spark.read.csv(config['raw_paths']['wb_edstats'], header=True)
    write_to_parquet(df, mode='overwrite', table_path= config['bronze_table_paths']['wb_edstats'], sanitize=True)

    wb_tables_list = [x for x in config['bronze_table_paths'].keys() if 'wb_' in x]
    annual_tables = [x for x in wb_tables_list if '_annual' in x]
    quarterly_tables = [x for x in wb_tables_list if '_quarterly' in x]
    monthly_tables = [x for x in wb_tables_list if '_monthly' in x]

    print(20*'-',f'Getting GEM Annual Data', 20*'-')
    tables_to_use = [x.replace('_annual','') for x in annual_tables]
    for x in tables_to_use:
        df = read_and_pre_clean_wb_gem_excel(config['raw_paths'][x], 'annual', 1, 1)
        print(f'Read {x}_annual')
        df = spark.createDataFrame(df)
        write_to_parquet(df, mode='overwrite', table_path= config['bronze_table_paths'][x+"_annual"], sanitize=False)


    print(20*'-',f'Getting GEM Monthly Data', 20*'-')
    tables_to_use = [x.replace('_monthly','') for x in monthly_tables]
    for x in tables_to_use:
        df = read_and_pre_clean_wb_gem_excel(config['raw_paths'][x], 'monthly', 1, 1)
        print(f'Read {x}_monthly')
        df = spark.createDataFrame(df)
        write_to_parquet(df, mode='overwrite', table_path= config['bronze_table_paths'][x+"_monthly"], sanitize=False)


    print(20*'-',f'Getting GEM Quarterly Data', 20*'-')
    tables_to_use = [x.replace('_quarterly','') for x in quarterly_tables]
    for x in tables_to_use:
        df = read_and_pre_clean_wb_gem_excel(config['raw_paths'][x], 'quarterly', 1, 1)
        print(f'Read {x}_quarterly')
        df = spark.createDataFrame(df)
        write_to_parquet(df, mode='overwrite', table_path= config['bronze_table_paths'][x+"_quarterly"], sanitize=False)

    print(20*'-',f'Ending raw_to_bronze_world_bank', 20*'-')

    spark.stop()