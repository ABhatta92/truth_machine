import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip
from prefect import task

from utils.common_utils import *

def cast_unodc_cols(df):
    for c in df.columns:
        if c == 'value':
            df = df.withColumn(c, col(c).cast('float'))
        elif c == 'year':
            df = df.withColumn(c, col(c).cast('int'))
        else:
            df = df.withColumn(c, col(c).cast('string'))
    return df

@task
def bronze_to_silver_unodc():    
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
    config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
    config = load_config(config_path)
    print(config)
    spark = create_spark_session('bronze_to_silver_unodc', config)
    init_dbs(spark, config)

    project_root = config['paths']['project_root']
    raw_root = config['paths']['raw_folder']
    print(project_root)
    print(raw_root)

    print(20*'-',f'Starting bronze_to_silver_unodc', 20*'-')

    df_unodc_econ_crime = spark.read.parquet(config['bronze_table_paths']['unodc_econ_crime']).withColumnRenamed('iso3_code', 'country_code')
    df_eunodc_func_justice = spark.read.parquet(config['bronze_table_paths']['unodc_func_justice']).withColumnRenamed('iso3_code', 'country_code')
    df_unodc_sexual_violent = spark.read.parquet(config['bronze_table_paths']['unodc_sexual_violent']).withColumnRenamed('iso3_code', 'country_code')

    country_cols = ['country_code','country','region','subregion']
    dim_country_unodc = df_unodc_econ_crime.select(*country_cols).union(df_eunodc_func_justice.select(*country_cols)).union(df_unodc_sexual_violent.select(*country_cols))
    dim_country_unodc = dim_country_unodc.drop_duplicates()
    for c in dim_country_unodc.columns:
        df = dim_country_unodc.withColumn(c, col(c).cast("string"))

    country_cols.remove('country_code')
    df_unodc_econ_crime = cast_unodc_cols(df_unodc_econ_crime.drop(*country_cols))
    df_eunodc_func_justice = cast_unodc_cols(df_eunodc_func_justice.drop(*country_cols))
    df_unodc_sexual_violent = cast_unodc_cols(df_unodc_sexual_violent.drop(*country_cols))

    fact_crime_and_justice_unodc = df_unodc_econ_crime.union(df_eunodc_func_justice).union(df_unodc_sexual_violent)

    write_to_parquet(dim_country_unodc, mode='overwrite', table_path= config['silver_table_paths']['dim_country_unodc'], sanitize=False)
    write_to_parquet(fact_crime_and_justice_unodc, mode='overwrite', table_path= config['silver_table_paths']['fact_crime_and_justice_unodc'], sanitize=False, partition_by='country_code')

    print(20*'-',f'Ending bronze_to_silver_unodc', 20*'-')
    spark.stop()