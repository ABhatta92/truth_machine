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
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from prefect import task

from utils.common_utils import *

def normalize_period(df, freq):

    df = df.withColumnRenamed('year', 'year_raw')


    if freq == 'monthly':
        df = df.withColumn('year', regexp_extract(col('year_raw'), r'^(\d{4})', 1))
        df = df.withColumn('month', regexp_extract(col('year_raw'), r'M(\d{1,2})', 1).cast('int'))
        df = df.withColumn('month', lpad(col('month').cast('string'), 2, '0'))
        df = df.withColumn('period', to_date(concat_ws('-', col('year'), col('month'), lit('01')), 'yyyy-MM-dd'))

    elif freq == 'quarterly':
        df = df.withColumn('year', regexp_extract(col('year_raw'), r'^(\d{4})', 1))
        df = df.withColumn('quarter', regexp_extract(col('year_raw'), r'Q([1-4])', 1).cast('int'))
        
        df = df.withColumn(
            'month',
            when(col('quarter') == 1, '01')
            .when(col('quarter') == 2, '04')
            .when(col('quarter') == 3, '07')
            .when(col('quarter') == 4, '10')
        )
        df = df.withColumn('period', to_date(concat_ws('-', col('year'), col('month'), lit('01')), 'yyyy-MM-dd'))

    elif freq == 'annual':
        df = df.withColumn('year_str', col('year_raw').cast('int').cast('string'))
        df = df.withColumn('period', to_date(concat_ws('-', col('year_str'), lit('01'), lit('01')), 'yyyy-MM-dd'))

    return (
        df.withColumn('frequency', lit(freq))
          .drop('year_raw', 'year', 'month', 'quarter', 'insert_timestamp', 'year_str')
    )

@task
def silver_to_gold():
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
    config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
    config = load_config(config_path)
    print(config)
    spark = create_spark_session('silver_to_gold', config)
    init_dbs(spark, config)

    project_root = config['paths']['project_root']
    raw_root = config['paths']['raw_folder']
    print(project_root)
    print(raw_root)

    dim_country = spark.read.parquet(config['gold_table_paths']['dim_country'])

    fact_crime_and_justice_unodc = spark.read.parquet(config['silver_table_paths']['fact_crime_and_justice_unodc'])
    unodc_crime_global = fact_crime_and_justice_unodc.alias('A').join(dim_country.alias('B'), 
                                                                    col('A.country_code') == col('B.country_code'))\
                                                                    .select(
                                                                        col('B.country_name').alias('country_name'),
                                                                        col('A.indicator').alias('indicator'),
                                                                        col('A.dimension').alias('dimension'),
                                                                        col('A.category').alias('category'),
                                                                        col('A.sex').alias('sex'),
                                                                        col('A.age').alias('age'),
                                                                        col('A.year').alias('year'),
                                                                        col('A.value').alias('value'),
                                                                        col('A.source').alias('source')
                                                                    )

    write_to_parquet(unodc_crime_global, mode='overwrite', table_path=config['gold_table_paths']['fact_unodc_crime_global'], sanitize=False)

    df_gem_annual = spark.read.parquet(config['silver_table_paths']['fact_gem_annual'])
    df_gem_monthly = spark.read.parquet(config['silver_table_paths']['fact_gem_monthly'])
    df_gem_quarterly = spark.read.parquet(config['silver_table_paths']['fact_gem_quarterly'])

    df_gem_annual = normalize_period(df_gem_annual, 'annual')
    df_gem_monthly = normalize_period(df_gem_monthly, 'monthly')
    df_gem_quarterly = normalize_period(df_gem_quarterly, 'quarterly')

    wb_gem_all_data = df_gem_annual.union(df_gem_monthly).union(df_gem_quarterly)
    write_to_parquet(wb_gem_all_data, mode='overwrite', table_path=config['gold_table_paths']['fact_wb_gem_all_data'], sanitize=False)

    df_non_gem = spark.read.parquet(config['silver_table_paths']['fact_wb_stats_non_gem'])
    dim_indicator = spark.read.parquet(config['silver_table_paths']['dim_indicator_wb'])

    df_non_gem = df_non_gem.drop('insert_timestamp')
    df_joined = df_non_gem.alias('gem').join(dim_country.alias('dc'), col('gem.country_code') == col('dc.country_code'))\
    .select(
        col('dc.country_name').alias('country_name'),
        col('gem.year').alias('year'),
        col('gem.indicator_code').alias('indicator_code'),
        col('gem.value').alias('value')
    )

    df_final = df_joined.alias('gem').join(dim_indicator.alias('ind'), col('gem.indicator_code') == col('ind.indicator_code'))\
    .select(
        col('gem.country_name').alias('country_name'),
        col('ind.indicator_name').alias('indicator_name'),
        col('gem.year').alias('year'),
        col('gem.value').alias('value')
    )

    write_to_parquet(df_final, mode='overwrite', table_path=config['gold_table_paths']['fact_wb_non_gem_all_data'], sanitize=False)

    df = spark.read.parquet(config['silver_table_paths']['fact_wb_edstats'])

    cols_to_keep = ['country_name','indicator_name', 'sex', 'urbanization', 'age', 'unit_type','unit_measure','year','value']

    df = df.select(*cols_to_keep)
    df = normalize_period(df, 'annual')
    df = df.drop('frequency')
    df = df.dropna()

    df = df.withColumn('sex', when(col('sex')=='_T', 'T').otherwise(col('sex')))\
    .withColumn('urbanization', when(col('urbanization')=='_T', 'T').otherwise(col('urbanization')))\
    .withColumn('age', when(col('age')=='_T', 'T').otherwise(col('age')))\
    .withColumn('value', when(col('value')=='NA', '-999.999').otherwise(col('value')))

    float_regex = r'^-?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$'

    df = df.filter(col('value').rlike(float_regex))

    df = df.withColumn("value", col("value").cast("double"))
    df = df.withColumn("value", when(col("value") == -999.999, None).otherwise(col("value")))

    write_to_parquet(df, mode='overwrite', table_path=config['gold_table_paths']['fact_edstats'], sanitize=False, partition_by='period')
    spark.stop()