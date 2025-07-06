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

@task
def silver_to_gold_dim_country():
       os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
       config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
       config = load_config(config_path)
       print(config)
       spark = create_spark_session('silver_to_gold_dim_country', config)
       init_dbs(spark, config)

       project_root = config['paths']['project_root']
       raw_root = config['paths']['raw_folder']
       print(project_root)
       print(raw_root)

       dim_country_unodc = spark.read.parquet(config['silver_table_paths']['dim_country_unodc'])
       dim_country_wb = spark.read.parquet(config['silver_table_paths']['dim_country_wb'])

       dim_country_wb = dim_country_wb.dropna()

       dim_country_wb = dim_country_wb.select(col('country_code'), col('country_name'))
       dim_country_wb = dim_country_wb.drop_duplicates()
       dim_country_wb.count()

       df_grouped = dim_country_wb.groupBy(col('country_code')).agg(count('*')).alias('count')
       df_grouped = df_grouped.filter(col('count(1)') > 1)
       duplicate_countries = [row['country_code'] for row in df_grouped.select('country_code').collect()]

       dim_country_wb_non_dup = dim_country_wb.filter(~col('country_code').isin(duplicate_countries))
       dim_country_wb_dup = dim_country_wb.filter(col('country_code').isin(duplicate_countries)).orderBy(col('country_code').desc())

       windowSpec = Window.partitionBy('country_code').orderBy((col('country_code').desc()))

       dim_country_wb_dup = dim_country_wb_dup.withColumn("rn", row_number().over(windowSpec))
       dim_country_wb_dup = dim_country_wb_dup.filter(col('rn') == 1)

       dim_country_wb_final = dim_country_wb_non_dup.union(dim_country_wb_dup.select(col('country_code'), col('country_name')))

       wb_countries = [row['country_code'] for row in dim_country_wb_final.select(col('country_code')).collect()]
       unodc_countries = [row['country_code'] for row in dim_country_unodc.select(col('country_code')).collect()]

       countries_union = set(wb_countries).union(set(unodc_countries))
       countries_intersection = set(wb_countries).intersection(set(unodc_countries))

       len(set(wb_countries)-countries_intersection)

       extra_countries_unodc = list(set(unodc_countries)-countries_intersection)
       dim_country_unodc_filtered = dim_country_unodc.filter(col('country_code').isin(extra_countries_unodc))
       dim_country_unodc_filtered.show(truncate=False)

       dim_country_unodc_filtered = dim_country_unodc_filtered.select(col('country_code'), col('country')).withColumnRenamed('country', 'country_name')

       dim_country_full = dim_country_wb_final.union(dim_country_unodc_filtered)
       dim_country_full.count()

       dim_country_final = dim_country_full.alias('wb').join(dim_country_unodc.alias('unodc'), col('wb.country_code') == col('unodc.country_code'), how='left')\
              .select(
                     col('wb.country_code').alias('country_code'),
                     col('wb.country_name').alias('country_name'),
                     col('unodc.region').alias('region'),
                     col('unodc.subregion').alias('subregion')
              )
       dim_country_final.show(truncate=False)

       write_to_parquet(dim_country_final, mode='overwrite', table_path=config['gold_table_paths']['dim_country'], sanitize=False)
       spark.stop()