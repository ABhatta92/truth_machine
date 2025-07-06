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

@task
def raw_to_bronze_unodc():
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"
    config_path = r'D:\python_projects\world_info\config.yaml'.replace("\\", "/")
    config = load_config(config_path)
    print(config)
    spark = create_spark_session('raw_to_bronze_unodc', config)
    init_dbs(spark, config)

    project_root = config['paths']['project_root']
    raw_root = config['paths']['raw_folder']

    df_func_justice = pd.read_excel(config['raw_paths']['unodc_func_justice'], engine='openpyxl', header=2)
    df_econ_crime = pd.read_excel(config['raw_paths']['unodc_econ_crime'], engine='openpyxl', header=2)
    df_violent_sexual = pd.read_excel(config['raw_paths']['unodc_sexual_violent'], engine='openpyxl', header=2)

    df_func_justice = spark.createDataFrame(df_func_justice)
    df_econ_crime = spark.createDataFrame(df_econ_crime)
    df_violent_sexual = spark.createDataFrame(df_violent_sexual)

    write_to_parquet(df_func_justice, mode='overwrite', table_path=config['bronze_table_paths']['unodc_func_justice'], sanitize=True)
    write_to_parquet(df_econ_crime, mode='overwrite', table_path=config['bronze_table_paths']['unodc_econ_crime'], sanitize=True)
    write_to_parquet(df_violent_sexual, mode='overwrite', table_path= config['bronze_table_paths']['unodc_sexual_violent'], sanitize=True)

    print(20*'-',f'Ending raw_to_bronze_unodc', 20*'-')

    spark.stop()