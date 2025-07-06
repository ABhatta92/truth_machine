import os
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\python_projects\world_info\venv\Scripts\python.exe"

config_path = 'D:\\python_projects\\world_info\\config.yaml'

def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


config = load_config(config_path)
project_root = os.path.dirname(os.getcwd())
data_root = os.path.join(project_root, 'data')
raw_folder = os.path.join(data_root, 'raw')
bronze = os.path.normpath(os.path.join(data_root, 'bronze')).replace("\\", "\\\\")
silver = os.path.normpath(os.path.join(data_root, 'silver')).replace("\\", "\\\\")
gold = os.path.normpath(os.path.join(data_root, 'gold')).replace("\\", "\\\\")

def create_spark_session(app_name, config):
    print(20*'-', f'Starting create_spark_session {app_name}', 20*'-')
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", config['paths']['data_root'])
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    print(20*'-', 'Ending create_spark_session', 20*'-')
    return spark


def sanitize_columns(df):
    print(20*'-',f'Starting sanitize_columns', 20*'-')
    new_cols = [col.lower().replace(' ', '_').replace(',', '').replace(';', '').replace('(', '').replace(')', '').replace('\n', '').replace('\t', '').replace('=', '') for col in df.columns]
    print(20*'-',f'Ending sanitize_columns', 20*'-')
    return df.toDF(*new_cols)


def init_dbs(spark, config):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{config['database_paths']['bronze']}'")
    print('Created bronze')
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{config['database_paths']['silver']}'")
    print('Created silver')


def write_to_parquet(df, mode, table_path, partition_by=None, sanitize=True):
    print(20*'-',f'Starting {mode}_to_parquet {table_path}', 20*'-')
    if sanitize:
        df = sanitize_columns(df)
    df = df.withColumn('insert_timestamp', lit(current_timestamp()))
    print(20*'-',f'Added insert_timestamp', 20*'-')
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(table_path)
    print(20*'-',f'Ending {mode}_to_parquet {table_path}', 20*'-')


def read_and_pre_clean_wb_gem_excel(table_path, sheet_name, drop_first = 1, rename_year = 1):
    df = pd.read_excel(table_path, sheet_name=sheet_name, engine='openpyxl', header=0)
    if drop_first == 1:
        df.drop(index=0, axis=0, inplace=True)
        print('dropped first row')
    else:
        print('drop_first == false')
    if rename_year == 1:
        df.rename(columns={"Unnamed: 0": "Year"}, inplace=True)
        print('renamed Unnamed: 0 to Year')
    else:
        print('rename_year == false')
    return df


# def write_to_parquet(df, table_path):
#     print(20*'-', f'Starting write_to_parquet {table_path}', 20*'-')
#     df = sanitize_columns(df)
#     df = df.withColumn('insert_timestamp', lit(current_timestamp()))
#     print(20*'-', f'Added insert_timestamp', 20*'-')
#     df.write \
#         .mode('overwrite') \
#         .parquet(table_path)
#     print(20*'-', f'Ending write_to_parquet {table_path}', 20*'-')


# def write_to_parquet_without_sanitize(df, table_path):
#     print(20*'-',f'Starting write_to_delta {table_path}', 20*'-')
#     df = df.withColumn('insert_timestamp', lit(current_timestamp()))
#     print(20*'-',f'Added insert_timestamp', 20*'-')
#     df.write \
#         .mode('overwrite') \
#         .parquet(table_path)
#     print(20*'-',f'Ending write_to_delta {table_path}', 20*'-')


# def init_bronze_tables(spark, config):
#     print(20*'-',f'Starting init_bronze_tables()',20*'-')
#     counter = 0
#     for x in config['bronze_tables'].keys():
#         spark.sql(f"""
#                     CREATE TABLE IF NOT EXISTS {config['bronze_tables'][x]}
#                     using delta
#                     location '{config['bronze_table_paths'][x]}'
#                 """)
#         counter+=1
#     print(f'Initialized {counter} of {len(config['bronze_tables'].keys())} tables')
#     print(20*'-',f'Ending init_bronze_tables()',20*'-')


# def append_to_parquet(df, table_path):
#     print(20*'-',f'Starting append_to_parquet {table_path}', 20*'-')
#     df = sanitize_columns(df)
#     df = df.withColumn('insert_timestamp', lit(current_timestamp()))
#     print(20*'-',f'Added insert_timestamp', 20*'-')
#     df.write \
#         .mode('append') \
#         .parquet(table_path)
#     print(20*'-',f'Ending append_to_parquet {table_path}', 20*'-')


# def write_to_parquet_with_partition(df, layer, table_path, partition_by=None):
#     print(20*'-', f'Starting write_to_parquet {table_path}', 20*'-')
#     df = sanitize_columns(df)
#     df = df.withColumn('insert_timestamp', lit(current_timestamp()))
#     print(20*'-', f'Added insert_timestamp', 20*'-')
#     if layer == 'bronze':
#         df.write.mode('overwrite').parquet(table_path)
#     elif partition_by == None:
#         df.write.mode('overwrite').parquet(table_path)
#     else:
#         df.write.mode('overwrite').parquet(table_path).partitionBy(partition_by)
#     print(20*'-', f'Ending write_to_parquet {table_path}', 20*'-')


# def append_to_parquet_with_partition(df, layer, table_path, partition_by=None):
#     print(20*'-',f'Starting append_to_parquet {table_path}', 20*'-')
#     df = sanitize_columns(df)
#     df = df.withColumn('insert_timestamp', lit(current_timestamp()))
#     print(20*'-',f'Added insert_timestamp', 20*'-')
#     if layer == 'bronze':
#         df.write.mode('append').parquet(table_path)
#     elif partition_by == None:
#         df.write.mode('append').parquet(table_path)
#     else:
#         df.write.mode('append').parquet(table_path).partitionBy(partition_by)
#     print(20*'-',f'Ending append_to_parquet {table_path}', 20*'-')


