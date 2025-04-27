import duckdb
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_DIR = os.path.join(BASE_DIR, 'incremental_data_process')


def get_timestamp():
    text_file = open(os.path.join(FILE_DIR, 'time_stamp.txt'), 'r+')
    time_stamp = text_file.read()
    print('time_stamp: ', time_stamp)
    if not time_stamp:
        time_stamp = '1970-01-01 12:00:00'
    text_file.close()
    return time_stamp


def write_to_parquet(table):
    conn = duckdb.connect(os.path.join(FILE_DIR, 'test.db'))
    conn.sql('select * from employees;').to_parquet(os.path.join(FILE_DIR, 'writer.parquet'))


spark = SparkSession.builder.appName('incrementalApp').master('local[*]').getOrCreate()

spark.stop()

# Create a function that fetches timestamp from a file
# create a table using duck db and insert fake data in regular basis
# Create a class that fetches incremental data using its timestamp from the table of the duck db and fetch and process
# using spark and then put it in the datalake.
