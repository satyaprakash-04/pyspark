import duckdb
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_DIR = os.path.join(BASE_DIR, 'incremental_data_process')
timestamp_format = '%Y-%m-%d %H:%M:%S.%f'


def get_timestamp():
    text_file = open(os.path.join(FILE_DIR, 'time_stamp.txt'), 'r+')
    time_stamp = text_file.read()
    print('time_stamp: ', time_stamp)
    if not time_stamp:
        time_stamp = '1970-01-01 12:00:00.000000'
    text_file.close()
    return time_stamp


def write_to_parquet(table):
    conn = duckdb.connect(os.path.join(FILE_DIR, 'test.db'))
    conn.sql('select * from {};'.format(table)).to_parquet(os.path.join(FILE_DIR, 'writer.parquet'))
    conn.close()


def write_ts_to_file(time_stamp):
    text_file = open(os.path.join(FILE_DIR, 'time_stamp.txt'), 'w')
    text_file.write(time_stamp)
    text_file.close()


if __name__ == '__main__':
    current_timestamp = datetime.now().strftime(timestamp_format)
    ts = get_timestamp()
    timestamp_obj = datetime.strptime(ts, timestamp_format)
    write_to_parquet(table='employees')
    spark = SparkSession.builder.appName('incrementalApp').getOrCreate()
    df = spark.read.parquet(os.path.join(FILE_DIR, 'writer.parquet'))
    df = df.filter(df.last_login >= timestamp_obj)
    df.printSchema()
    df.show()
    write_ts_to_file(time_stamp=current_timestamp)
    spark.stop()

# Create a function that fetches timestamp from a file
# create a table using duck db and insert fake data in regular basis
# Create a class that fetches incremental data using its timestamp from the table of the duck db and fetch and process
# using spark and then put it in the datalake.
