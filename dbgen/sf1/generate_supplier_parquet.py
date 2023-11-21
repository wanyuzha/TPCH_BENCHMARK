import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'supplier.tbl'
parquet_file = '../parquet/sf1/supplier.parquet'

directory = os.path.dirname(parquet_file)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    's_suppkey': 'int64',
    's_name': 'str',
    's_address': 'str',
    's_nationkey': 'int64',
    's_phone': 'str',
    's_acctbal': 'float64',
    's_comment': 'str'
}

names = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']

df = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)))
print(df.head())
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file, row_group_size=1024*1024)