import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'part.tbl'
parquet_file = '../parquet/sf1/part.parquet'

directory = os.path.dirname(parquet_file)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'p_partkey': 'int64',
    'p_name': 'str',
    'p_mfgr': 'str',
    'p_brand': 'str',
    'p_type': 'str',
    'p_size': 'int64',
    'p_container': 'str',
    'p_retailprice': 'float64',
    'p_comment': 'str'
}

names = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']

df = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)))
print(df.head())
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file, row_group_size=1024*1024)