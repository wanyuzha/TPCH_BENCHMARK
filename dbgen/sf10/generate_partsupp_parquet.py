import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'partsupp.tbl'
parquet_dir = '../parquet/sf10/partsupp/'

directory = os.path.dirname(parquet_dir)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'ps_partkey': 'int64',
    'ps_suppkey': 'int64',
    'ps_availqty': 'int64',
    'ps_supplycost': 'float64',
    'ps_comment': 'str'
}

names = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']

chunk_size = 10000000 
chunks = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)), chunksize=chunk_size)

for i, chunk in enumerate(chunks):
    table = pa.Table.from_pandas(chunk)
    pq.write_to_dataset(table, root_path=parquet_dir, partition_cols=None, row_group_size=1024*1024)