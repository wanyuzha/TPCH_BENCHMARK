import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'region.tbl'
parquet_dir = '../parquet/sf100/region/'

directory = os.path.dirname(parquet_dir)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'r_regionkey': 'int64',
    'r_name': 'str',
    'r_comment': 'str'
}

names = ['r_regionkey', 'r_name', 'r_comment']

chunk_size = 10000000 
chunks = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)), chunksize=chunk_size)

for i, chunk in enumerate(chunks):
    table = pa.Table.from_pandas(chunk)
    pq.write_to_dataset(table, root_path=parquet_dir, partition_cols=None, row_group_size=1024*1024)