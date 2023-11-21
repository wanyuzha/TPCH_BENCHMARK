import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'lineitem.tbl'
parquet_dir = '../parquet/sf10/lineitem/'

directory = os.path.dirname(parquet_dir)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'l_orderkey': 'int64',
    'l_partkey': 'int64',
    'l_suppkey': 'int64',
    'l_linenumber': 'int64',
    'l_quantity': 'float64',
    'l_extendedprice': 'float64',
    'l_discount': 'float64',
    'l_tax': 'float64',
    'l_returnflag': 'str',
    'l_linestatus': 'str',
    'l_shipdate': 'str',
    'l_commitdate': 'str',
    'l_receiptdate': 'str',
    'l_shipinstruct': 'str',
    'l_shipmode': 'str',
    'l_comment': 'str'
}

names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

chunk_size = 10000000 
chunks = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)), chunksize=chunk_size)

for i, chunk in enumerate(chunks):
    table = pa.Table.from_pandas(chunk)
    pq.write_to_dataset(table, root_path=parquet_dir, partition_cols=None, row_group_size=1024*1024)