import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'orders.tbl'
parquet_file = '../parquet/sf1/orders.parquet'

directory = os.path.dirname(parquet_file)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'o_orderkey': 'int64',
    'o_custkey': 'int64',
    'o_orderstatus': 'str',
    'o_totalprice': 'float64',
    'o_orderdate': 'str',  
    'o_orderpriority': 'str',
    'o_clerk': 'str',
    'o_shippriority': 'int64',
    'o_comment': 'str'
}


names = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']

df = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)))
print(df.head())
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file, row_group_size=1024*1024)