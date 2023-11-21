import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'nation.tbl'
parquet_file = '../parquet/sf1/nation.parquet'

directory = os.path.dirname(parquet_file)
if not os.path.exists(directory):
    os.makedirs(directory)

schema = {
    'n_nationkey': 'int64',
    'n_name': 'str',
    'n_regionkey': 'int64',
    'n_comment': 'str'
}


names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']

df = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=schema, usecols=range(len(names)))
print(df.head())
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file, row_group_size=1024*1024)