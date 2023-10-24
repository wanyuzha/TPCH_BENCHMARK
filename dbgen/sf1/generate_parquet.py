import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, DateType, CharType

tbl_file = 'lineitem.tbl'
parquet_file = '../parquet/sf1/lineitem.parquet'


pa_schema = StructType([
    StructField("l_orderkey", LongType(), False),
    StructField("l_partkey", LongType(), False),
    StructField("l_suppkey", LongType(), False),
    StructField("l_linenumber", LongType(), False),
    StructField("l_quantity", DoubleType(), False),
    StructField("l_extendedprice", DoubleType(), False),
    StructField("l_discount", DoubleType(), False),
    StructField("l_tax", DoubleType(), False),
    StructField("l_returnflag", StringType(), False),
    StructField("l_linestatus", StringType(), False),
    StructField("l_shipdate", DateType(), False),
    StructField("l_commitdate", DateType(), False),
    StructField("l_receiptdate", DateType(), False),
    StructField("l_shipinstruct", StringType(), False),  # Assuming CHAR(25) maps to StringType in PySpark
    StructField("l_shipmode", StringType(), False),      # Assuming CHAR(10) maps to StringType in PySpark
    StructField("l_comment", StringType(), False)
])


lineitem_schema = {
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
    # pandas没有专门的日期数据类型，通常使用str读取然后后续转换
    'l_shipdate': 'str',
    'l_commitdate': 'str',
    'l_receiptdate': 'str',
    'l_shipinstruct': 'str',
    'l_shipmode': 'str',
    'l_comment': 'str'
}

names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

df = pd.read_csv(tbl_file, sep='|', engine='python', header=None, names=names, dtype=lineitem_schema, usecols=range(len(names)))
print(df.head())
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file, row_group_size=1024*1024)