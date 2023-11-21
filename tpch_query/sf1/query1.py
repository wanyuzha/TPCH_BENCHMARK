from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName("SF1").getOrCreate()

    df = spark.read.parquet("hdfs://rcnfs:8020/tpch/sf1/lineitem.parquet")

    #df.printSchema()

    #df.show()

    df.createOrReplaceTempView("lineitem")

    result = spark.sql("""select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
    from
    lineitem
    where
    l_shipdate <= '1998-11-28'
    group by
    l_returnflag,
    l_linestatus
    order by
    l_returnflag,
    l_linestatus;""")

    result.show()
