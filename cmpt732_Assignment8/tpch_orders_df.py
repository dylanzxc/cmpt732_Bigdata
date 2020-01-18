from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch_orders_df') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def output_line(x):
    orderkey,totalprice, names=x
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey,totalprice, namestr)


def main(keyspace, outdir, orderkeys):
    orders = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='orders', keyspace=keyspace).load()
    lineitem = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='lineitem', keyspace=keyspace).load()
    part = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='part', keyspace=keyspace).load()
    orders.createOrReplaceTempView("orders")
    lineitem.createOrReplaceTempView("lineitem")
    part.createOrReplaceTempView("part")
    join=spark.sql("SELECT o.orderkey, o.totalprice, p.name FROM\
    orders o\
    JOIN lineitem l ON l.orderkey=o.orderkey\
    JOIN part p ON p.partkey=l.partkey\
    WHERE o.orderkey IN ({})".format(",".join([str(i) for i in orderkeys])))

    result=join.groupby(join.orderkey,join.totalprice).agg(functions.collect_set('name').alias('names')).orderBy('orderkey')
   # we don't need to use sort_array to join here because we have sort the names in output_line method
   # result=result.select(result['orderkey'],result['totalprice'],functions.sort_array('names').alias('names'))
    result.explain()
    result.rdd.map(output_line).saveAsTextFile(outdir)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)
