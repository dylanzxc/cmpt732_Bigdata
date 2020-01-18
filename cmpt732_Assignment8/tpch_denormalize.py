from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch_denormalize') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def output_line(x):
    orderkey,totalprice, names=x
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey,totalprice, namestr)


def main(keyspace, output_keyspace):
    orders = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='orders', keyspace=keyspace).load()
    lineitem = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='lineitem', keyspace=keyspace).load()
    part = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='part', keyspace=keyspace).load()
    orderkey_partkey=orders.join(lineitem,(orders['orderkey']==lineitem['orderkey'])).select(orders['*'],lineitem['partkey'])
    join=orderkey_partkey.join(part,(orderkey_partkey['partkey']==part['partkey'])).select(orderkey_partkey['*'], part['name'])
    #condition=join.schema.names[:-1]
    condition= ['orderkey', 'clerk', 'comment', 'custkey', 'order_priority', 'orderdate', 'orderstatus', 'ship_priority', 'totalprice']
    result=join.groupby(condition).agg(functions.collect_set('name').alias('part_names')).orderBy('orderkey')
    result.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts',keyspace=output_keyspace).save()

if __name__ == '__main__':
    keyspace = sys.argv[1]
    output_keyspace = sys.argv[2]
    main(keyspace, output_keyspace)
