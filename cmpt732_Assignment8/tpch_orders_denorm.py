from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('load_logs_spark') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def output_line(x):
    orderkey,totalprice, part_names=x
    namestr = ', '.join(sorted(list(part_names)))
    return 'Order #%d $%.2f: %s' % (orderkey,totalprice, namestr)

def main(keyspace, outdir, orderkeys):
    result = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table='orders_parts', keyspace=keyspace).load()
    result=result[result.orderkey.isin(orderkeys)] 
    result=result.select(result['orderkey'],result['totalprice'],functions.sort_array('part_names').alias('part_names')).orderBy(result['orderkey'])
    result.rdd.map(output_line).saveAsTextFile(outdir)


if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace, outdir, orderkeys)

