from pyspark import SparkConf, SparkContext
import sys,uuid
import re, string 
from datetime import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('load_logs_spark') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def disassemble(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    return line_re.split(line)

def change_timedate(line):
    line[2]=datetime.strptime(line[2],'%d/%b/%Y:%H:%M:%S')
    return line

def main(input_dir, keyspace, table):
    server_logs=sc.textFile(input_dir).repartition(80)
    server_logs_dis=server_logs.map(disassemble)
    server_logs_dis=server_logs_dis.filter(lambda x: len(x)==6)
    server_logs_dis=server_logs_dis.map(change_timedate)
    log_schema = types.StructType([
    types.StructField('empty_1', types.StringType()),    
    types.StructField('host', types.StringType()),
    types.StructField('datetime', types.TimestampType()),
    types.StructField('path', types.StringType()),
    types.StructField('bytes', types.StringType()),
    types.StructField('empty_2', types.StringType()),
    ])
    df=spark.createDataFrame(server_logs_dis,schema=log_schema)
    uuidUdf=functions.udf(lambda : str(uuid.uuid4()),types.StringType())
    logs_df=df.select(df['host'],df['bytes'].cast(types.IntegerType()),df['path'],df['datetime']).withColumn("id",uuidUdf())
    logs_df.write.format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace).save()
    
if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(input_dir, keyspace, table)
