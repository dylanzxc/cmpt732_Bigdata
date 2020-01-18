import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as f, types
spark = SparkSession.builder.appName('read_stream').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))
    values=values.withColumn('tmp', f.split(values['value'],  " "))
    xy=values.select(values['tmp'].getItem(0).alias('x'),values['tmp'].getItem(1).alias('y'))
    sums = xy.select(f.sum('x').alias('sum_x'), f.sum('y').alias('sum_y'), (f.sum(xy['x']*xy['y'])).alias('sum_xy'), \
        f.count('x').alias('n'), f.sum(f.pow(xy['x'],2)).alias('sum_x_square'))
    results = sums.withColumn('slope' ,((sums['sum_xy']-(1/sums['n'])*sums['sum_x']*sums['sum_y'])/(sums['sum_x_square']-(1/sums['n'])*(f.pow(sums['sum_x'],2)))))
    results = results.withColumn('intercept', (results['sum_y']/results['n'])-results['slope']*(results['sum_x']/results['n']))
    final = results.drop('sum_x','sum_y','sum_xy','n','sum_x_square')
    stream = final.writeStream.format('console').option("truncate", "false").outputMode('complete').start()
    stream.awaitTermination(60)

if __name__ == "__main__":
    topic = sys.argv[1]
    main(topic)

    


