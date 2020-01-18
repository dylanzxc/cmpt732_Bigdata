from pyspark import SparkConf, SparkContext
import sys
import re, string 
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('correlate_logs').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def disassemble(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    return line_re.split(line)

def main(inputs):
    server_logs=sc.textFile(inputs)
    server_logs_dis=server_logs.map(disassemble)
    server_logs_dis=server_logs_dis.filter(lambda x: len(x)==6)
    log_schema = types.StructType([
    types.StructField('empty_1', types.StringType()),    
    types.StructField('host_name', types.StringType()),
    types.StructField('datetime', types.StringType()),
    types.StructField('requested_path', types.StringType()),
    types.StructField('bytes', types.StringType()),
    types.StructField('empty_2', types.StringType()),
    ])
    df=spark.createDataFrame(server_logs_dis,schema=log_schema)
    logs_df=df.select(df['host_name'],df['bytes'].cast(types.IntegerType())).withColumn('count', functions.lit(1))
    logs_df_sum=logs_df.groupBy('host_name').sum().withColumnRenamed('sum(bytes)','y').withColumnRenamed('sum(count)','x')
    six_values=logs_df_sum.withColumn('x_square',functions.pow(logs_df_sum['x'],2)).withColumn('y_square',functions.pow(logs_df_sum['y'],2)).withColumn('xy',logs_df_sum['x']*logs_df_sum['y']).withColumn('1', functions.lit(1))
    six_sums=six_values.groupBy().sum().cache()
    numerator=six_sums.select((six_sums['sum(xy)']*six_sums['sum(1)']-six_sums['sum(x)']*six_sums['sum(y)'])).collect()
    denominator_1=six_sums.select(functions.sqrt(six_sums['sum(1)']*six_sums['sum(x_square)']-functions.pow(six_sums['sum(x)'],2))).collect()
    denominator_2=six_sums.select(functions.sqrt(six_sums['sum(1)']*six_sums['sum(y_square)']-functions.pow(six_sums['sum(y)'],2))).collect()
    result=numerator[0][0]/(denominator_1[0][0]*denominator_2[0][0])
    # result=logs_df_sum.corr('x','y')
    result_sqr=result**2
    print('r=',result)
    print('r^2=',result_sqr)

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
