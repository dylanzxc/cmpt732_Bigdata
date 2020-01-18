import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])
    
    weather = spark.read.csv(inputs, schema=observation_schema)

    weather_all=weather.where(weather['qflag'].isNull()).cache()
    weather_max=weather_all.where(weather_all['observation']=='TMAX').withColumnRenamed('value','max')
    weather_min=weather_all.where(weather_all['observation']=='TMIN').withColumnRenamed('value','min')
    cond=[weather_max['station']==weather_min['station'], weather_max['date']==weather_min['date']]
    temp_join=weather_max.join(weather_min,cond).select(weather_min['date'],weather_min['min'],weather_max['max'],weather_min['station'])
    range_join=temp_join.withColumn('range',(temp_join['max']-temp_join['min'])/10).select('date','range','station').cache()
    max_range=range_join.groupBy('date').max()
    cond=[max_range['date']==range_join['date'], max_range['max(range)']==range_join['range']]
    result=max_range.join(range_join,cond).select(max_range['date'],'station','range')  
    sorted_result=result.orderBy('date','station')
    sorted_result.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
