import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range_sql').getOrCreate()
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
    weather.createOrReplaceTempView("weather")
    weather_all=spark.sql("SELECT * FROM weather WHERE IsNull(qflag)").cache()
    weather_all.createOrReplaceTempView("weather_all")
    weather_max=spark.sql("SELECT date,station,value AS max FROM weather_all WHERE observation='TMAX'")
    weather_max.createOrReplaceTempView("weather_max")
    weather_min=spark.sql("SELECT date,station,value AS min FROM weather_all WHERE observation='TMIN'")
    weather_min.createOrReplaceTempView("weather_min")
    temp_join=spark.sql("SELECT weather_min.date,weather_min.station,weather_min.min,weather_max.max  FROM weather_min JOIN weather_max ON weather_max.date=weather_min.date AND weather_max.station=weather_min.station")
    temp_join.createOrReplaceTempView("temp_join")
    range_join=spark.sql("SELECT date,station,(max-min)/10 AS range FROM temp_join").cache()
    range_join.createOrReplaceTempView("range_join")
    max_range=spark.sql("SELECT date,max(range) as max_r FROM range_join GROUP BY date")
    max_range.createOrReplaceTempView("max_range")
    result=spark.sql("SELECT range_join.date,range_join.station,range_join.range FROM range_join JOIN max_range ON range_join.date=max_range.date AND range_join.range=max_range.max_r")
    result.createOrReplaceTempView("result")
    sorted_result=spark.sql("SELECT * FROM result ORDER BY date, station")
    sorted_result.write.csv(output,mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
