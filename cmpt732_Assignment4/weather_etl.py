import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather_etl').getOrCreate()
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
    # only keep the elements that its qflag is null
    weather = weather.where(weather['qflag'].isNull())
    # keep the canadian data only
    weather = weather.where(weather['station'].startswith('CA'))
    # keep the maximum temperature observations only
    weather = weather.where(weather['observation'] == 'TMAX')
    # divide the temperature by 10 and rename the resulting column
    weather = weather.select('*', (weather['value'] / 10).alias('tmax'))
    # keep only the columns station, date, tmax
    weather = weather.select(weather['station'], weather['date'], weather['tmax'])
    weather.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
